package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.Discovery
import com.rmjtromp.trawler.discovery.DiscoveryContext
import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.discovery.HarvestStrategy
import com.rmjtromp.trawler.event.CrawlerEvent
import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.event.Observer
import com.rmjtromp.trawler.event.ObserverResult
import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.extension.ReplayLifecycle
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.ExtractionContext
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.policy.BuiltInPolicies
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.ResourceHistory
import com.rmjtromp.trawler.policy.RequestIdentity
import com.rmjtromp.trawler.policy.WorkOrderingPolicy
import com.rmjtromp.trawler.response.CaptureResponsesWhenConfigured
import com.rmjtromp.trawler.response.NoResponseCachePolicy
import com.rmjtromp.trawler.response.ResponseCachePolicy
import com.rmjtromp.trawler.response.ResponseCapturePolicy
import com.rmjtromp.trawler.response.ResponseStore
import com.rmjtromp.trawler.state.AtomicEnqueueAndCheckpointStore
import com.rmjtromp.trawler.state.AtomicEnqueueCheckpointResult
import com.rmjtromp.trawler.state.AttemptOutcome
import com.rmjtromp.trawler.state.AttemptStartResult
import com.rmjtromp.trawler.state.AttemptStore
import com.rmjtromp.trawler.state.CheckpointQuarantineStore
import com.rmjtromp.trawler.state.CrawlStateCapability
import com.rmjtromp.trawler.state.CrawlStateStore
import com.rmjtromp.trawler.state.DiscoveryCheckpoint
import com.rmjtromp.trawler.state.DiscoveryCheckpointStore
import com.rmjtromp.trawler.state.DiscoveryWorkItem
import com.rmjtromp.trawler.state.DurableRunStatus
import com.rmjtromp.trawler.state.EnqueueResult
import com.rmjtromp.trawler.state.ExecutionAttempt
import com.rmjtromp.trawler.state.HarvestState
import com.rmjtromp.trawler.state.HarvestStateQuarantineStore
import com.rmjtromp.trawler.state.HarvestStateStore
import com.rmjtromp.trawler.state.InMemoryCrawlStateStore
import com.rmjtromp.trawler.state.LeaseMutationResult
import com.rmjtromp.trawler.state.LeaseStore
import com.rmjtromp.trawler.state.LeasedWork
import com.rmjtromp.trawler.state.PipelineEligibilityOverride
import com.rmjtromp.trawler.state.PipelineEligibilityOverrideStore
import com.rmjtromp.trawler.state.PipelineHealthSnapshot
import com.rmjtromp.trawler.state.PipelineHealthStore
import com.rmjtromp.trawler.state.PipelineScopedWorkQueueStore
import com.rmjtromp.trawler.state.ResourceHistoryStore
import com.rmjtromp.trawler.state.RunHistoryStore
import com.rmjtromp.trawler.state.WorkCancellationStore
import com.rmjtromp.trawler.state.WorkOrderingStore
import com.rmjtromp.trawler.state.WorkQueueMetrics
import com.rmjtromp.trawler.state.WorkQueueMetricsStore
import com.rmjtromp.trawler.state.WorkQueueStatus
import com.rmjtromp.trawler.state.WorkTransition
import com.rmjtromp.trawler.transport.Transport
import com.rmjtromp.trawler.transport.TransportRouter
import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/*
 * Shared handwritten fakes for the engine-runtime unit tests. This file is not
 * a test class (its name matches neither the Surefire nor the Failsafe
 * pattern); it only provides deterministic doubles that record calls.
 */

internal class SettableClock(var current: Instant = Instant.EPOCH) : Clock {
    override fun now(): Instant = current
}

internal class ScriptedTransport(
    override val id: String = "scripted-transport",
    private val handler: suspend (CrawlRequest) -> CrawlResponse = { okResponse(it) },
) : Transport {
    val calls: MutableList<CrawlRequest> = mutableListOf()

    override suspend fun execute(request: CrawlRequest): CrawlResponse {
        calls += request
        return handler(request)
    }
}

internal class SingleTransportRouter(
    private val transport: Transport,
    override val id: String = "single-router",
) : TransportRouter {
    override fun select(request: CrawlRequest, context: RequestContext): Transport = transport
}

internal class LambdaExtractor<T : Any>(
    override val id: String = "lambda-extractor",
    private val handler: suspend (CrawlResponse, ExtractionContext) -> ExtractionResult<T>,
) : Extractor<T> {
    val seen: MutableList<CrawlResponse> = mutableListOf()

    override suspend fun extract(response: CrawlResponse, context: ExtractionContext): ExtractionResult<T> {
        seen += response
        return handler(response, context)
    }
}

/** Extracts the UTF-8 body as one String value. */
internal fun bodyExtractor(id: String = "body-extractor"): LambdaExtractor<String> =
    LambdaExtractor(id) { response, _ -> ExtractionResult.Success(listOf(response.body.utf8())) }

internal open class CollectingOutput<T : Any>(
    override val id: String = "collecting-output",
    private val behavior: suspend (List<Produced<T>>) -> OutputResult = { OutputResult.Accepted(it.size) },
) : Output<T> {
    val batches: MutableList<List<Produced<T>>> = mutableListOf()

    override suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult {
        batches += items.toList()
        return behavior(items)
    }

    fun values(): List<T> = batches.flatten().map { it.value }
}

internal class EventLog(override val id: String = "event-log") : Observer {
    val events: MutableList<CrawlerEvent> = mutableListOf()

    override suspend fun onEvent(event: CrawlerEvent): ObserverResult {
        events += event
        return ObserverResult.Accepted
    }

    fun types(): List<CrawlerEventType> = events.map { it.type }

    fun details(type: CrawlerEventType): List<String?> = events.filter { it.type == type }.map { it.detail }
}

/** Lifecycle extension recording ordinary and replay start/stop calls into a shared log. */
internal open class LoggingExtension(
    override val id: String,
    val log: MutableList<String>,
    private val startFailure: Throwable? = null,
    private val stopFailure: Throwable? = null,
) : ExtensionLifecycle {
    override suspend fun start(context: ExtensionStartContext) {
        log += "start:$id"
        startFailure?.let { throw it }
    }

    override suspend fun stop(context: ExtensionStopContext) {
        log += "stop:$id:${context.reason}"
        stopFailure?.let { throw it }
    }
}

internal class LoggingReplayExtension(
    id: String,
    log: MutableList<String>,
    private val replayStartFailure: Throwable? = null,
    private val replayStopFailure: Throwable? = null,
) : LoggingExtension(id, log), ReplayLifecycle {
    override suspend fun startForReplay(context: ExtensionStartContext) {
        log += "replay-start:$id"
        replayStartFailure?.let { throw it }
    }

    override suspend fun stopForReplay(context: ExtensionStopContext) {
        log += "replay-stop:$id:${context.reason}"
        replayStopFailure?.let { throw it }
    }
}

internal class ListDiscovery(
    override val id: String = "list-discovery",
    private val emissions: suspend (DiscoveryContext) -> List<DiscoveryEmission>,
) : Discovery {
    val contexts: MutableList<DiscoveryContext> = mutableListOf()

    override fun discover(context: DiscoveryContext): Flow<DiscoveryEmission> = flow {
        contexts += context
        emissions(context).forEach { emit(it) }
    }
}

internal fun seeds(vararg targets: String): Discovery =
    com.rmjtromp.trawler.discovery.SeedDiscovery(requests = targets.map { CrawlRequest(it) })

internal fun okResponse(
    request: CrawlRequest,
    body: String = request.target,
    headers: Headers = Headers.empty(),
    attributes: ContextAttributes = ContextAttributes.empty(),
    finalTarget: String? = null,
): CrawlResponse = CrawlResponse(
    request = request,
    statusCode = 200,
    headers = headers,
    body = ResponseBody.from(body.toByteArray(Charsets.UTF_8)),
    receivedAt = Instant.EPOCH,
    finalTarget = finalTarget,
    attributes = attributes,
)

internal fun ResponseBody.utf8(): String = String(toByteArray(), Charsets.UTF_8)

internal fun <T : Any> testPipeline(
    id: String,
    extractor: Extractor<T>,
    clock: Clock,
    transport: Transport = ScriptedTransport(),
    policies: PipelinePolicies = BuiltInPolicies.ordinary(clock = clock),
    discoveries: List<Discovery> = emptyList(),
    transforms: List<Transform<T>> = emptyList(),
    validators: List<Validator<T>> = emptyList(),
    outputs: List<Pair<Output<T>, OutputRequirement>> = emptyList(),
    extensions: List<ExtensionLifecycle> = emptyList(),
    harvest: HarvestStrategy? = null,
): Pipeline<T> = PipelineBuilder<T>(id).apply {
    transport(SingleTransportRouter(transport, "$id-router"))
    extract(extractor)
    this.policies(policies)
    discoveries.forEach(::discover)
    transforms.forEach(::transform)
    validators.forEach(::validator)
    outputs.forEach { (output, requirement) -> output(output, requirement) }
    extensions.forEach(::extension)
    harvest?.let(::harvest)
}.build()

internal fun testEngine(
    pipelines: List<Pipeline<*>>,
    clock: Clock,
    state: CrawlStateStore? = null,
    responseStore: ResponseStore? = null,
    capturePolicy: ResponseCapturePolicy = CaptureResponsesWhenConfigured,
    cachePolicy: ResponseCachePolicy = NoResponseCachePolicy,
    observers: List<Observer> = emptyList(),
    extensions: List<ExtensionLifecycle> = emptyList(),
    options: EngineRuntimeOptions = EngineRuntimeOptions(),
): TrawlerEngine = TrawlerEngine(
    EngineConfiguration(
        pipelines = pipelines,
        stateStore = state ?: InMemoryCrawlStateStore(clock = clock),
        responseStore = responseStore,
        responseCapturePolicy = capturePolicy,
        responseCachePolicy = cachePolicy,
        observers = observers,
        extensions = extensions,
        clock = clock,
        runtimeOptions = options,
    ),
)

internal val baseRuntimeCapabilities: Set<CrawlStateCapability> = setOf(
    CrawlStateCapability.WORK_QUEUE,
    CrawlStateCapability.LEASE_FENCING,
    CrawlStateCapability.RESOURCE_HISTORY,
    CrawlStateCapability.ATTEMPT_HISTORY,
    CrawlStateCapability.PIPELINE_SCOPED_WORK,
    CrawlStateCapability.CANCELLATION,
    CrawlStateCapability.WORK_ORDERING,
    CrawlStateCapability.WORK_STATUS,
)

/**
 * Delegating state store that wraps the in-memory adapter and exposes
 * per-operation hooks so tests can inject stale, rejected, or failing state
 * results. It implements checkpoints, harvest state, health, and overrides,
 * but intentionally not atomic admission, quarantine, metrics, or run history;
 * subclasses add those capabilities.
 */
internal open class HookedStateStore(
    clock: Clock,
    override val id: String = "hooked-state",
    extraCapabilities: Set<CrawlStateCapability> = emptySet(),
    val delegate: InMemoryCrawlStateStore = InMemoryCrawlStateStore(clock = clock),
) : CrawlStateStore,
    PipelineScopedWorkQueueStore,
    LeaseStore,
    AttemptStore,
    ResourceHistoryStore,
    WorkCancellationStore,
    WorkOrderingStore,
    WorkQueueStatus,
    DiscoveryCheckpointStore,
    HarvestStateStore,
    PipelineHealthStore,
    PipelineEligibilityOverrideStore {
    override val capabilities: Set<CrawlStateCapability> = baseRuntimeCapabilities + setOf(
        CrawlStateCapability.DISCOVERY_CHECKPOINTS,
        CrawlStateCapability.HARVEST_STATE,
        CrawlStateCapability.PIPELINE_HEALTH,
        CrawlStateCapability.PIPELINE_ELIGIBILITY_OVERRIDES,
    ) + extraCapabilities

    var enqueueHook: (suspend (CrawlRequest, RequestIdentity, CrawlerContext) -> EnqueueResult?)? = null
    var claimHook: (suspend (String, Int, String, Instant) -> List<LeasedWork>?)? = null
    var renewHook: (suspend (LeasedWork, Instant) -> LeaseMutationResult?)? = null
    var checkCurrentHook: (suspend (LeasedWork) -> LeaseMutationResult?)? = null
    var transitionHook: (suspend (LeasedWork, WorkTransition) -> LeaseMutationResult?)? = null
    var beginAttemptHook: (suspend (LeasedWork, Instant) -> AttemptStartResult?)? = null
    var finishAttemptHook: (suspend (LeasedWork, ExecutionAttempt, AttemptOutcome) -> LeaseMutationResult?)? = null
    var checkpointCasHook: (suspend (String, String, Long?, DiscoveryCheckpoint) -> Boolean?)? = null
    var harvestCasHook: (suspend (String, String, Long?, HarvestState) -> Boolean?)? = null
    var healthCasHook: (suspend (String, Long?, PipelineHealthSnapshot) -> Boolean?)? = null
    var checkpointLoadHook: (suspend (String, String) -> DiscoveryCheckpoint?)? = null

    val transitions: MutableList<WorkTransition> = mutableListOf()
    val finishedAttempts: MutableList<AttemptOutcome> = mutableListOf()
    val checkpointWrites: MutableList<DiscoveryCheckpoint> = mutableListOf()
    val harvestWrites: MutableList<HarvestState> = mutableListOf()
    val healthWrites: MutableList<PipelineHealthSnapshot> = mutableListOf()
    var cancelQueuedCalls: Int = 0

    override suspend fun enqueue(request: CrawlRequest, identity: RequestIdentity, context: CrawlerContext): EnqueueResult =
        enqueueHook?.invoke(request, identity, context) ?: delegate.enqueue(request, identity, context)

    override suspend fun claim(limit: Int, ownerId: String, now: Instant): List<LeasedWork> =
        delegate.claim(limit, ownerId, now)

    override suspend fun claim(pipelineId: String, limit: Int, ownerId: String, now: Instant): List<LeasedWork> =
        claimHook?.invoke(pipelineId, limit, ownerId, now) ?: delegate.claim(pipelineId, limit, ownerId, now)

    override suspend fun checkCurrent(work: LeasedWork): LeaseMutationResult =
        checkCurrentHook?.invoke(work) ?: delegate.checkCurrent(work)

    override suspend fun renew(work: LeasedWork, expiresAt: Instant): LeaseMutationResult =
        renewHook?.invoke(work, expiresAt) ?: delegate.renew(work, expiresAt)

    override suspend fun transition(work: LeasedWork, transition: WorkTransition): LeaseMutationResult {
        transitions += transition
        return transitionHook?.invoke(work, transition) ?: delegate.transition(work, transition)
    }

    override suspend fun beginAttempt(work: LeasedWork, startedAt: Instant): AttemptStartResult =
        beginAttemptHook?.invoke(work, startedAt) ?: delegate.beginAttempt(work, startedAt)

    override suspend fun finishAttempt(
        work: LeasedWork,
        attempt: ExecutionAttempt,
        outcome: AttemptOutcome,
    ): LeaseMutationResult {
        finishedAttempts += outcome
        return finishAttemptHook?.invoke(work, attempt, outcome) ?: delegate.finishAttempt(work, attempt, outcome)
    }

    override suspend fun history(pipelineId: String, identity: RequestIdentity): ResourceHistory =
        delegate.history(pipelineId, identity)

    override suspend fun cancelQueued(pipelineId: String, transition: WorkTransition): Int {
        cancelQueuedCalls++
        return delegate.cancelQueued(pipelineId, transition)
    }

    override suspend fun setOrdering(pipelineId: String, policy: WorkOrderingPolicy) =
        delegate.setOrdering(pipelineId, policy)

    override suspend fun activeWorkCount(pipelineId: String): Int = delegate.activeWorkCount(pipelineId)

    override suspend fun load(pipelineId: String, discoveryId: String): DiscoveryCheckpoint? =
        checkpointLoadHook?.invoke(pipelineId, discoveryId) ?: delegate.load(pipelineId, discoveryId)

    override suspend fun compareAndSet(
        pipelineId: String,
        discoveryId: String,
        expectedRevision: Long?,
        next: DiscoveryCheckpoint,
    ): Boolean {
        checkpointWrites += next
        return checkpointCasHook?.invoke(pipelineId, discoveryId, expectedRevision, next)
            ?: delegate.compareAndSet(pipelineId, discoveryId, expectedRevision, next)
    }

    override suspend fun loadHarvestState(pipelineId: String, strategyId: String): HarvestState? =
        delegate.loadHarvestState(pipelineId, strategyId)

    override suspend fun compareAndSetHarvestState(
        pipelineId: String,
        strategyId: String,
        expectedRevision: Long?,
        next: HarvestState,
    ): Boolean {
        harvestWrites += next
        return harvestCasHook?.invoke(pipelineId, strategyId, expectedRevision, next)
            ?: delegate.compareAndSetHarvestState(pipelineId, strategyId, expectedRevision, next)
    }

    override suspend fun loadPipelineHealth(pipelineId: String): PipelineHealthSnapshot? =
        delegate.loadPipelineHealth(pipelineId)

    override suspend fun compareAndSetPipelineHealth(
        pipelineId: String,
        expectedRevision: Long?,
        next: PipelineHealthSnapshot,
    ): Boolean {
        healthWrites += next
        return healthCasHook?.invoke(pipelineId, expectedRevision, next)
            ?: delegate.compareAndSetPipelineHealth(pipelineId, expectedRevision, next)
    }

    override suspend fun loadEligibilityOverride(pipelineId: String): PipelineEligibilityOverride? =
        delegate.loadEligibilityOverride(pipelineId)

    override suspend fun setEligibilityOverride(pipelineId: String, override: PipelineEligibilityOverride?) =
        delegate.setEligibilityOverride(pipelineId, override)
}

/** Adds atomic admission, quarantine, and queue metrics on top of [HookedStateStore]. */
internal class AtomicHookedStateStore(
    clock: Clock,
    id: String = "atomic-hooked-state",
) : HookedStateStore(
    clock = clock,
    id = id,
    extraCapabilities = setOf(
        CrawlStateCapability.ATOMIC_ENQUEUE_AND_CHECKPOINT,
        CrawlStateCapability.CHECKPOINT_QUARANTINE,
        CrawlStateCapability.HARVEST_STATE_QUARANTINE,
        CrawlStateCapability.QUEUE_WATERMARKS,
    ),
),
    AtomicEnqueueAndCheckpointStore,
    CheckpointQuarantineStore,
    HarvestStateQuarantineStore,
    WorkQueueMetricsStore {
    var atomicHook: (suspend (DiscoveryCheckpoint, List<DiscoveryWorkItem>) -> AtomicEnqueueCheckpointResult?)? = null
    var metricsHook: (suspend (String) -> WorkQueueMetrics)? = null
    val quarantinedCheckpoints: MutableList<Pair<DiscoveryCheckpoint, String>> = mutableListOf()
    val quarantinedHarvestStates: MutableList<Pair<HarvestState, String>> = mutableListOf()

    override suspend fun enqueueAndCheckpoint(
        pipelineId: String,
        discoveryId: String,
        expectedRevision: Long?,
        next: DiscoveryCheckpoint,
        items: List<DiscoveryWorkItem>,
    ): AtomicEnqueueCheckpointResult {
        checkpointWrites += next
        return atomicHook?.invoke(next, items)
            ?: delegate.enqueueAndCheckpoint(pipelineId, discoveryId, expectedRevision, next, items)
    }

    override suspend fun quarantine(
        pipelineId: String,
        discoveryId: String,
        checkpoint: DiscoveryCheckpoint,
        reason: String,
    ) {
        quarantinedCheckpoints += checkpoint to reason
        delegate.quarantine(pipelineId, discoveryId, checkpoint, reason)
    }

    override suspend fun quarantineHarvestState(
        pipelineId: String,
        strategyId: String,
        state: HarvestState,
        reason: String,
    ) {
        quarantinedHarvestStates += state to reason
        delegate.quarantineHarvestState(pipelineId, strategyId, state, reason)
    }

    override suspend fun metrics(pipelineId: String): WorkQueueMetrics =
        metricsHook?.invoke(pipelineId) ?: delegate.metrics(pipelineId)
}

/** Recording run-history adapter; capabilities select RUN_HISTORY and/or RUN_RECOVERY. */
internal class RunHistoryStateStore(
    clock: Clock,
    historyCapabilities: Set<CrawlStateCapability>,
) : HookedStateStore(clock = clock, id = "run-history-state", extraCapabilities = historyCapabilities),
    RunHistoryStore {
    val calls: MutableList<String> = mutableListOf()
    var recoveredStaleAfter: Duration? = null
    var startPipelineFailure: (() -> Throwable)? = null
    var completePipelineFailure: (() -> Throwable)? = null
    var completeEngineFailure: (() -> Throwable)? = null

    override suspend fun recoverStaleRuns(now: Instant, staleAfter: Duration): Int {
        calls += "recover"
        recoveredStaleAfter = staleAfter
        return 0
    }

    override suspend fun startEngineRun(engineRunId: String, startedAt: Instant, reason: String?) {
        calls += "start-engine:$reason"
    }

    override suspend fun startPipelineRun(
        engineRunId: String,
        pipelineRunId: String,
        pipelineId: String,
        startedAt: Instant,
    ) {
        calls += "start-pipeline:$pipelineId"
        startPipelineFailure?.let { throw it() }
    }

    override suspend fun completePipelineRun(
        pipelineRunId: String,
        status: DurableRunStatus,
        completedAt: Instant,
        failureCount: Long,
    ) {
        calls += "complete-pipeline:$status:$failureCount"
        completePipelineFailure?.let { throw it() }
    }

    override suspend fun completeEngineRun(
        engineRunId: String,
        status: DurableRunStatus,
        completedAt: Instant,
        failureCount: Long,
    ) {
        calls += "complete-engine:$status:$failureCount"
        completeEngineFailure?.let { throw it() }
    }
}

/**
 * Suppressed failures attached to [this] or to the original exception when
 * coroutine stack-trace recovery rethrows a copy with the original as cause.
 */
internal fun Throwable.suppressedChain(): List<Throwable> =
    generateSequence(this) { it.cause }.take(4).flatMap { it.suppressed.asSequence() }.toList()
