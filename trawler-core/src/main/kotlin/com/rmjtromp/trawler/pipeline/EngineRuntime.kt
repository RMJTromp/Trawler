package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.DiscoveryContext
import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.event.CrawlerEvent
import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.event.EventContext
import com.rmjtromp.trawler.event.ObserverResult
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.extension.ExtensionStopReason
import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.ExtractionContext
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.model.ResponseContext
import com.rmjtromp.trawler.model.TransformContext
import com.rmjtromp.trawler.model.ValidationContext
import com.rmjtromp.trawler.output.OutputLifecycle
import com.rmjtromp.trawler.output.NoOutput
import com.rmjtromp.trawler.policy.AttemptSummary
import com.rmjtromp.trawler.policy.AuthorizationDecision
import com.rmjtromp.trawler.policy.BudgetSnapshot
import com.rmjtromp.trawler.policy.BudgetObservation
import com.rmjtromp.trawler.policy.DeduplicationDecision
import com.rmjtromp.trawler.policy.FailureContext
import com.rmjtromp.trawler.policy.RecrawlDecision
import com.rmjtromp.trawler.policy.RetryDecision
import com.rmjtromp.trawler.policy.ScopeDecision
import com.rmjtromp.trawler.policy.BudgetDecision
import com.rmjtromp.trawler.response.ResponseStore
import com.rmjtromp.trawler.schedule.ScheduleSubmission
import com.rmjtromp.trawler.schedule.Scheduler
import com.rmjtromp.trawler.schedule.SchedulerEvent
import com.rmjtromp.trawler.schedule.SchedulerHandle
import com.rmjtromp.trawler.state.AttemptStartResult
import com.rmjtromp.trawler.state.AttemptOutcome
import com.rmjtromp.trawler.state.AttemptStore
import com.rmjtromp.trawler.state.ExecutionAttempt
import com.rmjtromp.trawler.state.LeaseMutationResult
import com.rmjtromp.trawler.state.LeaseStore
import com.rmjtromp.trawler.state.PipelineScopedWorkQueueStore
import com.rmjtromp.trawler.state.ResourceHistoryStore
import com.rmjtromp.trawler.state.WorkCancellationStore
import com.rmjtromp.trawler.state.WorkItemId
import com.rmjtromp.trawler.state.WorkOrderingStore
import com.rmjtromp.trawler.state.WorkQueueStatus
import com.rmjtromp.trawler.state.WorkQueueStore
import com.rmjtromp.trawler.state.WorkTransition
import com.rmjtromp.trawler.state.LeasedWork
import com.rmjtromp.trawler.transport.RequestExecutor
import com.rmjtromp.trawler.transport.ResponseHandler
import com.rmjtromp.trawler.transport.Transport
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.take
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/** Structured handle for one selected scheduler daemon. */
public interface EngineHandle {
    /** Completes when the selected scheduler stops. */
    public val completion: Deferred<Unit>

    /** Submits a manual trigger through the selected scheduler. */
    public suspend fun trigger(): ScheduleSubmission

    /** Stops only this daemon scheduler and leaves the engine available. */
    public suspend fun cancel()

    /** Stops this daemon and then cooperatively shuts down the engine. */
    public suspend fun shutdown()
}

/**
 * Internal runtime owned by the non-generic [TrawlerEngine]. It stores only
 * type-preserving registration owners and never accepts a produced value at
 * the heterogeneous boundary.
 */
internal class EngineRuntime(
    private val configuration: EngineConfiguration,
) {
    private val lifecycleMutex = Mutex()
    private val runMutex = Mutex()
    private val rootJob = SupervisorJob()
    private val lifecycleScope = CoroutineScope(rootJob + Dispatchers.Default)
    private val activeRuns = linkedSetOf<Job>()
    private val daemonHandles = linkedSetOf<EngineDaemonHandle>()
    private val daemonReservations = mutableListOf<Set<String>?>()
    private val engineRunSequence = AtomicLong(0)
    private val observerFailureCount = AtomicLong(0)
    private val shutdownCompletion = CompletableDeferred<Unit>()
    private var lifecycleState: LifecycleState = LifecycleState.NEW
    private var startedExtensions: List<com.rmjtromp.trawler.extension.ExtensionLifecycle> = emptyList()

    suspend fun runOnce(request: RunRequest): RunResult = coroutineScope {
        val run = async(start = CoroutineStart.LAZY) {
            runSerialized(request)
        }
        lifecycleMutex.withLock {
            ensureAcceptingRuns()
            if (activeRuns.size >= configuration.runtimeOptions.maxPendingRuns) {
                throw IllegalStateException("Engine pending-run capacity is full")
            }
            activeRuns += run
        }
        run.start()
        try {
            run.await()
        } finally {
            lifecycleMutex.withLock { activeRuns.remove(run) }
        }
    }

    suspend fun awaitIdle(timeout: Duration): Boolean {
        require(!timeout.isNegative && !timeout.isZero) { "Idle timeout must be positive" }
        val timeoutMillis = timeout.toMillis().coerceAtLeast(1)
        val idle = withTimeoutOrNull<Boolean>(timeoutMillis) {
            while (true) {
                val runs = lifecycleMutex.withLock { activeRuns.filter(Job::isActive) }
                if (runs.isEmpty()) break
                runs.joinAll()
            }
            true
        } ?: false
        return idle
    }

    suspend fun startDaemon(config: DaemonConfig, scheduler: Scheduler): EngineHandle {
        val unknown = config.selectedPipelineIds.orEmpty() -
            configuration.pipelineRegistrations.map { it.pipelineId }.toSet()
        require(unknown.isEmpty()) { "Unknown pipeline IDs: ${unknown.sorted().joinToString()}" }
        ensureStarted()
        val selectedPipelineIds = config.selectedPipelineIds
        lifecycleMutex.withLock {
            pruneCompletedDaemons()
            if (daemonHandles.any { selectionsOverlap(it.selectedPipelineIds, selectedPipelineIds) } ||
                daemonReservations.any { selectionsOverlap(it, selectedPipelineIds) }
            ) {
                throw IllegalStateException(
                    "A scheduler is already registered for an overlapping pipeline selection; " +
                        "cancel it before replacing it",
                )
            }
            daemonReservations += selectedPipelineIds
        }
        val schedulerHandle = try {
            scheduler.start(
                scope = lifecycleScope,
                trigger = config.trigger,
                clock = configuration.clock,
                pipelineIds = selectedPipelineIds,
                run = ::runOnce,
                observe = ::observeSchedulerEvent,
            )
        } catch (error: Throwable) {
            lifecycleMutex.withLock { daemonReservations.remove(selectedPipelineIds) }
            throw error
        }
        val handle = EngineDaemonHandle(this, schedulerHandle, selectedPipelineIds)
        try {
            lifecycleMutex.withLock {
                daemonReservations.remove(selectedPipelineIds)
                if (lifecycleState != LifecycleState.RUNNING) {
                    throw IllegalStateException("Trawler engine is no longer accepting daemon schedulers")
                }
                daemonHandles += handle
            }
        } catch (error: Throwable) {
            schedulerHandle.cancel()
            throw error
        }
        return handle
    }

    suspend fun shutdown() {
        val ownsShutdown = lifecycleMutex.withLock {
            when (lifecycleState) {
                LifecycleState.STOPPED,
                LifecycleState.SHUTTING_DOWN,
                -> false

                LifecycleState.NEW,
                LifecycleState.RUNNING,
                LifecycleState.STARTING,
                LifecycleState.FAILED,
                -> {
                    lifecycleState = LifecycleState.SHUTTING_DOWN
                    true
                }
            }
        }
        if (!ownsShutdown) {
            shutdownCompletion.await()
            return
        }

        val daemons: List<EngineDaemonHandle>
        val runs: List<Job>
        val extensions: List<com.rmjtromp.trawler.extension.ExtensionLifecycle>
        lifecycleMutex.withLock {
            daemons = daemonHandles.toList()
            runs = activeRuns.toList()
            extensions = startedExtensions
        }

        val shutdownMillis = configuration.runtimeOptions.shutdownTimeout.toMillis().coerceAtLeast(1)
        try {
            withContext(NonCancellable) {
                var firstFailure: Throwable? = null
                daemons.forEach { daemon ->
                    val failure = boundedCleanup("scheduler cancellation", shutdownMillis) {
                        daemon.cancelScheduler()
                    }
                    if (firstFailure == null) firstFailure = failure
                }
                runs.forEach(Job::cancel)
                withTimeoutOrNull(shutdownMillis) { runs.joinAll() }
                lifecycleScope.cancel()
                withTimeoutOrNull(shutdownMillis) { rootJob.join() }

                extensions.asReversed().forEach { extension ->
                    val failure = boundedCleanup("extension ${extension.id} shutdown", shutdownMillis) {
                        extension.stop(ExtensionStopContext(ExtensionStopReason.ENGINE_SHUTDOWN))
                    }
                    if (firstFailure == null) firstFailure = failure
                }
                configuration.pipelines
                    .asSequence()
                    .flatMap { it.outputs.asSequence() }
                    .map { it.output }
                    .filterIsInstance<OutputLifecycle>()
                    .forEach { output ->
                        val failure = boundedCleanup(
                            "output ${output::class.simpleName ?: "lifecycle"} shutdown",
                            shutdownMillis,
                        ) {
                            output.close()
                        }
                        if (firstFailure == null) firstFailure = failure
                    }
                lifecycleMutex.withLock { lifecycleState = LifecycleState.STOPPED }
                firstFailure?.let { throw it }
            }
            shutdownCompletion.complete(Unit)
        } catch (error: Throwable) {
            withContext(NonCancellable) {
                lifecycleMutex.withLock { lifecycleState = LifecycleState.STOPPED }
                shutdownCompletion.completeExceptionally(error)
            }
            throw error
        }
    }

    private suspend fun runSerialized(request: RunRequest): RunResult = runMutex.withLock {
        val available = configuration.pipelineRegistrations
        val selected = request.selectedPipelineIds
        val unknown = selected.orEmpty() - available.map { it.pipelineId }.toSet()
        require(unknown.isEmpty()) { "Unknown pipeline IDs: ${unknown.sorted().joinToString()}" }
        ensureStarted()
        val registrations = available.filter { selected == null || it.pipelineId in selected }
        val engineRunId = "engine-run-${engineRunSequence.incrementAndGet()}"
        val startedAt = configuration.clock.now()
        emit(
            CrawlerEventType.ENGINE_RUN_STARTED,
            EventContext(startedAt, engineRunId = engineRunId),
            request.reason,
        )

        return@withLock try {
            val timedResult = withTimeoutOrNull<RunResult>(
                (request.timeout ?: configuration.runtimeOptions.runTimeout).toMillis().coerceAtLeast(1),
            ) {
                val pipelineRuns = supervisorScope {
                registrations.map { registration ->
                    async {
                        try {
                            val pipelineRunId = "$engineRunId/${registration.pipelineId}"
                            val state = configuration.stateStore
                            val workQueue = state as? PipelineScopedWorkQueueStore
                                ?: error("Configured state store lacks pipeline-scoped work")
                            val leaseStore = state as? LeaseStore
                                ?: error("Configured state store lacks lease fencing")
                            val attemptStore = state as? AttemptStore
                                ?: error("Configured state store lacks attempt history")
                            val historyStore = state as? ResourceHistoryStore
                                ?: error("Configured state store lacks resource history")
                            val cancellationStore = state as? WorkCancellationStore
                                ?: error("Configured state store lacks cancellation")
                            val orderingStore = state as? WorkOrderingStore
                                ?: error("Configured state store lacks work ordering")
                            val workStatus = state as? WorkQueueStatus
                                ?: error("Configured state store lacks work status")
                            registration.execute(
                                PipelineExecutionContext(
                                    configuration = configuration,
                                    pipelineId = registration.pipelineId,
                                    engineRunId = engineRunId,
                                    pipelineRunId = pipelineRunId,
                                    startedAt = startedAt,
                                    workQueue = workQueue,
                                    leaseStore = leaseStore,
                                    attemptStore = attemptStore,
                                    historyStore = historyStore,
                                    cancellationStore = cancellationStore,
                                    orderingStore = orderingStore,
                                    workStatus = workStatus,
                                    responseStore = configuration.responseStore,
                                    emit = ::emit,
                                ),
                            )
                        } catch (cancelled: CancellationException) {
                            throw cancelled
                        } catch (error: Throwable) {
                            val now = configuration.clock.now()
                            PipelineRunResult(
                                pipelineId = registration.pipelineId,
                                pipelineRunId = "$engineRunId/${registration.pipelineId}",
                                status = RunStatus.FAILED,
                                discoveryEmissions = 0,
                                acceptedWork = 0,
                                suppressedWork = 0,
                                requestsStarted = 0,
                                producedValues = 0,
                                deliveredValues = 0,
                                failures = listOf(RunFailure("pipeline-start", error)),
                                startedAt = now,
                                completedAt = now,
                            )
                        }
                    }
                }.awaitAll()
                }
                val status = when {
                    pipelineRuns.any { it.status == RunStatus.FAILED } -> RunStatus.FAILED
                    pipelineRuns.any { it.status == RunStatus.ABANDONED } -> RunStatus.ABANDONED
                    else -> RunStatus.COMPLETED
                }
                val result = RunResult(
                    engineRunId = engineRunId,
                    status = status,
                    pipelineRuns = pipelineRuns,
                    failures = pipelineRuns.flatMap { it.failures },
                    startedAt = startedAt,
                    completedAt = configuration.clock.now(),
                )
                emit(
                    when (status) {
                        RunStatus.COMPLETED -> CrawlerEventType.ENGINE_RUN_COMPLETED
                        RunStatus.ABANDONED -> CrawlerEventType.ENGINE_RUN_ABANDONED
                        RunStatus.FAILED -> CrawlerEventType.ENGINE_RUN_FAILED
                        RunStatus.RUNNING -> CrawlerEventType.ENGINE_RUN_FAILED
                        RunStatus.CANCELLED -> CrawlerEventType.ENGINE_RUN_CANCELLED
                    },
                    EventContext(result.completedAt, engineRunId = engineRunId),
                    status.name,
                )
                result
            }
            timedResult ?: run {
                val completedAt = configuration.clock.now()
                emit(
                    CrawlerEventType.ENGINE_RUN_ABANDONED,
                    EventContext(completedAt, engineRunId = engineRunId),
                    "run-timeout",
                )
                RunResult(
                    engineRunId = engineRunId,
                    status = RunStatus.ABANDONED,
                    pipelineRuns = emptyList(),
                    failures = listOf(
                        RunFailure(
                            "run-timeout",
                            IllegalStateException("Engine run exceeded its configured timeout"),
                        ),
                    ),
                    startedAt = startedAt,
                    completedAt = completedAt,
                )
            }
        } catch (cancelled: CancellationException) {
            val now = configuration.clock.now()
            withContext(NonCancellable) {
                emit(
                    CrawlerEventType.ENGINE_RUN_CANCELLED,
                    EventContext(now, engineRunId = engineRunId),
                    "cooperative cancellation",
                )
            }
            throw cancelled
        }
    }

    private suspend fun ensureStarted() {
        lifecycleMutex.withLock {
            when (lifecycleState) {
                LifecycleState.RUNNING -> return
                LifecycleState.SHUTTING_DOWN,
                LifecycleState.STOPPED,
                -> throw IllegalStateException("Trawler engine is shutting down or stopped")

                LifecycleState.FAILED -> throw IllegalStateException("Trawler engine startup previously failed")
                LifecycleState.NEW -> Unit
                LifecycleState.STARTING -> Unit
            }
            lifecycleState = LifecycleState.STARTING
            val started = mutableListOf<com.rmjtromp.trawler.extension.ExtensionLifecycle>()
            try {
                configuration.extensions.forEach { extension ->
                    extension.start(ExtensionStartContext(lifecycleScope))
                    started += extension
                }
                startedExtensions = started.toList()
                lifecycleState = LifecycleState.RUNNING
            } catch (error: Throwable) {
                withContext(NonCancellable) {
                    val cleanupTimeout = configuration.runtimeOptions.shutdownTimeout.toMillis().coerceAtLeast(1)
                    started.asReversed().forEach { extension ->
                        boundedCleanup("extension ${extension.id} startup rollback", cleanupTimeout) {
                            extension.stop(ExtensionStopContext(ExtensionStopReason.STARTUP_FAILURE))
                        }?.let(error::addSuppressed)
                    }
                }
                lifecycleState = LifecycleState.FAILED
                throw error
            }
        }
    }

    private fun ensureAcceptingRuns() {
        when (lifecycleState) {
            LifecycleState.NEW,
            LifecycleState.RUNNING,
            -> Unit

            LifecycleState.STARTING -> Unit
            LifecycleState.FAILED,
            LifecycleState.SHUTTING_DOWN,
            LifecycleState.STOPPED,
            -> throw IllegalStateException("Trawler engine is not accepting runs")
        }
    }

    private suspend fun observeSchedulerEvent(event: SchedulerEvent) {
        when (event) {
            is SchedulerEvent.Triggered -> emit(
                CrawlerEventType.SCHEDULER_TRIGGERED,
                EventContext(event.at),
                "sequence=${event.sequence}",
            )

            is SchedulerEvent.Skipped -> emit(
                CrawlerEventType.SCHEDULER_TRIGGER_SKIPPED,
                EventContext(configuration.clock.now()),
                "sequence=${event.sequence}; ${event.reason}",
            )

            is SchedulerEvent.Rejected -> emit(
                CrawlerEventType.SCHEDULER_TRIGGER_REJECTED,
                EventContext(configuration.clock.now()),
                "sequence=${event.sequence}; ${event.reason}",
            )

            is SchedulerEvent.RunFailed -> emit(
                CrawlerEventType.SCHEDULER_RUN_FAILED,
                EventContext(configuration.clock.now()),
                "sequence=${event.sequence}; ${safeFailureName(event.error)}",
            )

            is SchedulerEvent.RunCompleted,
            SchedulerEvent.Cancelled,
            -> Unit
        }
    }

    private suspend fun emit(
        type: CrawlerEventType,
        context: EventContext,
        detail: String?,
    ) {
        val event = CrawlerEvent(type, context, detail?.take(256))
        configuration.observers.forEach { observer ->
            val result = try {
                observer.onEvent(event)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (error: Throwable) {
                ObserverResult.Failed(error)
            }
            if (result is ObserverResult.Failed) observerFailureCount.incrementAndGet()
        }
    }

    private enum class LifecycleState {
        NEW,
        STARTING,
        RUNNING,
        FAILED,
        SHUTTING_DOWN,
        STOPPED,
    }

    private class EngineDaemonHandle(
        private val runtime: EngineRuntime,
        private val scheduler: SchedulerHandle,
        val selectedPipelineIds: Set<String>?,
    ) : EngineHandle {
        private val cancelled = AtomicBoolean(false)

        override val completion: Deferred<Unit>
            get() = scheduler.completion

        override suspend fun trigger(): ScheduleSubmission = scheduler.trigger()

        override suspend fun cancel() = cancelScheduler()

        override suspend fun shutdown() {
            cancelScheduler()
            runtime.shutdown()
        }

        suspend fun cancelScheduler() {
            if (cancelled.compareAndSet(false, true)) {
                try {
                    scheduler.cancel()
                } finally {
                    withContext(NonCancellable) {
                        runtime.lifecycleMutex.withLock { runtime.daemonHandles.remove(this@EngineDaemonHandle) }
                    }
                }
            }
        }
    }

    private fun pruneCompletedDaemons() {
        daemonHandles.removeIf { it.completion.isCompleted }
    }

    private fun selectionsOverlap(left: Set<String>?, right: Set<String>?): Boolean = when {
        left == null || right == null -> true
        else -> left.any(right::contains)
    }
}

private suspend fun boundedCleanup(
    label: String,
    timeoutMillis: Long,
    action: suspend () -> Unit,
): Throwable? = try {
    if (withTimeoutOrNull(timeoutMillis) {
            action()
            true
        } == true
    ) {
        null
    } else {
        IllegalStateException("$label exceeded the configured shutdown timeout")
    }
} catch (error: Throwable) {
    error
}

/** Typed runtime context captured by one internal registration owner. */
internal class PipelineExecutionContext(
    val configuration: EngineConfiguration,
    val pipelineId: String,
    val engineRunId: String,
    val pipelineRunId: String,
    val startedAt: Instant,
    val workQueue: PipelineScopedWorkQueueStore,
    val leaseStore: LeaseStore,
    val attemptStore: AttemptStore,
    val historyStore: ResourceHistoryStore,
    val cancellationStore: WorkCancellationStore,
    val orderingStore: WorkOrderingStore,
    val workStatus: WorkQueueStatus,
    val responseStore: ResponseStore?,
    val emit: suspend (CrawlerEventType, EventContext, String?) -> Unit,
) {
    suspend fun event(
        type: CrawlerEventType,
        crawler: CrawlerContext? = null,
        work: LeasedWork? = null,
        detail: String? = null,
    ) {
        emit(
            type,
            EventContext(
                emittedAt = configuration.clock.now(),
                engineRunId = engineRunId,
                pipelineRunId = pipelineRunId,
                pipelineId = pipelineId,
                requestId = crawler?.requestId,
                workItemId = work?.workItemId?.value,
            ),
            detail,
        )
    }
}

private class PipelineCounters(
    private val maxFailureRecords: Int,
) {
    val discoveryEmissions = AtomicLong(0)
    val acceptedWork = AtomicLong(0)
    val suppressedWork = AtomicLong(0)
    val requestsStarted = AtomicLong(0)
    val bytesRead = AtomicLong(0)
    val producedValues = AtomicLong(0)
    val deliveredValues = AtomicLong(0)
    val failuresCount = AtomicLong(0)
    val abandoned = AtomicBoolean(false)
    val terminalFailure = AtomicBoolean(false)
    private val failureMutex = Mutex()
    private val failures = mutableListOf<RunFailure>()

    fun reserveWork(limit: Int): Boolean {
        while (true) {
            val current = acceptedWork.get()
            if (current >= limit) return false
            if (acceptedWork.compareAndSet(current, current + 1)) return true
        }
    }

    fun releaseReservedWork() {
        acceptedWork.decrementAndGet()
    }

    fun reserveProducedValue(limit: Int): Boolean {
        while (true) {
            val current = producedValues.get()
            if (current >= limit) return false
            if (producedValues.compareAndSet(current, current + 1)) return true
        }
    }

    suspend fun failure(stage: String, error: Throwable, terminal: Boolean) {
        failuresCount.incrementAndGet()
        if (terminal) terminalFailure.set(true)
        failureMutex.withLock {
            if (failures.size < maxFailureRecords) failures += RunFailure(stage, error)
        }
    }

    suspend fun failureSnapshot(): List<RunFailure> = failureMutex.withLock { failures.toList() }
}

private suspend fun <T : Any> observeBudget(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    counters: PipelineCounters,
    observation: BudgetObservation,
) {
    val elapsed = Duration.between(context.startedAt, context.configuration.clock.now())
        .let { if (it.isNegative) Duration.ZERO else it }
    val decision = pipeline.policies.budget.evaluate(
        BudgetSnapshot(
            requestsStarted = counters.requestsStarted.get(),
            producedValues = counters.producedValues.get(),
            bytesRead = counters.bytesRead.get(),
            failures = counters.failuresCount.get(),
            discoveryEmissions = counters.discoveryEmissions.get(),
            elapsed = elapsed,
        ),
        observation,
    )
    if (decision is BudgetDecision.Stop) {
        counters.abandoned.set(true)
        throw BudgetReached(decision.reason)
    }
}

private class StageFailure(
    val stage: String,
    val original: Throwable,
) : RuntimeException(stage, original)

private class BudgetReached(
    val reason: String,
) : RuntimeException(reason)

private class PolicyRejection(
    val reason: String,
) : RuntimeException(reason)

internal suspend fun <T : Any> executeTypedPipeline(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
): PipelineRunResult {
    val startedAt = context.configuration.clock.now()
    val counters = PipelineCounters(context.configuration.runtimeOptions.maxQueueSize)
    val responses = LinkedHashMap<WorkItemId, CrawlResponse>()
    val responseMutex = Mutex()
    val pipelineExtensions = mutableListOf<com.rmjtromp.trawler.extension.ExtensionLifecycle>()
    var cancelled = false

    context.event(CrawlerEventType.PIPELINE_RUN_STARTED)
    try {
        context.orderingStore.setOrdering(pipeline.id, pipeline.policies.ordering)
        pipeline.extensions.forEach { extension ->
            extension.start(ExtensionStartContext(CoroutineScope(currentCoroutineContext())))
            pipelineExtensions += extension
        }

        val accept: suspend (CrawlRequest, CrawlResponse?) -> Unit = { request, existingResponse ->
            acceptWork(pipeline, context, counters, responses, responseMutex, request, existingResponse)
        }

        pipeline.discoveries.forEach { discovery ->
            context.event(CrawlerEventType.DISCOVERY_STARTED, detail = discovery.id)
            try {
                val discoveryCrawler = CrawlerContext(
                    engineRunId = context.engineRunId,
                    pipelineRunId = context.pipelineRunId,
                    pipelineId = context.pipelineId,
                )
                val executor = RequestExecutor { request, requestContext ->
                    executeDiscoveryRequest(pipeline, context, request, requestContext)
                }
                discovery.discover(DiscoveryContext(discoveryCrawler, executor))
                    .take(context.configuration.runtimeOptions.maxDiscoveryEmissionsPerDiscovery)
                    .collect { emission ->
                        if (counters.discoveryEmissions.get() >=
                            context.configuration.runtimeOptions.maxDiscoveryEmissionsPerRun
                        ) {
                            throw BudgetReached("discovery-per-run-bound")
                        }
                        counters.discoveryEmissions.incrementAndGet()
                        observeBudget(
                            pipeline,
                            context,
                            counters,
                            BudgetObservation.DiscoveryEmitted(1),
                        )
                        context.event(CrawlerEventType.DISCOVERY_EMITTED, detail = discovery.id)
                        when (emission) {
                            is DiscoveryEmission.Request -> accept(emission.request, null)
                            is DiscoveryEmission.ExistingResponse ->
                                accept(emission.request, emission.response)
                        }
                    }
                context.event(CrawlerEventType.DISCOVERY_COMPLETED, detail = discovery.id)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (budget: BudgetReached) {
                counters.abandoned.set(true)
                context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "budget:${budget.reason}")
            } catch (error: Throwable) {
                counters.failure("discovery:${discovery.id}", error, terminal = true)
                context.event(CrawlerEventType.DISCOVERY_FAILED, detail = discovery.id)
            }
        }

        supervisorScope {
            repeat(context.configuration.runtimeOptions.workerCount) { index ->
                launch {
                    workerLoop(
                        pipeline = pipeline,
                        context = context,
                        counters = counters,
                        responses = responses,
                        responseMutex = responseMutex,
                        ownerId = "${context.pipelineRunId}/worker-$index",
                        accept = accept,
                    )
                }
            }
        }
        if (context.workStatus.activeWorkCount(context.pipelineId) > 0) {
            counters.abandoned.set(true)
            context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "resumable-work-remains")
        }
    } catch (cancellation: CancellationException) {
        cancelled = true
        withContext(NonCancellable) {
            val cleanupTimeout = context.configuration.runtimeOptions.shutdownTimeout.toMillis().coerceAtLeast(1)
            withTimeoutOrNull(cleanupTimeout) {
                context.cancellationStore.cancelQueued(
                    context.pipelineId,
                    WorkTransition.Cancelled("pipeline run cancelled"),
                )
            }
            withTimeoutOrNull(cleanupTimeout) {
                context.event(CrawlerEventType.PIPELINE_RUN_CANCELLED, detail = "cooperative cancellation")
            }
        }
        throw cancellation
    } catch (error: Throwable) {
        counters.failure("pipeline", error, terminal = true)
    } finally {
        withContext(NonCancellable) {
            val cleanupTimeout = context.configuration.runtimeOptions.shutdownTimeout.toMillis().coerceAtLeast(1)
            pipelineExtensions.asReversed().forEach { extension ->
                val failure = boundedCleanup("pipeline extension ${extension.id} shutdown", cleanupTimeout) {
                    extension.stop(
                        ExtensionStopContext(
                            when {
                                cancelled -> ExtensionStopReason.CANCELLATION
                                counters.terminalFailure.get() -> ExtensionStopReason.PIPELINE_RUN_FAILED
                                counters.abandoned.get() -> ExtensionStopReason.PIPELINE_RUN_ABANDONED
                                else -> ExtensionStopReason.PIPELINE_RUN_COMPLETED
                            },
                        ),
                    )
                }
                failure?.let { counters.failure("extension-stop:${extension.id}", it, terminal = true) }
            }
        }
    }

    val failures = counters.failureSnapshot()
    val status = when {
        counters.terminalFailure.get() -> RunStatus.FAILED
        counters.abandoned.get() -> RunStatus.ABANDONED
        else -> RunStatus.COMPLETED
    }
    val completedAt = context.configuration.clock.now()
    val result = PipelineRunResult(
        pipelineId = context.pipelineId,
        pipelineRunId = context.pipelineRunId,
        status = status,
        discoveryEmissions = counters.discoveryEmissions.get(),
        acceptedWork = counters.acceptedWork.get(),
        suppressedWork = counters.suppressedWork.get(),
        requestsStarted = counters.requestsStarted.get(),
        producedValues = counters.producedValues.get(),
        deliveredValues = counters.deliveredValues.get(),
        failures = failures,
        startedAt = startedAt,
        completedAt = completedAt,
    )
    context.event(
        when (status) {
            RunStatus.COMPLETED -> CrawlerEventType.PIPELINE_RUN_COMPLETED
            RunStatus.ABANDONED -> CrawlerEventType.PIPELINE_RUN_ABANDONED
            RunStatus.FAILED -> CrawlerEventType.PIPELINE_RUN_FAILED
            RunStatus.CANCELLED -> CrawlerEventType.PIPELINE_RUN_CANCELLED
            RunStatus.RUNNING -> CrawlerEventType.PIPELINE_RUN_FAILED
        },
        detail = status.name,
    )
    return result
}

private suspend fun <T : Any> acceptWork(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    counters: PipelineCounters,
    responses: MutableMap<WorkItemId, CrawlResponse>,
    responseMutex: Mutex,
    request: CrawlRequest,
    existingResponse: CrawlResponse?,
) {
    val options = context.configuration.runtimeOptions
    if (!counters.reserveWork(options.maxWorkItemsPerRun)) {
        counters.abandoned.set(true)
        context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "work-per-run-bound")
        return
    }
    var accepted = false
    try {
        if ((request.body?.size ?: 0) > options.maxRequestBodyBytes) {
            counters.abandoned.set(true)
            context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "request-body-bound")
            return
        }
        if (existingResponse != null && existingResponse.body.size > options.maxResponseBodyBytes) {
            counters.abandoned.set(true)
            context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "response-body-bound")
            return
        }
        val crawler = CrawlerContext(context.engineRunId, context.pipelineRunId, context.pipelineId)
        val requestContext = RequestContext(crawler, request)
        val canonical = pipeline.policies.requestCanonicalizer.canonicalize(request, requestContext)
        val canonicalContext = requestContext.copy(request = canonical)
        val identity = pipeline.policies.requestIdentity.identify(canonical, canonicalContext)
        val history = context.historyStore.history(context.pipelineId, identity)
        when (val recrawl = pipeline.policies.recrawl.decide(canonical, history, canonicalContext)) {
            RecrawlDecision.Eligible -> Unit
            is RecrawlDecision.Deferred -> {
                counters.abandoned.set(true)
                context.event(
                    CrawlerEventType.REQUEST_FAILED,
                    detail = "recrawl-deferred:${recrawl.reason ?: "policy"}",
                )
                return
            }

            is RecrawlDecision.Rejected -> {
                counters.suppressedWork.incrementAndGet()
                context.event(CrawlerEventType.REQUEST_FAILED, detail = "recrawl-rejected:${recrawl.reason}")
                return
            }
        }
        when (val scope = pipeline.policies.scope.evaluate(canonical, canonicalContext)) {
            ScopeDecision.Allowed -> Unit
            is ScopeDecision.Rejected -> {
                counters.suppressedWork.incrementAndGet()
                context.event(CrawlerEventType.REQUEST_FAILED, detail = "scope-rejected:${scope.reason}")
                return
            }
        }
        when (val dedup = pipeline.policies.deduplication.decide(canonical, identity, canonicalContext)) {
            DeduplicationDecision.Admit -> Unit
            is DeduplicationDecision.Suppress -> {
                counters.suppressedWork.incrementAndGet()
                context.event(CrawlerEventType.REQUEST_FAILED, detail = "deduplicated:${dedup.reason}")
                return
            }
        }
        when (val result = context.workQueue.enqueue(canonical, identity, crawler)) {
            is com.rmjtromp.trawler.state.EnqueueResult.Accepted -> {
                accepted = true
                if (existingResponse != null) {
                    responseMutex.withLock {
                        responses[result.workItemId] = existingResponse.withRuntimeAttributes(
                            context,
                            result.workItemId.value,
                        )
                    }
                }
                context.event(CrawlerEventType.REQUEST_QUEUED, work = null, detail = result.workItemId.value)
            }

            is com.rmjtromp.trawler.state.EnqueueResult.AlreadyActive -> {
                counters.suppressedWork.incrementAndGet()
                context.event(CrawlerEventType.REQUEST_FAILED, detail = "active-duplicate")
            }

            is com.rmjtromp.trawler.state.EnqueueResult.Rejected -> {
                counters.abandoned.set(true)
                context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "work-rejected:${result.reason}")
            }
        }
    } finally {
        if (!accepted) counters.releaseReservedWork()
    }
}

private suspend fun <T : Any> workerLoop(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    counters: PipelineCounters,
    responses: MutableMap<WorkItemId, CrawlResponse>,
    responseMutex: Mutex,
    ownerId: String,
    accept: suspend (CrawlRequest, CrawlResponse?) -> Unit,
) {
    while (currentCoroutineContext().isActive) {
        val claimed = try {
            context.workQueue.claim(
                pipelineId = context.pipelineId,
                limit = context.configuration.runtimeOptions.claimBatchSize,
                ownerId = ownerId,
                now = context.configuration.clock.now(),
            )
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (error: Throwable) {
            counters.failure("claim", error, terminal = true)
            return
        }
        if (claimed.isEmpty()) return
        claimed.forEach { work ->
            try {
                processWork(pipeline, context, counters, responses, responseMutex, work, accept)
            } catch (cancellation: CancellationException) {
                withContext(NonCancellable) {
                    withTimeoutOrNull(context.configuration.runtimeOptions.shutdownTimeout.toMillis().coerceAtLeast(1)) {
                        context.leaseStore.transition(work, WorkTransition.Cancelled("worker cancelled"))
                    }
                }
                throw cancellation
            } catch (error: Throwable) {
                counters.failure("worker", error, terminal = true)
                withContext(NonCancellable) {
                    context.leaseStore.transition(work, WorkTransition.Failed("worker"))
                }
            }
        }
    }
}

private suspend fun <T : Any> processWork(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    counters: PipelineCounters,
    responses: MutableMap<WorkItemId, CrawlResponse>,
    responseMutex: Mutex,
    work: LeasedWork,
    accept: suspend (CrawlRequest, CrawlResponse?) -> Unit,
) = coroutineScope {
    val leaseLost = AtomicBoolean(false)
    val processJob = launch {
        processLeasedWork(
            pipeline = pipeline,
            context = context,
            counters = counters,
            responses = responses,
            responseMutex = responseMutex,
            work = work,
            accept = accept,
            leaseLost = leaseLost,
        )
    }
    val renewalJob = launch {
        val renewalDelay = context.configuration.runtimeOptions.leaseDuration.toMillis()
            .coerceAtLeast(3)
            .div(3)
            .coerceAtLeast(1)
        try {
            while (isActive) {
                kotlinx.coroutines.delay(renewalDelay)
                when (val result = context.leaseStore.renew(
                    work,
                    context.configuration.clock.now().plus(context.configuration.runtimeOptions.leaseDuration),
                )) {
                    LeaseMutationResult.Accepted -> Unit
                    LeaseMutationResult.Stale -> {
                        leaseLost.set(true)
                        processJob.cancel(LeaseLostCancellationException("Lease was reclaimed"))
                        return@launch
                    }

                    is LeaseMutationResult.Rejected -> {
                        leaseLost.set(true)
                        processJob.cancel(LeaseLostCancellationException(result.reason))
                        return@launch
                    }
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Throwable) {
            leaseLost.set(true)
            processJob.cancel(LeaseLostCancellationException("Lease renewal failed", error))
        }
    }
    try {
        processJob.join()
    } finally {
        renewalJob.cancelAndJoin()
    }
}

private suspend fun <T : Any> processLeasedWork(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    counters: PipelineCounters,
    responses: MutableMap<WorkItemId, CrawlResponse>,
    responseMutex: Mutex,
    work: LeasedWork,
    accept: suspend (CrawlRequest, CrawlResponse?) -> Unit,
    leaseLost: AtomicBoolean,
) {
    context.event(CrawlerEventType.REQUEST_CLAIMED, work = work)
    val baseCrawler = CrawlerContext(
        engineRunId = context.engineRunId,
        pipelineRunId = context.pipelineRunId,
        pipelineId = context.pipelineId,
        requestId = work.workItemId.value,
    )
    when (val scope = pipeline.policies.scope.evaluate(work.request, RequestContext(baseCrawler, work.request))) {
        ScopeDecision.Allowed -> Unit
        is ScopeDecision.Rejected -> {
            context.leaseStore.transition(work, WorkTransition.Skipped(scope.reason))
            context.event(CrawlerEventType.REQUEST_FAILED, baseCrawler, work, "scope-rejected:${scope.reason}")
            return
        }
    }
    when (val authorization = pipeline.policies.authorization.authorize(
        work.request,
        RequestContext(baseCrawler, work.request),
    )) {
        AuthorizationDecision.Authorized -> Unit
        is AuthorizationDecision.Denied -> {
            context.leaseStore.transition(work, WorkTransition.Skipped(authorization.reason))
            context.event(CrawlerEventType.REQUEST_FAILED, baseCrawler, work, "authorization-denied")
            return
        }
    }

    var permit: com.rmjtromp.trawler.policy.ConcurrencyPermit? = null
    var pacing: com.rmjtromp.trawler.policy.PacingReservation? = null
    var attempt: ExecutionAttempt? = null
    try {
        val requestContext = RequestContext(baseCrawler, work.request)
        permit = pipeline.policies.concurrency.acquire(work.request, requestContext)
        pacing = pipeline.policies.pacing.acquire(work.request, requestContext)
        when (val started = context.attemptStore.beginAttempt(work, context.configuration.clock.now())) {
            AttemptStartResult.Stale -> return
            is AttemptStartResult.Rejected -> throw StageFailure("attempt-start", IllegalStateException(started.reason))
            is AttemptStartResult.Started -> {
                attempt = started.attempt
                counters.requestsStarted.incrementAndGet()
                observeBudget(pipeline, context, counters, BudgetObservation.RequestStarted)
            }
        }
        val crawler = baseCrawler.copy(attemptId = requireNotNull(attempt).id.value)
        context.event(CrawlerEventType.REQUEST_STARTED, crawler, work)
        val existing = responseMutex.withLock { responses.remove(work.workItemId) }
        val currentResponse = existing ?: executeWorkRequest(pipeline, context, work.request, crawler)
        if (currentResponse.body.size > context.configuration.runtimeOptions.maxResponseBodyBytes) {
            throw StageFailure(
                "response-body-bound",
                IllegalStateException("Transport response body exceeds the configured bound"),
            )
        }
        counters.bytesRead.addAndGet(currentResponse.body.size.toLong())
        observeBudget(
            pipeline,
            context,
            counters,
            BudgetObservation.BytesRead(currentResponse.body.size.toLong()),
        )
        pipeline.policies.pacing.observe(currentResponse, RequestContext(crawler, work.request))
        val storedReference = storeResponse(context, currentResponse, crawler, work)
        context.event(CrawlerEventType.EXTRACTION_STARTED, crawler, work)
        val extraction = try {
            pipeline.extractor.extract(
                currentResponse,
                ExtractionContext(crawler, work.request, currentResponse),
            )
        } catch (error: Throwable) {
            context.event(CrawlerEventType.EXTRACTION_FAILED, crawler, work)
            throw StageFailure("extraction", error)
        }
        when (extraction) {
            is ExtractionResult.Ignore -> {
                context.event(CrawlerEventType.EXTRACTION_IGNORED, crawler, work, extraction.reason)
            }

            is ExtractionResult.Failure -> {
                context.event(CrawlerEventType.EXTRACTION_FAILED, crawler, work)
                throw StageFailure("extraction", extraction.error)
            }

            is ExtractionResult.Success -> {
                if (extraction.values.size > context.configuration.runtimeOptions.maxValuesPerResponse) {
                    throw StageFailure(
                        "extraction-bound",
                        IllegalStateException("Extractor returned more values than the configured bounded runtime can process"),
                    )
                }
                if (extraction.followUps.size > context.configuration.runtimeOptions.maxFollowUpRequestsPerWorkItem) {
                    throw StageFailure(
                        "follow-up-bound",
                        IllegalStateException("Extractor returned more follow-up requests than the configured bound"),
                    )
                }
                extraction.followUps.forEach { followUp -> accept(followUp, null) }
                context.event(CrawlerEventType.EXTRACTION_COMPLETED, crawler, work)
                val produced = mutableListOf<Produced<T>>()
                extraction.values.forEach { value ->
                    var current: T? = value
                    for (transform in pipeline.transforms) {
                        val decision = transform.apply(
                            requireNotNull(current),
                            TransformContext(crawler, work.request),
                        )
                        current = when (decision) {
                            is TransformDecision.Accept -> decision.value
                            is TransformDecision.Mutate -> {
                                context.event(CrawlerEventType.VALUE_TRANSFORMED, crawler, work, transform.id)
                                decision.value
                            }

                            is TransformDecision.Drop -> {
                                context.event(CrawlerEventType.VALUE_DROPPED, crawler, work, decision.reason)
                                null
                            }
                        }
                        if (current == null) break
                    }
                    if (current == null) return@forEach
                    for (validator in pipeline.validators) {
                        when (val validation = validator.validate(
                            requireNotNull(current),
                            ValidationContext(crawler, work.request),
                        )) {
                            com.rmjtromp.trawler.pipeline.ValidationResult.Accepted -> Unit
                            is com.rmjtromp.trawler.pipeline.ValidationResult.Rejected -> {
                                context.event(CrawlerEventType.VALUE_DROPPED, crawler, work, validation.reason)
                                current = null
                            }

                            is com.rmjtromp.trawler.pipeline.ValidationResult.Failed ->
                                throw StageFailure("validation:${validator.id}", validation.error)
                        }
                        if (current == null) break
                    }
                    if (current != null) {
                        if (!counters.reserveProducedValue(
                                context.configuration.runtimeOptions.maxProducedValuesPerRun,
                            )
                        ) {
                            throw BudgetReached("produced-value-per-run-bound")
                        }
                        val producedValue = Produced(
                            value = requireNotNull(current),
                            context = ProductionContext(
                                pipelineId = context.pipelineId,
                                engineRunId = context.engineRunId,
                                pipelineRunId = context.pipelineRunId,
                                requestId = work.workItemId.value,
                                responseReference = storedReference,
                                originalTarget = work.request.target,
                                finalTarget = currentResponse.finalTarget,
                                producedAt = context.configuration.clock.now(),
                                attributes = currentResponse.attributes,
                            ),
                        )
                        produced += producedValue
                        observeBudget(
                            pipeline,
                            context,
                            counters,
                            BudgetObservation.ValuesProduced(1),
                        )
                        context.event(CrawlerEventType.VALUE_PRODUCED, crawler, work)
                    }
                }
                deliverOutputs(pipeline, context, counters, produced, crawler)
            }
        }
        context.attemptStore.finishAttempt(work, requireNotNull(attempt), AttemptOutcome.SUCCEEDED)
        context.leaseStore.transition(work, WorkTransition.Succeeded)
        context.event(CrawlerEventType.REQUEST_COMPLETED, crawler, work)
    } catch (cancellation: CancellationException) {
        if (leaseLost.get()) {
            withContext(NonCancellable) {
                val cleanupTimeout = context.configuration.runtimeOptions.shutdownTimeout.toMillis().coerceAtLeast(1)
                attempt?.let { withTimeoutOrNull(cleanupTimeout) {
                    context.attemptStore.finishAttempt(work, it, AttemptOutcome.ABANDONED)
                } }
                counters.abandoned.set(true)
                withTimeoutOrNull(cleanupTimeout) {
                    context.event(CrawlerEventType.REQUEST_FAILED, baseCrawler, work, "lease-lost")
                }
            }
            return
        }
        withContext(NonCancellable) {
            val cleanupTimeout = context.configuration.runtimeOptions.shutdownTimeout.toMillis().coerceAtLeast(1)
            attempt?.let { withTimeoutOrNull(cleanupTimeout) {
                context.attemptStore.finishAttempt(work, it, AttemptOutcome.CANCELLED)
            } }
            withTimeoutOrNull(cleanupTimeout) {
                context.leaseStore.transition(work, WorkTransition.Cancelled("work cancelled"))
            }
            withTimeoutOrNull(cleanupTimeout) {
                context.event(CrawlerEventType.REQUEST_FAILED, baseCrawler, work, "cancelled")
            }
        }
        throw cancellation
    } catch (budget: BudgetReached) {
        attempt?.let { context.attemptStore.finishAttempt(work, it, AttemptOutcome.ABANDONED) }
        context.leaseStore.transition(work, WorkTransition.Abandoned(budget.reason))
        counters.abandoned.set(true)
        context.event(CrawlerEventType.CAPABILITY_WARNING, baseCrawler, work, "budget:${budget.reason}")
    } catch (failure: StageFailure) {
        attempt?.let { context.attemptStore.finishAttempt(work, it, AttemptOutcome.FAILED) }
        handleWorkFailure(pipeline, context, counters, work, attempt, failure.stage, failure.original, baseCrawler)
    } catch (error: Throwable) {
        attempt?.let { context.attemptStore.finishAttempt(work, it, AttemptOutcome.FAILED) }
        handleWorkFailure(pipeline, context, counters, work, attempt, "work", error, baseCrawler)
    } finally {
        pacing?.release()
        permit?.release()
    }
}

private class LeaseLostCancellationException(
    message: String,
    cause: Throwable? = null,
) : CancellationException(message) {
    init {
        if (cause != null) initCause(cause)
    }
}

private suspend fun <T : Any> deliverOutputs(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    counters: PipelineCounters,
    produced: List<Produced<T>>,
    crawler: CrawlerContext,
) {
    if (produced.isEmpty()) return
    if (pipeline.outputs.isEmpty()) {
        context.event(CrawlerEventType.VALUE_DROPPED, crawler, detail = "no-output-bound")
        return
    }
    pipeline.outputs.forEach { binding ->
        produced.chunked(context.configuration.runtimeOptions.maxOutputBatchSize).forEach { batch ->
            val result = try {
                binding.output.write(batch, OutputContext(crawler))
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (error: Throwable) {
                com.rmjtromp.trawler.pipeline.OutputResult.Failed(error)
            }
            when (result) {
                is OutputResult.Accepted -> {
                    if (result.itemCount != batch.size) {
                        val error = IllegalStateException("Output acknowledged an unexpected item count")
                        if (binding.requirement == OutputRequirement.REQUIRED) throw StageFailure("output:${binding.output.id}", error)
                        counters.failure("output:${binding.output.id}", error, terminal = false)
                    } else {
                        counters.deliveredValues.addAndGet(batch.size.toLong())
                        context.event(CrawlerEventType.VALUE_ACCEPTED, crawler, detail = binding.output.id)
                    }
                }

                is OutputResult.Skipped -> {
                    if (binding.output is NoOutput<*>) {
                        context.event(CrawlerEventType.VALUE_DROPPED, crawler, detail = result.reason)
                    } else if (binding.requirement == OutputRequirement.REQUIRED) {
                        throw StageFailure(
                            "output:${binding.output.id}",
                            IllegalStateException(result.reason ?: "Required output skipped delivery"),
                        )
                    }
                    context.event(CrawlerEventType.VALUE_DROPPED, crawler, detail = result.reason)
                }

                is OutputResult.Failed -> {
                    context.event(CrawlerEventType.DELIVERY_FAILED, crawler, detail = binding.output.id)
                    if (binding.requirement == OutputRequirement.REQUIRED) {
                        throw StageFailure("output:${binding.output.id}", result.error)
                    }
                    counters.failure("output:${binding.output.id}", result.error, terminal = false)
                }
            }
        }
    }
}

private suspend fun <T : Any> handleWorkFailure(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    counters: PipelineCounters,
    work: LeasedWork,
    attempt: ExecutionAttempt?,
    stage: String,
    error: Throwable,
    crawler: CrawlerContext,
) {
    counters.failure(stage, error, terminal = false)
    val attemptSummary = AttemptSummary(
        attemptNumber = attempt?.number ?: 0,
        startedAt = attempt?.startedAt,
        previousFailures = (attempt?.number ?: 1) - 1,
    )
    val failureContext = FailureContext(crawler, work.request)
    val classification = pipeline.policies.failureClassifier.classify(error, failureContext)
    val retryDecision = if (attempt != null &&
        attempt.number < context.configuration.runtimeOptions.maxAttemptsPerWorkItem
    ) {
        pipeline.policies.retry.decide(classification, attemptSummary, failureContext)
    } else {
        RetryDecision.DoNotRetry
    }
    when (retryDecision) {
        RetryDecision.RetryNow -> {
            context.leaseStore.transition(work, WorkTransition.Retry(context.configuration.clock.now()))
            context.event(CrawlerEventType.RETRY_SCHEDULED, crawler, work, "now")
        }

        is RetryDecision.RetryAt -> {
            context.leaseStore.transition(work, WorkTransition.Retry(retryDecision.at))
            counters.abandoned.set(true)
            context.event(CrawlerEventType.RETRY_SCHEDULED, crawler, work, retryDecision.at.toString())
        }

        RetryDecision.DoNotRetry -> {
            counters.terminalFailure.set(true)
            context.leaseStore.transition(work, WorkTransition.Failed(stage))
            context.event(CrawlerEventType.REQUEST_FAILED, crawler, work, stage)
        }

        is RetryDecision.Abandon -> {
            counters.abandoned.set(true)
            context.leaseStore.transition(work, WorkTransition.Abandoned(retryDecision.reason))
            context.event(CrawlerEventType.REQUEST_FAILED, crawler, work, "abandoned")
        }
    }
}

private suspend fun <T : Any> executeDiscoveryRequest(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    request: CrawlRequest,
    suppliedContext: RequestContext,
): CrawlResponse {
    if ((request.body?.size ?: 0) > context.configuration.runtimeOptions.maxRequestBodyBytes) {
        throw PolicyRejection("request body exceeds the configured bound")
    }
    when (val scope = pipeline.policies.scope.evaluate(request, suppliedContext)) {
        ScopeDecision.Allowed -> Unit
        is ScopeDecision.Rejected -> throw PolicyRejection(scope.reason)
    }
    when (val authorization = pipeline.policies.authorization.authorize(request, suppliedContext)) {
        AuthorizationDecision.Authorized -> Unit
        is AuthorizationDecision.Denied -> throw PolicyRejection(authorization.reason)
    }
    val permit = pipeline.policies.concurrency.acquire(request, suppliedContext)
    val pacing = pipeline.policies.pacing.acquire(request, suppliedContext)
    return try {
        val response = executeThroughMiddleware(pipeline, context, request, suppliedContext)
        if (response.body.size > context.configuration.runtimeOptions.maxResponseBodyBytes) {
            throw IllegalStateException("Discovery response exceeds the configured body bound")
        }
        pipeline.policies.pacing.observe(response, suppliedContext)
        response
    } finally {
        pacing.release()
        permit.release()
    }
}

private suspend fun <T : Any> executeWorkRequest(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    request: CrawlRequest,
    crawler: CrawlerContext,
): CrawlResponse = executeThroughMiddleware(
    pipeline,
    context,
    request,
    RequestContext(crawler, request),
)

private suspend fun <T : Any> executeThroughMiddleware(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    request: CrawlRequest,
    suppliedContext: RequestContext,
): CrawlResponse {
    val requestWithMetadata = request.withRuntimeAttributes(context, suppliedContext.crawler.requestId)
    val requestContext = suppliedContext.copy(request = requestWithMetadata)
    val responseHandler = object : ResponseHandler {
        override suspend fun handle(response: CrawlResponse, context: ResponseContext): CrawlResponse =
            executeResponseMiddleware(pipeline, response, context, 0)
    }
    val terminal = RequestExecutor { nextRequest, nextRequestContext ->
        val transport: Transport = pipeline.transportRouter.select(nextRequest, nextRequestContext)
        val raw = transport.execute(nextRequest)
        responseHandler.handle(
            raw.withRuntimeAttributes(context, nextRequestContext.crawler.requestId),
            ResponseContext(nextRequestContext.crawler, nextRequest, raw),
        )
    }
    var next = terminal
    pipeline.requestMiddlewares.asReversed().forEach { middleware ->
        val downstream = next
        next = RequestExecutor { nextRequest, nextContext ->
            middleware.intercept(nextRequest, nextContext, downstream)
        }
    }
    return next.execute(requestWithMetadata, requestContext)
}

private suspend fun <T : Any> executeResponseMiddleware(
    pipeline: Pipeline<T>,
    response: CrawlResponse,
    context: ResponseContext,
    index: Int,
): CrawlResponse {
    if (index >= pipeline.responseMiddlewares.size) return response
    val middleware = pipeline.responseMiddlewares[index]
    return middleware.intercept(
        response,
        context.copy(response = response),
        ResponseHandler { nextResponse, nextContext ->
            executeResponseMiddleware(pipeline, nextResponse, nextContext, index + 1)
        },
    )
}

private suspend fun storeResponse(
    context: PipelineExecutionContext,
    response: CrawlResponse,
    crawler: CrawlerContext,
    work: LeasedWork,
): com.rmjtromp.trawler.model.ResponseReference? {
    val store = context.responseStore ?: return null
    return try {
        val reference = store.put(response)
        if (reference == null) {
            context.event(CrawlerEventType.RESPONSE_STORE_FAILED, crawler, work, "store-declined")
        } else {
            context.event(CrawlerEventType.RESPONSE_STORED, crawler, work)
        }
        reference
    } catch (error: Throwable) {
        context.event(CrawlerEventType.RESPONSE_STORE_FAILED, crawler, work, safeFailureName(error))
        throw StageFailure("response-store", error)
    }
}

private fun CrawlRequest.withRuntimeAttributes(
    context: PipelineExecutionContext,
    requestId: String? = null,
): CrawlRequest = copy(
    attributes = attributes
        .plus(ContextAttribute("trawler.pipeline-id", context.pipelineId))
        .let { current ->
            if (requestId == null) current else current.plus(ContextAttribute("trawler.request-id", requestId))
        },
)

private fun CrawlResponse.withRuntimeAttributes(
    context: PipelineExecutionContext,
    requestId: String? = null,
): CrawlResponse = copy(
    request = request.withRuntimeAttributes(context, requestId),
)

private fun safeFailureName(error: Throwable): String = error::class.simpleName ?: "failure"

private class EngineDaemonHandleMarker
