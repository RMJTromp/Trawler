package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.DiscoveryContext
import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.discovery.DiscoveryCheckpointCandidate
import com.rmjtromp.trawler.discovery.HarvestCompletion
import com.rmjtromp.trawler.discovery.HarvestCompletionContext
import com.rmjtromp.trawler.discovery.HarvestContext
import com.rmjtromp.trawler.discovery.HarvestRunOutcome
import com.rmjtromp.trawler.discovery.ResumableDiscovery
import com.rmjtromp.trawler.event.CrawlerEvent
import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.event.EventContext
import com.rmjtromp.trawler.event.ObserverResult
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.extension.ExtensionStopReason
import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ReplayLifecycle
import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
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
import com.rmjtromp.trawler.policy.CircuitBreakerDecision
import com.rmjtromp.trawler.policy.DiscoveryAdmissionContext
import com.rmjtromp.trawler.policy.DiscoveryAdmissionDecision
import com.rmjtromp.trawler.policy.PipelineEligibilityContext
import com.rmjtromp.trawler.policy.PipelineEligibilityDecision
import com.rmjtromp.trawler.policy.PipelineHealthObservation
import com.rmjtromp.trawler.policy.PipelineHealthOutcome
import com.rmjtromp.trawler.response.ResponseStore
import com.rmjtromp.trawler.response.ResponseReplayStore
import com.rmjtromp.trawler.response.ResponseStoreCapability
import com.rmjtromp.trawler.response.ResponseCacheDecision
import com.rmjtromp.trawler.response.ResponseCacheContext
import com.rmjtromp.trawler.response.ResponseCaptureContext
import com.rmjtromp.trawler.response.ResponseCaptureDecision
import com.rmjtromp.trawler.response.ResponseStoreLifecycle
import com.rmjtromp.trawler.schedule.ScheduleSubmission
import com.rmjtromp.trawler.schedule.Scheduler
import com.rmjtromp.trawler.schedule.SchedulerEvent
import com.rmjtromp.trawler.schedule.SchedulerHandle
import com.rmjtromp.trawler.state.AttemptStartResult
import com.rmjtromp.trawler.state.AttemptOutcome
import com.rmjtromp.trawler.state.AttemptStore
import com.rmjtromp.trawler.state.AtomicEnqueueCheckpointResult
import com.rmjtromp.trawler.state.AtomicEnqueueAndCheckpointStore
import com.rmjtromp.trawler.state.CheckpointPayload
import com.rmjtromp.trawler.state.CheckpointQuarantineStore
import com.rmjtromp.trawler.state.CrawlStateCapability
import com.rmjtromp.trawler.state.DiscoveryCheckpoint
import com.rmjtromp.trawler.state.DiscoveryCheckpointStore
import com.rmjtromp.trawler.state.DiscoveryWorkItem
import com.rmjtromp.trawler.state.ExecutionAttempt
import com.rmjtromp.trawler.state.DurableRunStatus
import com.rmjtromp.trawler.state.LeaseMutationResult
import com.rmjtromp.trawler.state.LeaseStore
import com.rmjtromp.trawler.state.HarvestState
import com.rmjtromp.trawler.state.HarvestStateQuarantineStore
import com.rmjtromp.trawler.state.HarvestStateResolution
import com.rmjtromp.trawler.state.HarvestStateStore
import com.rmjtromp.trawler.state.PipelineEligibilityOverrideStore
import com.rmjtromp.trawler.state.PipelineHealthSnapshot
import com.rmjtromp.trawler.state.PipelineHealthStore
import com.rmjtromp.trawler.state.PipelineScopedWorkQueueStore
import com.rmjtromp.trawler.state.ResourceHistoryStore
import com.rmjtromp.trawler.state.RunHistoryStore
import com.rmjtromp.trawler.state.WorkCancellationStore
import com.rmjtromp.trawler.state.WorkItemId
import com.rmjtromp.trawler.state.WorkOrderingStore
import com.rmjtromp.trawler.state.WorkQueueStatus
import com.rmjtromp.trawler.state.WorkQueueStore
import com.rmjtromp.trawler.state.WorkQueueMetrics
import com.rmjtromp.trawler.state.WorkQueueMetricsStore
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
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
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
    private val runGate = PipelineRunGate()
    private val activeRunTimeouts = ConcurrentHashMap<String, Duration>()
    private val rootJob = SupervisorJob()
    private val lifecycleScope = CoroutineScope(rootJob + Dispatchers.Default)
    private val activeRuns = linkedSetOf<Job>()
    private val daemonHandles = linkedSetOf<EngineDaemonHandle>()
    private val daemonReservations = mutableListOf<Set<String>?>()
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

    /**
     * Reprocesses bounded stored responses through one registered typed
     * pipeline. Replay excludes every ordinary run, performs no network
     * call, and never enqueues or transitions crawl work.
     */
    suspend fun <T : Any> replay(selection: ReplaySelection<T>): ReplayResult = coroutineScope {
        val replay = async(start = CoroutineStart.LAZY) {
            replaySerialized(selection)
        }
        lifecycleMutex.withLock {
            ensureAcceptingRuns()
            if (activeRuns.size >= configuration.runtimeOptions.maxPendingRuns) {
                throw IllegalStateException("Engine pending-run capacity is full")
            }
            activeRuns += replay
        }
        replay.start()
        try {
            replay.await()
        } finally {
            lifecycleMutex.withLock { activeRuns.remove(replay) }
        }
    }

    private fun registeredPipelineIds(): Set<String> =
        configuration.pipelineRegistrations.map { it.pipelineId }.toSet()

    private suspend fun <T : Any> replaySerialized(selection: ReplaySelection<T>): ReplayResult = runGate.withPipelines(registeredPipelineIds()) {
        if (selection.query is com.rmjtromp.trawler.response.ResponseReplayQuery.ByPipelineId &&
            selection.query.pipelineId != selection.pipeline.id
        ) {
            throw InvalidConfigurationException(
                listOf(
                    ConfigurationIssue(
                        path = "replay.query.pipelineId",
                        code = ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION,
                        message = "Replay query pipeline ID must match the selected pipeline",
                    ),
                ),
            )
        }
        val store = configuration.responseStore
        val replayStore = store as? ResponseReplayStore
            ?: throw InvalidConfigurationException(
                listOf(
                    ConfigurationIssue(
                        path = "engine.responseStore.replay",
                        code = ConfigurationIssueCode.UNSUPPORTED_CAPABILITY,
                        message = "Replay requires a selected response store implementing ResponseReplayStore",
                    ),
                ),
            )
        if (store.capabilities.contains(ResponseStoreCapability.REPLAY).not()) {
            throw InvalidConfigurationException(
                listOf(
                    ConfigurationIssue(
                        path = "engine.responseStore.capabilities.REPLAY",
                        code = ConfigurationIssueCode.UNSUPPORTED_CAPABILITY,
                        message = "Response store '${store.id}' does not advertise REPLAY",
                    ),
                ),
            )
        }
        require(selection.limit <= configuration.runtimeOptions.maxReplayResponsesPerRun) {
            "Replay limit exceeds the configured runtime bound"
        }
        val registration = configuration.pipelineRegistrations.firstOrNull {
            it.asPipeline() === selection.pipeline
        } ?: throw IllegalArgumentException(
            "Replay selection pipeline '${selection.pipeline.id}' is not registered in this engine",
        )
        validateReplayLifecycle(selection.pipeline, store)
        val replayLifecycle = beginReplayLifecycle()
        var replayFailure: Throwable? = null
        var stopReason = ExtensionStopReason.PIPELINE_RUN_COMPLETED
        try {
            val responses = replayStore.replay(
                query = selection.query,
                limit = selection.limit,
                pipelineId = selection.pipeline.id,
            )
            require(responses.size <= selection.limit) {
                "Response replay store returned more responses than requested"
            }
            val foreignResponse = responses.firstOrNull { it.pipelineId != selection.pipeline.id }
            if (foreignResponse != null) {
                throw InvalidConfigurationException(
                    listOf(
                        ConfigurationIssue(
                            path = "replay.responseStore.pipelineOwnership",
                            code = ConfigurationIssueCode.UNSUPPORTED_CAPABILITY,
                            message = "Response replay returned a response not owned by pipeline '${selection.pipeline.id}'",
                        ),
                    ),
                )
            }
            val startedAt = configuration.clock.now()
            val executionContext = ReplayExecutionContext(
                configuration = configuration,
                pipelineId = selection.pipeline.id,
                engineRunId = "replay-engine-${UUID.randomUUID()}",
                pipelineRunId = "replay-pipeline-${UUID.randomUUID()}",
                startedAt = startedAt,
                responseStore = store,
                emit = ::emit,
            )
            val result = registration.executeReplay(executionContext, responses)
            stopReason = when (result.status) {
                RunStatus.CANCELLED -> ExtensionStopReason.CANCELLATION
                RunStatus.FAILED -> ExtensionStopReason.PIPELINE_RUN_FAILED
                RunStatus.ABANDONED -> ExtensionStopReason.PIPELINE_RUN_ABANDONED
                RunStatus.COMPLETED -> ExtensionStopReason.PIPELINE_RUN_COMPLETED
                RunStatus.RUNNING -> ExtensionStopReason.PIPELINE_RUN_FAILED
            }
            result
        } catch (cancellation: CancellationException) {
            replayFailure = cancellation
            stopReason = ExtensionStopReason.CANCELLATION
            throw cancellation
        } catch (error: Throwable) {
            replayFailure = error
            stopReason = ExtensionStopReason.PIPELINE_RUN_FAILED
            throw error
        } finally {
            try {
                endReplayLifecycle(replayLifecycle, stopReason)
            } catch (cleanupFailure: Throwable) {
                replayFailure?.addSuppressed(cleanupFailure) ?: throw cleanupFailure
            }
        }
    }

    private suspend fun beginReplayLifecycle(): ReplayLifecycleSession = lifecycleMutex.withLock {
        val candidates = configuration.extensions.filterIsInstance<ReplayLifecycle>()
        when (lifecycleState) {
            LifecycleState.RUNNING -> {
                require(candidates.all { candidate -> startedExtensions.any { it === candidate } }) {
                    "Replay lifecycle extensions are not all started"
                }
                ReplayLifecycleSession(temporary = false, started = emptyList())
            }

            LifecycleState.NEW -> {
                lifecycleState = LifecycleState.REPLAYING
                val started = mutableListOf<ReplayLifecycle>()
                try {
                    candidates.forEach { extension ->
                        extension.startForReplay(ExtensionStartContext(lifecycleScope))
                        started += extension
                    }
                    startedExtensions = started.toList()
                    ReplayLifecycleSession(temporary = true, started = started.toList())
                } catch (error: Throwable) {
                    withContext(NonCancellable) {
                        started.asReversed().forEach { extension ->
                            boundedCleanup("replay extension ${extension.id} startup rollback", configuration.runtimeOptions.shutdownTimeout.toMillis().coerceAtLeast(1)) {
                                extension.stopForReplay(ExtensionStopContext(ExtensionStopReason.STARTUP_FAILURE))
                            }?.let(error::addSuppressed)
                        }
                    }
                    startedExtensions = emptyList()
                    lifecycleState = LifecycleState.NEW
                    throw error
                }
            }

            LifecycleState.STARTING,
            LifecycleState.REPLAYING,
            -> throw IllegalStateException("Trawler engine is already starting or replaying")

            LifecycleState.FAILED -> throw IllegalStateException("Trawler engine startup previously failed")
            LifecycleState.SHUTTING_DOWN,
            LifecycleState.STOPPED,
            -> throw IllegalStateException("Trawler engine is shutting down or stopped")
        }
    }

    private suspend fun endReplayLifecycle(
        session: ReplayLifecycleSession,
        reason: ExtensionStopReason,
    ) {
        if (!session.temporary) return
        withContext(NonCancellable) {
            lifecycleMutex.withLock {
                if (lifecycleState != LifecycleState.REPLAYING) return@withLock
                var firstFailure: Throwable? = null
                session.started.asReversed().forEach { extension ->
                    boundedCleanup(
                        "replay extension ${extension.id} shutdown",
                        configuration.runtimeOptions.shutdownTimeout.toMillis().coerceAtLeast(1),
                    ) {
                        extension.stopForReplay(ExtensionStopContext(reason))
                    }?.let { failure -> if (firstFailure == null) firstFailure = failure }
                }
                startedExtensions = emptyList()
                lifecycleState = LifecycleState.NEW
                firstFailure?.let { throw it }
            }
        }
    }

    private fun validateReplayLifecycle(pipeline: Pipeline<*>, store: ResponseStore) {
        val selected = buildList {
            if (store is ExtensionLifecycle) add("engine.responseStore" to store)
            pipeline.outputs.forEach { binding ->
                if (binding.output is ExtensionLifecycle) add("pipeline[${pipeline.id}].output[${binding.output.id}]" to binding.output)
            }
        }
        val issues = selected.flatMap { (path, extension) ->
            buildList {
                if (extension !is ReplayLifecycle) {
                    add(
                        ConfigurationIssue(
                            path = "$path.replayLifecycle",
                            code = ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION,
                            message = "Replay-selected lifecycle '${extension.id}' must implement ReplayLifecycle",
                        ),
                    )
                }
                if (configuration.extensions.none { it === extension }) {
                    add(
                        ConfigurationIssue(
                            path = "$path.registration",
                            code = ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION,
                            message = "Replay-selected lifecycle '${extension.id}' must be registered as an engine extension",
                        ),
                    )
                }
            }
        }
        val pipelineExtensionIssues = pipeline.extensions.flatMap { extension ->
            if (extension is ReplayLifecycle) {
                emptyList()
            } else {
                listOf(
                    ConfigurationIssue(
                        path = "pipeline[${pipeline.id}].extension[${extension.id}].replayLifecycle",
                        code = ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION,
                        message = "Replay-selected lifecycle '${extension.id}' must implement ReplayLifecycle",
                    ),
                )
            }
        }
        val allIssues = issues + pipelineExtensionIssues
        if (allIssues.isNotEmpty()) throw InvalidConfigurationException(allIssues)
    }

    private data class ReplayLifecycleSession(
        val temporary: Boolean,
        val started: List<ReplayLifecycle>,
    )

    suspend fun awaitIdle(timeout: Duration): Boolean {
        require(!timeout.isNegative && !timeout.isZero) { "Idle timeout must be positive" }
        val timeoutMillis = timeout.toMillis().coerceAtLeast(1)
        val idle = withTimeoutOrNull<Boolean>(timeoutMillis) {
            while (true) {
                val runs = lifecycleMutex.withLock { activeRuns.filterNot(Job::isCompleted) }
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
        val shutdownState = lifecycleMutex.withLock {
            when (lifecycleState) {
                LifecycleState.STOPPED,
                LifecycleState.SHUTTING_DOWN,
                -> null

                LifecycleState.NEW,
                LifecycleState.RUNNING,
                LifecycleState.STARTING,
                LifecycleState.REPLAYING,
                LifecycleState.FAILED,
                -> {
                    val wasReplaying = lifecycleState == LifecycleState.REPLAYING
                    lifecycleState = LifecycleState.SHUTTING_DOWN
                    wasReplaying
                }
            }
        }
        if (shutdownState == null) {
            shutdownCompletion.await()
            return
        }
        val replaying = shutdownState

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
                        if (replaying && extension is ReplayLifecycle) {
                            extension.stopForReplay(ExtensionStopContext(ExtensionStopReason.ENGINE_SHUTDOWN))
                        } else {
                            extension.stop(ExtensionStopContext(ExtensionStopReason.ENGINE_SHUTDOWN))
                        }
                    }
                    if (firstFailure == null) firstFailure = failure
                }
                val stateLifecycle = configuration.stateStore as? ExtensionLifecycle
                if (stateLifecycle != null && extensions.none { it === stateLifecycle }) {
                    val failure = boundedCleanup(
                        "state store ${stateLifecycle.id} shutdown",
                        shutdownMillis,
                    ) {
                        stateLifecycle.stop(ExtensionStopContext(ExtensionStopReason.ENGINE_SHUTDOWN))
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
                (configuration.responseStore as? ResponseStoreLifecycle)?.let { responseStore ->
                    val failure = boundedCleanup("response store ${responseStore::class.simpleName ?: "store"} shutdown", shutdownMillis) {
                        responseStore.close()
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

    /** Runs [request] once no other run holds any pipeline it selects; runs over disjoint pipelines overlap. */
    private suspend fun runSerialized(request: RunRequest): RunResult {
        val registered = registeredPipelineIds()
        val selected = request.selectedPipelineIds
        val unknown = selected.orEmpty() - registered
        require(unknown.isEmpty()) { "Unknown pipeline IDs: ${unknown.sorted().joinToString()}" }
        return runGate.withPipelines(selected ?: registered) {
            val engineRunId = "engine-run-${UUID.randomUUID()}"
            activeRunTimeouts[engineRunId] = request.timeout ?: configuration.runtimeOptions.runTimeout
            try {
                executeRun(request, engineRunId)
            } finally {
                activeRunTimeouts.remove(engineRunId)
            }
        }
    }

    private suspend fun executeRun(request: RunRequest, engineRunId: String): RunResult {
        val available = configuration.pipelineRegistrations
        val selected = request.selectedPipelineIds
        ensureStarted()
        val registrations = available.filter { selected == null || it.pipelineId in selected }
        val startedAt = configuration.clock.now()
        val runHistory = (configuration.stateStore as? RunHistoryStore)?.takeIf {
            configuration.stateStore.capabilities.any {
                it == CrawlStateCapability.RUN_HISTORY ||
                    it == CrawlStateCapability.RUN_RECOVERY
            }
        }
        if (runHistory != null &&
            configuration.stateStore.capabilities.contains(
                CrawlStateCapability.RUN_RECOVERY,
            )
        ) {
            runHistory.recoverStaleRuns(
                now = startedAt,
                // An overlapping run may have a longer timeout than this one; it must not look stale.
                staleAfter = activeRunTimeouts.values.max().plus(configuration.runtimeOptions.shutdownTimeout),
            )
        }
        if (runHistory != null &&
            configuration.stateStore.capabilities.contains(
                CrawlStateCapability.RUN_HISTORY,
            )
        ) {
            runHistory.startEngineRun(engineRunId, startedAt, request.reason)
        }
        emit(
            CrawlerEventType.ENGINE_RUN_STARTED,
            EventContext(startedAt, engineRunId = engineRunId),
            request.reason,
        )

        return try {
            val timedResult = withTimeoutOrNull<RunResult>(
                (request.timeout ?: configuration.runtimeOptions.runTimeout).toMillis().coerceAtLeast(1),
            ) {
                val pipelineRuns = supervisorScope {
                    registrations.map { registration ->
                        async {
                            val pipelineRunId = "pipeline-run-${UUID.randomUUID()}"
                            var durableStart = false
                            try {
                                if (runHistory != null && configuration.stateStore.capabilities.contains(
                                        CrawlStateCapability.RUN_HISTORY,
                                    )
                                ) {
                                    runHistory.startPipelineRun(
                                        engineRunId = engineRunId,
                                        pipelineRunId = pipelineRunId,
                                        pipelineId = registration.pipelineId,
                                        startedAt = startedAt,
                                    )
                                    durableStart = true
                                }
                                val pipelineResult = try {
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
                                        pipelineRunId = pipelineRunId,
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
                                if (durableStart) {
                                    runHistory?.completePipelineRun(
                                        pipelineRunId = pipelineRunId,
                                        status = pipelineResult.toDurableStatus(),
                                        completedAt = pipelineResult.completedAt,
                                        failureCount = pipelineResult.failures.size.toLong(),
                                    )
                                }
                                pipelineResult
                            } catch (cancelled: CancellationException) {
                                if (durableStart) {
                                    withContext(NonCancellable) {
                                        runHistory?.completePipelineRun(
                                            pipelineRunId = pipelineRunId,
                                            status = DurableRunStatus.CANCELLED,
                                            completedAt = configuration.clock.now(),
                                            failureCount = 0,
                                        )
                                    }
                                }
                                throw cancelled
                            } catch (error: Throwable) {
                                val now = configuration.clock.now()
                                if (durableStart) {
                                    try {
                                        runHistory?.completePipelineRun(
                                            pipelineRunId = pipelineRunId,
                                            status = DurableRunStatus.FAILED,
                                            completedAt = now,
                                            failureCount = 1,
                                        )
                                    } catch (completionFailure: Throwable) {
                                        error.addSuppressed(completionFailure)
                                    }
                                }
                                PipelineRunResult(
                                    pipelineId = registration.pipelineId,
                                    pipelineRunId = pipelineRunId,
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
                if (runHistory != null && configuration.stateStore.capabilities.contains(
                        CrawlStateCapability.RUN_HISTORY,
                    )
                ) {
                    runHistory.completeEngineRun(
                        engineRunId = engineRunId,
                        status = result.toDurableStatus(),
                        completedAt = result.completedAt,
                        failureCount = result.failures.size.toLong(),
                    )
                }
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
                if (runHistory != null && configuration.stateStore.capabilities.contains(
                        CrawlStateCapability.RUN_HISTORY,
                    )
                ) {
                    runHistory.completeEngineRun(
                        engineRunId = engineRunId,
                        status = DurableRunStatus.ABANDONED,
                        completedAt = completedAt,
                        failureCount = 1,
                    )
                }
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
                if (runHistory != null && configuration.stateStore.capabilities.contains(
                        CrawlStateCapability.RUN_HISTORY,
                    )
                ) {
                    try {
                        runHistory.completeEngineRun(
                            engineRunId = engineRunId,
                            status = DurableRunStatus.CANCELLED,
                            completedAt = now,
                            failureCount = 0,
                        )
                    } catch (completionFailure: Throwable) {
                        cancelled.addSuppressed(completionFailure)
                    }
                }
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
                LifecycleState.REPLAYING -> throw IllegalStateException("Trawler engine is replaying stored responses")
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
            LifecycleState.REPLAYING,
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
        REPLAYING,
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

/** Shared typed context for ordinary execution and network-free replay. */
internal interface TypedPipelineExecutionContext {
    val configuration: EngineConfiguration
    val pipelineId: String
    val engineRunId: String
    val pipelineRunId: String
    val startedAt: Instant
    val responseStore: ResponseStore?
    val emit: suspend (CrawlerEventType, EventContext, String?) -> Unit

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

/** Typed runtime context captured by one internal registration owner. */
internal class PipelineExecutionContext(
    override val configuration: EngineConfiguration,
    override val pipelineId: String,
    override val engineRunId: String,
    override val pipelineRunId: String,
    override val startedAt: Instant,
    val workQueue: PipelineScopedWorkQueueStore,
    val leaseStore: LeaseStore,
    val attemptStore: AttemptStore,
    val historyStore: ResourceHistoryStore,
    val cancellationStore: WorkCancellationStore,
    val orderingStore: WorkOrderingStore,
    val workStatus: WorkQueueStatus,
    override val responseStore: ResponseStore?,
    override val emit: suspend (CrawlerEventType, EventContext, String?) -> Unit,
) : TypedPipelineExecutionContext

/** Replay context intentionally exposes no crawl-state capability. */
internal class ReplayExecutionContext(
    override val configuration: EngineConfiguration,
    override val pipelineId: String,
    override val engineRunId: String,
    override val pipelineRunId: String,
    override val startedAt: Instant,
    override val responseStore: ResponseStore?,
    override val emit: suspend (CrawlerEventType, EventContext, String?) -> Unit,
) : TypedPipelineExecutionContext

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

    /** A terminal failure that is not a settled item failure; it always blocks harvest progress. */
    val blockingFailure = AtomicBoolean(false)

    /** Work items that ended in a permanent item-level failure this run. */
    val settledItemFailures = AtomicLong(0)
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
        if (terminal) {
            terminalFailure.set(true)
            blockingFailure.set(true)
        }
        failureMutex.withLock {
            if (failures.size < maxFailureRecords) failures += RunFailure(stage, error)
        }
    }

    suspend fun failureSnapshot(): List<RunFailure> = failureMutex.withLock { failures.toList() }

    fun snapshot(startedAt: Instant, clock: com.rmjtromp.trawler.policy.Clock): BudgetSnapshot {
        val elapsed = Duration.between(startedAt, clock.now())
            .let { if (it.isNegative) Duration.ZERO else it }
        return BudgetSnapshot(
            requestsStarted = requestsStarted.get(),
            producedValues = producedValues.get(),
            bytesRead = bytesRead.get(),
            failures = failuresCount.get(),
            discoveryEmissions = discoveryEmissions.get(),
            elapsed = elapsed,
        )
    }
}

private suspend fun <T : Any> observeBudget(
    pipeline: Pipeline<T>,
    context: TypedPipelineExecutionContext,
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

private sealed interface WorkAcceptance {
    data class Accepted(
        val item: DiscoveryWorkItem,
        val result: com.rmjtromp.trawler.state.EnqueueResult.Accepted,
    ) : WorkAcceptance

    data class AlreadyActive(
        val item: DiscoveryWorkItem,
        val result: com.rmjtromp.trawler.state.EnqueueResult.AlreadyActive,
    ) : WorkAcceptance

    data object Suppressed : WorkAcceptance

    /** Not admitted now but still owed a visit; progress must not pass it. */
    data object Deferred : WorkAcceptance

    data object Rejected : WorkAcceptance
}

private class DiscoveryProgress(
    val discoveryId: String,
    val resumable: ResumableDiscovery?,
    val checkpointStore: DiscoveryCheckpointStore?,
    val quarantineStore: CheckpointQuarantineStore?,
    var checkpoint: DiscoveryCheckpoint? = null,
) {
    var reachedNaturalEnd: Boolean = false
    var deferred: Boolean = false
    var failed: Boolean = false
    var incrementalWindow: com.rmjtromp.trawler.policy.IncrementalBoundaryPolicy.Window? = null

    /** Set when an emission of this walk was rejected or deferred; later checkpoint markers must not skip it. */
    var emissionHeldBack: Boolean = false
}

/**
 * Applies a [DiscoveryEmission.Checkpoint] marker. Earlier emissions were
 * handled sequentially, so advancing here cannot skip accepted work. A marker
 * after a rejected or recrawl-deferred emission is not applied, so the
 * resumed walk revisits that item; a compare-and-set conflict reloads progress and abandons the
 * run exactly like an item-attached checkpoint.
 */
private suspend fun advanceCheckpointMarker(
    context: PipelineExecutionContext,
    counters: PipelineCounters,
    candidate: com.rmjtromp.trawler.discovery.DiscoveryCheckpointCandidate,
    progress: DiscoveryProgress,
) {
    val options = context.configuration.runtimeOptions
    require(candidate.payload.size <= options.maxCheckpointPayloadBytes) {
        "Discovery checkpoint exceeds the configured payload bound"
    }
    val resumable = progress.resumable
        ?: throw IllegalStateException("Checkpoint marker requires a resumable discovery")
    require(candidate.descriptor == resumable.checkpointCodec.descriptor) {
        "Discovery checkpoint candidate codec does not match its discovery codec"
    }
    require(candidate.strategyId == resumable.checkpointCodec.strategyId) {
        "Discovery checkpoint candidate strategy does not match its discovery codec"
    }
    if (progress.emissionHeldBack) {
        counters.abandoned.set(true)
        context.event(
            CrawlerEventType.CAPABILITY_WARNING,
            detail = "checkpoint-marker-after-held-back-emission:${progress.discoveryId}",
        )
        return
    }
    val store = progress.checkpointStore
        ?: throw IllegalStateException("Checkpoint store disappeared during discovery")
    val next = DiscoveryCheckpoint(
        revision = (progress.checkpoint?.revision ?: -1L) + 1L,
        codecId = candidate.descriptor.id,
        codecVersion = candidate.descriptor.version,
        payload = candidate.payload,
        generationId = candidate.generationId,
        strategyId = candidate.strategyId,
        completed = false,
    )
    if (store.compareAndSet(context.pipelineId, progress.discoveryId, progress.checkpoint?.revision, next)) {
        progress.checkpoint = next
        context.event(CrawlerEventType.DISCOVERY_CHECKPOINTED, detail = progress.discoveryId)
    } else {
        counters.abandoned.set(true)
        progress.checkpoint = store.load(context.pipelineId, progress.discoveryId)
        context.event(
            CrawlerEventType.CAPABILITY_WARNING,
            detail = "checkpoint-revision-conflict:${progress.discoveryId}",
        )
    }
}

internal suspend fun <T : Any> executeTypedReplay(
    pipeline: Pipeline<T>,
    context: ReplayExecutionContext,
    responses: List<com.rmjtromp.trawler.response.StoredResponse>,
): ReplayResult {
    val startedAt = context.configuration.clock.now()
    val counters = PipelineCounters(context.configuration.runtimeOptions.maxQueueSize)
    val pipelineExtensions = mutableListOf<ReplayLifecycle>()
    var cancelled = false
    var lastReference: com.rmjtromp.trawler.model.ResponseReference? = null
    context.event(CrawlerEventType.PIPELINE_RUN_STARTED, detail = "replay")
    try {
        pipeline.extensions.filterIsInstance<ReplayLifecycle>()
            .filterNot { extension -> context.configuration.extensions.any { it === extension } }
            .forEach { extension ->
            extension.startForReplay(ExtensionStartContext(CoroutineScope(currentCoroutineContext())))
            pipelineExtensions += extension
        }
        for (stored in responses) {
            val requestId = stored.response.request.attributes["trawler.request-id"]
            val replayResponse = stored.response.withRuntimeAttributes(context, requestId)
            val crawler = CrawlerContext(
                engineRunId = context.engineRunId,
                pipelineRunId = context.pipelineRunId,
                pipelineId = context.pipelineId,
                requestId = requestId,
            )
            try {
                processTypedResponse(
                    pipeline = pipeline,
                    context = context,
                    counters = counters,
                    currentResponse = replayResponse,
                    crawler = crawler,
                    work = null,
                    storedReference = stored.reference,
                )
                lastReference = stored.reference
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (budget: BudgetReached) {
                counters.abandoned.set(true)
                counters.failure("replay-budget", budget, terminal = false)
                break
            } catch (failure: StageFailure) {
                lastReference = stored.reference
                counters.failure("replay:${failure.stage}", failure.original, terminal = true)
            } catch (error: Throwable) {
                lastReference = stored.reference
                counters.failure("replay", error, terminal = true)
            }
        }
    } catch (cancellation: CancellationException) {
        cancelled = true
        throw cancellation
    } finally {
        withContext(NonCancellable) {
            val cleanupTimeout = context.configuration.runtimeOptions.shutdownTimeout.toMillis().coerceAtLeast(1)
            pipelineExtensions.asReversed().forEach { extension ->
                boundedCleanup("replay extension ${extension.id} shutdown", cleanupTimeout) {
                    extension.stopForReplay(
                        ExtensionStopContext(
                            when {
                                cancelled -> ExtensionStopReason.CANCELLATION
                                counters.terminalFailure.get() -> ExtensionStopReason.PIPELINE_RUN_FAILED
                                counters.abandoned.get() -> ExtensionStopReason.PIPELINE_RUN_ABANDONED
                                else -> ExtensionStopReason.PIPELINE_RUN_COMPLETED
                            },
                        ),
                    )
                }?.let { error -> counters.failure("replay-extension-stop:${extension.id}", error, true) }
            }
        }
    }
    val status = when {
        counters.terminalFailure.get() -> RunStatus.FAILED
        counters.abandoned.get() -> RunStatus.ABANDONED
        else -> RunStatus.COMPLETED
    }
    val completedAt = context.configuration.clock.now()
    val result = ReplayResult(
        pipelineId = context.pipelineId,
        responsesRead = responses.size.toLong(),
        producedValues = counters.producedValues.get(),
        deliveredValues = counters.deliveredValues.get(),
        status = status,
        failures = counters.failureSnapshot(),
        startedAt = startedAt,
        completedAt = completedAt,
        lastReference = lastReference,
    )
    context.event(
        when (status) {
            RunStatus.COMPLETED -> CrawlerEventType.PIPELINE_RUN_COMPLETED
            RunStatus.ABANDONED -> CrawlerEventType.PIPELINE_RUN_ABANDONED
            RunStatus.FAILED -> CrawlerEventType.PIPELINE_RUN_FAILED
            RunStatus.CANCELLED -> CrawlerEventType.PIPELINE_RUN_CANCELLED
            RunStatus.RUNNING -> CrawlerEventType.PIPELINE_RUN_FAILED
        },
        detail = "replay:${status.name}",
    )
    return result
}

internal suspend fun <T : Any> executeTypedPipeline(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
): PipelineRunResult {
    val startedAt = context.configuration.clock.now()
    val counters = PipelineCounters(context.configuration.runtimeOptions.maxQueueSize)
    val responses = LinkedHashMap<WorkItemId, CrawlResponse>()
    val responseMutex = Mutex()
    val pipelineExtensions = mutableListOf<com.rmjtromp.trawler.extension.ExtensionLifecycle>()
    val discoveryProgress = mutableListOf<DiscoveryProgress>()
    var discoveryBudgetStopped = false
    var cancelled = false

    context.event(CrawlerEventType.PIPELINE_RUN_STARTED)
    try {
        val state = context.configuration.stateStore
        val healthStore = state as? PipelineHealthStore
        val overrideStore = state as? PipelineEligibilityOverrideStore
        val health = healthStore?.loadPipelineHealth(pipeline.id) ?: PipelineHealthSnapshot()
        val now = context.configuration.clock.now()
        val override = overrideStore?.loadEligibilityOverride(pipeline.id)
            ?.takeIf { it.expiresAt?.isAfter(now) != false }
        val eligibility = pipeline.policies.eligibility.decide(
            PipelineEligibilityContext(pipeline.id, now, health, override),
        )
        val circuit = if (eligibility == PipelineEligibilityDecision.Eligible) {
            pipeline.policies.circuitBreaker.decide(health, context.configuration.clock.now())
        } else {
            CircuitBreakerDecision.Closed
        }
        val blockedReason = when (eligibility) {
            PipelineEligibilityDecision.Eligible -> when (circuit) {
                CircuitBreakerDecision.Closed -> null
                is CircuitBreakerDecision.Open -> "circuit-open:${circuit.reason}"
            }

            is PipelineEligibilityDecision.Deferred ->
                "pipeline-deferred:${eligibility.reason}"

            is PipelineEligibilityDecision.Disabled ->
                "pipeline-disabled:${eligibility.reason}"
        }
        if (blockedReason != null) {
            counters.abandoned.set(true)
            context.event(CrawlerEventType.CAPABILITY_WARNING, detail = blockedReason)
        } else {
            context.orderingStore.setOrdering(pipeline.id, pipeline.policies.ordering)
            pipeline.extensions.forEach { extension ->
                extension.start(ExtensionStartContext(CoroutineScope(currentCoroutineContext())))
                pipelineExtensions += extension
            }

            val harvestState = loadHarvestState(pipeline, context)

            val accept: suspend (CrawlRequest, CrawlResponse?) -> Unit = { request, existingResponse ->
                acceptWork(pipeline, context, counters, responses, responseMutex, request, existingResponse)
            }

            for (discovery in pipeline.discoveries) {
                if (discoveryBudgetStopped) break
                context.event(CrawlerEventType.DISCOVERY_STARTED, detail = discovery.id)
                val progress = try {
                    loadDiscoveryProgress(pipeline, context, discovery)
                } catch (error: Throwable) {
                    counters.failure("discovery-checkpoint:${discovery.id}", error, terminal = true)
                    context.event(CrawlerEventType.DISCOVERY_FAILED, detail = discovery.id)
                    continue
                }
                discoveryProgress += progress
                try {
                    val openMetrics = queueMetrics(context)
                    val harvestContext = HarvestContext(
                        pipelineId = pipeline.id,
                        discoveryId = discovery.id,
                        checkpoint = progress.checkpoint,
                        state = harvestState,
                        openWorkCount = openMetrics.openWorkCount,
                        now = context.configuration.clock.now(),
                    )
                    val harvestWindow = pipeline.harvestStrategy.window(harvestContext)
                    progress.incrementalWindow = harvestWindow
                    val harvestAdmission = pipeline.harvestStrategy.admit(
                        harvestContext.copy(incrementalWindow = harvestWindow),
                    )
                    when (harvestAdmission) {
                        is com.rmjtromp.trawler.discovery.HarvestAdmission.Defer -> {
                            progress.deferred = true
                            counters.abandoned.set(true)
                            context.event(
                                CrawlerEventType.CAPABILITY_WARNING,
                                detail = "discovery-deferred:${harvestAdmission.reason}",
                            )
                            continue
                        }

                        com.rmjtromp.trawler.discovery.HarvestAdmission.Run -> Unit
                    }
                    when (val admission = pipeline.policies.admission.decide(
                        DiscoveryAdmissionContext(
                            pipelineId = pipeline.id,
                            discoveryId = discovery.id,
                            openWorkCount = openMetrics.openWorkCount,
                            oldestOpenWorkAt = openMetrics.oldestOpenWorkAt,
                            checkpoint = progress.checkpoint,
                            harvestState = harvestState,
                            budget = counters.snapshot(context.startedAt, context.configuration.clock),
                            health = health,
                            now = context.configuration.clock.now(),
                        ),
                    )) {
                        DiscoveryAdmissionDecision.Admit -> Unit
                        is DiscoveryAdmissionDecision.Defer -> {
                            progress.deferred = true
                            counters.abandoned.set(true)
                            context.event(
                                CrawlerEventType.CAPABILITY_WARNING,
                                detail = "discovery-deferred:${admission.reason}",
                            )
                            continue
                        }
                    }
                    val discoveryCrawler = CrawlerContext(
                        engineRunId = context.engineRunId,
                        pipelineRunId = context.pipelineRunId,
                        pipelineId = context.pipelineId,
                    )
                    val executor = RequestExecutor { request, requestContext ->
                        executeDiscoveryRequest(pipeline, context, request, requestContext)
                    }
                    var emissionsForDiscovery = 0
                    discovery.discover(
                        DiscoveryContext(
                            crawler = discoveryCrawler,
                            executor = executor,
                            checkpoint = progress.checkpoint,
                            incrementalWindow = harvestWindow,
                        ),
                    )
                        .collect { emission ->
                            if (emissionsForDiscovery >=
                                context.configuration.runtimeOptions.maxDiscoveryEmissionsPerDiscovery
                            ) {
                                throw BudgetReached("discovery-per-discovery-bound")
                            }
                            emissionsForDiscovery++
                            val (request, existingResponse) = when (emission) {
                                is DiscoveryEmission.Checkpoint -> {
                                    advanceCheckpointMarker(context, counters, emission.checkpoint, progress)
                                    return@collect
                                }

                                is DiscoveryEmission.Request -> emission.request to null
                                is DiscoveryEmission.ExistingResponse -> emission.request to emission.response
                            }
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
                            val acceptance = acceptWork(
                                pipeline,
                                context,
                                counters,
                                responses,
                                responseMutex,
                                request,
                                existingResponse,
                                emission.checkpoint,
                                progress,
                            )
                            if (acceptance == WorkAcceptance.Rejected || acceptance == WorkAcceptance.Deferred) {
                                progress.emissionHeldBack = true
                            }
                        }
                    progress.reachedNaturalEnd = true
                    context.event(CrawlerEventType.DISCOVERY_COMPLETED, detail = discovery.id)
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (budget: BudgetReached) {
                    counters.abandoned.set(true)
                    discoveryBudgetStopped = true
                    context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "budget:${budget.reason}")
                } catch (error: Throwable) {
                    progress.failed = true
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
            val openWork = context.workStatus.activeWorkCount(context.pipelineId)
            if (openWork > 0) {
                counters.abandoned.set(true)
                context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "resumable-work-remains")
            }
            finalizeDiscoveryProgress(
                pipeline,
                context,
                discoveryProgress,
                counters,
                openWork,
                harvestState,
            )
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
    recordPipelineHealth(pipeline, context, result)
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

private suspend fun <T : Any> loadDiscoveryProgress(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    discovery: com.rmjtromp.trawler.discovery.Discovery,
): DiscoveryProgress {
    val resumable = discovery as? ResumableDiscovery
        ?: return DiscoveryProgress(discovery.id, null, null, null)
    val state = context.configuration.stateStore
    val checkpointStore = state as? DiscoveryCheckpointStore
        ?: throw IllegalStateException(
            "Resumable discovery '${discovery.id}' requires a discovery checkpoint store",
        )
    val quarantineStore = state as? CheckpointQuarantineStore
    var checkpoint = checkpointStore.load(pipeline.id, discovery.id)
    if (checkpoint != null) {
        check(checkpoint.payload.size <= context.configuration.runtimeOptions.maxCheckpointPayloadBytes) {
            "Discovery checkpoint exceeds the configured payload bound"
        }
        val current = checkpoint
        when (val resolution = resumable.checkpointCodec.resolve(current)) {
            is com.rmjtromp.trawler.state.CheckpointResolution.Use -> {
                check(resolution.checkpoint.codecId == resumable.checkpointCodec.descriptor.id &&
                    resolution.checkpoint.codecVersion == resumable.checkpointCodec.descriptor.version &&
                    resolution.checkpoint.strategyId == resumable.checkpointCodec.strategyId
                ) { "Checkpoint codec returned an incompatible usable state" }
                check(resolution.checkpoint.payload.size <=
                    context.configuration.runtimeOptions.maxCheckpointPayloadBytes
                ) { "Resolved discovery checkpoint exceeds the configured payload bound" }
                checkpoint = resolution.checkpoint
            }
            is com.rmjtromp.trawler.state.CheckpointResolution.Migrated -> {
                check(resolution.checkpoint.payload.size <=
                    context.configuration.runtimeOptions.maxCheckpointPayloadBytes
                ) { "Migrated discovery checkpoint exceeds the configured payload bound" }
                val migrated = resolution.checkpoint.copy(
                    revision = current.revision + 1,
                    codecId = resumable.checkpointCodec.descriptor.id,
                    codecVersion = resumable.checkpointCodec.descriptor.version,
                    strategyId = resumable.checkpointCodec.strategyId,
                    completed = false,
                )
                check(checkpointStore.compareAndSet(
                    pipeline.id,
                    discovery.id,
                    current.revision,
                    migrated,
                )) { "Checkpoint migration lost a compare-and-set race" }
                checkpoint = migrated
                context.event(CrawlerEventType.DISCOVERY_CHECKPOINTED, detail = "migrated:${discovery.id}")
            }

            is com.rmjtromp.trawler.state.CheckpointResolution.Reset -> {
                val reset = resetCheckpoint(current, resumable)
                check(checkpointStore.compareAndSet(
                    pipeline.id,
                    discovery.id,
                    current.revision,
                    reset,
                )) { "Checkpoint reset lost a compare-and-set race" }
                checkpoint = reset
                context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "checkpoint-reset:${resolution.reason}")
            }

            is com.rmjtromp.trawler.state.CheckpointResolution.Quarantine -> {
                val quarantine = quarantineStore
                    ?: throw IllegalStateException(
                        "Checkpoint codec mismatch requires a quarantine-capable state store",
                    )
                quarantine.quarantine(pipeline.id, discovery.id, current, resolution.reason)
                val reset = resetCheckpoint(current, resumable)
                check(checkpointStore.compareAndSet(
                    pipeline.id,
                    discovery.id,
                    current.revision,
                    reset,
                )) { "Checkpoint quarantine reset lost a compare-and-set race" }
                checkpoint = reset
                context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "checkpoint-quarantined:${resolution.reason}")
            }
        }
    }
    return DiscoveryProgress(discovery.id, resumable, checkpointStore, quarantineStore, checkpoint)
}

private fun resetCheckpoint(
    previous: DiscoveryCheckpoint,
    discovery: ResumableDiscovery,
): DiscoveryCheckpoint = DiscoveryCheckpoint(
    revision = previous.revision + 1,
    codecId = discovery.checkpointCodec.descriptor.id,
    codecVersion = discovery.checkpointCodec.descriptor.version,
    payload = CheckpointPayload.from(ByteArray(0)),
    generationId = null,
    strategyId = discovery.checkpointCodec.strategyId,
    completed = false,
)

private suspend fun queueMetrics(context: PipelineExecutionContext): WorkQueueMetrics {
    val metricsStore = context.configuration.stateStore as? WorkQueueMetricsStore
    return metricsStore?.metrics(context.pipelineId)
        ?: WorkQueueMetrics(context.workStatus.activeWorkCount(context.pipelineId), null)
}

private suspend fun <T : Any> loadHarvestState(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
): HarvestState? {
    val codec = pipeline.harvestStrategy.stateCodec ?: return null
    val store = context.configuration.stateStore as? HarvestStateStore
        ?: throw IllegalStateException(
            "Harvest strategy '${pipeline.harvestStrategy.id}' requires harvest state storage",
        )
    val quarantineStore = context.configuration.stateStore as? HarvestStateQuarantineStore
    var state = store.loadHarvestState(pipeline.id, pipeline.harvestStrategy.id)
    if (state == null) return null
    check(state.payload.size <= context.configuration.runtimeOptions.maxCheckpointPayloadBytes) {
        "Harvest state exceeds the configured payload bound"
    }
    when (val resolution = codec.resolve(state)) {
        is HarvestStateResolution.Use -> {
            check(resolution.state.strategyId == codec.strategyId &&
                resolution.state.codecId == codec.descriptor.id &&
                resolution.state.codecVersion == codec.descriptor.version
            ) { "Harvest-state codec returned an incompatible usable state" }
            check(resolution.state.payload.size <=
                context.configuration.runtimeOptions.maxCheckpointPayloadBytes
            ) { "Resolved harvest state exceeds the configured payload bound" }
            state = resolution.state
        }
        is HarvestStateResolution.Migrated -> {
            check(resolution.state.payload.size <=
                context.configuration.runtimeOptions.maxCheckpointPayloadBytes
            ) { "Migrated harvest state exceeds the configured payload bound" }
            val migrated = resolution.state.copy(
                revision = state.revision + 1,
                strategyId = codec.strategyId,
                codecId = codec.descriptor.id,
                codecVersion = codec.descriptor.version,
            )
            check(store.compareAndSetHarvestState(
                pipeline.id,
                pipeline.harvestStrategy.id,
                state.revision,
                migrated,
            )) { "Harvest-state migration lost a compare-and-set race" }
            state = migrated
            context.event(CrawlerEventType.DISCOVERY_CHECKPOINTED, detail = "harvest-state-migrated")
        }

        is HarvestStateResolution.Reset -> {
            val reset = HarvestState(
                revision = state.revision + 1,
                strategyId = codec.strategyId,
                codecId = codec.descriptor.id,
                codecVersion = codec.descriptor.version,
                payload = CheckpointPayload.from(ByteArray(0)),
                historicalComplete = false,
            )
            check(store.compareAndSetHarvestState(
                pipeline.id,
                pipeline.harvestStrategy.id,
                state.revision,
                reset,
            )) { "Harvest-state reset lost a compare-and-set race" }
            state = reset
            context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "harvest-state-reset:${resolution.reason}")
        }

        is HarvestStateResolution.Quarantine -> {
            val quarantine = quarantineStore
                ?: throw IllegalStateException(
                    "Harvest-state codec mismatch requires a quarantine-capable state store",
                )
            quarantine.quarantineHarvestState(
                pipeline.id,
                pipeline.harvestStrategy.id,
                state,
                resolution.reason,
            )
            val reset = HarvestState(
                revision = state.revision + 1,
                strategyId = codec.strategyId,
                codecId = codec.descriptor.id,
                codecVersion = codec.descriptor.version,
                payload = CheckpointPayload.from(ByteArray(0)),
                historicalComplete = false,
            )
            check(store.compareAndSetHarvestState(
                pipeline.id,
                pipeline.harvestStrategy.id,
                state.revision,
                reset,
            )) { "Harvest-state quarantine reset lost a compare-and-set race" }
            state = reset
            context.event(
                CrawlerEventType.CAPABILITY_WARNING,
                detail = "harvest-state-quarantined:${resolution.reason}",
            )
        }
    }
    return state
}

private suspend fun <T : Any> finalizeDiscoveryProgress(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    progress: List<DiscoveryProgress>,
    counters: PipelineCounters,
    openWork: Int,
    previousState: HarvestState?,
) {
    val settledItemFailures = counters.settledItemFailures.get()
    val outcome = when {
        counters.blockingFailure.get() -> HarvestRunOutcome.FAILED
        counters.abandoned.get() -> HarvestRunOutcome.ABANDONED
        settledItemFailures > 0 -> HarvestRunOutcome.COMPLETED_WITH_SETTLED_ITEM_FAILURES
        counters.terminalFailure.get() -> HarvestRunOutcome.FAILED
        else -> HarvestRunOutcome.COMPLETED
    }
    val resumable = progress.filter { it.resumable != null }
    if (resumable.isEmpty()) return
    val completion = pipeline.harvestStrategy.complete(
        HarvestCompletionContext(
            discoveryReachedNaturalEnd = resumable.all {
                it.reachedNaturalEnd && !it.deferred && !it.failed
            },
            checkpointWorkSettled = openWork == 0,
            generationWorkOpen = openWork,
            runOutcome = outcome,
            completionPredicatePassed = outcome == HarvestRunOutcome.COMPLETED,
            now = context.configuration.clock.now(),
            previousState = previousState,
            settledItemFailures = if (outcome == HarvestRunOutcome.COMPLETED_WITH_SETTLED_ITEM_FAILURES) {
                settledItemFailures
            } else {
                0
            },
            incrementalWindow = resumable.mapNotNull { it.incrementalWindow }.minByOrNull { it.endExclusive },
        ),
    )
    when (completion) {
        is HarvestCompletion.Incomplete -> context.event(
            CrawlerEventType.CAPABILITY_WARNING,
            detail = "harvest-incomplete:${completion.reason}",
        )

        is HarvestCompletion.Commit -> {
            val commitAllowed = (
                outcome == HarvestRunOutcome.COMPLETED ||
                    outcome == HarvestRunOutcome.COMPLETED_WITH_SETTLED_ITEM_FAILURES
                ) &&
                openWork == 0 &&
                resumable.all { it.reachedNaturalEnd && !it.deferred && !it.failed }
            if (!commitAllowed) {
                counters.abandoned.set(true)
                context.event(
                    CrawlerEventType.CAPABILITY_WARNING,
                    detail = "harvest-commit-rejected-by-engine",
                )
                return
            }

            val nextHarvestState: Pair<HarvestStateStore, HarvestState>? = completion.state?.let { candidate ->
                val codec = pipeline.harvestStrategy.stateCodec
                    ?: run {
                        counters.abandoned.set(true)
                        context.event(
                            CrawlerEventType.CAPABILITY_WARNING,
                            detail = "harvest-state-codec-missing",
                        )
                        return
                    }
                require(candidate.strategyId == codec.strategyId) {
                    "Harvest candidate strategy does not match its strategy codec"
                }
                require(candidate.codecId == codec.descriptor.id &&
                    candidate.codecVersion == codec.descriptor.version
                ) { "Harvest candidate codec does not match its strategy codec" }
                require(candidate.payload.size <= context.configuration.runtimeOptions.maxCheckpointPayloadBytes) {
                    "Harvest state exceeds the configured payload bound"
                }
                val store = context.configuration.stateStore as? HarvestStateStore
                    ?: run {
                        counters.abandoned.set(true)
                        context.event(
                            CrawlerEventType.CAPABILITY_WARNING,
                            detail = "harvest-state-store-missing",
                        )
                        return
                    }
                store to HarvestState(
                    revision = (previousState?.revision ?: -1L) + 1L,
                    strategyId = candidate.strategyId,
                    codecId = candidate.codecId,
                    codecVersion = candidate.codecVersion,
                    payload = candidate.payload,
                    historicalComplete = candidate.historicalComplete,
                )
            }

            var checkpointsCommitted = true
            resumable.forEach { currentProgress ->
                if (!checkpointsCommitted) return@forEach
                val checkpoint = currentProgress.checkpoint
                if (checkpoint == null || checkpoint.completed) return@forEach
                val completed = checkpoint.copy(
                    revision = checkpoint.revision + 1,
                    completed = true,
                )
                val advanced = currentProgress.checkpointStore?.compareAndSet(
                    context.pipelineId,
                    currentProgress.discoveryId,
                    checkpoint.revision,
                    completed,
                ) == true
                if (advanced) {
                    currentProgress.checkpoint = completed
                    context.event(
                        CrawlerEventType.DISCOVERY_CHECKPOINTED,
                        detail = "completed:${currentProgress.discoveryId}",
                    )
                } else {
                    checkpointsCommitted = false
                    counters.abandoned.set(true)
                    context.event(
                        CrawlerEventType.CAPABILITY_WARNING,
                        detail = "checkpoint-completion-conflict:${currentProgress.discoveryId}",
                    )
                }
            }
            if (!checkpointsCommitted) return

            nextHarvestState?.let { (store, next) ->
                if (!store.compareAndSetHarvestState(
                    context.pipelineId,
                    next.strategyId,
                    previousState?.revision,
                    next,
                )) {
                    counters.abandoned.set(true)
                    context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "harvest-state-conflict")
                }
            }
        }
    }
}

private suspend fun <T : Any> recordPipelineHealth(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    result: PipelineRunResult,
) {
    val store = context.configuration.stateStore as? PipelineHealthStore ?: return
    val stored = store.loadPipelineHealth(pipeline.id)
    val current = stored ?: PipelineHealthSnapshot()
    val outcome = when (result.status) {
        RunStatus.COMPLETED -> PipelineHealthOutcome.SUCCESS
        RunStatus.FAILED -> PipelineHealthOutcome.FAILURE
        RunStatus.ABANDONED -> PipelineHealthOutcome.ABANDONED
        RunStatus.CANCELLED -> PipelineHealthOutcome.CANCELLED
        RunStatus.RUNNING -> PipelineHealthOutcome.FAILURE
    }
    val next = pipeline.policies.health.observe(
        current,
        PipelineHealthObservation(outcome, result.completedAt),
    ).copy(revision = (stored?.revision ?: -1L) + 1L)
    if (!store.compareAndSetPipelineHealth(pipeline.id, stored?.revision, next)) {
        context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "pipeline-health-conflict")
    }
}

private suspend fun <T : Any> acceptWork(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    counters: PipelineCounters,
    responses: MutableMap<WorkItemId, CrawlResponse>,
    responseMutex: Mutex,
    request: CrawlRequest,
    existingResponse: CrawlResponse?,
    checkpointCandidate: DiscoveryCheckpointCandidate? = null,
    progress: DiscoveryProgress? = null,
): WorkAcceptance {
    val options = context.configuration.runtimeOptions
    val admissionBoundsFailure = runCatching {
        validateRequestBounds(request, options)
        existingResponse?.let { validateResponseBounds(it, options) }
    }.exceptionOrNull()
    if (admissionBoundsFailure != null) {
        counters.abandoned.set(true)
        context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "request-metadata-bound")
        return WorkAcceptance.Rejected
    }
    if (!counters.reserveWork(options.maxWorkItemsPerRun)) {
        counters.abandoned.set(true)
        context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "work-per-run-bound")
        return WorkAcceptance.Rejected
    }
    var accepted = false
    try {
        val crawler = CrawlerContext(context.engineRunId, context.pipelineRunId, context.pipelineId)
        val requestContext = RequestContext(crawler, request)
        val canonical = pipeline.policies.requestCanonicalizer.canonicalize(request, requestContext)
        if (runCatching { validateRequestBounds(canonical, options) }.isFailure) {
            counters.abandoned.set(true)
            context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "canonical-request-metadata-bound")
            return WorkAcceptance.Rejected
        }
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
                return WorkAcceptance.Deferred
            }

            is RecrawlDecision.Rejected -> {
                counters.suppressedWork.incrementAndGet()
                context.event(CrawlerEventType.REQUEST_FAILED, detail = "recrawl-rejected:${recrawl.reason}")
                return WorkAcceptance.Suppressed
            }
        }
        when (val scope = pipeline.policies.scope.evaluate(canonical, canonicalContext)) {
            ScopeDecision.Allowed -> Unit
            is ScopeDecision.Rejected -> {
                counters.suppressedWork.incrementAndGet()
                context.event(CrawlerEventType.REQUEST_FAILED, detail = "scope-rejected:${scope.reason}")
                return WorkAcceptance.Suppressed
            }
        }
        when (val dedup = pipeline.policies.deduplication.decide(canonical, identity, canonicalContext)) {
            DeduplicationDecision.Admit -> Unit
            is DeduplicationDecision.Suppress -> {
                counters.suppressedWork.incrementAndGet()
                context.event(CrawlerEventType.REQUEST_FAILED, detail = "deduplicated:${dedup.reason}")
                return WorkAcceptance.Suppressed
            }
        }
        val item = DiscoveryWorkItem(canonical, identity, crawler)
        val nextCheckpoint = checkpointCandidate?.let { candidate ->
            require(candidate.payload.size <= options.maxCheckpointPayloadBytes) {
                "Discovery checkpoint exceeds the configured payload bound"
            }
            val activeProgress = progress
                ?: throw IllegalStateException("Checkpoint candidate was emitted outside discovery")
            val resumable = activeProgress.resumable
                ?: throw IllegalStateException("Checkpoint candidate requires a resumable discovery")
            require(candidate.descriptor == resumable.checkpointCodec.descriptor) {
                "Discovery checkpoint candidate codec does not match its discovery codec"
            }
            require(candidate.strategyId == resumable.checkpointCodec.strategyId) {
                "Discovery checkpoint candidate strategy does not match its discovery codec"
            }
            DiscoveryCheckpoint(
                revision = (activeProgress.checkpoint?.revision ?: -1L) + 1L,
                codecId = candidate.descriptor.id,
                codecVersion = candidate.descriptor.version,
                payload = candidate.payload,
                generationId = candidate.generationId,
                strategyId = candidate.strategyId,
                completed = false,
            )
        }
        val enqueueResult = if (nextCheckpoint != null) {
            val activeProgress = requireNotNull(progress)
            val state = context.configuration.stateStore
            val atomic = state as? AtomicEnqueueAndCheckpointStore
            if (atomic != null) {
                when (val atomicResult = atomic.enqueueAndCheckpoint(
                    pipelineId = context.pipelineId,
                    discoveryId = activeProgress.discoveryId,
                    expectedRevision = activeProgress.checkpoint?.revision,
                    next = nextCheckpoint,
                    items = listOf(item),
                )) {
                    is AtomicEnqueueCheckpointResult.Advanced -> {
                        activeProgress.checkpoint = atomicResult.checkpoint
                        context.event(
                            CrawlerEventType.DISCOVERY_CHECKPOINTED,
                            detail = activeProgress.discoveryId,
                        )
                        atomicResult.enqueueResults.single()
                    }

                    AtomicEnqueueCheckpointResult.RevisionConflict -> {
                        counters.abandoned.set(true)
                        context.event(
                            CrawlerEventType.CAPABILITY_WARNING,
                            detail = "checkpoint-revision-conflict:${activeProgress.discoveryId}",
                        )
                        return WorkAcceptance.Rejected
                    }

                    is AtomicEnqueueCheckpointResult.Rejected -> {
                        counters.abandoned.set(true)
                        context.event(
                            CrawlerEventType.CAPABILITY_WARNING,
                            detail = "work-rejected:${atomicResult.reason}",
                        )
                        return WorkAcceptance.Rejected
                    }
                }
            } else {
                val result = context.workQueue.enqueue(canonical, identity, crawler)
                if (result is com.rmjtromp.trawler.state.EnqueueResult.Accepted ||
                    result is com.rmjtromp.trawler.state.EnqueueResult.AlreadyActive
                ) {
                    val store = activeProgress.checkpointStore
                        ?: throw IllegalStateException("Checkpoint store disappeared during discovery")
                    if (store.compareAndSet(
                            context.pipelineId,
                            activeProgress.discoveryId,
                            activeProgress.checkpoint?.revision,
                            nextCheckpoint,
                        )
                    ) {
                        activeProgress.checkpoint = nextCheckpoint
                        context.event(
                            CrawlerEventType.DISCOVERY_CHECKPOINTED,
                            detail = activeProgress.discoveryId,
                        )
                    } else {
                        counters.abandoned.set(true)
                        activeProgress.checkpoint = store.load(
                            context.pipelineId,
                            activeProgress.discoveryId,
                        )
                        context.event(
                            CrawlerEventType.CAPABILITY_WARNING,
                            detail = "checkpoint-revision-conflict:${activeProgress.discoveryId}",
                        )
                    }
                }
                result
            }
        } else {
            context.workQueue.enqueue(canonical, identity, crawler)
        }
        when (val result = enqueueResult) {
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
                return WorkAcceptance.Accepted(item, result)
            }

            is com.rmjtromp.trawler.state.EnqueueResult.AlreadyActive -> {
                counters.suppressedWork.incrementAndGet()
                context.event(CrawlerEventType.REQUEST_FAILED, detail = "active-duplicate")
                return WorkAcceptance.AlreadyActive(item, result)
            }

            is com.rmjtromp.trawler.state.EnqueueResult.Rejected -> {
                counters.abandoned.set(true)
                context.event(CrawlerEventType.CAPABILITY_WARNING, detail = "work-rejected:${result.reason}")
                return WorkAcceptance.Rejected
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
    val transportFailure = java.util.concurrent.atomic.AtomicReference<Throwable?>(null)
    when (val scope = pipeline.policies.scope.evaluate(work.request, RequestContext(baseCrawler, work.request))) {
        ScopeDecision.Allowed -> Unit
        is ScopeDecision.Rejected -> {
            if (recordLeaseMutationFailure(
                    context = context,
                    counters = counters,
                    work = work,
                    crawler = baseCrawler,
                    operation = "scope-rejection",
                    result = context.leaseStore.transition(work, WorkTransition.Skipped(scope.reason)),
                )
            ) {
                return
            }
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
            if (recordLeaseMutationFailure(
                    context = context,
                    counters = counters,
                    work = work,
                    crawler = baseCrawler,
                    operation = "authorization-denial",
                    result = context.leaseStore.transition(work, WorkTransition.Skipped(authorization.reason)),
                )
            ) {
                return
            }
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
        val executed = existing?.let { ExecutedResponse(it, null) }
            ?: executeWorkRequest(pipeline, context, work.request, crawler, transportFailure)
        val currentResponse = executed.response
        pipeline.policies.pacing.observe(currentResponse, RequestContext(crawler, work.request))
        val storedReference = executed.storedReference ?: if (context.responseStore != null) {
            ensureLeaseCurrent(context, work, leaseLost)
            storeResponse(context, currentResponse, crawler, work).also {
                ensureLeaseCurrent(context, work, leaseLost)
            }
        } else {
            null
        }
        ensureLeaseCurrent(context, work, leaseLost)
        processTypedResponse(
            pipeline = pipeline,
            context = context,
            counters = counters,
            currentResponse = currentResponse,
            crawler = crawler,
            work = work,
            storedReference = storedReference,
            beforeResponseAttachment = { ensureLeaseCurrent(context, work, leaseLost) },
            accept = accept,
        )
        if (recordLeaseMutationFailure(
                context = context,
                counters = counters,
                work = work,
                crawler = crawler,
                operation = "attempt-finish",
                result = context.attemptStore.finishAttempt(work, requireNotNull(attempt), AttemptOutcome.SUCCEEDED),
            )
        ) {
            return
        }
        if (recordLeaseMutationFailure(
                context = context,
                counters = counters,
                work = work,
                crawler = crawler,
                operation = "completion",
                result = context.leaseStore.transition(work, WorkTransition.Succeeded),
            )
        ) {
            return
        }
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
        val attemptFinished = attempt?.let {
            context.attemptStore.finishAttempt(work, it, AttemptOutcome.ABANDONED)
        }
        if (attemptFinished != null && recordLeaseMutationFailure(
                context = context,
                counters = counters,
                work = work,
                crawler = baseCrawler,
                operation = "budget-attempt-finish",
                result = attemptFinished,
            )
        ) {
            return
        }
        if (recordLeaseMutationFailure(
                context = context,
                counters = counters,
                work = work,
                crawler = baseCrawler,
                operation = "budget-abandonment",
                result = context.leaseStore.transition(work, WorkTransition.Abandoned(budget.reason)),
            )
        ) {
            return
        }
        counters.abandoned.set(true)
        context.event(CrawlerEventType.CAPABILITY_WARNING, baseCrawler, work, "budget:${budget.reason}")
    } catch (failure: StageFailure) {
        val attemptFinished = attempt?.let {
            context.attemptStore.finishAttempt(work, it, AttemptOutcome.FAILED)
        }
        if (attemptFinished != null && recordLeaseMutationFailure(
                context = context,
                counters = counters,
                work = work,
                crawler = baseCrawler,
                operation = "failure-attempt-finish",
                result = attemptFinished,
            )
        ) {
            return
        }
        handleWorkFailure(
            pipeline,
            context,
            counters,
            work,
            attempt,
            failure.stage,
            failure.original,
            baseCrawler,
            itemLevel = isItemLevelStage(failure.stage),
        )
    } catch (error: Throwable) {
        val attemptFinished = attempt?.let {
            context.attemptStore.finishAttempt(work, it, AttemptOutcome.FAILED)
        }
        if (attemptFinished != null && recordLeaseMutationFailure(
                context = context,
                counters = counters,
                work = work,
                crawler = baseCrawler,
                operation = "failure-attempt-finish",
                result = attemptFinished,
            )
        ) {
            return
        }
        handleWorkFailure(
            pipeline,
            context,
            counters,
            work,
            attempt,
            "work",
            error,
            baseCrawler,
            itemLevel = error === transportFailure.get(),
        )
    } finally {
        pacing?.release()
        permit?.release()
    }
}

/**
 * Shared typed response path for ordinary work and replay. The optional
 * [work] is deliberately absent during replay, so follow-up requests are
 * bounded and ignored rather than becoming ordinary crawl jobs.
 */
private suspend fun <T : Any> processTypedResponse(
    pipeline: Pipeline<T>,
    context: TypedPipelineExecutionContext,
    counters: PipelineCounters,
    currentResponse: CrawlResponse,
    crawler: CrawlerContext,
    work: LeasedWork?,
    storedReference: com.rmjtromp.trawler.model.ResponseReference?,
    beforeResponseAttachment: suspend () -> Unit = {},
    accept: suspend (CrawlRequest, CrawlResponse?) -> Unit = { _, _ -> },
) {
    try {
        validateResponseBounds(currentResponse, context.configuration.runtimeOptions)
    } catch (error: IllegalArgumentException) {
        throw StageFailure(
            "response-metadata-bound",
            error,
        )
    }
    counters.bytesRead.addAndGet(currentResponse.body.size.toLong())
    observeBudget(
        pipeline,
        context,
        counters,
        BudgetObservation.BytesRead(currentResponse.body.size.toLong()),
    )
    val request = work?.request ?: currentResponse.request
    context.event(CrawlerEventType.EXTRACTION_STARTED, crawler, work)
    val extraction = try {
        pipeline.extractor.extract(
            currentResponse,
            ExtractionContext(crawler, request, currentResponse),
        )
    } catch (cancellation: CancellationException) {
        throw cancellation
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
            if (work == null) {
                if (extraction.followUps.isNotEmpty()) {
                    context.event(
                        CrawlerEventType.CAPABILITY_WARNING,
                        crawler,
                        detail = "replay-follow-ups-suppressed:${extraction.followUps.size}",
                    )
                }
            } else {
                beforeResponseAttachment()
                extraction.followUps.forEach { followUp -> accept(followUp, null) }
            }
            context.event(CrawlerEventType.EXTRACTION_COMPLETED, crawler, work)
            val produced = mutableListOf<Produced<T>>()
            extraction.values.forEach { value ->
                var current: T? = value
                for (transform in pipeline.transforms) {
                    val decision = transform.apply(
                        requireNotNull(current),
                        TransformContext(crawler, request),
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
                        ValidationContext(crawler, request),
                    )) {
                        ValidationResult.Accepted -> Unit
                        is ValidationResult.Rejected -> {
                            context.event(CrawlerEventType.VALUE_DROPPED, crawler, work, validation.reason)
                            current = null
                        }

                        is ValidationResult.Failed ->
                            throw StageFailure("validation:${validator.id}", validation.error)
                    }
                    if (current == null) break
                }
                if (current != null) {
                    beforeResponseAttachment()
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
                            requestId = work?.workItemId?.value ?: crawler.requestId,
                            responseReference = storedReference,
                            originalTarget = request.target,
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
            deliverOutputs(
                pipeline = pipeline,
                context = context,
                counters = counters,
                produced = produced,
                crawler = crawler,
                beforeDelivery = beforeResponseAttachment,
            )
        }
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

private suspend fun ensureLeaseCurrent(
    context: PipelineExecutionContext,
    work: LeasedWork,
    leaseLost: AtomicBoolean,
) {
    when (val result = context.leaseStore.checkCurrent(work)) {
        LeaseMutationResult.Accepted -> Unit
        LeaseMutationResult.Stale -> {
            leaseLost.set(true)
            throw LeaseLostCancellationException("Lease was reclaimed before response attachment")
        }

        is LeaseMutationResult.Rejected -> {
            leaseLost.set(true)
            throw LeaseLostCancellationException("Lease validation was rejected: ${result.reason}")
        }
    }
}

private suspend fun <T : Any> deliverOutputs(
    pipeline: Pipeline<T>,
    context: TypedPipelineExecutionContext,
    counters: PipelineCounters,
    produced: List<Produced<T>>,
    crawler: CrawlerContext,
    beforeDelivery: suspend () -> Unit,
) {
    if (produced.isEmpty()) return
    if (pipeline.outputs.isEmpty()) {
        context.event(CrawlerEventType.VALUE_DROPPED, crawler, detail = "no-output-bound")
        return
    }
    pipeline.outputs.forEach { binding ->
        produced.chunked(context.configuration.runtimeOptions.maxOutputBatchSize).forEach { batch ->
            beforeDelivery()
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
                    if (binding.output !is NoOutput<*> && binding.requirement == OutputRequirement.REQUIRED) {
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
    itemLevel: Boolean,
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
            if (recordLeaseMutationFailure(
                    context = context,
                    counters = counters,
                    work = work,
                    crawler = crawler,
                    operation = "retry-now",
                    result = context.leaseStore.transition(work, WorkTransition.Retry(context.configuration.clock.now())),
                )
            ) {
                return
            }
            context.event(CrawlerEventType.RETRY_SCHEDULED, crawler, work, "now")
        }

        is RetryDecision.RetryAt -> {
            if (recordLeaseMutationFailure(
                    context = context,
                    counters = counters,
                    work = work,
                    crawler = crawler,
                    operation = "retry-at",
                    result = context.leaseStore.transition(work, WorkTransition.Retry(retryDecision.at)),
                )
            ) {
                return
            }
            counters.abandoned.set(true)
            context.event(CrawlerEventType.RETRY_SCHEDULED, crawler, work, retryDecision.at.toString())
        }

        RetryDecision.DoNotRetry -> {
            val settled = !classification.retryableByDefault && itemLevel
            counters.terminalFailure.set(true)
            if (!settled) counters.blockingFailure.set(true)
            if (recordLeaseMutationFailure(
                    context = context,
                    counters = counters,
                    work = work,
                    crawler = crawler,
                    operation = "terminal-failure",
                    result = context.leaseStore.transition(work, WorkTransition.Failed(stage)),
                )
            ) {
                return
            }
            if (settled) counters.settledItemFailures.incrementAndGet()
            context.event(CrawlerEventType.REQUEST_FAILED, crawler, work, stage)
        }

        is RetryDecision.Abandon -> {
            if (recordLeaseMutationFailure(
                    context = context,
                    counters = counters,
                    work = work,
                    crawler = crawler,
                    operation = "retry-abandonment",
                    result = context.leaseStore.transition(work, WorkTransition.Abandoned(retryDecision.reason)),
                )
            ) {
                return
            }
            counters.abandoned.set(true)
            context.event(CrawlerEventType.REQUEST_FAILED, crawler, work, "abandoned")
        }
    }
}

/**
 * Named stages whose permanent failure describes the item itself rather than
 * the engine, its stores, or a required output. The generic `work` stage is
 * item-level only for the exact throwable raised by the transport, which the
 * caller tracks separately; every other `work` failure blocks.
 */
private fun isItemLevelStage(stage: String): Boolean =
    stage == "extraction" ||
        stage == "extraction-bound" ||
        stage == "follow-up-bound" ||
        stage == "response-metadata-bound" ||
        stage.startsWith("validation:")

/**
 * Prevents a stale or rejected lease mutation from being reported as a
 * successful state transition. A stale worker is abandoned and stops
 * producing transition-specific events; an operational rejection is a
 * terminal run failure while the adapter retains ownership of recovery.
 */
private suspend fun recordLeaseMutationFailure(
    context: PipelineExecutionContext,
    counters: PipelineCounters,
    work: LeasedWork,
    crawler: CrawlerContext,
    operation: String,
    result: LeaseMutationResult,
): Boolean = when (result) {
    LeaseMutationResult.Accepted -> false
    LeaseMutationResult.Stale -> {
        counters.abandoned.set(true)
        context.event(CrawlerEventType.REQUEST_FAILED, crawler, work, "$operation:lease-lost")
        true
    }

    is LeaseMutationResult.Rejected -> {
        val error = IllegalStateException("Lease mutation '$operation' was rejected: ${result.reason}")
        counters.failure("lease:$operation", error, terminal = true)
        context.event(CrawlerEventType.REQUEST_FAILED, crawler, work, "$operation:rejected")
        true
    }
}

private suspend fun <T : Any> executeDiscoveryRequest(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    request: CrawlRequest,
    suppliedContext: RequestContext,
): CrawlResponse {
    try {
        validateRequestBounds(request, context.configuration.runtimeOptions)
    } catch (error: IllegalArgumentException) {
        throw PolicyRejection("request exceeds the configured runtime bounds")
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
        val response = executeThroughMiddleware(pipeline, context, request, suppliedContext).response
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
    transportFailure: java.util.concurrent.atomic.AtomicReference<Throwable?>,
): ExecutedResponse = executeThroughMiddleware(
    pipeline,
    context,
    request,
    RequestContext(crawler, request),
    transportFailure,
)

private suspend fun <T : Any> executeThroughMiddleware(
    pipeline: Pipeline<T>,
    context: PipelineExecutionContext,
    request: CrawlRequest,
    suppliedContext: RequestContext,
    transportFailure: java.util.concurrent.atomic.AtomicReference<Throwable?>? = null,
): ExecutedResponse {
    val requestWithMetadata = request.withRuntimeAttributes(context, suppliedContext.crawler.requestId)
    validateRequestBounds(requestWithMetadata, context.configuration.runtimeOptions)
    val requestContext = suppliedContext.copy(request = requestWithMetadata)
    context.responseStore?.let { store ->
        when (val cache = context.configuration.responseCachePolicy.lookup(
            ResponseCacheContext(
                crawler = requestContext.crawler,
                request = requestWithMetadata,
                now = context.configuration.clock.now(),
            ),
            store,
        )) {
            ResponseCacheDecision.Miss -> Unit
            is ResponseCacheDecision.Hit -> {
                require(cache.stored.pipelineId == requestContext.crawler.pipelineId) {
                    "Response cache returned a response not owned by the selected pipeline"
                }
                validateResponseBounds(cache.stored.response, context.configuration.runtimeOptions)
                return ExecutedResponse(
                    response = cache.stored.response.copy(request = requestWithMetadata),
                    storedReference = cache.stored.reference,
                )
            }
        }
    }
    val responseHandler = object : ResponseHandler {
        override suspend fun handle(response: CrawlResponse, context: ResponseContext): CrawlResponse =
            executeResponseMiddleware(pipeline, response, context, 0)
    }
    val terminal = RequestExecutor { nextRequest, nextRequestContext ->
        val transport: Transport = pipeline.transportRouter.select(nextRequest, nextRequestContext)
        val raw = try {
            transport.execute(nextRequest)
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (error: Throwable) {
            // Recorded by identity, not wrapped, so middleware and discovery see the original type.
            transportFailure?.set(error)
            throw error
        }
        val processed = responseHandler.handle(
            raw.withRuntimeAttributes(context, nextRequestContext.crawler.requestId),
            ResponseContext(nextRequestContext.crawler, nextRequest, raw),
        )
        validateResponseBounds(processed, context.configuration.runtimeOptions)
        processed
    }
    var next = terminal
    pipeline.requestMiddlewares.asReversed().forEach { middleware ->
        val downstream = next
        next = RequestExecutor { nextRequest, nextContext ->
            middleware.intercept(nextRequest, nextContext, downstream)
        }
    }
    return ExecutedResponse(next.execute(requestWithMetadata, requestContext), null)
}

private data class ExecutedResponse(
    val response: CrawlResponse,
    val storedReference: com.rmjtromp.trawler.model.ResponseReference?,
)

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
    when (val decision = context.configuration.responseCapturePolicy.decide(
        ResponseCaptureContext(crawler, response.request, response),
    )) {
        is ResponseCaptureDecision.Skip -> return null
        is ResponseCaptureDecision.Fail -> throw StageFailure("response-policy", decision.error)
        ResponseCaptureDecision.Store -> Unit
    }
    return try {
        val reference = store.put(response)
        if (reference == null) {
            context.event(CrawlerEventType.RESPONSE_STORE_FAILED, crawler, work, "store-declined")
            if (context.configuration.responseCapturePolicy.requiresStore) {
                throw StageFailure(
                    "response-store",
                    IllegalStateException("Required response store declined the response"),
                )
            }
        } else {
            context.event(CrawlerEventType.RESPONSE_STORED, crawler, work)
        }
        reference
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (failure: StageFailure) {
        throw failure
    } catch (error: Throwable) {
        context.event(CrawlerEventType.RESPONSE_STORE_FAILED, crawler, work, safeFailureName(error))
        throw StageFailure("response-store", error)
    }
}

private fun CrawlRequest.withRuntimeAttributes(
    context: TypedPipelineExecutionContext,
    requestId: String?,
): CrawlRequest = copy(
    attributes = ContextAttributes.from(
        attributes.entries
            .filterNot { it.name == PIPELINE_ID_ATTRIBUTE || it.name == REQUEST_ID_ATTRIBUTE }
            .plus(ContextAttribute(PIPELINE_ID_ATTRIBUTE, context.pipelineId))
            .let { current ->
                if (requestId == null) current else current + ContextAttribute(REQUEST_ID_ATTRIBUTE, requestId)
            },
    ),
)

private fun CrawlResponse.withRuntimeAttributes(
    context: TypedPipelineExecutionContext,
    requestId: String?,
): CrawlResponse = copy(
    request = request.withRuntimeAttributes(context, requestId),
)

private fun validateRequestBounds(
    request: CrawlRequest,
    options: EngineRuntimeOptions,
) {
    requireUtf8Bound(request.target, options.maxTargetBytes, "request target")
    requireUtf8Bound(request.method.value, options.maxMethodBytes, "request method")
    validateHeaders(request.headers, options)
    validateAttributes(request.attributes, options)
    request.body?.let { body ->
        require(body.size <= options.maxRequestBodyBytes) {
            "request body exceeds the configured bound"
        }
    }
}

private fun validateResponseBounds(
    response: CrawlResponse,
    options: EngineRuntimeOptions,
) {
    validateRequestBounds(response.request, options)
    validateHeaders(response.headers, options)
    validateAttributes(response.attributes, options)
    require(response.body.size <= options.maxResponseBodyBytes) {
        "response body exceeds the configured bound"
    }
    response.finalTarget?.let { finalTarget ->
        requireUtf8Bound(finalTarget, options.maxTargetBytes, "response final target")
    }
}

private fun validateHeaders(
    headers: com.rmjtromp.trawler.model.Headers,
    options: EngineRuntimeOptions,
) {
    require(headers.entries.size <= options.maxHeaderCount) {
        "header count exceeds the configured bound"
    }
    headers.entries.forEach { header ->
        requireUtf8Bound(header.name, options.maxHeaderNameBytes, "header name")
        requireUtf8Bound(header.value, options.maxHeaderValueBytes, "header value")
    }
}

private fun validateAttributes(
    attributes: com.rmjtromp.trawler.model.ContextAttributes,
    options: EngineRuntimeOptions,
) {
    require(attributes.entries.size <= options.maxAttributeCount) {
        "attribute count exceeds the configured bound"
    }
    attributes.entries.forEach { attribute ->
        requireUtf8Bound(attribute.name, options.maxAttributeNameBytes, "attribute name")
        requireUtf8Bound(attribute.value, options.maxAttributeValueBytes, "attribute value")
    }
}

private fun requireUtf8Bound(value: String, maxBytes: Int, label: String) {
    require(value.toByteArray(Charsets.UTF_8).size <= maxBytes) {
        "$label exceeds the configured bound"
    }
}

private const val PIPELINE_ID_ATTRIBUTE: String = "trawler.pipeline-id"
private const val REQUEST_ID_ATTRIBUTE: String = "trawler.request-id"

private fun safeFailureName(error: Throwable): String = error::class.simpleName ?: "failure"

private fun PipelineRunResult.toDurableStatus(): DurableRunStatus = when (status) {
    RunStatus.RUNNING -> DurableRunStatus.FAILED
    RunStatus.COMPLETED -> DurableRunStatus.COMPLETED
    RunStatus.ABANDONED -> DurableRunStatus.ABANDONED
    RunStatus.FAILED -> DurableRunStatus.FAILED
    RunStatus.CANCELLED -> DurableRunStatus.CANCELLED
}

private fun RunResult.toDurableStatus(): DurableRunStatus = when (status) {
    RunStatus.RUNNING -> DurableRunStatus.FAILED
    RunStatus.COMPLETED -> DurableRunStatus.COMPLETED
    RunStatus.ABANDONED -> DurableRunStatus.ABANDONED
    RunStatus.FAILED -> DurableRunStatus.FAILED
    RunStatus.CANCELLED -> DurableRunStatus.CANCELLED
}
