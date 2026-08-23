package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.model.immutableSnapshot
import com.rmjtromp.trawler.schedule.ScheduleTrigger
import java.time.Duration
import java.time.Instant
import java.util.Collections

/**
 * Finite engine-owned bounds for the in-memory runtime.
 *
 * These values bound admission, pending direct runs, discovery, worker
 * batches, output batches, lifecycle waits, and the default run duration.
 * Adapter implementations may
 * impose tighter limits, but the engine never silently grows these queues or
 * batches. Durations are used for runtime lifecycle decisions; scheduler
 * trigger time calculations use the configured [com.rmjtromp.trawler.policy.Clock].
 * The engine owns cancellation of its work within these bounds; a failure
 * exceeding a bound is reported as an abandoned or failed run rather than
 * being silently discarded.
 */
public data class EngineRuntimeOptions(
    public val maxQueueSize: Int = 1_024,
    public val maxWorkItemsPerRun: Int = 10_000,
    public val maxDiscoveryEmissionsPerDiscovery: Int = 1_024,
    public val maxDiscoveryEmissionsPerRun: Int = 10_000,
    public val maxFollowUpRequestsPerWorkItem: Int = 128,
    public val maxValuesPerResponse: Int = 1_024,
    public val maxRequestBodyBytes: Int = 8_388_608,
    public val maxResponseBodyBytes: Int = 8_388_608,
    public val maxProducedValuesPerRun: Int = 100_000,
    public val maxAttemptsPerWorkItem: Int = 3,
    public val maxPendingRuns: Int = 64,
    public val workerCount: Int = 4,
    public val claimBatchSize: Int = 1,
    public val maxOutputBatchSize: Int = 128,
    public val leaseDuration: Duration = Duration.ofMinutes(5),
    public val runTimeout: Duration = Duration.ofMinutes(15),
    public val awaitIdleTimeout: Duration = Duration.ofSeconds(30),
    public val shutdownTimeout: Duration = Duration.ofSeconds(10),
    public val maxRetainedJobs: Int = 8_192,
    public val maxRetainedResources: Int = 8_192,
    public val maxRetainedDiscoveryState: Int = 2_048,
    public val maxCheckpointPayloadBytes: Int = 2 * 1024 * 1024,
) {
    init {
        require(maxQueueSize > 0) { "Runtime queue size must be positive" }
        require(maxWorkItemsPerRun > 0) { "Runtime work-item bound must be positive" }
        require(maxDiscoveryEmissionsPerDiscovery > 0) {
            "Runtime discovery emission bound must be positive"
        }
        require(maxDiscoveryEmissionsPerRun > 0) {
            "Runtime discovery-per-run bound must be positive"
        }
        require(maxFollowUpRequestsPerWorkItem >= 0) {
            "Runtime follow-up bound must not be negative"
        }
        require(maxAttemptsPerWorkItem > 0) { "Runtime attempt bound must be positive" }
        require(maxValuesPerResponse > 0) { "Runtime response value bound must be positive" }
        require(maxRequestBodyBytes > 0) { "Runtime request body bound must be positive" }
        require(maxResponseBodyBytes > 0) { "Runtime response body bound must be positive" }
        require(maxProducedValuesPerRun > 0) { "Runtime produced-value bound must be positive" }
        require(workerCount > 0) { "Runtime worker count must be positive" }
        require(maxPendingRuns > 0) { "Runtime pending-run bound must be positive" }
        require(claimBatchSize > 0) { "Runtime claim batch size must be positive" }
        require(maxOutputBatchSize > 0) { "Runtime output batch size must be positive" }
        require(!leaseDuration.isNegative && !leaseDuration.isZero) {
            "Runtime lease duration must be positive"
        }
        require(!runTimeout.isNegative && !runTimeout.isZero) { "Runtime run timeout must be positive" }
        require(!awaitIdleTimeout.isNegative && !awaitIdleTimeout.isZero) {
            "Runtime idle timeout must be positive"
        }
        require(!shutdownTimeout.isNegative && !shutdownTimeout.isZero) {
            "Runtime shutdown timeout must be positive"
        }
        require(maxRetainedJobs >= maxQueueSize) {
            "Runtime retained jobs must be at least the queue size"
        }
        require(maxRetainedResources >= maxQueueSize) {
            "Runtime retained resources must be at least the queue size"
        }
        require(maxRetainedDiscoveryState > 0) {
            "Runtime retained discovery state bound must be positive"
        }
        require(maxCheckpointPayloadBytes > 0) {
            "Runtime checkpoint payload bound must be positive"
        }
    }
}

/**
 * Selects which registered pipelines participate in one finite engine run.
 * The selected IDs are copied at construction. The caller owns cancellation
 * of the suspending [TrawlerEngine.runOnce] call; completed work and failures
 * are returned in [RunResult] when the call reaches a terminal state.
 */
public class RunRequest(
    pipelineIds: Set<String>? = null,
    public val reason: String? = null,
    public val timeout: Duration? = null,
) {
    /** Immutable selected-pipeline snapshot, or `null` for every pipeline. */
    public val selectedPipelineIds: Set<String>? = pipelineIds?.let(::immutableIdSet)

    init {
        selectedPipelineIds?.forEach { require(it.isNotBlank()) { "Selected pipeline ID must not be blank" } }
        require(reason == null || reason.isNotBlank()) { "Run reason must not be blank when present" }
        require(timeout == null || (!timeout.isNegative && !timeout.isZero)) {
            "Run timeout must be positive when present"
        }
    }
}

/** Terminal and in-progress status shared by engine and pipeline run results. */
public enum class RunStatus {
    RUNNING,
    COMPLETED,
    ABANDONED,
    FAILED,
    CANCELLED,
}

/** Stable failure stage plus original cause for host inspection. */
public data class RunFailure(
    public val stage: String,
    public val error: Throwable,
) {
    init {
        require(stage.isNotBlank()) { "Run failure stage must not be blank" }
    }
}

/**
 * Result for one typed pipeline run; no produced values are erased into it.
 * The result contains bounded counters and failure metadata only. Output
 * ownership, cancellation, and duplicate handling remain with each output.
 */
public class PipelineRunResult(
    public val pipelineId: String,
    public val pipelineRunId: String,
    public val status: RunStatus,
    public val discoveryEmissions: Long,
    public val acceptedWork: Long,
    public val suppressedWork: Long,
    public val requestsStarted: Long,
    public val producedValues: Long,
    public val deliveredValues: Long,
    failures: List<RunFailure>,
    public val startedAt: Instant,
    public val completedAt: Instant,
) {
    /** Immutable bounded failure snapshot. */
    public val failures: List<RunFailure> = immutableSnapshot(failures)
}

/**
 * Aggregate result for one finite engine run across selected pipelines.
 * Pipeline failures are isolated and represented in the result; caller
 * cancellation instead propagates as coroutine cancellation.
 */
public class RunResult(
    public val engineRunId: String,
    public val status: RunStatus,
    pipelineRuns: List<PipelineRunResult>,
    failures: List<RunFailure>,
    public val startedAt: Instant,
    public val completedAt: Instant,
) {
    /** Immutable pipeline-result snapshot in registration order. */
    public val pipelineRuns: List<PipelineRunResult> = immutableSnapshot(pipelineRuns)

    /** Immutable aggregate failure snapshot. */
    public val failures: List<RunFailure> = immutableSnapshot(failures)
}

/**
 * Daemon selection passed to [TrawlerEngine.startDaemon]. Scheduling is
 * opt-in: constructing an engine or calling [TrawlerEngine.runOnce] never
 * starts a scheduler. The selected IDs are copied; daemon cancellation is
 * cooperative and scheduler failures are surfaced through scheduler events.
 */
public class DaemonConfig(
    public val trigger: ScheduleTrigger = ScheduleTrigger.Manual(),
    pipelineIds: Set<String>? = null,
) {
    /** Immutable selected-pipeline snapshot, or `null` for every pipeline. */
    public val selectedPipelineIds: Set<String>? = pipelineIds?.let(::immutableIdSet)

    init {
        require(selectedPipelineIds == null || selectedPipelineIds.isNotEmpty()) {
            "Daemon pipeline selection must not be empty"
        }
        selectedPipelineIds?.forEach { require(it.isNotBlank()) { "Selected pipeline ID must not be blank" } }
    }
}

private fun immutableIdSet(values: Set<String>): Set<String> =
    Collections.unmodifiableSet(values.toSet())
