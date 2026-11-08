package com.rmjtromp.trawler.discovery

import com.rmjtromp.trawler.policy.IncrementalBoundaryPolicy
import com.rmjtromp.trawler.state.CheckpointPayload
import com.rmjtromp.trawler.state.DiscoveryCheckpoint
import com.rmjtromp.trawler.state.HarvestState
import com.rmjtromp.trawler.state.HarvestStateCodecDescriptor
import com.rmjtromp.trawler.state.HarvestStateResolution
import java.nio.ByteBuffer
import java.time.Duration
import java.time.Instant

/** Recurring-discovery state model selected explicitly by a pipeline. */
public enum class HarvestMode {
    STATELESS,
    CURSOR,
    WATERMARK,
    HISTORICAL_THEN_INCREMENTAL,
    CUSTOM,
}

/** Bounded state candidate returned by a strategy after a successful phase. */
public data class HarvestStateCandidate(
    public val strategyId: String,
    public val codecId: String,
    public val codecVersion: Int,
    public val payload: CheckpointPayload,
    public val historicalComplete: Boolean,
) {
    init {
        require(strategyId.isNotBlank()) { "Harvest strategy ID must not be blank" }
        require(codecId.isNotBlank()) { "Harvest state codec ID must not be blank" }
        require(codecVersion >= 0) { "Harvest state codec version must not be negative" }
    }
}

/** Context used before discovery admission for one recurring strategy. */
public data class HarvestContext(
    public val pipelineId: String,
    public val discoveryId: String,
    public val checkpoint: DiscoveryCheckpoint?,
    public val state: HarvestState?,
    public val openWorkCount: Int,
    public val now: Instant,
    public val incrementalWindow: IncrementalBoundaryPolicy.Window? = null,
) {
    init {
        require(pipelineId.isNotBlank()) { "Pipeline ID must not be blank" }
        require(discoveryId.isNotBlank()) { "Discovery ID must not be blank" }
        require(openWorkCount >= 0) { "Harvest open work count must not be negative" }
    }
}

/** Optional strategy-specific admission result. */
public sealed interface HarvestAdmission {
    /** The strategy may run its discovery walk. */
    public data object Run : HarvestAdmission

    /** The walk is deferred while state and work remain resumable. */
    public data class Defer(public val reason: String) : HarvestAdmission {
        init {
            require(reason.isNotBlank()) { "Harvest deferral reason must not be blank" }
        }
    }
}

/**
 * Conditions evaluated before historical coverage can be committed.
 *
 * [completionPredicatePassed] is true only for [HarvestRunOutcome.COMPLETED].
 * [settledItemFailures] counts work items that reached a terminal failed
 * state in this run through a permanent, item-level failure; it is non-zero
 * only for [HarvestRunOutcome.COMPLETED_WITH_SETTLED_ITEM_FAILURES].
 * [incrementalWindow] is the most conservative window supplied to this run's
 * resumable discoveries, or `null` when the strategy supplied none.
 */
public data class HarvestCompletionContext(
    public val discoveryReachedNaturalEnd: Boolean,
    public val checkpointWorkSettled: Boolean,
    public val generationWorkOpen: Int,
    public val runOutcome: HarvestRunOutcome,
    public val completionPredicatePassed: Boolean,
    public val now: Instant,
    public val previousState: HarvestState? = null,
    public val settledItemFailures: Long = 0,
    public val incrementalWindow: IncrementalBoundaryPolicy.Window? = null,
) {
    init {
        require(generationWorkOpen >= 0) { "Generation work count must not be negative" }
        require(settledItemFailures >= 0) { "Settled item failure count must not be negative" }
        require(settledItemFailures == 0L || runOutcome == HarvestRunOutcome.COMPLETED_WITH_SETTLED_ITEM_FAILURES) {
            "Settled item failures require the settled-failure run outcome"
        }
    }
}

/** Run outcome visible to harvest completion, without exposing pipeline values. */
public enum class HarvestRunOutcome {
    /** Every discovered item succeeded or was intentionally ignored, dropped, or suppressed. */
    COMPLETED,

    /**
     * The run itself completed and every work item reached a terminal state,
     * but at least one item failed permanently at an item-level stage. Only
     * failures classified as not retryable by default, raised by transport,
     * extraction, response bounds, or validation, are settled. Exhausted
     * transient failures, response-store and capture-policy failures,
     * required-output failures, and state-store failures remain [FAILED].
     * The pipeline run status is still reported as failed; this outcome only
     * lets a strategy that opted in commit harvest progress.
     */
    COMPLETED_WITH_SETTLED_ITEM_FAILURES,
    ABANDONED,
    FAILED,
    CANCELLED,
}

/**
 * Explicit choice for whether settled item failures may commit harvest progress.
 *
 * The built-in strategies default to [BLOCKS_COMPLETION]. Selecting
 * [ALLOWS_COMPLETION] means a permanently failed item does not prevent a
 * cursor walk, watermark, or historical phase from completing; the host is
 * then responsible for recording and revisiting those items, because the
 * next incremental window will not rediscover them. It requires a failure
 * classifier that marks transient failures as retryable by default, such as
 * the web adapter classifier; the core default classifier marks every
 * failure as non-retryable.
 */
public enum class SettledItemFailurePolicy {
    BLOCKS_COMPLETION,
    ALLOWS_COMPLETION,
}

/** True when the run reached a terminal state this strategy may commit. */
private fun HarvestCompletionContext.runSettledFor(policy: SettledItemFailurePolicy): Boolean =
    (runOutcome == HarvestRunOutcome.COMPLETED && completionPredicatePassed) ||
        (runOutcome == HarvestRunOutcome.COMPLETED_WITH_SETTLED_ITEM_FAILURES &&
            policy == SettledItemFailurePolicy.ALLOWS_COMPLETION)

/**
 * Boundary covered by this run. A watermark must advance only to the end of
 * the window the discovery searched, never to the completion time: changes
 * made while the run processed its work lie outside that window.
 */
private fun HarvestCompletionContext.coveredBoundary(): Instant =
    incrementalWindow?.endExclusive ?: now

/** Expected harvest completion decisions. */
public sealed interface HarvestCompletion {
    /** The strategy may persist [state] as its new durable state. */
    public data class Commit(public val state: HarvestStateCandidate?) : HarvestCompletion

    /** Historical coverage remains incomplete and must be resumed/replayed. */
    public data class Incomplete(public val reason: String) : HarvestCompletion {
        init {
            require(reason.isNotBlank()) { "Harvest incomplete reason must not be blank" }
        }
    }
}

/**
 * Optional recurring-harvest strategy. It owns state meaning and completion
 * predicates; the engine owns checkpoint ordering, persistence CAS, and
 * cancellation. Implementations must be deterministic for the supplied
 * immutable contexts and must not create network clients.
 */
public interface HarvestStrategy {
    /** Stable identifier used for compatibility and configuration validation. */
    public val id: String

    /** Recurring mode represented by this strategy. */
    public val mode: HarvestMode

    /** Optional codec for strategy-owned durable harvest state. */
    public val stateCodec: HarvestStateCodec?
        get() = null

    /** Decides whether the strategy itself may start this discovery walk. */
    public fun admit(context: HarvestContext): HarvestAdmission = HarvestAdmission.Run

    /** Provides a deterministic incremental window to the discovery, if used. */
    public fun window(context: HarvestContext): IncrementalBoundaryPolicy.Window? = null

    /** Evaluates conservative completion after discovery and work finalization. */
    public fun complete(context: HarvestCompletionContext): HarvestCompletion
}

/** Stateless discovery has no checkpoint or harvest-store requirement. */
public object StatelessHarvestStrategy : HarvestStrategy {
    override val id: String = "stateless"
    override val mode: HarvestMode = HarvestMode.STATELESS

    override fun complete(context: HarvestCompletionContext): HarvestCompletion =
        HarvestCompletion.Commit(state = null)
}

/** Cursor state is resumable but does not claim historical completion semantics. */
public class CursorHarvestStrategy(
    override val id: String = "cursor",
    public val settledItemFailures: SettledItemFailurePolicy = SettledItemFailurePolicy.BLOCKS_COMPLETION,
) : HarvestStrategy {
    override val mode: HarvestMode = HarvestMode.CURSOR

    init {
        require(id.isNotBlank()) { "Cursor harvest strategy ID must not be blank" }
    }

    override fun complete(context: HarvestCompletionContext): HarvestCompletion =
        if (context.runSettledFor(settledItemFailures) &&
            context.discoveryReachedNaturalEnd &&
            context.checkpointWorkSettled &&
            context.generationWorkOpen == 0
        ) {
            HarvestCompletion.Commit(null)
        } else {
            HarvestCompletion.Incomplete("cursor walk is not settled")
        }
}

/**
 * Watermark state is resumable and uses an explicit boundary policy.
 *
 * A committed boundary is the exclusive end of the window supplied to this
 * run's discovery, so the next window starts where this one stopped searching
 * (minus the policy overlap) regardless of how long the run took to process
 * its work. A settled item failure commits only when [settledItemFailures]
 * allows it.
 */
public class WatermarkHarvestStrategy(
    public val boundaryPolicy: IncrementalBoundaryPolicy,
    override val id: String = "watermark",
    public val settledItemFailures: SettledItemFailurePolicy = SettledItemFailurePolicy.BLOCKS_COMPLETION,
) : HarvestStrategy {
    override val mode: HarvestMode = HarvestMode.WATERMARK
    private val boundaryCodec = InstantBoundaryHarvestStateCodec(id)

    override val stateCodec: HarvestStateCodec
        get() = boundaryCodec

    init {
        require(id.isNotBlank()) { "Watermark harvest strategy ID must not be blank" }
    }

    override fun complete(context: HarvestCompletionContext): HarvestCompletion =
        if (context.runSettledFor(settledItemFailures) &&
            context.discoveryReachedNaturalEnd &&
            context.checkpointWorkSettled &&
            context.generationWorkOpen == 0
        ) {
            HarvestCompletion.Commit(
                boundaryCodec.candidate(context.coveredBoundary(), historicalComplete = false),
            )
        } else {
            HarvestCompletion.Incomplete("watermark boundary is not settled")
        }

    override fun window(context: HarvestContext): IncrementalBoundaryPolicy.Window =
        boundaryPolicy.next(boundaryCodec.decodeBoundary(context.state), context.now)
}

/**
 * Historical coverage is committed only after every required condition is
 * true. A failed, cancelled, abandoned, incomplete, or open-work run can
 * never mark historical coverage complete; settled item failures may commit
 * only when [settledItemFailures] allows it.
 *
 * Incremental runs commit the end of the window they searched. The historical
 * phase has no window, so its completion records the completion time; items
 * changed while a long, multi-run historical walk was in progress are covered
 * only if the discovery's own ordering reaches them. Run a separate
 * [WatermarkHarvestStrategy] pipeline beside a long backfill when that gap
 * matters.
 */
public class HistoricalThenIncrementalHarvestStrategy(
    public val boundaryPolicy: IncrementalBoundaryPolicy,
    override val id: String = "historical-then-incremental",
    public val settledItemFailures: SettledItemFailurePolicy = SettledItemFailurePolicy.BLOCKS_COMPLETION,
) : HarvestStrategy {
    override val mode: HarvestMode = HarvestMode.HISTORICAL_THEN_INCREMENTAL
    private val boundaryCodec = InstantBoundaryHarvestStateCodec(id)

    override val stateCodec: HarvestStateCodec
        get() = boundaryCodec

    init {
        require(id.isNotBlank()) { "Historical harvest strategy ID must not be blank" }
    }

    override fun complete(context: HarvestCompletionContext): HarvestCompletion =
        if (context.discoveryReachedNaturalEnd &&
            context.checkpointWorkSettled &&
            context.generationWorkOpen == 0 &&
            context.runSettledFor(settledItemFailures)
        ) {
            HarvestCompletion.Commit(
                boundaryCodec.candidate(context.coveredBoundary(), historicalComplete = true),
            )
        } else {
            HarvestCompletion.Incomplete("historical coverage completion conditions are not met")
        }

    override fun window(context: HarvestContext): IncrementalBoundaryPolicy.Window? =
        if (context.state?.historicalComplete == true) {
            boundaryPolicy.next(boundaryCodec.decodeBoundary(context.state), context.now)
        } else {
            null
        }
}

/** Codec boundary for strategy-owned opaque harvest state. */
public interface HarvestStateCodec {
    /** Stable payload descriptor persisted beside the state. */
    public val descriptor: HarvestStateCodecDescriptor

    /** Strategy ID this codec belongs to. */
    public val strategyId: String

    /** Resolves compatibility without creating resources or performing I/O. */
    public fun resolve(state: HarvestState): HarvestStateResolution =
        if (state.strategyId == strategyId &&
            state.codecId == descriptor.id &&
            state.codecVersion == descriptor.version
        ) {
            HarvestStateResolution.Use(state)
        } else {
            HarvestStateResolution.Quarantine(
                "Harvest state does not match ${descriptor.id}@${descriptor.version}/$strategyId",
            )
        }
}

private class InstantBoundaryHarvestStateCodec(
    override val strategyId: String,
) : HarvestStateCodec {
    override val descriptor: HarvestStateCodecDescriptor = HarvestStateCodecDescriptor(
        id = "trawler-core-instant-boundary",
        version = 1,
    )

    fun decodeBoundary(state: HarvestState?): Instant? {
        if (state == null) return null
        if (state.payload.size == 0) return null
        require(state.payload.size == 8) { "Instant harvest state payload must contain one timestamp" }
        return Instant.ofEpochMilli(ByteBuffer.wrap(state.payload.toByteArray()).long)
    }

    fun candidate(now: Instant, historicalComplete: Boolean): HarvestStateCandidate =
        HarvestStateCandidate(
            strategyId = strategyId,
            codecId = descriptor.id,
            codecVersion = descriptor.version,
            payload = CheckpointPayload.from(ByteBuffer.allocate(8).putLong(now.toEpochMilli()).array()),
            historicalComplete = historicalComplete,
        )
}

/** Fixed overlap policy for deterministic incremental windows. */
public class FixedOverlapBoundaryPolicy(
    private val overlap: Duration,
    override val id: String = "fixed-overlap-${overlap.toMillis()}ms",
) : IncrementalBoundaryPolicy {
    init {
        require(!overlap.isNegative) { "Incremental overlap must not be negative" }
        require(id.isNotBlank()) { "Incremental boundary policy ID must not be blank" }
    }

    override fun next(lastBoundary: Instant?, now: Instant): IncrementalBoundaryPolicy.Window =
        IncrementalBoundaryPolicy.Window(
            startInclusive = lastBoundary?.minus(overlap),
            endExclusive = now,
        )
}
