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

/** Conditions evaluated before historical coverage can be committed. */
public data class HarvestCompletionContext(
    public val discoveryReachedNaturalEnd: Boolean,
    public val checkpointWorkSettled: Boolean,
    public val generationWorkOpen: Int,
    public val runOutcome: HarvestRunOutcome,
    public val completionPredicatePassed: Boolean,
    public val now: Instant,
    public val previousState: HarvestState? = null,
) {
    init {
        require(generationWorkOpen >= 0) { "Generation work count must not be negative" }
    }
}

/** Run outcome visible to harvest completion, without exposing pipeline values. */
public enum class HarvestRunOutcome {
    COMPLETED,
    ABANDONED,
    FAILED,
    CANCELLED,
}

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
) : HarvestStrategy {
    override val mode: HarvestMode = HarvestMode.CURSOR

    init {
        require(id.isNotBlank()) { "Cursor harvest strategy ID must not be blank" }
    }

    override fun complete(context: HarvestCompletionContext): HarvestCompletion =
        if (context.runOutcome == HarvestRunOutcome.COMPLETED &&
            context.discoveryReachedNaturalEnd &&
            context.checkpointWorkSettled &&
            context.generationWorkOpen == 0 &&
            context.completionPredicatePassed
        ) {
            HarvestCompletion.Commit(null)
        } else {
            HarvestCompletion.Incomplete("cursor walk is not settled")
        }
}

/** Watermark state is resumable and uses an explicit boundary policy. */
public class WatermarkHarvestStrategy(
    public val boundaryPolicy: IncrementalBoundaryPolicy,
    override val id: String = "watermark",
) : HarvestStrategy {
    override val mode: HarvestMode = HarvestMode.WATERMARK
    private val boundaryCodec = InstantBoundaryHarvestStateCodec(id)

    override val stateCodec: HarvestStateCodec
        get() = boundaryCodec

    init {
        require(id.isNotBlank()) { "Watermark harvest strategy ID must not be blank" }
    }

    override fun complete(context: HarvestCompletionContext): HarvestCompletion =
        if (context.runOutcome == HarvestRunOutcome.COMPLETED &&
            context.discoveryReachedNaturalEnd &&
            context.checkpointWorkSettled &&
            context.generationWorkOpen == 0 &&
            context.completionPredicatePassed
        ) {
            HarvestCompletion.Commit(
                boundaryCodec.candidate(context.now, historicalComplete = false),
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
 * never mark historical coverage complete.
 */
public class HistoricalThenIncrementalHarvestStrategy(
    public val boundaryPolicy: IncrementalBoundaryPolicy,
    override val id: String = "historical-then-incremental",
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
            context.runOutcome == HarvestRunOutcome.COMPLETED &&
            context.completionPredicatePassed
        ) {
            HarvestCompletion.Commit(
                boundaryCodec.candidate(context.now, historicalComplete = true),
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
