package com.rmjtromp.trawler.state

import java.time.Instant

/**
 * Opaque strategy-owned harvest state. It is operational coordination data,
 * not a produced value or a universal serialization envelope.
 */
public data class HarvestState(
    public val revision: Long,
    public val strategyId: String,
    public val codecId: String,
    public val codecVersion: Int,
    public val payload: CheckpointPayload,
    public val historicalComplete: Boolean = false,
) {
    init {
        require(revision >= 0) { "Harvest state revision must not be negative" }
        require(strategyId.isNotBlank()) { "Harvest strategy ID must not be blank" }
        require(codecId.isNotBlank()) { "Harvest state codec ID must not be blank" }
        require(codecVersion >= 0) { "Harvest state codec version must not be negative" }
    }
}

/** Codec identity persisted beside one opaque harvest-state payload. */
public data class HarvestStateCodecDescriptor(
    public val id: String,
    public val version: Int,
) {
    init {
        require(id.isNotBlank()) { "Harvest-state codec ID must not be blank" }
        require(version >= 0) { "Harvest-state codec version must not be negative" }
    }
}

/** Explicit resolution of persisted harvest state against a current codec. */
public sealed interface HarvestStateResolution {
    /** The state is compatible and may be supplied to the strategy. */
    public data class Use(public val state: HarvestState) : HarvestStateResolution

    /** The strategy-owned codec converted an older payload. */
    public data class Migrated(public val state: HarvestState) : HarvestStateResolution

    /** The strategy explicitly resets incompatible state. */
    public data class Reset(public val reason: String) : HarvestStateResolution {
        init {
            require(reason.isNotBlank()) { "Harvest-state reset reason must not be blank" }
        }
    }

    /** The incompatible state must be retained for operator action. */
    public data class Quarantine(public val reason: String) : HarvestStateResolution {
        init {
            require(reason.isNotBlank()) { "Harvest-state quarantine reason must not be blank" }
        }
    }
}

/** Compare-and-set storage for optional harvest state. */
public interface HarvestStateStore {
    /** Loads one pipeline's state for the selected strategy. */
    public suspend fun loadHarvestState(pipelineId: String, strategyId: String): HarvestState?

    /** Advances state only when the stored revision still equals the expectation. */
    public suspend fun compareAndSetHarvestState(
        pipelineId: String,
        strategyId: String,
        expectedRevision: Long?,
        next: HarvestState,
    ): Boolean
}

/** Optional retention for incompatible harvest-state payloads. */
public interface HarvestStateQuarantineStore {
    /** Stores one bounded opaque state and its explicit incompatibility reason. */
    public suspend fun quarantineHarvestState(
        pipelineId: String,
        strategyId: String,
        state: HarvestState,
        reason: String,
    )
}

/** An explicit operator override for pipeline admission. */
public data class PipelineEligibilityOverride(
    public val enabled: Boolean,
    public val reason: String? = null,
    public val expiresAt: Instant? = null,
) {
    init {
        require(reason == null || reason.isNotBlank()) {
            "Pipeline eligibility override reason must not be blank when present"
        }
    }
}

/** Optional durable/local store for operator eligibility overrides. */
public interface PipelineEligibilityOverrideStore {
    /** Loads the current override, or `null` when no override is installed. */
    public suspend fun loadEligibilityOverride(pipelineId: String): PipelineEligibilityOverride?

    /** Replaces the override explicitly; `null` clears it. */
    public suspend fun setEligibilityOverride(
        pipelineId: String,
        override: PipelineEligibilityOverride?,
    )
}

/** Immutable operational health counters for one pipeline. */
public data class PipelineHealthSnapshot(
    public val revision: Long = 0,
    public val consecutiveFailures: Int = 0,
    public val totalSuccesses: Long = 0,
    public val totalFailures: Long = 0,
    public val lastObservedAt: Instant? = null,
    public val circuitOpenUntil: Instant? = null,
) {
    init {
        require(revision >= 0) { "Pipeline health revision must not be negative" }
        require(consecutiveFailures >= 0) { "Consecutive failures must not be negative" }
        require(totalSuccesses >= 0) { "Total successes must not be negative" }
        require(totalFailures >= 0) { "Total failures must not be negative" }
    }
}

/** Optional compare-and-set storage for pipeline health snapshots. */
public interface PipelineHealthStore {
    /** Loads one pipeline's bounded health snapshot. */
    public suspend fun loadPipelineHealth(pipelineId: String): PipelineHealthSnapshot?

    /** Stores the next health snapshot only when its expected revision matches. */
    public suspend fun compareAndSetPipelineHealth(
        pipelineId: String,
        expectedRevision: Long?,
        next: PipelineHealthSnapshot,
    ): Boolean
}
