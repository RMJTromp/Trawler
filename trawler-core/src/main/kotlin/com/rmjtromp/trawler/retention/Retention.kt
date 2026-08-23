package com.rmjtromp.trawler.retention

import java.time.Instant

/** Independent retention categories; no single store is required to own all of them. */
public enum class RetentionCategory {
    /** Terminal visit jobs in an operational state adapter. */
    COMPLETED_JOBS,

    /** Finished meaningful attempts in an operational state adapter. */
    ATTEMPTS,

    /** Terminal engine and pipeline run summaries. */
    RUN_HISTORY,

    /** Raw response history owned by a response store. */
    RESPONSES,

    /** Accepted produced-data history owned by an output. */
    OUTPUT_HISTORY,

    /** Pending/delivered output-owned outbox history. */
    OUTBOX_HISTORY,

    /** Optional durable operational event history. */
    OPERATIONAL_EVENTS,
}

/** Bounded age window shared by focused retention contracts. */
public data class RetentionWindow(
    public val cutoff: Instant,
    public val maxEntries: Int,
) {
    init {
        require(maxEntries > 0) { "Retention batch must be positive" }
    }
}

/** Generic bounded result used by adapter-owned retention reports. */
public data class RetentionSummary(
    public val category: RetentionCategory,
    public val deleted: Long,
    public val protectedEntries: Long = 0,
)
