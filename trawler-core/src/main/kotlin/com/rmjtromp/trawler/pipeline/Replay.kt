package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.model.ResponseReference
import com.rmjtromp.trawler.response.ResponseReplayQuery
import java.time.Instant

/**
 * Typed replay selection bound to the exact [Pipeline] instance registered in
 * an engine configuration. Binding the pipeline object, rather than accepting
 * only a string ID, prevents a caller from accidentally requesting a result
 * type unrelated to the selected registration.
 */
public class ReplaySelection<T : Any>(
    public val pipeline: Pipeline<T>,
    public val query: ResponseReplayQuery,
    public val limit: Int = 1_024,
) {
    init {
        require(limit > 0) { "Replay response limit must be positive" }
    }
}

/** Builds a typed replay selection from one pipeline definition. */
public fun <T : Any> Pipeline<T>.replay(
    query: ResponseReplayQuery,
    limit: Int = 1_024,
): ReplaySelection<T> = ReplaySelection(this, query, limit)

/**
 * Outcome of replaying stored responses through one typed pipeline.
 *
 * [lastReference] is the store reference of the last response, in store
 * order, whose processing finished either successfully or with a recorded
 * failure. A response interrupted by a budget stop is excluded, so the next
 * page replays it again. It is `null` when no response finished. Pass it as
 * [com.rmjtromp.trawler.response.ResponseReplayQuery.ByPipelineId.after] to
 * continue a paged replay; when [responsesRead] is below the selection limit
 * and the run was not stopped by a budget, the selection was exhausted at the
 * time it was read. Per-response failures are reported in [failures] and do
 * not stop the page, so a caller decides whether to continue past them.
 */
public class ReplayResult(
    public val pipelineId: String,
    public val responsesRead: Long,
    public val producedValues: Long,
    public val deliveredValues: Long,
    public val status: RunStatus,
    failures: List<RunFailure>,
    public val startedAt: Instant,
    public val completedAt: Instant,
    public val lastReference: ResponseReference? = null,
) {
    /** Immutable bounded failure snapshot. */
    public val failures: List<RunFailure> = com.rmjtromp.trawler.model.immutableSnapshot(failures)

    init {
        require(pipelineId.isNotBlank()) { "Replay pipeline ID must not be blank" }
        require(responsesRead >= 0) { "Replay response count must not be negative" }
        require(producedValues >= 0) { "Replay produced count must not be negative" }
        require(deliveredValues >= 0) { "Replay delivered count must not be negative" }
        require(status != RunStatus.RUNNING) { "Replay result must be terminal" }
    }
}
