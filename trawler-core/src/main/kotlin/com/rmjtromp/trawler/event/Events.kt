package com.rmjtromp.trawler.event

import com.rmjtromp.trawler.model.ContextAttributes
import java.time.Instant

/** Generic lifecycle and operational event categories. */
public enum class CrawlerEventType {
    ENGINE_RUN_STARTED,
    ENGINE_RUN_COMPLETED,
    ENGINE_RUN_ABANDONED,
    ENGINE_RUN_FAILED,
    ENGINE_RUN_CANCELLED,
    PIPELINE_RUN_STARTED,
    PIPELINE_RUN_COMPLETED,
    PIPELINE_RUN_ABANDONED,
    PIPELINE_RUN_FAILED,
    PIPELINE_RUN_CANCELLED,
    DISCOVERY_STARTED,
    DISCOVERY_EMITTED,
    DISCOVERY_CHECKPOINTED,
    DISCOVERY_COMPLETED,
    DISCOVERY_FAILED,
    REQUEST_QUEUED,
    REQUEST_CLAIMED,
    REQUEST_STARTED,
    REQUEST_REDIRECTED,
    REQUEST_COMPLETED,
    REQUEST_FAILED,
    RETRY_SCHEDULED,
    COOLDOWN_SCHEDULED,
    EXTRACTION_STARTED,
    EXTRACTION_COMPLETED,
    EXTRACTION_IGNORED,
    EXTRACTION_FAILED,
    VALUE_PRODUCED,
    VALUE_TRANSFORMED,
    VALUE_DROPPED,
    VALUE_ACCEPTED,
    DELIVERY_FAILED,
    RESPONSE_STORED,
    RESPONSE_STORE_FAILED,
    CONFIGURATION_REJECTED,
    CAPABILITY_WARNING,
}

/** Immutable identifiers and safe metadata shared by crawler events. */
public data class EventContext(
    public val emittedAt: Instant,
    public val engineRunId: String? = null,
    public val pipelineRunId: String? = null,
    public val pipelineId: String? = null,
    public val requestId: String? = null,
    public val workItemId: String? = null,
    public val attributes: ContextAttributes = ContextAttributes.empty(),
) {
    init {
        require(engineRunId == null || engineRunId.isNotBlank()) {
            "Event engine run ID must not be blank when present"
        }
        require(pipelineRunId == null || pipelineRunId.isNotBlank()) {
            "Event pipeline run ID must not be blank when present"
        }
        require(pipelineId == null || pipelineId.isNotBlank()) {
            "Event pipeline ID must not be blank when present"
        }
        require(requestId == null || requestId.isNotBlank()) {
            "Event request ID must not be blank when present"
        }
        require(workItemId == null || workItemId.isNotBlank()) {
            "Event work item ID must not be blank when present"
        }
    }
}

/**
 * Generic event without a produced value, response body, secret, or
 * serialization envelope.
 *
 * Live event delivery is observational and may be lossy. Durable audit
 * history requires an explicitly selected observer adapter. [detail] must be
 * bounded and redacted by the event producer.
 */
public data class CrawlerEvent(
    public val type: CrawlerEventType,
    public val context: EventContext,
    public val detail: String? = null,
) {
    init {
        require(detail == null || detail.isNotBlank()) { "Event detail must not be blank when present" }
    }
}

/** Expected observer handling outcomes. */
public sealed interface ObserverResult {
    /** The observer accepted the event. */
    public data object Accepted : ObserverResult

    /** The observer intentionally dropped the event. */
    public data object Dropped : ObserverResult

    /** The observer failed and preserved the original cause. */
    public data class Failed(public val error: Throwable) : ObserverResult
}

/**
 * Receives generic crawler events in registration order.
 *
 * Observers do not own crawler state or produced values. The suspending call
 * runs in the engine-owned structured scope, must honor cancellation, and
 * must not turn a live event stream into an implicit durable audit log.
 */
public interface Observer {
    /** Stable identifier unique within the effective observer list. */
    public val id: String

    /** Handles one bounded event payload. */
    public suspend fun onEvent(event: CrawlerEvent): ObserverResult
}
