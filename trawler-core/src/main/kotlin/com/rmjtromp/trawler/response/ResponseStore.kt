package com.rmjtromp.trawler.response

import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.ResponseReference
import java.time.Instant

/** Optional capability advertised by a response-store adapter. */
public enum class ResponseStoreCapability {
    /** The store can retrieve one stored response by a supported lookup. */
    LOOKUP,

    /** The store can serve a response as a cache candidate. */
    CACHE,

    /** Multiple historical responses for a logical request may coexist. */
    HISTORY,

    /** The store can enumerate/replay stored responses without network calls. */
    REPLAY,

    /** The store can evict data according to its own bounded retention policy. */
    TTL_EVICTION,

    /** The store can execute an adapter-defined bounded range query. */
    RANGE_QUERY,
}

/** A typed lookup request with no generic record or serializer field. */
public sealed interface ResponseLookup {
    /** Finds exactly one response by an opaque store reference. */
    public data class ByReference(
        public val reference: ResponseReference,
    ) : ResponseLookup

    /** Finds the latest stored response associated with a crawler request ID. */
    public data class ByRequestId(
        public val requestId: String,
    ) : ResponseLookup {
        init {
            require(requestId.isNotBlank()) { "Response request ID must not be blank" }
        }
    }

    /** Finds a response by an adapter-defined target string. */
    public data class ByTarget(
        public val target: String,
    ) : ResponseLookup {
        init {
            require(target.isNotBlank()) { "Response target must not be blank" }
        }
    }
}

/** Immutable response plus its optional store-owned reference. */
public data class StoredResponse(
    public val reference: ResponseReference,
    public val response: CrawlResponse,
)

/** Bounded replay selection owned by a response adapter. */
public sealed interface ResponseReplayQuery {
    /** Replays responses associated with one crawler request ID. */
    public data class ByRequestId(public val requestId: String) : ResponseReplayQuery {
        init {
            require(requestId.isNotBlank()) { "Replay request ID must not be blank" }
        }
    }

    /** Replays responses associated with one pipeline identifier. */
    public data class ByPipelineId(public val pipelineId: String) : ResponseReplayQuery {
        init {
            require(pipelineId.isNotBlank()) { "Replay pipeline ID must not be blank" }
        }
    }
}

/**
 * Optional raw-response retention and lookup boundary.
 *
 * Response storage is independent of crawl state and produced-data output.
 * The adapter owns retention, encryption, persistence, replay enumeration,
 * and duplicate behavior. Calls are suspending and must honor cancellation;
 * core never serializes `T` or requires this capability for ordinary
 * extraction. A missing response store is a valid configuration.
 */
public interface ResponseStore {
    /** Stable identifier used for configuration diagnostics. */
    public val id: String

    /** Immutable capability set advertised by this adapter. */
    public val capabilities: Set<ResponseStoreCapability>

    /** Stores [response] and returns an opaque reference when accepted. */
    public suspend fun put(response: CrawlResponse): ResponseReference?

    /** Looks up one stored response, or returns `null` when absent. */
    public suspend fun find(query: ResponseLookup): StoredResponse?
}

/**
 * Focused replay capability for a response store.
 *
 * Replay must read stored responses only, use a caller-provided positive
 * [limit], make zero network calls, and never create ordinary crawl jobs.
 * Cancellation belongs to the adapter-owned query and must release its
 * resources deterministically.
 */
public interface ResponseReplayStore {
    /** Replays at most [limit] responses for [query] in adapter-defined order. */
    public suspend fun replay(query: ResponseReplayQuery, limit: Int): List<StoredResponse>
}

/** Focused bounded-history capability independent of operational crawl state. */
public interface ResponseHistoryStore {
    /** Returns at most [limit] historical responses for one request ID. */
    public suspend fun history(requestId: String, limit: Int): List<StoredResponse>
}

/** Focused retention capability owned by the response adapter. */
public interface ResponseRetentionStore {
    /** Evicts adapter-owned responses older than [cutoff] and returns the count removed. */
    public suspend fun evictBefore(cutoff: Instant): Long
}
