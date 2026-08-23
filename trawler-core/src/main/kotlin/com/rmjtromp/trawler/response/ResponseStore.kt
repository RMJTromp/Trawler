package com.rmjtromp.trawler.response

import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.ResponseReference
import java.time.Instant
import java.util.Collections

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
    /** Pipeline ownership captured by the store, when the response was produced by the engine. */
    public val pipelineId: String? = response.request.attributes[InMemoryResponseStore.PIPELINE_ID_ATTRIBUTE],
) {
    init {
        require(pipelineId == null || pipelineId.isNotBlank()) {
            "Stored response pipeline ID must not be blank when present"
        }
    }
}

/** Bounded replay selection owned by a response adapter. */
public sealed interface ResponseReplayQuery {
    /** Replays one response by its opaque store reference. */
    public data class ByReference(public val reference: ResponseReference) : ResponseReplayQuery

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
 * Caller-supplied response encoding used by file and PostgreSQL stores.
 *
 * The codec owns the representation of a raw [CrawlResponse]. Core only
 * frames and bounds the returned bytes at the adapter boundary; it never
 * selects JSON, annotations, a schema, or a serializer. Encoding is
 * synchronous and must be bounded, deterministic for the caller's chosen
 * format, and free of network or database access. Decoding failures are
 * propagated to the selected store's caller. The store owns cancellation and
 * closes its file/database resources around codec calls.
 */
public interface ResponseCodec {
    /** Stable codec identity persisted beside encoded response bytes. */
    public val descriptor: ResponseCodecDescriptor

    /** Encodes one immutable response into caller-owned bytes. */
    public fun encode(response: CrawlResponse): ByteArray

    /** Decodes one payload written by [descriptor]. */
    public fun decode(payload: ByteArray): CrawlResponse
}

/** Versioned identity for one caller-supplied response codec. */
public data class ResponseCodecDescriptor(
    public val id: String,
    public val version: Int,
) {
    init {
        require(id.isNotBlank()) { "Response codec ID must not be blank" }
        require(version >= 0) { "Response codec version must not be negative" }
    }
}

/**
 * Bounded registry for active and historical response codecs. Stores select
 * codecs by the exact persisted descriptor; a missing historical codec is a
 * visible decode failure rather than silent reinterpretation.
 */
public class ResponseCodecRegistry(
    codecs: List<ResponseCodec>,
    active: ResponseCodecDescriptor? = null,
) {
    private val codecs: List<ResponseCodec> = codecs.toList()
    private val activeDescriptor: ResponseCodecDescriptor = active ?: this.codecs.firstOrNull()?.descriptor
        ?: error("At least one response codec is required")

    init {
        require(this.codecs.isNotEmpty()) { "At least one response codec is required" }
        require(this.codecs.map { it.descriptor }.distinct().size == this.codecs.size) {
            "Response codec ID/version pairs must be unique"
        }
        require(this.codecs.any { it.descriptor == activeDescriptor }) {
            "Active response codec is not registered"
        }
    }

    /** Encodes with the explicitly active codec. */
    public fun encode(response: CrawlResponse): EncodedResponse {
        val codec = codec(activeDescriptor)
        return EncodedResponse.from(codec.descriptor, codec.encode(response))
    }

    /** Decodes using the exact descriptor persisted with the response. */
    public fun decode(descriptor: ResponseCodecDescriptor, payload: ByteArray): CrawlResponse =
        codec(descriptor).decode(payload.copyOf())

    private fun codec(descriptor: ResponseCodecDescriptor): ResponseCodec =
        codecs.firstOrNull { it.descriptor == descriptor }
            ?: throw IllegalStateException(
                "No response codec is registered for ${descriptor.id}@${descriptor.version}",
            )
}

/** Immutable encoded response payload with defensive byte ownership. */
public class EncodedResponse private constructor(
    public val descriptor: ResponseCodecDescriptor,
    private val bytes: ByteArray,
) {
    /** Number of encoded bytes. */
    public val size: Int
        get() = bytes.size

    /** Returns a defensive payload copy. */
    public fun toByteArray(): ByteArray = bytes.copyOf()

    public companion object {
        /** Creates an immutable encoded response snapshot. */
        public fun from(descriptor: ResponseCodecDescriptor, bytes: ByteArray): EncodedResponse =
            EncodedResponse(descriptor, bytes.copyOf())
    }
}

/**
 * Optional lifecycle hook for response stores that own dispatchers or file/
 * database handles. The engine closes selected stores during shutdown when
 * they implement this capability; standalone callers must close them in a
 * `finally` path. Close is idempotent and cancellation-safe at the adapter
 * boundary.
 */
public interface ResponseStoreLifecycle {
    /** Releases adapter-owned resources and prevents further operations. */
    public suspend fun close()
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
    /**
     * Replays at most [limit] responses for [query] in adapter-defined order.
     * When [pipelineId] is supplied, the adapter must apply that ownership
     * predicate before limiting results; post-read filtering is insufficient.
     */
    public suspend fun replay(
        query: ResponseReplayQuery,
        limit: Int,
        pipelineId: String? = null,
    ): List<StoredResponse>
}

/** Focused bounded-history capability independent of operational crawl state. */
public interface ResponseHistoryStore {
    /** Returns at most [limit] historical responses for one request ID. */
    public suspend fun history(requestId: String, limit: Int): List<StoredResponse>
}

/** Bounded, caller-supplied retention request for response history. */
public class ResponseRetentionRequest(
    public val cutoff: Instant,
    public val maxEntries: Int,
    protectedReferences: Set<ResponseReference> = emptySet(),
) {
    /** References that must remain available for an external lineage. */
    public val protectedReferences: Set<ResponseReference> =
        Collections.unmodifiableSet(protectedReferences.toSet())

    init {
        require(maxEntries > 0) { "Response retention batch must be positive" }
        require(this.protectedReferences.size <= MAX_PROTECTED_REFERENCES) {
            "Response retention protected-reference bound is $MAX_PROTECTED_REFERENCES"
        }
    }

    private companion object {
        const val MAX_PROTECTED_REFERENCES: Int = 4_096
    }
}

/** Result of one bounded response-retention operation. */
public data class ResponseRetentionResult(
    public val deleted: Long,
    public val protectedEntries: Long,
)

/** Focused retention capability owned by the response adapter. */
public interface ResponseRetentionStore {
    /** Evicts adapter-owned responses older than [cutoff] and returns the count removed. */
    public suspend fun evictBefore(cutoff: Instant): Long

    /**
     * Evicts at most [request.maxEntries] eligible responses. Implementations
     * must not remove [ResponseRetentionRequest.protectedReferences]. A
     * durable adapter must also honor any lineage it owns internally; callers
     * must not interpret a deletion count as proof that no external lineage
     * existed.
     */
    public suspend fun prune(request: ResponseRetentionRequest): ResponseRetentionResult
}
