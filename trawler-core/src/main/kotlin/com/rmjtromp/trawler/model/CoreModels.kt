package com.rmjtromp.trawler.model

import java.time.Instant
import java.util.Collections

/**
 * A named, non-sensitive crawler attribute.
 *
 * Attributes belong to the caller or extension that created the surrounding
 * context; core treats them as opaque diagnostics and does not serialize,
 * persist, or log them. The immutable snapshot can be shared between
 * suspending operations and is never mutated by core. An extension must keep
 * ownership of any sensitive values and redact them before handing them to an
 * observer or durable adapter.
 */
public data class ContextAttribute(
    public val name: String,
    public val value: String,
) {
    init {
        require(name.isNotBlank()) { "Attribute name must not be blank" }
    }
}

/**
 * Immutable string metadata attached to a request, response, or crawler
 * context.
 *
 * This is deliberately not a generic map or a serialization envelope. It is
 * safe to pass across suspension points, and it carries no cleanup or
 * cancellation responsibility. Values are only meaningful to the owner that
 * defines their names.
 */
public class ContextAttributes private constructor(
    attributes: List<ContextAttribute>,
) {
    public val entries: List<ContextAttribute> = immutableSnapshot(attributes)

    /** Returns the first value stored under [name], or `null` when absent. */
    public operator fun get(name: String): String? = entries.firstOrNull { it.name == name }?.value

    /** Returns a new immutable attribute set containing [attribute]. */
    public fun plus(attribute: ContextAttribute): ContextAttributes =
        from(entries + attribute)

    override fun equals(other: Any?): Boolean =
        other is ContextAttributes && entries == other.entries

    override fun hashCode(): Int = entries.hashCode()

    override fun toString(): String = "ContextAttributes(entries=$entries)"

    public companion object {
        /** Returns an empty immutable attribute set. */
        public fun empty(): ContextAttributes = ContextAttributes(emptyList())

        /** Creates an immutable snapshot of [attributes]. */
        public fun from(attributes: Iterable<ContextAttribute>): ContextAttributes =
            ContextAttributes(attributes.toList())

        /** Creates an immutable attribute set from the supplied entries. */
        public fun of(vararg attributes: ContextAttribute): ContextAttributes =
            ContextAttributes(attributes.toList())
    }
}

/** A single immutable header entry. Duplicate names are allowed. */
public data class Header(
    public val name: String,
    public val value: String,
) {
    init {
        require(name.isNotBlank()) { "Header name must not be blank" }
    }
}

/**
 * Immutable ordered headers used by any transport.
 *
 * Core does not interpret header names, redact values, or assume HTTP
 * semantics. A transport or middleware owns those decisions. Header values
 * may contain secrets, so implementations must not place them in default
 * events or logs.
 */
public class Headers private constructor(
    headers: List<Header>,
) {
    public val entries: List<Header> = immutableSnapshot(headers)

    /** Returns all values for [name], preserving their original order. */
    public operator fun get(name: String): List<String> =
        immutableSnapshot(entries.filter { it.name.equals(name, ignoreCase = true) }.map { it.value })

    /** Returns a new immutable header set containing [header]. */
    public fun plus(header: Header): Headers = from(entries + header)

    override fun equals(other: Any?): Boolean = other is Headers && entries == other.entries

    override fun hashCode(): Int = entries.hashCode()

    override fun toString(): String = "Headers(entries=$entries)"

    public companion object {
        /** Returns an empty immutable header set. */
        public fun empty(): Headers = Headers(emptyList())

        /** Creates an immutable snapshot of [headers]. */
        public fun from(headers: Iterable<Header>): Headers = Headers(headers.toList())

        /** Creates an immutable header set from the supplied entries. */
        public fun of(vararg headers: Header): Headers = Headers(headers.toList())
    }
}

/**
 * An immutable request body owned by the caller or transport.
 *
 * The byte array is copied on construction and access. Core never interprets
 * or serializes the content, and a transport remains responsible for closing
 * any resources it uses to produce a request body before cancellation.
 */
public class RequestBody private constructor(
    private val content: ByteArray,
) {
    public val size: Int
        get() = content.size

    /** Returns a defensive copy of the body bytes. */
    public fun toByteArray(): ByteArray = content.copyOf()

    override fun equals(other: Any?): Boolean =
        other is RequestBody && content.contentEquals(other.content)

    override fun hashCode(): Int = content.contentHashCode()

    override fun toString(): String = "RequestBody(size=$size)"

    public companion object {
        /** Creates an immutable body by copying [content]. */
        public fun from(content: ByteArray): RequestBody = RequestBody(content.copyOf())

        /** Returns an immutable empty request body. */
        public fun empty(): RequestBody = RequestBody(ByteArray(0))
    }
}

/**
 * An immutable response body owned by the transport or response store.
 *
 * The byte array is copied on construction and access. Response retention is
 * optional and belongs to [com.rmjtromp.trawler.response.ResponseStore]; core
 * does not archive or replay this content by itself.
 */
public class ResponseBody private constructor(
    private val content: ByteArray,
) {
    public val size: Int
        get() = content.size

    /** Returns a defensive copy of the body bytes. */
    public fun toByteArray(): ByteArray = content.copyOf()

    override fun equals(other: Any?): Boolean =
        other is ResponseBody && content.contentEquals(other.content)

    override fun hashCode(): Int = content.contentHashCode()

    override fun toString(): String = "ResponseBody(size=$size)"

    public companion object {
        /** Creates an immutable body by copying [content]. */
        public fun from(content: ByteArray): ResponseBody = ResponseBody(content.copyOf())

        /** Returns an immutable empty response body. */
        public fun empty(): ResponseBody = ResponseBody(ByteArray(0))
    }
}

/** A transport-neutral operation name, such as `GET`, `PUT`, or `READ`. */
@JvmInline
public value class RequestMethod(public val value: String) {
    init {
        require(value.isNotBlank()) { "Request method must not be blank" }
    }

    public companion object {
        /** Conventional read operation for HTTP-like transports. */
        public val GET: RequestMethod = RequestMethod("GET")
    }
}

/**
 * Immutable work description passed to a transport.
 *
 * The target is an opaque caller-owned identifier; it need not be a URL. The
 * request can be used by custom, file, cached, API, browser, or HTTP
 * transports. Core does not assign identity, canonicalize the target, or
 * create hidden network clients.
 */
public data class CrawlRequest(
    public val target: String,
    public val method: RequestMethod = RequestMethod.GET,
    public val headers: Headers = Headers.empty(),
    public val body: RequestBody? = null,
    public val attributes: ContextAttributes = ContextAttributes.empty(),
) {
    init {
        require(target.isNotBlank()) { "Request target must not be blank" }
    }
}

/**
 * Immutable result returned by a transport.
 *
 * A response is owned by the transport until an optional response store
 * accepts it. The transport/store implementation must apply a configured
 * body-size bound before constructing the response. Extraction and middleware
 * may read it across suspension points; cancellation and resource cleanup
 * remain the responsibility of the transport/store implementation.
 */
public data class CrawlResponse(
    public val request: CrawlRequest,
    public val statusCode: Int?,
    public val headers: Headers,
    public val body: ResponseBody,
    public val receivedAt: Instant,
    public val finalTarget: String? = null,
    public val attributes: ContextAttributes = ContextAttributes.empty(),
) {
    init {
        require(statusCode == null || statusCode in 0..999) {
            "Response status code must be between 0 and 999 when present"
        }
        require(finalTarget == null || finalTarget.isNotBlank()) {
            "Final target must not be blank when present"
        }
    }
}

/**
 * Shared run/request context owned by the engine.
 *
 * Context identifiers are metadata only; they are not user record keys. The
 * context is immutable and safe to pass to suspending extensions. Extensions
 * must stop their work when the owning coroutine is cancelled and must not
 * retain the context after their configured lifecycle ends.
 */
public data class CrawlerContext(
    public val engineRunId: String,
    public val pipelineRunId: String,
    public val pipelineId: String,
    public val requestId: String? = null,
    public val attemptId: String? = null,
    public val attributes: ContextAttributes = ContextAttributes.empty(),
) {
    init {
        require(engineRunId.isNotBlank()) { "Engine run ID must not be blank" }
        require(pipelineRunId.isNotBlank()) { "Pipeline run ID must not be blank" }
        require(pipelineId.isNotBlank()) { "Pipeline ID must not be blank" }
        require(requestId == null || requestId.isNotBlank()) { "Request ID must not be blank when present" }
        require(attemptId == null || attemptId.isNotBlank()) { "Attempt ID must not be blank when present" }
    }
}

/** Context supplied to request middleware and transport routing. */
public data class RequestContext(
    public val crawler: CrawlerContext,
    public val request: CrawlRequest,
)

/** Context supplied to response middleware. */
public data class ResponseContext(
    public val crawler: CrawlerContext,
    public val request: CrawlRequest,
    public val response: CrawlResponse,
)

/** Context supplied to extraction of one response. */
public data class ExtractionContext(
    public val crawler: CrawlerContext,
    public val request: CrawlRequest,
    public val response: CrawlResponse,
)

/** Context supplied to a same-type transformation. */
public data class TransformContext(
    public val crawler: CrawlerContext,
    public val request: CrawlRequest?,
)

/** Context supplied to validation of one transformed value. */
public data class ValidationContext(
    public val crawler: CrawlerContext,
    public val request: CrawlRequest?,
)

/** Context supplied to a typed output for one acknowledged batch. */
public data class OutputContext(
    public val crawler: CrawlerContext,
)

/** Opaque reference returned by a response store. */
@JvmInline
public value class ResponseReference(public val value: String) {
    init {
        require(value.isNotBlank()) { "Response reference must not be blank" }
    }
}

internal fun <T> immutableSnapshot(values: Iterable<T>): List<T> =
    Collections.unmodifiableList(values.toList())
