package com.rmjtromp.trawler.response

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.policy.Clock
import java.time.Duration

/** Context supplied to the selected response-capture policy. */
public data class ResponseCaptureContext(
    public val crawler: CrawlerContext,
    public val request: CrawlRequest,
    public val response: CrawlResponse,
)

/** Expected decision made before an optional response store is called. */
public sealed interface ResponseCaptureDecision {
    /** Retain the response when a store is selected. */
    public data object Store : ResponseCaptureDecision

    /** Continue extraction without retaining this response. */
    public data class Skip(public val reason: String? = null) : ResponseCaptureDecision

    /** Treat inability to retain this response as a processing failure. */
    public data class Fail(public val error: Throwable) : ResponseCaptureDecision
}

/**
 * Replaceable response-capture policy.
 *
 * The policy decides only whether the independent response store is asked to
 * retain a response. It does not own crawl state, produced values, or output
 * serialization. The decision runs in the pipeline coroutine and must be
 * cancellation-cooperative; a thrown exception is preserved by the engine.
 */
public interface ResponseCapturePolicy {
    /** Stable identifier used in deterministic configuration diagnostics. */
    public val id: String

    /** Whether configuration must provide a response store. */
    public val requiresStore: Boolean

    /** Decides whether [context.response] should be offered to the store. */
    public fun decide(context: ResponseCaptureContext): ResponseCaptureDecision
}

/** Default policy: retain every response when a store is explicitly selected. */
public object CaptureResponsesWhenConfigured : ResponseCapturePolicy {
    override val id: String = "capture-when-configured"
    override val requiresStore: Boolean = false

    override fun decide(context: ResponseCaptureContext): ResponseCaptureDecision =
        ResponseCaptureDecision.Store
}

/** Explicit no-retention policy even when a response store is installed. */
public object DoNotCaptureResponses : ResponseCapturePolicy {
    override val id: String = "capture-none"
    override val requiresStore: Boolean = false

    override fun decide(context: ResponseCaptureContext): ResponseCaptureDecision =
        ResponseCaptureDecision.Skip("response-capture-disabled")
}

/**
 * Required archival policy. A selected store must accept every offered
 * response or the engine fails the work before extraction. This is useful for
 * workflows whose replay or audit contract depends on complete capture.
 */
public object RequireResponseCapture : ResponseCapturePolicy {
    override val id: String = "capture-required"
    override val requiresStore: Boolean = true

    override fun decide(context: ResponseCaptureContext): ResponseCaptureDecision =
        ResponseCaptureDecision.Store
}

/** Context supplied to a cache lookup before a transport is invoked. */
public data class ResponseCacheContext(
    public val crawler: CrawlerContext,
    public val request: CrawlRequest,
    public val now: java.time.Instant,
)

/** Expected result of a response-cache lookup. */
public sealed interface ResponseCacheDecision {
    /** No usable stored response exists; the normal transport path continues. */
    public data object Miss : ResponseCacheDecision

    /** The normal transport path may use this stored response without a network call. */
    public data class Hit(public val stored: StoredResponse) : ResponseCacheDecision
}

/**
 * Optional cache policy consulted by the engine before transport execution.
 *
 * A cache policy is independent from response retention and requires an
 * explicitly selected store with the capabilities it declares. The lookup is
 * bounded by the store contract and runs under the engine's structured scope;
 * a cache hit must never create a crawl job or hidden client.
 */
public interface ResponseCachePolicy {
    /** Stable identifier used in configuration diagnostics. */
    public val id: String

    /** Store capabilities required before this policy can be activated. */
    public val requiredCapabilities: Set<ResponseStoreCapability>

    /** Looks up a response or returns [ResponseCacheDecision.Miss]. */
    public suspend fun lookup(
        context: ResponseCacheContext,
        store: ResponseStore,
    ): ResponseCacheDecision
}

/** Disabled cache policy; the transport is always consulted. */
public object NoResponseCachePolicy : ResponseCachePolicy {
    override val id: String = "cache-disabled"
    override val requiredCapabilities: Set<ResponseStoreCapability> = emptySet()

    override suspend fun lookup(
        context: ResponseCacheContext,
        store: ResponseStore,
    ): ResponseCacheDecision = ResponseCacheDecision.Miss
}

/**
 * Uses the newest stored response for the same opaque target while it is
 * younger than [maxAge]. The target remains transport-neutral; callers that
 * need method/body/credential variants must provide a stricter custom policy.
 */
public class LatestTargetResponseCachePolicy(
    public val maxAge: Duration? = null,
) : ResponseCachePolicy {
    override val id: String = "latest-target-cache"
    override val requiredCapabilities: Set<ResponseStoreCapability> = setOf(
        ResponseStoreCapability.LOOKUP,
        ResponseStoreCapability.CACHE,
    )

    init {
        require(maxAge == null || (!maxAge.isNegative && !maxAge.isZero)) {
            "Response cache max age must be positive when present"
        }
    }

    override suspend fun lookup(
        context: ResponseCacheContext,
        store: ResponseStore,
    ): ResponseCacheDecision {
        val stored = store.find(ResponseLookup.ByTarget(context.request.target)) ?: return ResponseCacheDecision.Miss
        if (stored.pipelineId != context.crawler.pipelineId) return ResponseCacheDecision.Miss
        val age = Duration.between(stored.response.receivedAt, context.now)
        if (maxAge != null && !age.isNegative && !age.isZero && age > maxAge) {
            return ResponseCacheDecision.Miss
        }
        return ResponseCacheDecision.Hit(stored)
    }
}

/**
 * Convenience lookup helper for callers implementing a custom cache policy.
 * It intentionally does not interpret headers, status codes, or freshness
 * metadata; those decisions remain extension-owned.
 */
public suspend fun ResponseStore.latestByRequestTarget(request: CrawlRequest): StoredResponse? =
    find(ResponseLookup.ByTarget(request.target))

/** Builds a cache context from an existing request context and an injected clock. */
public fun ResponseCacheContext(
    requestContext: RequestContext,
    clock: Clock,
): ResponseCacheContext = ResponseCacheContext(
    crawler = requestContext.crawler,
    request = requestContext.request,
    now = clock.now(),
)
