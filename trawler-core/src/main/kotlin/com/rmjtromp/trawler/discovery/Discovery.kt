package com.rmjtromp.trawler.discovery

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.transport.RequestExecutor
import kotlinx.coroutines.flow.Flow

/**
 * Produces crawl work or already-obtained responses for one pipeline.
 *
 * Discovery is an extension-owned policy and does not own a transport client.
 * When it needs I/O it must use the engine-provided [DiscoveryContext.executor]
 * so routing, middleware, safety, pacing, observability, cancellation, and
 * attempt accounting remain visible to the engine. The returned flow is
 * collected in the caller's structured scope; implementations must honor
 * cancellation and must not create unbounded internal buffers.
 */
public interface Discovery {
    /** Stable identifier unique within one pipeline's ordered discovery list. */
    public val id: String

    /** Emits requests or responses in the discovery-defined order. */
    public fun discover(context: DiscoveryContext): Flow<DiscoveryEmission>
}

/** Context and engine-controlled I/O surface supplied to [Discovery]. */
public data class DiscoveryContext(
    public val crawler: CrawlerContext,
    public val executor: RequestExecutor,
)

/** Emission accepted from discovery before admission/deduplication policy. */
public sealed interface DiscoveryEmission {
    /** A new request that may become crawl work. */
    public data class Request(
        public val request: CrawlRequest,
    ) : DiscoveryEmission

    /** A response already obtained by discovery and therefore not fetched again. */
    public data class ExistingResponse(
        public val request: CrawlRequest,
        public val response: CrawlResponse,
    ) : DiscoveryEmission
}
