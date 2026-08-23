package com.rmjtromp.trawler.transport

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.model.ResponseContext

/**
 * Executes one immutable request using an adapter-owned transport.
 *
 * Core owns the execution boundary and lifecycle; the implementation owns
 * clients, sockets, files, browser sessions, or other resources. The
 * suspending call must propagate cancellation and close those resources
 * deterministically. Retries hidden inside a transport are forbidden because
 * they would bypass engine attempt accounting and policy observability.
 */
public interface Transport {
    /** Stable identifier used for routing diagnostics and validation. */
    public val id: String

    /** Executes [request] and returns an immutable transport response. */
    public suspend fun execute(request: CrawlRequest): CrawlResponse
}

/**
 * Selects the transport for a request.
 *
 * Routing is an extension policy. It may select a custom, cached, file,
 * browser, API, or HTTP transport, but the selected implementation remains
 * engine-owned for the duration of the call and must honor cancellation.
 */
public interface TransportRouter {
    /** Stable identifier used for deterministic configuration diagnostics. */
    public val id: String

    /** Selects exactly one transport for [request]. */
    public fun select(request: CrawlRequest, context: RequestContext): Transport
}

/** Engine-controlled request path exposed to discovery and other extensions. */
public fun interface RequestExecutor {
    /** Executes [request] through routing, middleware, transport, and response handling. */
    public suspend fun execute(request: CrawlRequest, context: RequestContext): CrawlResponse
}

/**
 * Wraps the request path before the selected transport runs.
 *
 * Middleware composition is ordered. Implementations must call [next] at
 * most as defined by their contract, must not hide retry loops, and must
 * propagate cancellation. They may return a response directly for a
 * deliberate cache/offline decision, but that behavior must be documented by
 * the owning extension.
 */
public interface RequestMiddleware {
    /** Stable identifier unique within the effective request chain. */
    public val id: String

    /** Intercepts [request] and optionally delegates to [next]. */
    public suspend fun intercept(
        request: CrawlRequest,
        context: RequestContext,
        next: RequestExecutor,
    ): CrawlResponse
}

/**
 * Wraps an immutable response after transport execution.
 *
 * Response middleware runs in declared order and remains inside the owning
 * structured scope. It may classify or enrich a response, but it must not
 * create a hidden client or turn retries into invisible transport behavior.
 */
public interface ResponseMiddleware {
    /** Stable identifier unique within the effective response chain. */
    public val id: String

    /** Intercepts [response] and optionally delegates to [next]. */
    public suspend fun intercept(
        response: CrawlResponse,
        context: ResponseContext,
        next: ResponseHandler,
    ): CrawlResponse
}

/** Continuation used by response middleware composition. */
public fun interface ResponseHandler {
    /** Handles [response] at the next response stage. */
    public suspend fun handle(response: CrawlResponse, context: ResponseContext): CrawlResponse
}
