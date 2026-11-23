package com.rmjtromp.trawler.flaresolverr.challenge

import com.rmjtromp.trawler.flaresolverr.FlareSolverrFailureCategories
import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.model.ResponseContext
import com.rmjtromp.trawler.policy.AttemptSummary
import com.rmjtromp.trawler.policy.FailureClassification
import com.rmjtromp.trawler.policy.FailureContext
import com.rmjtromp.trawler.policy.RetryDecision
import com.rmjtromp.trawler.policy.RetryPolicy
import com.rmjtromp.trawler.transport.RequestExecutor
import com.rmjtromp.trawler.transport.RequestMiddleware
import com.rmjtromp.trawler.transport.ResponseHandler
import com.rmjtromp.trawler.transport.ResponseMiddleware
import com.rmjtromp.trawler.web.policy.CookieHeaderLimitException
import com.rmjtromp.trawler.web.session.CookieSession
import com.rmjtromp.trawler.web.transport.WebOrigin
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.net.URI
import java.time.Instant
import java.util.HashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Solve-then-reuse challenge clearance for an ordinary HTTP transport.
 *
 * Ordinary requests keep using the pipeline's normal transport (typically
 * `KtorTransport`). The pieces exposed here cooperate through one
 * [ClearanceStore]:
 *
 * 1. [responseMiddleware] runs the [detector]; a detected challenge records a
 *    marker for the origin of the hop that served it (the response's final
 *    target) and throws [ChallengeDetectedException], which fails the attempt
 *    visibly. When that hop is on another origin than the request target, the
 *    request target is also linked to the challenged hop target.
 * 2. `FlareSolverrFailureClassifier` maps that exception to
 *    [FlareSolverrFailureCategories.CHALLENGE_DETECTED] and
 *    [ChallengeRetryPolicy] asks the engine for a new attempt.
 * 3. On the next attempt [requestMiddleware] sees the marker for the request
 *    origin, or for the hop origin linked to the request target, and asks the
 *    [resolver] once per origin (concurrent requests to the same origin wait
 *    for and share that single resolution). A hop origin is resolved with the
 *    challenged hop target. For the request origin it then replaces the
 *    request's `User-Agent` with the clearance User-Agent and tags the
 *    request with the clearance generation.
 * 4. The transport's [cookieSession] decorator adds the clearance cookies and
 *    the clearance User-Agent to every hop whose target origin has a
 *    clearance, merged with the delegate cookie jar's cookies.
 *
 * No step retries or loops: every retry is an engine attempt. Resolution
 * happens only after a challenge was observed, never proactively.
 *
 * The instance owns the resolver's lifecycle when the resolver is an
 * [ExtensionLifecycle]: register this object (not the resolver) as an engine
 * extension. Start and stop are forwarded; stop also clears the store.
 * Waiting for an in-progress resolution is bounded by the resolver's own
 * timeouts and by caller cancellation; a cancelled waiter does not cancel the
 * shared resolution. A resolution held by a cancelled leader is cancelled and
 * the next waiter becomes the leader.
 */
public class ChallengeClearance(
    private val resolver: ChallengeResolver,
    private val detector: ChallengeDetector,
    public val store: ClearanceStore,
    override val id: String = "flaresolverr-challenge-clearance",
) : ExtensionLifecycle {
    private class Gate {
        val mutex = Mutex()
        var users: Int = 0
    }

    private val gatesLock = ReentrantLock()
    private val gates = HashMap<WebOrigin, Gate>()

    init {
        require(id.isNotBlank()) { "Challenge clearance ID must not be blank" }
    }

    override suspend fun start(context: ExtensionStartContext) {
        (resolver as? ExtensionLifecycle)?.start(context)
    }

    override suspend fun stop(context: ExtensionStopContext) {
        try {
            (resolver as? ExtensionLifecycle)?.stop(context)
        } finally {
            store.clearAll()
        }
    }

    /**
     * Request middleware that resolves pending challenges and applies the
     * clearance User-Agent. Install it in the pipeline that uses
     * [responseMiddleware] and [cookieSession].
     */
    public val requestMiddleware: RequestMiddleware = object : RequestMiddleware {
        override val id: String = "${this@ChallengeClearance.id}:request"

        override suspend fun intercept(
            request: CrawlRequest,
            context: RequestContext,
            next: RequestExecutor,
        ): CrawlResponse {
            val origin = originOrNull(request.target) ?: return next.execute(request, context)
            val stored = clearanceFor(origin, request.target)
            // A link is recorded only for a hop on another origin than its request target.
            store.redirectedChallenge(request.target)?.let { hop -> clearanceFor(hop.origin, hop.target) }
            if (stored == null) return next.execute(request, context)
            val cleared = request.copy(
                headers = withUserAgent(request.headers, stored.clearance.userAgent),
                attributes = ContextAttributes.from(
                    request.attributes.entries.filterNot { it.name == GENERATION_ATTRIBUTE } +
                        ContextAttribute(GENERATION_ATTRIBUTE, stored.generation.toString()),
                ),
            )
            return next.execute(cleared, context.copy(request = cleared))
        }
    }

    /**
     * Response middleware that converts a detected challenge into
     * [ChallengeDetectedException]. Install it before response middleware
     * that would otherwise consume or reinterpret challenge responses.
     *
     * The challenge is recorded for the origin of the response's final
     * target. A current clearance for that origin is invalidated only when it
     * was sent: on the request origin, by the generation attribute; on a
     * redirect hop to another origin, by the clearance cookies (or, for a
     * cookie-less clearance, the User-Agent) in the headers the transport
     * reports for the final hop. When a current clearance exists whose
     * cookies are all `Secure` and the challenged hop is plain `http`, the
     * clearance can never be sent there and the middleware throws
     * [ChallengeResolutionException] with reason `clearance-not-applicable`
     * instead of asking for a retry that cannot succeed.
     */
    public val responseMiddleware: ResponseMiddleware = object : ResponseMiddleware {
        override val id: String = "${this@ChallengeClearance.id}:response"

        override suspend fun intercept(
            response: CrawlResponse,
            context: ResponseContext,
            next: ResponseHandler,
        ): CrawlResponse {
            val detection = detector.detect(response)
            if (detection !is ChallengeDetection.Detected) return next.handle(response, context)
            val challengedTarget = response.finalTarget ?: context.request.target
            val origin = originOrNull(challengedTarget) ?: return next.handle(response, context)
            val current = store.current(origin)
            if (current != null && current.clearance.blockedByPlainHttp(challengedTarget, store.now())) {
                throw ChallengeResolutionException(detection.kind, "clearance-not-applicable")
            }
            val requestOrigin = originOrNull(context.request.target)
            val sentGeneration = if (requestOrigin == origin) {
                context.request.attributes[GENERATION_ATTRIBUTE]?.toLongOrNull()
            } else {
                current?.takeIf { it.clearance.wasSentOn(response.request, origin) }?.generation
            }
            store.recordChallenge(origin, detection.kind, sentGeneration)
            if (requestOrigin != null && requestOrigin != origin) {
                store.recordRedirectedChallenge(context.request.target, origin, challengedTarget)
            }
            throw ChallengeDetectedException(detection.kind, detection.signal, origin)
        }
    }

    /**
     * Wraps [delegate] so clearance is added for matching origins.
     *
     * The transport consults the session for every redirect hop, so this is
     * where a hop on another origin than the request target receives that
     * origin's clearance. The decorator honors the `CookieSession` contract:
     * when the caller already supplied a `Cookie` header, the headers are
     * passed to the delegate unchanged and no clearance is added. Otherwise,
     * when the hop's exact origin has a current clearance, every `User-Agent`
     * header is replaced by the clearance User-Agent (clearance is bound to
     * it), the delegate's cookies are kept, clearance cookies replace delegate
     * cookies of the same name, and the merged header is bounded by
     * [ClearanceStoreConfig.maxCookieHeaderLength]. Responses are observed
     * only by the delegate; clearance is never written into its jar.
     */
    public fun cookieSession(delegate: CookieSession): CookieSession = ClearanceCookieSession(delegate, store)

    private suspend fun clearanceFor(origin: WebOrigin, target: String): StoredClearance? {
        store.current(origin)?.let { return it }
        if (store.pendingChallenge(origin) == null) return null
        val gate = acquireGate(origin)
        try {
            return gate.mutex.withLock {
                store.current(origin)?.let { return@withLock it }
                val pending = store.pendingChallenge(origin) ?: return@withLock null
                if (store.inResolutionCooldown(origin)) {
                    throw ChallengeResolutionException(pending.kind, "resolution-cooldown")
                }
                val clearance = try {
                    resolver.resolve(ChallengeResolutionRequest(origin, target, pending.kind))
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (error: Throwable) {
                    store.recordResolutionFailure(origin)
                    throw ChallengeResolutionException(pending.kind, "resolver-failed", error)
                }
                if (clearance.origin != origin) {
                    store.recordResolutionFailure(origin)
                    throw ChallengeResolutionException(pending.kind, "origin-mismatch")
                }
                store.store(clearance)
            }
        } finally {
            releaseGate(origin, gate)
        }
    }

    private fun acquireGate(origin: WebOrigin): Gate = gatesLock.withLock {
        gates.getOrPut(origin) { Gate() }.also { it.users++ }
    }

    private fun releaseGate(origin: WebOrigin, gate: Gate) {
        gatesLock.withLock {
            gate.users--
            if (gate.users == 0 && gates[origin] === gate) gates.remove(origin)
        }
    }

    public companion object {
        /** Non-sensitive request attribute carrying the applied clearance generation. */
        public const val GENERATION_ATTRIBUTE: String = "trawler.flaresolverr.clearance-generation"

        private const val USER_AGENT: String = "User-Agent"
        private const val COOKIE: String = "Cookie"

        internal fun withUserAgent(headers: Headers, userAgent: String): Headers = Headers.from(
            headers.entries.filterNot { it.name.equals(USER_AGENT, ignoreCase = true) } + Header(USER_AGENT, userAgent),
        )

        /** Whether this clearance's cookies are on [request], a hop the transport reports as sent to [origin]. */
        private fun Clearance.wasSentOn(request: CrawlRequest, origin: WebOrigin): Boolean {
            if (originOrNull(request.target) != origin) return false
            if (cookies.isEmpty()) return request.headers[USER_AGENT].any { it == userAgent }
            val sent = request.headers[COOKIE].flatMap { it.split(';') }.map { it.trim() }.toSet()
            return cookies.any { "${it.name}=${it.value}" in sent }
        }

        /** Whether no cookie can be sent to plain-http [target] although one would apply over https. */
        private fun Clearance.blockedByPlainHttp(target: String, now: Instant): Boolean {
            val uri = URI(target)
            if (uri.scheme.equals("https", ignoreCase = true)) return false
            val path = uri.rawPath ?: "/"
            return cookies.none { it.appliesTo(path, https = false, now = now) } &&
                cookies.any { it.appliesTo(path, https = true, now = now) }
        }
    }
}

/**
 * Retry decorator that requests a visible engine retry after a challenge.
 *
 * Failures classified as [FlareSolverrFailureCategories.CHALLENGE_DETECTED]
 * return [RetryDecision.RetryNow] while the failed attempt number is below
 * [maxChallengeAttempts]; the next engine attempt then resolves the
 * challenge in [ChallengeClearance.requestMiddleware]. All other failures,
 * including resolution failures, are decided by [delegate]. The engine's own
 * `maxAttemptsPerWorkItem` bound still applies.
 */
public class ChallengeRetryPolicy(
    private val delegate: RetryPolicy,
    private val maxChallengeAttempts: Int = 3,
) : RetryPolicy {
    override val id: String = "challenge-retry+${delegate.id}"

    init {
        require(maxChallengeAttempts > 0) { "Challenge attempt bound must be positive" }
    }

    override fun decide(
        failure: FailureClassification,
        attempt: AttemptSummary,
        context: FailureContext,
    ): RetryDecision {
        if (failure.category != FlareSolverrFailureCategories.CHALLENGE_DETECTED) {
            return delegate.decide(failure, attempt, context)
        }
        return if (attempt.attemptNumber < maxChallengeAttempts) RetryDecision.RetryNow else RetryDecision.DoNotRetry
    }
}

private class ClearanceCookieSession(
    private val delegate: CookieSession,
    private val store: ClearanceStore,
) : CookieSession {
    override val id: String = "flaresolverr-clearance+${delegate.id}"

    override suspend fun apply(target: String, headers: Headers): Headers {
        if (headers[COOKIE].isNotEmpty()) return delegate.apply(target, headers)
        val base = delegate.apply(target, headers)
        val origin = originOrNull(target) ?: return base
        val stored = store.current(origin) ?: return base
        val withAgent = ChallengeClearance.withUserAgent(base, stored.clearance.userAgent)
        val uri = URI(target)
        val now = store.now()
        val https = uri.scheme.equals("https", ignoreCase = true)
        val applicable = stored.clearance.cookies.filter { it.appliesTo(uri.rawPath ?: "/", https, now) }
        if (applicable.isEmpty()) return withAgent
        val names = applicable.map { it.name }.toSet()
        val delegatePairs = base[COOKIE]
            .flatMap { it.split(';') }
            .map { it.trim() }
            .filter { it.isNotEmpty() && it.substringBefore('=').trim() !in names }
        val value = (delegatePairs + applicable.map { "${it.name}=${it.value}" }).joinToString("; ")
        val limit = store.configuration.maxCookieHeaderLength
        if (value.length > limit) throw CookieHeaderLimitException(limit)
        return Headers.from(withAgent.entries.filterNot { it.name.equals(COOKIE, ignoreCase = true) } + Header(COOKIE, value))
    }

    override suspend fun observe(target: String, setCookieHeaders: List<String>) {
        delegate.observe(target, setCookieHeaders)
    }

    private companion object {
        const val COOKIE: String = "Cookie"
    }
}

internal fun originOrNull(target: String): WebOrigin? = try {
    WebOrigin.fromTarget(target).takeIf { it.scheme == "http" || it.scheme == "https" }
} catch (_: IllegalArgumentException) {
    null
}
