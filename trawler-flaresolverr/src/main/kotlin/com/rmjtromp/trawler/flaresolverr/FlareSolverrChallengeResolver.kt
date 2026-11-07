package com.rmjtromp.trawler.flaresolverr

import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.flaresolverr.challenge.ChallengeResolutionRequest
import com.rmjtromp.trawler.flaresolverr.challenge.ChallengeResolver
import com.rmjtromp.trawler.flaresolverr.challenge.Clearance
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.SystemClock
import com.rmjtromp.trawler.web.transport.WebDestinationPolicy
import io.ktor.client.engine.HttpClientEngine
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.util.concurrent.atomic.AtomicBoolean

/** Immutable bounds and identity for [FlareSolverrChallengeResolver]. */
public data class FlareSolverrResolverConfig(
    public val service: FlareSolverrServiceConfig,
    public val id: String = "flaresolverr-challenge-resolver",
    public val maxTargetLength: Int = 16 * 1024,
    public val maxCookies: Int = 64,
    public val maxCookieValueLength: Int = 8_192,
    public val maxUserAgentLength: Int = 1_024,
) {
    init {
        require(id.isNotBlank()) { "FlareSolverr resolver ID must not be blank" }
        require(maxTargetLength > 0) { "Target length bound must be positive" }
        require(maxCookies in 1..1_024) { "Cookie bound must be between 1 and 1024" }
        require(maxCookieValueLength > 0) { "Cookie value bound must be positive" }
        require(maxUserAgentLength > 0) { "User-Agent length bound must be positive" }
        require(service.proxy?.hasCredentials != true) {
            "FlareSolverr accepts proxy credentials only through sessions.create, which the resolver does not use"
        }
    }
}

/**
 * [ChallengeResolver] that asks FlareSolverr to load the challenged target
 * once with `returnOnlyCookies`, then returns the browser's cookies for the
 * challenged origin together with the browser User-Agent.
 *
 * The target must be HTTP(S) on the requested origin and is evaluated by
 * [WebDestinationPolicy] before the call; the final URL is evaluated again
 * when it differs. Navigation inside FlareSolverr's browser cannot be checked
 * per hop. No FlareSolverr session is used, so each resolution launches a
 * fresh browser and leaves no service-side state. Each call is one command
 * with no retry, bounded by the service timeouts and call permits.
 *
 * Cookies from other domains or with values unsafe for a `Cookie` header are
 * dropped. The resolver owns one Ktor CIO client; it is normally owned by a
 * `ChallengeClearance`, which forwards [start] and [stop].
 */
public class FlareSolverrChallengeResolver private constructor(
    private val configuration: FlareSolverrResolverConfig,
    private val destination: WebDestinationPolicy,
    private val clock: Clock,
    engine: HttpClientEngine?,
) : ChallengeResolver, ExtensionLifecycle, AutoCloseable {
    override val id: String = configuration.id

    private val client = FlareSolverrClient(configuration.service, engine)
    private val closed = AtomicBoolean(false)
    private val limits = SolutionLimits(
        maxTargetLength = configuration.maxTargetLength,
        maxResponseHeaders = 256,
        maxHeaderNameLength = 1_024,
        maxHeaderValueLength = configuration.maxCookieValueLength,
        maxResponseCookies = configuration.maxCookies * 4,
        maxCookieValueLength = configuration.maxCookieValueLength,
        maxUserAgentLength = configuration.maxUserAgentLength,
    )

    /** Performs no network work; fails when the resolver was already closed. */
    override suspend fun start(context: ExtensionStartContext) {
        check(!closed.get()) { "FlareSolverr resolver is closed" }
    }

    /** Closes the owned client. Idempotent. */
    override suspend fun stop(context: ExtensionStopContext) {
        close()
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) client.close()
    }

    override suspend fun resolve(request: ChallengeResolutionRequest): Clearance {
        check(!closed.get()) { "FlareSolverr resolver is closed" }
        val targetOrigin = requireHttpTarget(request.target, configuration.maxTargetLength)
        if (targetOrigin != request.origin) throw UnsupportedFlareSolverrRequestException("target-origin-mismatch")
        val probe = CrawlRequest(request.target)
        admitDestination(destination, probe)
        val root = client.execute(
            buildJsonObject {
                put("cmd", "request.get")
                put("url", request.target)
                put("maxTimeout", configuration.service.maxTimeout.toMillis())
                put("returnOnlyCookies", true)
                configuration.service.proxy?.let { putProxy(it, includeCredentials = false) }
            },
            configuration.service.solveCallTimeout,
        )
        val solution = parseSolution(root, limits)
        admitFinalTarget(destination, probe, solution.url, configuration.maxTargetLength)
        val userAgent = solution.userAgent ?: throw FlareSolverrProtocolException("solution has no userAgent")
        return clearanceFrom(request.origin, solution.cookies, userAgent, clock.now(), configuration.maxCookies)
    }

    public companion object {
        /**
         * Creates a resolver. [destination] is mandatory so the safety policy
         * applied to FlareSolverr-loaded targets is an explicit choice.
         */
        public fun create(
            configuration: FlareSolverrResolverConfig,
            destination: WebDestinationPolicy,
            clock: Clock = SystemClock,
        ): FlareSolverrChallengeResolver = FlareSolverrChallengeResolver(configuration, destination, clock, null)

        /** Module-internal seam: uses [engine] instead of CIO for the FlareSolverr client. */
        internal fun createWithEngine(
            configuration: FlareSolverrResolverConfig,
            destination: WebDestinationPolicy,
            engine: HttpClientEngine,
            clock: Clock = SystemClock,
        ): FlareSolverrChallengeResolver = FlareSolverrChallengeResolver(configuration, destination, clock, engine)
    }
}
