package com.rmjtromp.trawler.web.transport

import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.SystemClock
import com.rmjtromp.trawler.web.policy.CachingRobotsPolicy
import com.rmjtromp.trawler.web.policy.DefaultHeaderRedactionPolicy
import com.rmjtromp.trawler.web.policy.DefaultHttpFailureClassifier
import com.rmjtromp.trawler.web.policy.DefaultRedirectHeaderPolicy
import com.rmjtromp.trawler.web.policy.DefaultRetryAfterPolicy
import com.rmjtromp.trawler.web.policy.HeaderRedactionPolicy
import com.rmjtromp.trawler.web.policy.HttpFailureClassifier
import com.rmjtromp.trawler.web.policy.PublicWebSafetyConfig
import com.rmjtromp.trawler.web.policy.PublicWebSafetyPolicy
import com.rmjtromp.trawler.web.policy.RobotsPolicy
import com.rmjtromp.trawler.web.policy.RobotsPolicyConfig
import com.rmjtromp.trawler.web.session.CookieSession
import com.rmjtromp.trawler.web.session.CookieSessionConfig
import com.rmjtromp.trawler.web.session.InMemoryCookieSession
import java.net.URI
import java.net.URISyntaxException
import java.time.Duration
import java.util.Locale

/**
 * Immutable Ktor transport bounds and timeout configuration.
 *
 * @property tls13Hosts Exact, case-insensitive hostnames (no scheme, port, or
 * path; bracket IPv6 literals) whose requests use the transport-owned JDK
 * `java.net.http` client, which can negotiate TLS 1.3. Every other host keeps
 * the CIO path, which offers at most TLS 1.2. Matching is per request and per
 * redirect hop, including robots fetches; subdomains are not implied. Empty by
 * default, in which case no JDK client is created. A non-empty set requires a
 * Java 21 or newer runtime so the client can be shut down deterministically.
 * @property proxy Forward proxy every request of this transport leaves through,
 * on both clients and for every redirect hop and robots fetch. A request whose
 * proxy fails throws [com.rmjtromp.trawler.web.policy.WebProxyFailureException]
 * and is never retried directly. `null` leaves routing as it was without this
 * option. JVM proxy properties and environment variables never populate this field.
 */
public data class KtorTransportConfig(
    public val id: String = "ktor-cio",
    public val connectTimeout: Duration = Duration.ofSeconds(10),
    public val requestTimeout: Duration = Duration.ofSeconds(30),
    public val socketTimeout: Duration = Duration.ofSeconds(30),
    public val totalRequestTimeout: Duration = Duration.ofMinutes(2),
    public val maxRequestBodyBytes: Int = 8 * 1024 * 1024,
    public val maxCompressedBodyBytes: Int = 8 * 1024 * 1024,
    public val maxDecompressedBodyBytes: Int = 8 * 1024 * 1024,
    public val maxRobotsBodyBytes: Int = 128 * 1024,
    public val maxResponseHeaders: Int = 256,
    public val maxRequestHeaders: Int = 256,
    public val maxTargetLength: Int = 16 * 1024,
    public val maxMethodLength: Int = 256,
    public val maxHeaderNameLength: Int = 1_024,
    public val maxHeaderValueLength: Int = 16 * 1024,
    public val maxRedirects: Int = 8,
    public val maxLocationLength: Int = 16 * 1024,
    public val maxClientConnections: Int = 64,
    public val maxConnectionsPerOrigin: Int = 8,
    public val userAgent: String = "TrawlerBot/0.1",
    public val tls13Hosts: Set<String> = emptySet(),
    public val proxy: WebProxy? = null,
) {
    /** Lowercase bare hostnames in the form [WebOrigin.host] reports them. */
    internal val normalizedTls13Hosts: Set<String> =
        tls13Hosts.mapTo(LinkedHashSet(), ::normalizeTls13Host)

    init {
        require(id.isNotBlank()) { "Ktor transport ID must not be blank" }
        require(connectTimeout.isPositive()) { "Connect timeout must be positive" }
        require(requestTimeout.isPositive()) { "Request timeout must be positive" }
        require(socketTimeout.isPositive()) { "Socket timeout must be positive" }
        require(totalRequestTimeout.isPositive()) { "Total request timeout must be positive" }
        require(connectTimeout.toMillis() > 0) { "Connect timeout must be at least one millisecond" }
        require(requestTimeout.toMillis() > 0) { "Request timeout must be at least one millisecond" }
        require(socketTimeout.toMillis() > 0) { "Socket timeout must be at least one millisecond" }
        require(totalRequestTimeout.toMillis() > 0) { "Total request timeout must be at least one millisecond" }
        require(maxRequestBodyBytes > 0) { "Request body bound must be positive" }
        require(maxCompressedBodyBytes > 0) { "Compressed body bound must be positive" }
        require(maxDecompressedBodyBytes > 0) { "Decompressed body bound must be positive" }
        require(maxRobotsBodyBytes > 0) { "Robots body bound must be positive" }
        require(maxResponseHeaders > 0) { "Response header bound must be positive" }
        require(maxRequestHeaders > 0) { "Request header bound must be positive" }
        require(maxTargetLength > 0) { "Target length bound must be positive" }
        require(maxMethodLength > 0) { "Method length bound must be positive" }
        require(maxHeaderNameLength > 0) { "Header-name length bound must be positive" }
        require(maxHeaderValueLength > 0) { "Header value bound must be positive" }
        require(maxRedirects >= 0) { "Redirect limit must not be negative" }
        require(maxLocationLength > 0) { "Location header bound must be positive" }
        require(maxClientConnections > 0) { "Client connection bound must be positive" }
        require(maxConnectionsPerOrigin > 0) { "Per-origin connection bound must be positive" }
        require(userAgent.isNotBlank()) { "Transport user agent must not be blank" }
    }
}

private fun normalizeTls13Host(entry: String): String {
    val parsedHost = try {
        URI("https://$entry/").host
    } catch (_: URISyntaxException) {
        null
    }
    require(parsedHost != null && parsedHost.equals(entry, ignoreCase = true)) {
        "TLS 1.3 host entries must be bare hostnames without scheme, port, or path: '$entry'"
    }
    return parsedHost.lowercase(Locale.ROOT).removePrefix("[").removeSuffix("]")
}

/** Immutable set of independently replaceable web transport policies. */
public data class WebTransportPolicies(
    public val destination: WebDestinationPolicy,
    public val robots: RobotsPolicy,
    public val concurrency: OriginConcurrencyPolicy,
    public val pacing: OriginPacingPolicy,
    public val cookies: CookieSession,
    public val failureClassifier: HttpFailureClassifier,
    public val retryAfter: com.rmjtromp.trawler.web.policy.RetryAfterPolicy,
    public val redirectHeaders: com.rmjtromp.trawler.web.policy.RedirectHeaderPolicy,
    public val headerRedaction: HeaderRedactionPolicy,
) {
    init {
        require(destination.id.isNotBlank()) { "Destination policy ID must not be blank" }
        require(robots.id.isNotBlank()) { "Robots policy ID must not be blank" }
        require(concurrency.id.isNotBlank()) { "Concurrency policy ID must not be blank" }
        require(pacing.id.isNotBlank()) { "Pacing policy ID must not be blank" }
        require(cookies.id.isNotBlank()) { "Cookie session ID must not be blank" }
        require(failureClassifier.id.isNotBlank()) { "HTTP classifier ID must not be blank" }
        require(retryAfter.id.isNotBlank()) { "Retry-After policy ID must not be blank" }
        require(redirectHeaders.id.isNotBlank()) { "Redirect-header policy ID must not be blank" }
        require(headerRedaction.id.isNotBlank()) { "Header-redaction policy ID must not be blank" }
    }
}

/** Options for the assembled public-web policy preset. */
public data class PublicWebPresetConfig(
    public val safety: PublicWebSafetyConfig = PublicWebSafetyConfig(),
    public val robots: RobotsPolicyConfig = RobotsPolicyConfig(),
    public val permitsPerOrigin: Int = 2,
    public val maxOrigins: Int = 256,
    public val minimumOriginDelay: Duration = Duration.ofMillis(250),
    public val maximumPacingWait: Duration = Duration.ofMinutes(5),
    public val cookies: CookieSessionConfig = CookieSessionConfig(),
) {
    init {
        require(permitsPerOrigin > 0) { "Public-web origin permits must be positive" }
        require(maxOrigins > 0) { "Public-web origin bound must be positive" }
        require(!minimumOriginDelay.isNegative) { "Public-web origin delay must not be negative" }
        require(maximumPacingWait.isPositive()) { "Public-web pacing wait must be positive" }
    }
}

/** Constructors for the replaceable web policy bundle used by the transport. */
public object WebPresets {
    /**
     * Builds public-web policies around the caller-owned [resolver]. The
     * resolver is deliberately supplied rather than creating an HTTP client
     * or hidden network service inside a policy factory.
     */
    public fun publicWebPolicies(
        resolver: NetworkAddressResolver,
        configuration: PublicWebPresetConfig = PublicWebPresetConfig(),
        clock: Clock = SystemClock,
    ): WebTransportPolicies = WebTransportPolicies(
        destination = PublicWebSafetyPolicy(resolver, configuration.safety),
        robots = CachingRobotsPolicy(configuration.robots, clock),
        concurrency = BoundedOriginConcurrencyPolicy(
            permitsPerOrigin = configuration.permitsPerOrigin,
            maxOrigins = configuration.maxOrigins,
        ),
        pacing = FixedOriginPacingPolicy(
            minimumDelay = configuration.minimumOriginDelay,
            maxWait = configuration.maximumPacingWait,
            maxOrigins = configuration.maxOrigins,
            clock = clock,
            retryAfterPolicy = DefaultRetryAfterPolicy,
        ),
        cookies = InMemoryCookieSession(configuration.cookies, clock),
        failureClassifier = DefaultHttpFailureClassifier,
        retryAfter = DefaultRetryAfterPolicy,
        redirectHeaders = DefaultRedirectHeaderPolicy(),
        headerRedaction = DefaultHeaderRedactionPolicy(),
    )
}
