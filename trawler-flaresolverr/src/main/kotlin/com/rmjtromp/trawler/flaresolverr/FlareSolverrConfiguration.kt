package com.rmjtromp.trawler.flaresolverr

import java.net.URI
import java.time.Duration
import java.util.Locale

/**
 * Proxy that FlareSolverr's browser uses for outbound traffic.
 *
 * The proxy is a FlareSolverr command parameter, not a Trawler network
 * client. FlareSolverr v3 accepts proxy credentials only when a browser
 * session is created (`sessions.create`); per-request proxies on
 * `request.get`/`request.post` must not carry credentials, and a request that
 * names a session ignores its per-request proxy. Consumers validate these
 * rules when they are constructed.
 *
 * Credentials are sensitive. [toString] never includes them, and they are
 * never placed in response attributes, events, or exception messages.
 */
public class FlareSolverrProxy(
    public val url: String,
    public val username: String? = null,
    public val password: String? = null,
) {
    init {
        require(url.isNotBlank()) { "FlareSolverr proxy URL must not be blank" }
        require(url.length <= MAX_PROXY_URL_LENGTH) { "FlareSolverr proxy URL exceeds $MAX_PROXY_URL_LENGTH characters" }
        val uri = try {
            URI(url)
        } catch (error: Exception) {
            throw IllegalArgumentException("FlareSolverr proxy URL is not a valid URI", error)
        }
        val scheme = uri.scheme?.lowercase(Locale.ROOT)
        require(scheme in PROXY_SCHEMES) { "FlareSolverr proxy URL scheme must be one of $PROXY_SCHEMES" }
        require(!uri.host.isNullOrBlank()) { "FlareSolverr proxy URL must include a host" }
        require(uri.rawUserInfo == null) {
            "FlareSolverr proxy credentials must use the username/password fields, not URL user-info"
        }
        require((username == null) == (password == null)) {
            "FlareSolverr proxy username and password must be supplied together"
        }
        require(username == null || username.isNotEmpty()) { "FlareSolverr proxy username must not be empty" }
    }

    /** Whether this proxy carries credentials that only `sessions.create` accepts. */
    public val hasCredentials: Boolean
        get() = username != null

    override fun toString(): String =
        "FlareSolverrProxy(url=${URI(url).let { "${it.scheme}://${it.host}${if (it.port == -1) "" else ":${it.port}"}" }}, " +
            "credentials=${if (hasCredentials) "[REDACTED]" else "none"})"

    internal companion object {
        const val MAX_PROXY_URL_LENGTH: Int = 2_048
        val PROXY_SCHEMES: Set<String> = setOf("http", "socks4", "socks5")
    }
}

/**
 * Connection and bound settings for one FlareSolverr v3 service endpoint.
 *
 * The endpoint is operator-configured and typically on a loopback or private
 * network; it is not evaluated by the target destination policy. It has no
 * default because selecting a network service must be explicit. The adapter
 * creates one Ktor CIO client per consumer for this endpoint and closes it on
 * lifecycle stop.
 *
 * [maxTimeout] is sent as FlareSolverr's `maxTimeout` for challenge solving.
 * Each solve call is bounded on the Trawler side by `maxTimeout +
 * responseTimeoutMargin`; session commands are bounded by
 * [sessionCommandTimeout]. [maxConcurrentCalls] bounds in-flight calls from
 * one consumer, because every FlareSolverr call drives a real browser.
 * Waiting for a call permit is caller-bounded by the surrounding engine run
 * timeout or cancellation.
 */
public data class FlareSolverrServiceConfig(
    public val endpoint: String,
    public val maxTimeout: Duration = Duration.ofSeconds(60),
    public val responseTimeoutMargin: Duration = Duration.ofSeconds(30),
    public val sessionCommandTimeout: Duration = Duration.ofSeconds(60),
    public val connectTimeout: Duration = Duration.ofSeconds(10),
    public val maxServiceResponseBytes: Int = 16 * 1024 * 1024,
    public val maxConcurrentCalls: Int = 2,
    public val proxy: FlareSolverrProxy? = null,
) {
    init {
        require(endpoint.isNotBlank()) { "FlareSolverr endpoint must not be blank" }
        require(endpoint.length <= MAX_ENDPOINT_LENGTH) { "FlareSolverr endpoint exceeds $MAX_ENDPOINT_LENGTH characters" }
        val uri = try {
            URI(endpoint)
        } catch (error: Exception) {
            throw IllegalArgumentException("FlareSolverr endpoint is not a valid URI", error)
        }
        val scheme = uri.scheme?.lowercase(Locale.ROOT)
        require(scheme == "http" || scheme == "https") { "FlareSolverr endpoint must use http or https" }
        require(!uri.host.isNullOrBlank()) { "FlareSolverr endpoint must include a host" }
        require(uri.rawUserInfo == null) { "FlareSolverr endpoint must not contain URL user-info credentials" }
        require(uri.rawFragment == null) { "FlareSolverr endpoint must not contain a fragment" }
        require(maxTimeout.toMillis() >= 1_000) { "FlareSolverr maxTimeout must be at least one second" }
        require(maxTimeout.toMillis() <= Int.MAX_VALUE) { "FlareSolverr maxTimeout exceeds the protocol's integer range" }
        require(!responseTimeoutMargin.isNegative) { "FlareSolverr response timeout margin must not be negative" }
        require(sessionCommandTimeout.toMillis() > 0) { "FlareSolverr session command timeout must be positive" }
        require(connectTimeout.toMillis() > 0) { "FlareSolverr connect timeout must be positive" }
        require(maxServiceResponseBytes > 0) { "FlareSolverr service response bound must be positive" }
        require(maxConcurrentCalls in 1..MAX_CONCURRENT_CALLS) {
            "FlareSolverr concurrent call bound must be between 1 and $MAX_CONCURRENT_CALLS"
        }
    }

    /** Trawler-side bound for one `request.get`/`request.post` call. */
    internal val solveCallTimeout: Duration
        get() = maxTimeout.plus(responseTimeoutMargin)

    internal companion object {
        const val MAX_ENDPOINT_LENGTH: Int = 2_048
        const val MAX_CONCURRENT_CALLS: Int = 64
    }
}

/**
 * Optional FlareSolverr browser-session pool owned by a [FlareSolverrTransport].
 *
 * When present, the transport creates exactly [count] sessions during
 * lifecycle start and destroys them during lifecycle stop. Each in-flight
 * request borrows one session exclusively, so at most [count] requests use
 * sessions concurrently. [ttl], when set, is sent as `session_ttl_minutes`
 * so FlareSolverr rotates an expired browser under the same session ID.
 * Session IDs are generated by FlareSolverr and treated as sensitive.
 */
public data class FlareSolverrSessionConfig(
    public val count: Int = 1,
    public val ttl: Duration? = null,
) {
    init {
        require(count in 1..MAX_SESSIONS) { "FlareSolverr session count must be between 1 and $MAX_SESSIONS" }
        require(ttl == null || ttl.toMinutes() >= 1) { "FlareSolverr session TTL must be at least one minute" }
        require(ttl == null || ttl.toMinutes() <= Int.MAX_VALUE) { "FlareSolverr session TTL exceeds the protocol range" }
    }

    internal companion object {
        const val MAX_SESSIONS: Int = 32
    }
}

/**
 * Immutable bounds and identity for [FlareSolverrTransport].
 *
 * Request bounds apply before any FlareSolverr call; response bounds apply to
 * the parsed `solution` before a core response is constructed. The engine's
 * own runtime bounds are still enforced afterwards by core.
 */
public data class FlareSolverrTransportConfig(
    public val service: FlareSolverrServiceConfig,
    public val id: String = "flaresolverr",
    public val sessions: FlareSolverrSessionConfig? = null,
    public val maxTargetLength: Int = 16 * 1024,
    public val maxRequestBodyBytes: Int = 64 * 1024,
    public val maxRequestCookies: Int = 64,
    public val maxResponseBodyBytes: Int = 8 * 1024 * 1024,
    public val maxResponseHeaders: Int = 256,
    public val maxHeaderNameLength: Int = 1_024,
    public val maxHeaderValueLength: Int = 16 * 1024,
    public val maxResponseCookies: Int = 128,
    public val maxCookieValueLength: Int = 8_192,
    public val maxUserAgentLength: Int = 1_024,
) {
    init {
        require(id.isNotBlank()) { "FlareSolverr transport ID must not be blank" }
        require(maxTargetLength > 0) { "Target length bound must be positive" }
        require(maxRequestBodyBytes > 0) { "Request body bound must be positive" }
        require(maxRequestCookies >= 0) { "Request cookie bound must not be negative" }
        require(maxResponseBodyBytes > 0) { "Response body bound must be positive" }
        require(maxResponseHeaders >= 0) { "Response header bound must not be negative" }
        require(maxHeaderNameLength > 0) { "Header-name length bound must be positive" }
        require(maxHeaderValueLength > 0) { "Header-value length bound must be positive" }
        require(maxResponseCookies >= 0) { "Response cookie bound must not be negative" }
        require(maxCookieValueLength > 0) { "Cookie value bound must be positive" }
        require(maxUserAgentLength > 0) { "User-Agent length bound must be positive" }
        require(sessions != null || service.proxy?.hasCredentials != true) {
            "FlareSolverr accepts proxy credentials only through sessions.create; configure sessions or remove the credentials"
        }
    }
}
