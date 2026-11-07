package com.rmjtromp.trawler.web.session

import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.SystemClock
import com.rmjtromp.trawler.web.policy.CookieHeaderLimitException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.net.URI
import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.LinkedHashMap
import java.util.Locale

/**
 * HTTP session cookie capability owned by the web adapter.
 *
 * A session may add destination-scoped cookies to a request and consume
 * `Set-Cookie` response headers. It owns cookie values and must not emit them
 * in events or logs. The transport supplies the same session for redirect
 * hops, so destination matching and cross-origin credential stripping remain
 * explicit.
 */
public interface CookieSession {
    /** Stable identifier used by transport diagnostics. */
    public val id: String

    /** Adds matching cookies unless the caller already supplied a Cookie header. */
    public suspend fun apply(target: String, headers: Headers): Headers

    /** Consumes bounded Set-Cookie header values from one response. */
    public suspend fun observe(target: String, setCookieHeaders: List<String>)
}

/** Explicit no-cookie session. */
public object NoCookieSession : CookieSession {
    override val id: String = "no-cookie-session"

    override suspend fun apply(target: String, headers: Headers): Headers = headers

    override suspend fun observe(target: String, setCookieHeaders: List<String>) = Unit
}

/** Bounds for the process-local cookie session. */
public data class CookieSessionConfig(
    public val maxCookies: Int = 512,
    public val maxCookieValueLength: Int = 8_192,
    public val maxCookieHeaderLength: Int = 16_384,
    public val maxCookieLifetime: Duration = Duration.ofDays(30),
) {
    init {
        require(maxCookies > 0) { "Cookie count bound must be positive" }
        require(maxCookieValueLength > 0) { "Cookie value bound must be positive" }
        require(maxCookieHeaderLength > 0) { "Cookie header bound must be positive" }
        require(!maxCookieLifetime.isNegative && !maxCookieLifetime.isZero) {
            "Cookie lifetime bound must be positive"
        }
    }
}

/**
 * Bounded process-local cookie jar for one transport/session ownership scope.
 *
 * It is intentionally not durable and does not share state across transport
 * instances. Eviction is oldest-entry first. Invalid or overlong cookies are
 * ignored as policy input rather than allowing an unbounded header or jar.
 */
public class InMemoryCookieSession(
    private val configuration: CookieSessionConfig = CookieSessionConfig(),
    private val clock: Clock = SystemClock,
) : CookieSession {
    override val id: String = "memory-cookie-session"

    private data class StoredCookie(
        val name: String,
        val value: String,
        val domain: String,
        val path: String,
        val secure: Boolean,
        val hostOnly: Boolean,
        val expiresAt: Instant,
        var lastUsed: Long,
    )

    private val mutex = Mutex()
    private val cookies = LinkedHashMap<String, StoredCookie>()
    private var sequence: Long = 0

    override suspend fun apply(target: String, headers: Headers): Headers {
        if (headers["Cookie"].isNotEmpty()) return headers
        val uri = URI(target)
        val host = uri.host?.lowercase(Locale.ROOT) ?: return headers
        val now = clock.now()
        val matching = mutex.withLock {
            evictExpired(now)
            cookies.values
                .filter { it.matches(host, uri.pathOrRoot(), uri.scheme, now) }
                .sortedWith(compareByDescending<StoredCookie> { it.path.length }.thenBy { it.lastUsed })
                .onEach { it.lastUsed = ++sequence }
                .toList()
        }
        if (matching.isEmpty()) return headers
        val value = matching.joinToString("; ") { "${it.name}=${it.value}" }
        if (value.length > configuration.maxCookieHeaderLength) {
            throw CookieHeaderLimitException(configuration.maxCookieHeaderLength)
        }
        return headers.plus(Header("Cookie", value))
    }

    override suspend fun observe(target: String, setCookieHeaders: List<String>) {
        if (setCookieHeaders.isEmpty()) return
        val uri = URI(target)
        val host = uri.host?.lowercase(Locale.ROOT) ?: return
        val now = clock.now()
        mutex.withLock {
            evictExpired(now)
            setCookieHeaders
                .asSequence()
                .take(configuration.maxCookies)
                .mapNotNull { parse(it, host, uri.defaultCookiePath(), now) }
                .forEach { cookie ->
                    val key = key(cookie)
                    if (cookie.expiresAt <= now) cookies.remove(key) else cookies[key] = cookie
                }
            while (cookies.size > configuration.maxCookies) {
                cookies.entries.minByOrNull { it.value.lastUsed }?.let { cookies.remove(it.key) }
                    ?: break
            }
        }
    }

    /** Clears all session state owned by this jar. */
    public suspend fun clear() {
        mutex.withLock { cookies.clear() }
    }

    private fun parse(header: String, host: String, defaultPath: String, now: Instant): StoredCookie? {
        if (header.length > configuration.maxCookieHeaderLength) return null
        val parts = header.split(';', limit = 32)
        val first = parts.firstOrNull() ?: return null
        val separator = first.indexOf('=')
        if (separator <= 0) return null
        val name = first.substring(0, separator).trim()
        val value = first.substring(separator + 1).trim()
        if (!validToken(name) || value.length > configuration.maxCookieValueLength) return null

        var domain = host
        var hostOnly = true
        var path = defaultPath
        var secure = false
        var expiresAt = now.plus(configuration.maxCookieLifetime)
        var delete = false
        var maxAgeSeen = false
        parts.drop(1).forEach { rawAttribute ->
            val attribute = rawAttribute.trim()
            val equals = attribute.indexOf('=')
            val attributeName = (if (equals >= 0) attribute.substring(0, equals) else attribute)
                .trim().lowercase(Locale.ROOT)
            val attributeValue = if (equals >= 0) attribute.substring(equals + 1).trim() else ""
            when (attributeName) {
                "domain" -> {
                    val candidate = attributeValue.removePrefix(".").lowercase(Locale.ROOT)
                    if (candidate.isBlank() || !domainMatches(host, candidate)) return null
                    domain = candidate
                    hostOnly = false
                }

                "path" -> if (attributeValue.startsWith('/')) path = attributeValue.take(2_048)
                "secure" -> secure = true
                "max-age" -> {
                    val seconds = attributeValue.toLongOrNull() ?: return null
                    maxAgeSeen = true
                    delete = seconds <= 0
                    if (!delete) {
                        val bounded = minOf(seconds, configuration.maxCookieLifetime.seconds)
                        expiresAt = now.plusSeconds(bounded)
                    }
                }

                // RFC 6265 section 5.3: Max-Age takes precedence over Expires in either order.
                "expires" -> if (!maxAgeSeen) {
                    parseExpiry(attributeValue)?.let {
                        expiresAt = minOf(it, now.plus(configuration.maxCookieLifetime))
                    }
                }
            }
        }
        if (delete) expiresAt = now
        return StoredCookie(name, value, domain, path, secure, hostOnly, expiresAt, ++sequence)
    }

    private fun evictExpired(now: Instant) {
        cookies.entries.removeIf { !it.value.expiresAt.isAfter(now) }
    }

    private fun key(cookie: StoredCookie): String =
        "${cookie.domain}\u0000${cookie.path}\u0000${cookie.name}"

    private fun StoredCookie.matches(host: String, requestPath: String, scheme: String?, now: Instant): Boolean {
        if (!expiresAt.isAfter(now)) return false
        if (hostOnly) {
            if (host != domain) return false
        } else if (!domainMatches(host, domain)) {
            return false
        }
        if (secure && !scheme.equals("https", ignoreCase = true)) return false
        return requestPath == path || requestPath.startsWith(path.trimEnd('/') + "/") || path == "/"
    }

    private fun validToken(value: String): Boolean {
        val separators = setOf('(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '[', ']', '?', '=', '{', '}', ' ', '\t')
        return value.isNotBlank() && value.all { it.code in 0x21..0x7e && it !in separators }
    }

    private fun parseExpiry(value: String): Instant? = try {
        ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant()
    } catch (_: DateTimeParseException) {
        null
    }

    /** RFC 6265 section 5.1.3: suffix matching applies to host names, never to IP literals. */
    private fun domainMatches(host: String, domain: String): Boolean =
        host == domain || (!host.isIpLiteral() && host.endsWith(".$domain"))

    private fun String.isIpLiteral(): Boolean = startsWith('[') || all { it == '.' || it in '0'..'9' }
}

private fun URI.pathOrRoot(): String = rawPath.takeIf { it.isNotEmpty() } ?: "/"

private fun URI.defaultCookiePath(): String {
    val path = pathOrRoot()
    if (!path.startsWith('/')) return "/"
    val lastSlash = path.lastIndexOf('/')
    return if (lastSlash <= 0) "/" else path.substring(0, lastSlash)
}
