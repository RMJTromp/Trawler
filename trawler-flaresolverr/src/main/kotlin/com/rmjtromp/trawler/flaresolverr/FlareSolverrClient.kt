package com.rmjtromp.trawler.flaresolverr

import io.ktor.client.HttpClient
import io.ktor.client.HttpClientConfig
import io.ktor.client.engine.HttpClientEngine
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.timeout
import io.ktor.client.request.preparePost
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsChannel
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.contentType
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.readAvailable
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.channels.UnresolvedAddressException
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Bounded FlareSolverr v3 `/v1` command client.
 *
 * One instance owns one Ktor CIO client dedicated to the configured service
 * endpoint, so FlareSolverr calls never share or create a target-fetching
 * client. Redirect following, `expectSuccess`, and retries are disabled; a
 * call is executed exactly once. In-flight calls are bounded by a semaphore
 * and by the per-call request/socket timeout. JSON uses the
 * kotlinx-serialization tree API only and never encodes produced values.
 */
internal class FlareSolverrClient(
    private val config: FlareSolverrServiceConfig,
    engine: HttpClientEngine? = null,
) : AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val permits = Semaphore(config.maxConcurrentCalls)
    private val longestCallMillis: Long =
        maxOf(config.solveCallTimeout.toMillis(), config.sessionCommandTimeout.toMillis())

    /**
     * The owned client. Production always uses CIO; [engine] is an internal
     * seam that lets module tests substitute a controlled engine. A supplied
     * engine is owned by its caller and is not closed by [close].
     */
    private val client: HttpClient = if (engine == null) {
        HttpClient(CIO) {
            applyServiceDefaults()
            engine {
                requestTimeout = longestCallMillis
                maxConnectionsCount = config.maxConcurrentCalls
                endpoint.maxConnectionsPerRoute = config.maxConcurrentCalls
                endpoint.connectTimeout = config.connectTimeout.toMillis()
                endpoint.socketTimeout = longestCallMillis
            }
        }
    } else {
        HttpClient(engine) { applyServiceDefaults() }
    }

    private fun HttpClientConfig<*>.applyServiceDefaults() {
        expectSuccess = false
        followRedirects = false
        install(HttpTimeout) {
            requestTimeoutMillis = longestCallMillis
            connectTimeoutMillis = config.connectTimeout.toMillis()
            socketTimeoutMillis = longestCallMillis
        }
    }

    /**
     * Sends one command and returns the parsed root object of an `ok`
     * response. FlareSolverr may hold the connection silently while its
     * browser solves, so the socket timeout equals the call timeout.
     */
    suspend fun execute(command: JsonObject, timeout: Duration): JsonObject {
        check(!closed.get()) { "FlareSolverr client is closed" }
        val payload = command.toString().toByteArray(Charsets.UTF_8)
        val callMillis = timeout.toMillis().coerceAtLeast(1)
        return permits.withPermit {
            val (status, bytes) = try {
                client.preparePost(config.endpoint) {
                    contentType(ContentType.Application.Json)
                    setBody(payload)
                    timeout {
                        requestTimeoutMillis = callMillis
                        socketTimeoutMillis = callMillis
                        connectTimeoutMillis = config.connectTimeout.toMillis()
                    }
                }.execute { response ->
                    response.status.value to readBounded(response)
                }
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (limit: FlareSolverrException) {
                throw limit
            } catch (error: IOException) {
                throw FlareSolverrUnavailableException(error)
            } catch (error: UnresolvedAddressException) {
                throw FlareSolverrUnavailableException(error)
            }
            interpret(status, bytes)
        }
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) client.close()
    }

    private fun interpret(httpStatus: Int, bytes: ByteArray): JsonObject {
        val element = try {
            Json.parseToJsonElement(String(bytes, Charsets.UTF_8))
        } catch (error: SerializationException) {
            throw FlareSolverrProtocolException("response is not JSON (HTTP $httpStatus)", error)
        }
        val root = element as? JsonObject
            ?: throw FlareSolverrProtocolException("response is not a JSON object (HTTP $httpStatus)")
        return when (root.string("status")) {
            STATUS_OK -> if (httpStatus == 200) {
                root
            } else {
                throw FlareSolverrProtocolException("ok status with HTTP $httpStatus")
            }

            STATUS_ERROR -> throw FlareSolverrErrorException(sanitize(root.string("message") ?: "no message"))
            null -> throw FlareSolverrProtocolException("missing status (HTTP $httpStatus)")
            else -> throw FlareSolverrProtocolException("unknown status (HTTP $httpStatus)")
        }
    }

    /** Removes control characters and configured proxy credentials, then bounds the text. */
    internal fun sanitize(message: String): String {
        // Secrets are removed from the complete (byte-bounded) message before
        // truncation so a secret split by a truncation boundary cannot survive.
        var text = message
        config.proxy?.let { proxy ->
            listOfNotNull(proxy.username, proxy.password)
                .filter { it.isNotEmpty() }
                .forEach { secret -> text = text.replace(secret, "[REDACTED]") }
        }
        return text.map { if (it.isISOControl()) ' ' else it }
            .joinToString("")
            .trim()
            .take(MAX_SERVICE_MESSAGE_LENGTH)
            .ifEmpty { "no message" }
    }

    private suspend fun readBounded(response: io.ktor.client.statement.HttpResponse): ByteArray {
        val maximum = config.maxServiceResponseBytes
        val declared = response.headers[HttpHeaders.ContentLength]?.toLongOrNull()
        if (declared != null && declared > maximum.toLong()) {
            throw FlareSolverrResponseLimitException("service response bytes", maximum.toLong())
        }
        return readChannel(response.bodyAsChannel(), maximum)
    }

    private suspend fun readChannel(channel: ByteReadChannel, maximum: Int): ByteArray {
        val output = ByteArrayOutputStream(minOf(maximum, 8 * 1024))
        val buffer = ByteArray(minOf(maximum, 8 * 1024).coerceAtLeast(1))
        while (true) {
            val read = channel.readAvailable(buffer, 0, buffer.size)
            if (read == -1) break
            if (read == 0) continue
            if (output.size() > maximum - read) {
                throw FlareSolverrResponseLimitException("service response bytes", maximum.toLong())
            }
            output.write(buffer, 0, read)
        }
        return output.toByteArray()
    }

    internal companion object {
        const val STATUS_OK: String = "ok"
        const val STATUS_ERROR: String = "error"
        const val MAX_SERVICE_MESSAGE_LENGTH: Int = 512
    }
}

/** Builds the `proxy` command object; credentials are included only when requested by the caller. */
internal fun JsonObjectBuilder.putProxy(proxy: FlareSolverrProxy, includeCredentials: Boolean) {
    putJsonObject("proxy") {
        put("url", proxy.url)
        if (includeCredentials && proxy.username != null && proxy.password != null) {
            put("username", proxy.username)
            put("password", proxy.password)
        }
    }
}

/** One cookie reported by FlareSolverr's browser for the solved page. */
internal class SolvedCookie(
    val name: String,
    val value: String,
    val domain: String?,
    val path: String,
    val secure: Boolean,
    val expiresAt: Instant?,
) {
    /**
     * RFC 6265 domain match against [host]. FlareSolverr reports the browser
     * cookie domain with an optional leading dot; a missing domain is treated
     * as a host-only cookie of the solved page.
     */
    fun appliesToHost(host: String): Boolean {
        val normalizedHost = host.lowercase(java.util.Locale.ROOT)
        val cookieDomain = domain?.removePrefix(".")?.lowercase(java.util.Locale.ROOT) ?: return true
        if (cookieDomain.isEmpty()) return false
        return normalizedHost == cookieDomain || normalizedHost.endsWith(".$cookieDomain")
    }

    override fun toString(): String = "SolvedCookie(name=$name, value=[REDACTED])"
}

/** Bounded, parsed `solution` object. Header values and cookie values may be sensitive. */
internal class Solution(
    val url: String?,
    val status: Int?,
    val headers: List<Pair<String, String>>,
    val response: String?,
    val cookies: List<SolvedCookie>,
    val userAgent: String?,
)

/** Bounds applied while parsing a `solution`. */
internal data class SolutionLimits(
    val maxTargetLength: Int,
    val maxResponseHeaders: Int,
    val maxHeaderNameLength: Int,
    val maxHeaderValueLength: Int,
    val maxResponseCookies: Int,
    val maxCookieValueLength: Int,
    val maxUserAgentLength: Int,
)

/** Parses and bounds the `solution` member of an `ok` command response. */
internal fun parseSolution(root: JsonObject, limits: SolutionLimits): Solution {
    val solution = root["solution"] as? JsonObject
        ?: throw FlareSolverrProtocolException("missing solution object")
    val url = solution.string("url")?.also {
        if (it.length > limits.maxTargetLength) {
            throw FlareSolverrResponseLimitException("solution url length", limits.maxTargetLength.toLong())
        }
    }?.takeIf { it.isNotBlank() }
    val status = (solution["status"] as? JsonPrimitive)?.takeUnless { it.isString }?.intOrNull
        ?.takeIf { it in 0..999 }
    val headers = parseHeaders(solution["headers"], limits)
    val response = solution.string("response")
    val cookies = parseCookies(solution["cookies"], limits)
    val userAgent = solution.string("userAgent")?.also {
        if (it.length > limits.maxUserAgentLength) {
            throw FlareSolverrResponseLimitException("user-agent length", limits.maxUserAgentLength.toLong())
        }
    }?.takeIf { it.isNotBlank() && it.none(Char::isISOControl) }
    return Solution(url, status, headers, response, cookies, userAgent)
}

private fun parseHeaders(element: JsonElement?, limits: SolutionLimits): List<Pair<String, String>> {
    if (element == null || element is JsonNull) return emptyList()
    val headers = element as? JsonObject ?: throw FlareSolverrProtocolException("solution headers is not an object")
    if (headers.size > limits.maxResponseHeaders) {
        throw FlareSolverrResponseLimitException("response header count", limits.maxResponseHeaders.toLong())
    }
    return headers.entries.mapNotNull { (name, value) ->
        if (name.length > limits.maxHeaderNameLength) {
            throw FlareSolverrResponseLimitException("header name length", limits.maxHeaderNameLength.toLong())
        }
        val text = (value as? JsonPrimitive)?.takeIf { it.isString }?.content ?: return@mapNotNull null
        if (text.length > limits.maxHeaderValueLength) {
            throw FlareSolverrResponseLimitException("header value length", limits.maxHeaderValueLength.toLong())
        }
        if (name.isBlank()) null else name to text
    }
}

private fun parseCookies(element: JsonElement?, limits: SolutionLimits): List<SolvedCookie> {
    if (element == null || element is JsonNull) return emptyList()
    val cookies = element as? JsonArray ?: throw FlareSolverrProtocolException("solution cookies is not an array")
    if (cookies.size > limits.maxResponseCookies) {
        throw FlareSolverrResponseLimitException("response cookie count", limits.maxResponseCookies.toLong())
    }
    return cookies.mapNotNull { item ->
        val cookie = item as? JsonObject ?: throw FlareSolverrProtocolException("solution cookie is not an object")
        val name = cookie.string("name")?.takeIf { it.isNotBlank() } ?: return@mapNotNull null
        val value = cookie.string("value") ?: return@mapNotNull null
        if (name.length > limits.maxHeaderNameLength) {
            throw FlareSolverrResponseLimitException("cookie name length", limits.maxHeaderNameLength.toLong())
        }
        if (value.length > limits.maxCookieValueLength) {
            throw FlareSolverrResponseLimitException("cookie value length", limits.maxCookieValueLength.toLong())
        }
        val session = (cookie["session"] as? JsonPrimitive)?.booleanOrNull == true
        val expires = (cookie["expires"] as? JsonPrimitive)?.takeUnless { it.isString }?.doubleOrNull
        val expiresAt = if (session || expires == null || expires <= 0.0 || expires > MAX_EPOCH_SECONDS) {
            null
        } else {
            Instant.ofEpochMilli((expires * 1_000).toLong())
        }
        SolvedCookie(
            name = name,
            value = value,
            domain = cookie.string("domain")?.takeIf { it.isNotBlank() }?.take(limits.maxHeaderNameLength),
            path = cookie.string("path")?.takeIf { it.startsWith('/') }?.take(2_048) ?: "/",
            secure = (cookie["secure"] as? JsonPrimitive)?.booleanOrNull == true,
            expiresAt = expiresAt,
        )
    }
}

internal fun JsonObject.string(name: String): String? =
    (this[name] as? JsonPrimitive)?.takeIf { it.isString }?.content

/** Year 9999 in epoch seconds; larger browser expiries are treated as session-bounded. */
private const val MAX_EPOCH_SECONDS: Double = 253_402_300_799.0
