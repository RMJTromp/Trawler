package com.rmjtromp.trawler.flaresolverr

import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.extension.ExtensionStopReason
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.FailureContext
import com.rmjtromp.trawler.web.transport.DestinationDecision
import com.rmjtromp.trawler.web.transport.RedirectHistory
import com.rmjtromp.trawler.web.transport.WebDestinationPolicy
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.respond
import io.ktor.client.request.HttpRequestData
import io.ktor.client.request.HttpResponseData
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.OutgoingContent
import io.ktor.http.headersOf
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

internal const val ENDPOINT: String = "http://flaresolverr.test:8191/v1"
internal const val SITE: String = "https://example.test"
internal const val BROWSER_UA: String = "Mozilla/5.0 (X11; Linux x86_64) Chrome/152.0.0.0 Safari/537.36"

internal val START_CONTEXT: ExtensionStartContext = ExtensionStartContext(CoroutineScope(Dispatchers.Default))
internal val STOP_CONTEXT: ExtensionStopContext = ExtensionStopContext(ExtensionStopReason.ENGINE_SHUTDOWN)
internal val FAILURE_CONTEXT: FailureContext = FailureContext(CrawlerContext("engine", "run", "pipeline"), null)

internal fun serviceConfig(
    maxConcurrentCalls: Int = 4,
    maxServiceResponseBytes: Int = 1024 * 1024,
    proxy: FlareSolverrProxy? = null,
): FlareSolverrServiceConfig = FlareSolverrServiceConfig(
    endpoint = ENDPOINT,
    maxTimeout = Duration.ofSeconds(5),
    responseTimeoutMargin = Duration.ofSeconds(5),
    sessionCommandTimeout = Duration.ofSeconds(5),
    maxServiceResponseBytes = maxServiceResponseBytes,
    maxConcurrentCalls = maxConcurrentCalls,
    proxy = proxy,
)

/** Deterministic, manually advanced clock. */
internal class MutableClock(start: Instant = Instant.parse("2026-01-01T00:00:00Z")) : Clock {
    @Volatile
    var instant: Instant = start

    override fun now(): Instant = instant

    fun advance(duration: Duration) {
        instant = instant.plus(duration)
    }
}

/** Destination policy that records evaluated targets and rejects targets containing [rejectMarker]. */
internal class RecordingDestinationPolicy(
    private val rejectMarker: String? = null,
    override val id: String = "recording-destination",
) : WebDestinationPolicy {
    val evaluated = CopyOnWriteArrayList<String>()

    override suspend fun evaluate(request: CrawlRequest, history: RedirectHistory): DestinationDecision {
        evaluated += request.target
        return if (rejectMarker != null && request.target.contains(rejectMarker)) {
            DestinationDecision.Rejected("blocked-by-test")
        } else {
            DestinationDecision.Allowed
        }
    }
}

/** One command received by [FakeFlareSolverr]. */
internal class FakeCall(val request: HttpRequestData, val json: JsonObject) {
    val cmd: String get() = (json["cmd"] as JsonPrimitive).content

    fun string(name: String): String? = (json[name] as? JsonPrimitive)?.takeIf { it.isString }?.content
}

/**
 * Controlled FlareSolverr `/v1` endpoint backed by Ktor MockEngine. Every
 * command body is parsed and recorded. `request.get`/`request.post` replies
 * come from [onRequest]; session commands behave like FlareSolverr unless
 * [onSessionCreate]/[onSessionDestroy] override them. Session IDs are
 * deterministic (`session-1`, `session-2`, ...).
 */
internal class FakeFlareSolverr(
    var onRequest: suspend MockRequestHandleScope.(FakeCall) -> HttpResponseData = { replyJson(okRoot(solution())) },
) {
    var onSessionCreate: (suspend MockRequestHandleScope.(FakeCall) -> HttpResponseData)? = null
    var onSessionDestroy: (suspend MockRequestHandleScope.(FakeCall) -> HttpResponseData)? = null

    val calls = CopyOnWriteArrayList<FakeCall>()
    val liveSessions = CopyOnWriteArrayList<String>()
    private val sessionSequence = AtomicInteger()

    val engine: MockEngine = MockEngine { request ->
        val body = when (val content = request.body) {
            is OutgoingContent.ByteArrayContent -> content.bytes()
            else -> error("unexpected request body type ${content::class}")
        }
        val call = FakeCall(request, Json.parseToJsonElement(String(body, Charsets.UTF_8)).jsonObject)
        calls += call
        when (call.cmd) {
            "sessions.create" -> onSessionCreate?.invoke(this, call) ?: run {
                val id = "session-${sessionSequence.incrementAndGet()}"
                liveSessions += id
                replyJson(buildJsonObject {
                    put("status", "ok")
                    put("message", "Session created successfully.")
                    put("session", id)
                })
            }

            "sessions.destroy" -> onSessionDestroy?.invoke(this, call) ?: run {
                liveSessions.remove(call.string("session"))
                replyJson(buildJsonObject {
                    put("status", "ok")
                    put("message", "The session has been removed.")
                })
            }

            else -> onRequest(call)
        }
    }

    fun commands(cmd: String): List<FakeCall> = calls.filter { it.cmd == cmd }
}

internal fun MockRequestHandleScope.replyJson(
    body: JsonElement,
    status: HttpStatusCode = HttpStatusCode.OK,
): HttpResponseData = replyText(body.toString(), status)

internal fun MockRequestHandleScope.replyText(
    body: String,
    status: HttpStatusCode = HttpStatusCode.OK,
    extraHeaders: List<Pair<String, String>> = emptyList(),
): HttpResponseData = respond(
    content = body,
    status = status,
    headers = headersOf(
        *(listOf(HttpHeaders.ContentType to listOf("application/json")) +
            extraHeaders.map { it.first to listOf(it.second) }).toTypedArray(),
    ),
)

internal fun okRoot(solution: JsonObject?, message: String = "Challenge not detected!"): JsonObject = buildJsonObject {
    put("status", "ok")
    put("message", message)
    if (solution != null) put("solution", solution)
    put("version", "3.5.2")
}

internal fun errorRoot(message: String): JsonObject = buildJsonObject {
    put("status", "error")
    put("message", message)
}

internal fun solution(
    url: String? = "$SITE/page",
    status: Int? = 200,
    headers: JsonObject = JsonObject(emptyMap()),
    response: String? = "<html><body>ok</body></html>",
    cookies: JsonArray = JsonArray(emptyList()),
    userAgent: String? = BROWSER_UA,
): JsonObject = buildJsonObject {
    if (url != null) put("url", url)
    if (status != null) put("status", status)
    put("headers", headers)
    if (response != null) put("response", response)
    put("cookies", cookies)
    if (userAgent != null) put("userAgent", userAgent)
}

internal fun cookie(
    name: String,
    value: String,
    domain: String? = ".example.test",
    path: String? = "/",
    expires: Double = -1.0,
    secure: Boolean = false,
    session: Boolean = false,
): JsonObject = buildJsonObject {
    put("name", name)
    put("value", value)
    if (domain != null) put("domain", domain)
    if (path != null) put("path", path)
    put("expires", expires)
    put("secure", secure)
    put("session", session)
    put("httpOnly", true)
}

internal fun cookies(vararg values: JsonObject): JsonArray = buildJsonArray { values.forEach { add(it) } }
