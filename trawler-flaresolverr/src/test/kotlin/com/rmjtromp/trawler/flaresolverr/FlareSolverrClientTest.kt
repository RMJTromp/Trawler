package com.rmjtromp.trawler.flaresolverr

import io.ktor.client.engine.mock.respond
import io.ktor.client.engine.mock.respondError
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.utils.io.ByteReadChannel
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.io.IOException
import java.nio.channels.UnresolvedAddressException
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class FlareSolverrClientTest {
    private val command = buildJsonObject { put("cmd", "request.get") }
    private val timeout = Duration.ofSeconds(5)

    private fun client(fake: FakeFlareSolverr, config: FlareSolverrServiceConfig = serviceConfig()) =
        FlareSolverrClient(config, fake.engine)

    @Test
    fun `posts the command as JSON to the endpoint and returns an ok root`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        client(fake).use { client ->
            val root = client.execute(command, timeout)
            assertEquals("ok", root.string("status"))
            val call = fake.calls.single()
            assertEquals(ENDPOINT, call.request.url.toString())
            assertEquals("POST", call.request.method.value)
            assertEquals("application/json", call.request.body.contentType?.toString())
            assertEquals(command, call.json)
        }
    }

    @Test
    fun `protocol errors are distinct and include only the HTTP status`(): Unit = runBlocking {
        val cases = listOf(
            "not json" to "response is not JSON (HTTP 200)",
            "[1,2]" to "response is not a JSON object (HTTP 200)",
            "{\"message\":\"x\"}" to "missing status (HTTP 200)",
            "{\"status\":\"weird\"}" to "unknown status (HTTP 200)",
            "{\"status\":42}" to "missing status (HTTP 200)",
        )
        cases.forEach { (body, reason) ->
            val fake = FakeFlareSolverr { replyText(body) }
            client(fake).use { client ->
                val error = assertFailsWith<FlareSolverrProtocolException> { client.execute(command, timeout) }
                assertEquals(reason, error.reason)
            }
        }
        val okWithError = FakeFlareSolverr { replyJson(okRoot(null), HttpStatusCode.Accepted) }
        client(okWithError).use { client ->
            assertEquals(
                "ok status with HTTP 202",
                assertFailsWith<FlareSolverrProtocolException> { client.execute(command, timeout) }.reason,
            )
        }
        val notFound = FakeFlareSolverr { replyText("{\"error\":\"Not found\",\"status_code\":404}", HttpStatusCode.NotFound) }
        client(notFound).use { client ->
            assertEquals(
                "missing status (HTTP 404)",
                assertFailsWith<FlareSolverrProtocolException> { client.execute(command, timeout) }.reason,
            )
        }
    }

    @Test
    fun `status error becomes a sanitized service error regardless of HTTP status`(): Unit = runBlocking {
        val fake = FakeFlareSolverr { replyJson(errorRoot("Error: Timeout\nafter 60.0 seconds.\t"), HttpStatusCode.InternalServerError) }
        client(fake).use { client ->
            val error = assertFailsWith<FlareSolverrErrorException> { client.execute(command, timeout) }
            assertEquals("Error: Timeout after 60.0 seconds.", error.serviceMessage)
        }
        val noMessage = FakeFlareSolverr { replyJson(buildJsonObject { put("status", "error") }, HttpStatusCode.InternalServerError) }
        client(noMessage).use { client ->
            assertEquals("no message", assertFailsWith<FlareSolverrErrorException> { client.execute(command, timeout) }.serviceMessage)
        }
    }

    @Test
    fun `sanitize removes proxy credentials before truncation and bounds the message`() {
        val proxy = FlareSolverrProxy("http://proxy.test", "proxy-user", "s3cr3t-password")
        FlareSolverrClient(serviceConfig(proxy = proxy), FakeFlareSolverr().engine).use { client ->
            val leaked = client.sanitize("proxy proxy-user:s3cr3t-password failed")
            assertEquals("proxy [REDACTED]:[REDACTED] failed", leaked)
            assertEquals("no message", client.sanitize(" \n\t "))
            val long = client.sanitize("x".repeat(5_000))
            assertEquals(FlareSolverrClient.MAX_SERVICE_MESSAGE_LENGTH, long.length)
            // A secret straddling the old pre-truncation boundary must not survive partially.
            val longSecret = "S".repeat(600)
            val straddling = FlareSolverrClient(
                serviceConfig(proxy = FlareSolverrProxy("http://proxy.test", "user", longSecret)),
                FakeFlareSolverr().engine,
            )
            straddling.use {
                val text = it.sanitize(longSecret + "-" + "a".repeat(400) + longSecret)
                assertFalse(text.contains("SSSS"), text)
            }
        }
        FlareSolverrClient(serviceConfig(), FakeFlareSolverr().engine).use { client ->
            assertEquals("plain", client.sanitize("plain"))
        }
    }

    @Test
    fun `declared and streamed oversized responses are rejected`(): Unit = runBlocking {
        val declared = FakeFlareSolverr {
            respond(ByteReadChannel("{}"), HttpStatusCode.OK, headersOf(HttpHeaders.ContentLength, "2048"))
        }
        client(declared, serviceConfig(maxServiceResponseBytes = 1024)).use { client ->
            val error = assertFailsWith<FlareSolverrResponseLimitException> { client.execute(command, timeout) }
            assertEquals("service response bytes", error.limitKind)
            assertEquals(1024L, error.limit)
        }
        val streamed = FakeFlareSolverr { respond(ByteReadChannel(ByteArray(1025) { 'a'.code.toByte() }), HttpStatusCode.OK) }
        client(streamed, serviceConfig(maxServiceResponseBytes = 1024)).use { client ->
            assertFailsWith<FlareSolverrResponseLimitException> { client.execute(command, timeout) }
        }
        val exact = FakeFlareSolverr {
            val body = okRoot(null).toString()
            respond(ByteReadChannel(body.toByteArray()), HttpStatusCode.OK)
        }
        val exactSize = okRoot(null).toString().toByteArray().size
        client(exact, serviceConfig(maxServiceResponseBytes = exactSize)).use { client ->
            assertEquals("ok", client.execute(command, timeout).string("status"))
        }
    }

    @Test
    fun `transport failures become unavailable with the original cause`(): Unit = runBlocking {
        val io = IOException("connection refused")
        client(FakeFlareSolverr { throw io }).use { client ->
            val error = assertFailsWith<FlareSolverrUnavailableException> { client.execute(command, timeout) }
            // Coroutine stack-trace recovery may copy the cause; type and message are preserved.
            assertIs<IOException>(error.cause)
            assertEquals(io.message, error.cause!!.message)
        }
        client(FakeFlareSolverr { throw UnresolvedAddressException() }).use { client ->
            assertIs<UnresolvedAddressException>(
                assertFailsWith<FlareSolverrUnavailableException> { client.execute(command, timeout) }.cause,
            )
        }
        client(FakeFlareSolverr { respondError(HttpStatusCode.BadGateway, "gateway") }).use { client ->
            assertFailsWith<FlareSolverrProtocolException> { client.execute(command, timeout) }
        }
        val programmer = IllegalStateException("not mapped")
        client(FakeFlareSolverr { throw programmer }).use { client ->
            val error = assertFailsWith<IllegalStateException> { client.execute(command, timeout) }
            assertFalse(error is FlareSolverrException)
            assertEquals("not mapped", error.message)
        }
    }

    @Test
    fun `closed client rejects calls and close is idempotent`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        val client = client(fake)
        client.close()
        client.close()
        assertFailsWith<IllegalStateException> { client.execute(command, timeout) }
        assertTrue(fake.calls.isEmpty())
    }

    @Test
    fun `in-flight calls never exceed the configured bound`(): Unit = runBlocking {
        val inFlight = AtomicInteger()
        val maxInFlight = AtomicInteger()
        val entered = kotlinx.coroutines.channels.Channel<Unit>(capacity = 3)
        val release = CompletableDeferred<Unit>()
        val fake = FakeFlareSolverr {
            val now = inFlight.incrementAndGet()
            maxInFlight.accumulateAndGet(now) { a, b -> maxOf(a, b) }
            entered.send(Unit)
            release.await()
            inFlight.decrementAndGet()
            replyJson(okRoot(null))
        }
        client(fake, serviceConfig(maxConcurrentCalls = 2)).use { client ->
            withTimeout(10_000) {
                val calls = (1..3).map { async { client.execute(command, timeout) } }
                entered.receive()
                entered.receive()
                assertEquals(2, inFlight.get())
                release.complete(Unit)
                calls.awaitAll()
                entered.receive()
            }
        }
        assertEquals(2, maxInFlight.get())
        assertEquals(3, fake.calls.size)
    }

    @Test
    fun `cancellation propagates and releases the call permit`(): Unit = runBlocking {
        val entered = CompletableDeferred<Unit>()
        val never = CompletableDeferred<Unit>()
        var first = true
        val fake = FakeFlareSolverr {
            if (first) {
                first = false
                entered.complete(Unit)
                never.await()
            }
            replyJson(okRoot(null))
        }
        client(fake, serviceConfig(maxConcurrentCalls = 1)).use { client ->
            withTimeout(10_000) {
                val job = launch(start = CoroutineStart.UNDISPATCHED) { client.execute(command, timeout) }
                entered.await()
                job.cancelAndJoin()
                assertTrue(job.isCancelled)
                assertEquals("ok", client.execute(command, timeout).string("status"))
            }
        }
    }

    @Test
    fun `solution parsing maps every field and applies defaults`() {
        val parsed = parseSolution(
            okRoot(
                solution(
                    url = "$SITE/final",
                    status = 203,
                    headers = buildJsonObject {
                        put("content-type", "text/html")
                        put("x-number", 5)
                        put(" ", "blank-name")
                    },
                    response = "<p>x</p>",
                    cookies = cookies(
                        cookie("a", "1", expires = 1_800_000_000.5),
                        cookie("b", "2", domain = null, path = null, expires = 0.0),
                        cookie("c", "3", path = "relative", expires = 9.9e12),
                        cookie("d", "4", session = true, expires = 1_800_000_000.0, secure = true),
                        buildJsonObject { put("value", "nameless") },
                        buildJsonObject { put("name", "valueless") },
                        buildJsonObject { put("name", " "); put("value", "v") },
                    ),
                ),
            ),
            limits(),
        )
        assertEquals("$SITE/final", parsed.url)
        assertEquals(203, parsed.status)
        assertEquals(listOf("content-type" to "text/html"), parsed.headers)
        assertEquals("<p>x</p>", parsed.response)
        assertEquals(BROWSER_UA, parsed.userAgent)
        assertEquals(listOf("a", "b", "c", "d"), parsed.cookies.map { it.name })
        assertEquals(Instant.ofEpochMilli(1_800_000_000_500), parsed.cookies[0].expiresAt)
        assertNull(parsed.cookies[1].expiresAt)
        assertNull(parsed.cookies[1].domain)
        assertEquals("/", parsed.cookies[1].path)
        assertEquals("/", parsed.cookies[2].path)
        assertNull(parsed.cookies[2].expiresAt)
        assertNull(parsed.cookies[3].expiresAt)
        assertTrue(parsed.cookies[3].secure)
        assertFalse(parsed.cookies[0].toString().contains("1\""))
        assertEquals("SolvedCookie(name=a, value=[REDACTED])", parsed.cookies[0].toString())
    }

    @Test
    fun `solution parsing tolerates absent optional members and rejects wrong shapes`() {
        val minimal = parseSolution(
            okRoot(buildJsonObject { put("headers", JsonNull); put("cookies", JsonNull) }),
            limits(),
        )
        assertNull(minimal.url)
        assertNull(minimal.status)
        assertNull(minimal.response)
        assertNull(minimal.userAgent)
        assertTrue(minimal.headers.isEmpty())
        assertTrue(minimal.cookies.isEmpty())
        assertNull(parseSolution(okRoot(JsonObject(emptyMap())), limits()).url)
        assertNull(parseSolution(okRoot(solution(url = " ")), limits()).url)
        assertNull(parseSolution(okRoot(solution(status = 1000)), limits()).status)
        assertNull(parseSolution(okRoot(solution(status = -1)), limits()).status)
        assertNull(
            parseSolution(okRoot(buildJsonObject { put("status", "200") }), limits()).status,
        )
        assertNull(parseSolution(okRoot(solution(userAgent = " ")), limits()).userAgent)
        assertNull(parseSolution(okRoot(solution(userAgent = "bad\nagent")), limits()).userAgent)

        val protocol = listOf(
            okRoot(null) to "missing solution object",
            okRoot(buildJsonObject { put("headers", JsonArray(emptyList())) }) to "solution headers is not an object",
            okRoot(buildJsonObject { put("cookies", JsonObject(emptyMap())) }) to "solution cookies is not an array",
            okRoot(buildJsonObject { put("cookies", JsonArray(listOf(JsonPrimitive("x")))) }) to "solution cookie is not an object",
        )
        protocol.forEach { (root, reason) ->
            assertEquals(reason, assertFailsWith<FlareSolverrProtocolException> { parseSolution(root, limits()) }.reason)
        }
    }

    @Test
    fun `solution parsing enforces every response bound at the boundary`() {
        val limits = limits(
            maxTargetLength = 30,
            maxResponseHeaders = 1,
            maxHeaderNameLength = 4,
            maxHeaderValueLength = 3,
            maxResponseCookies = 1,
            maxCookieValueLength = 3,
            maxUserAgentLength = 5,
        )
        val ok = parseSolution(
            okRoot(
                solution(
                    url = "https://e.test/" + "p".repeat(15),
                    headers = buildJsonObject { put("abcd", "123") },
                    cookies = cookies(cookie("abcd", "123")),
                    userAgent = "12345",
                ),
            ),
            limits,
        )
        assertEquals(30, ok.url!!.length)
        val over = listOf(
            solution(url = "https://e.test/" + "p".repeat(16)) to "solution url length",
            solution(headers = buildJsonObject { put("a", "1"); put("b", "2") }) to "response header count",
            solution(headers = buildJsonObject { put("abcde", "1") }) to "header name length",
            solution(headers = buildJsonObject { put("a", "1234") }) to "header value length",
            solution(cookies = cookies(cookie("a", "1"), cookie("b", "2"))) to "response cookie count",
            solution(cookies = cookies(cookie("abcde", "1"))) to "cookie name length",
            solution(cookies = cookies(cookie("a", "1234"))) to "cookie value length",
            solution(userAgent = "123456") to "user-agent length",
        )
        over.forEach { (solution, kind) ->
            val error = assertFailsWith<FlareSolverrResponseLimitException> {
                parseSolution(okRoot(solution.withShortDefaults()), limits)
            }
            assertEquals(kind, error.limitKind)
        }
    }

    @Test
    fun `cookie domain matching follows RFC 6265 host rules`() {
        fun cookieFor(domain: String?) = SolvedCookie("n", "v", domain, "/", false, null)
        assertTrue(cookieFor(".example.test").appliesToHost("example.test"))
        assertTrue(cookieFor("example.test").appliesToHost("www.EXAMPLE.test"))
        assertTrue(cookieFor(null).appliesToHost("anything.test"))
        assertFalse(cookieFor("example.test").appliesToHost("badexample.test"))
        assertFalse(cookieFor("other.test").appliesToHost("example.test"))
        assertFalse(cookieFor(".").appliesToHost("example.test"))
    }

    private fun kotlinx.serialization.json.JsonObject.withShortDefaults(): JsonObject = JsonObject(
        toMutableMap().apply {
            if (this["url"] == JsonPrimitive("$SITE/page")) put("url", JsonPrimitive("https://e.test/"))
            if (this["userAgent"] == JsonPrimitive(BROWSER_UA)) put("userAgent", JsonPrimitive("ua"))
        },
    )

    private fun limits(
        maxTargetLength: Int = 16 * 1024,
        maxResponseHeaders: Int = 256,
        maxHeaderNameLength: Int = 1024,
        maxHeaderValueLength: Int = 16 * 1024,
        maxResponseCookies: Int = 128,
        maxCookieValueLength: Int = 8192,
        maxUserAgentLength: Int = 1024,
    ) = SolutionLimits(
        maxTargetLength,
        maxResponseHeaders,
        maxHeaderNameLength,
        maxHeaderValueLength,
        maxResponseCookies,
        maxCookieValueLength,
        maxUserAgentLength,
    )
}
