package com.rmjtromp.trawler.flaresolverr

import com.rmjtromp.trawler.flaresolverr.challenge.ClearanceStore
import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.RequestBody
import com.rmjtromp.trawler.model.RequestMethod
import com.rmjtromp.trawler.web.policy.WebPolicyRejectedException
import com.rmjtromp.trawler.web.transport.WebOrigin
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class FlareSolverrTransportTest {
    private val clock = MutableClock(Instant.parse("2026-03-04T05:06:07Z"))

    private fun transport(
        fake: FakeFlareSolverr,
        config: FlareSolverrTransportConfig = FlareSolverrTransportConfig(serviceConfig()),
        destination: RecordingDestinationPolicy = RecordingDestinationPolicy(),
        store: ClearanceStore? = null,
    ) = FlareSolverrTransport.createWithEngine(config, destination, fake.engine, store, clock)

    @Test
    fun `GET maps to request get with maxTimeout cookies and per-request proxy`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        val proxy = FlareSolverrProxy("socks5://proxy.test:1080")
        val config = FlareSolverrTransportConfig(serviceConfig(proxy = proxy))
        transport(fake, config).use { transport ->
            transport.execute(
                CrawlRequest(
                    "$SITE/page",
                    headers = Headers.of(
                        Header("Cookie", "a=1; b = two=2"),
                        Header("Cookie", "c=3;"),
                        Header("User-Agent", "ignored"),
                        Header("Authorization", "Bearer never-forwarded"),
                    ),
                    body = RequestBody.empty(),
                ),
            )
        }
        val json = fake.calls.single().json
        assertEquals("request.get", json.str("cmd"))
        assertEquals("$SITE/page", json.str("url"))
        assertEquals(5_000, json["maxTimeout"]!!.jsonPrimitive.content.toInt())
        assertEquals(
            listOf("a" to "1", "b" to "two=2", "c" to "3"),
            json["cookies"]!!.jsonArray.map { it.jsonObject.str("name") to it.jsonObject.str("value") },
        )
        assertEquals(buildJsonObject { put("url", "socks5://proxy.test:1080") }, json["proxy"])
        assertNull(json["session"])
        assertNull(json["postData"])
        assertFalse(json.toString().contains("never-forwarded"))
        assertFalse(json.toString().contains("ignored"))
    }

    @Test
    fun `GET without cookies omits the cookies member`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        transport(fake).use { it.execute(CrawlRequest("$SITE/")) }
        assertNull(fake.calls.single().json["cookies"])
        assertNull(fake.calls.single().json["proxy"])
    }

    @Test
    fun `form POST maps to request post with UTF-8 postData`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        transport(fake).use { transport ->
            transport.execute(
                CrawlRequest(
                    "$SITE/submit",
                    method = RequestMethod("post"),
                    headers = Headers.of(Header("Content-Type", " Application/X-WWW-Form-Urlencoded; charset=UTF-8")),
                    body = RequestBody.from("a=1&name=J%C3%BCrgen&u=ü".toByteArray()),
                ),
            )
        }
        val json = fake.calls.single().json
        assertEquals("request.post", json.str("cmd"))
        assertEquals("a=1&name=J%C3%BCrgen&u=ü", json.str("postData"))
    }

    @Test
    fun `unsupported methods bodies cookies and targets fail before any call`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        val config = FlareSolverrTransportConfig(
            serviceConfig(),
            maxRequestBodyBytes = 8,
            maxRequestCookies = 2,
            maxTargetLength = 64,
        )
        val form = Header("Content-Type", "application/x-www-form-urlencoded")
        val cases = listOf(
            CrawlRequest("$SITE/", method = RequestMethod("PUT")) to "method-not-supported",
            CrawlRequest("$SITE/", method = RequestMethod("HEAD")) to "method-not-supported",
            CrawlRequest("$SITE/", body = RequestBody.from(byteArrayOf(1))) to "get-with-body",
            CrawlRequest("$SITE/", method = RequestMethod("POST")) to "post-without-body",
            CrawlRequest("$SITE/", method = RequestMethod("POST"), headers = Headers.of(form), body = RequestBody.from(ByteArray(9))) to
                "request-body-bound",
            CrawlRequest("$SITE/", method = RequestMethod("POST"), body = RequestBody.from("a=1".toByteArray())) to
                "post-body-not-form-urlencoded",
            CrawlRequest(
                "$SITE/",
                method = RequestMethod("POST"),
                headers = Headers.of(Header("Content-Type", "application/json")),
                body = RequestBody.from("{}".toByteArray()),
            ) to "post-body-not-form-urlencoded",
            CrawlRequest("$SITE/", method = RequestMethod("POST"), headers = Headers.of(form, form), body = RequestBody.from("a".toByteArray())) to
                "post-body-not-form-urlencoded",
            CrawlRequest("$SITE/", method = RequestMethod("POST"), headers = Headers.of(form), body = RequestBody.from(byteArrayOf(0xC3.toByte()))) to
                "post-body-not-utf8",
            CrawlRequest("$SITE/", headers = Headers.of(Header("Cookie", "a=1; b=2; c=3"))) to "request-cookie-bound",
            CrawlRequest("$SITE/", headers = Headers.of(Header("Cookie", "novalue"))) to "invalid-cookie-header",
            CrawlRequest("$SITE/", headers = Headers.of(Header("Cookie", "=value"))) to "invalid-cookie-header",
            CrawlRequest("$SITE/" + "x".repeat(64)) to "target-length-bound",
            CrawlRequest("$SITE/a b") to "target-contains-control-or-whitespace",
            CrawlRequest("$SITE/a\u0001") to "target-contains-control-or-whitespace",
            CrawlRequest("file:///etc/passwd") to "target-is-not-an-absolute-web-url",
            CrawlRequest("ftp://example.test:21/file") to "target-scheme-not-http",
            CrawlRequest("not a url") to "target-contains-control-or-whitespace",
            CrawlRequest("relative/path") to "target-is-not-an-absolute-web-url",
        )
        val destination = RecordingDestinationPolicy()
        transport(fake, config, destination).use { transport ->
            cases.forEach { (request, reason) ->
                val error = assertFailsWith<UnsupportedFlareSolverrRequestException>(reason) { transport.execute(request) }
                assertEquals(reason, error.reason)
            }
        }
        assertTrue(fake.calls.isEmpty())
        assertTrue(destination.evaluated.isEmpty())
    }

    @Test
    fun `initial target is evaluated by the destination policy before any call`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        val destination = RecordingDestinationPolicy(rejectMarker = "/private")
        transport(fake, destination = destination).use { transport ->
            val error = assertFailsWith<WebPolicyRejectedException> { transport.execute(CrawlRequest("$SITE/private")) }
            assertEquals("recording-destination", error.policyId)
            assertEquals("blocked-by-test", error.reason)
        }
        assertEquals(listOf("$SITE/private"), destination.evaluated)
        assertTrue(fake.calls.isEmpty())
    }

    @Test
    fun `a different final URL is evaluated again and a rejection discards the solution`(): Unit = runBlocking {
        val fake = FakeFlareSolverr { replyJson(okRoot(solution(url = "$SITE/private/landing"))) }
        val destination = RecordingDestinationPolicy(rejectMarker = "/private")
        val store = ClearanceStore(clock = clock)
        transport(fake, destination = destination, store = store).use { transport ->
            assertFailsWith<WebPolicyRejectedException> { transport.execute(CrawlRequest("$SITE/start")) }
        }
        assertEquals(listOf("$SITE/start", "$SITE/private/landing"), destination.evaluated)
        assertNull(store.current(WebOrigin.fromTarget(SITE)))
    }

    @Test
    fun `an unchanged or absent final URL is not evaluated twice`(): Unit = runBlocking {
        val destination = RecordingDestinationPolicy()
        val same = FakeFlareSolverr { replyJson(okRoot(solution(url = "$SITE/start"))) }
        transport(same, destination = destination).use { assertEquals("$SITE/start", it.execute(CrawlRequest("$SITE/start")).finalTarget) }
        val absent = FakeFlareSolverr { replyJson(okRoot(solution(url = null))) }
        transport(absent, destination = destination).use { assertEquals("$SITE/start", it.execute(CrawlRequest("$SITE/start")).finalTarget) }
        assertEquals(listOf("$SITE/start", "$SITE/start"), destination.evaluated)
    }

    @Test
    fun `a non-HTTP final URL is a protocol failure`(): Unit = runBlocking {
        val fake = FakeFlareSolverr { replyJson(okRoot(solution(url = "chrome-error://chromewebdata/"))) }
        transport(fake).use { transport ->
            val error = assertFailsWith<FlareSolverrProtocolException> { transport.execute(CrawlRequest("$SITE/start")) }
            assertEquals("final url rejected: target-is-not-an-absolute-web-url", error.reason)
            assertIs<UnsupportedFlareSolverrRequestException>(error.cause)
        }
    }

    @Test
    fun `solution maps to a core response with non-sensitive attributes only`(): Unit = runBlocking {
        val fake = FakeFlareSolverr {
            replyJson(
                okRoot(
                    solution(
                        url = "$SITE/final",
                        status = 200,
                        headers = buildJsonObject { put("content-type", "text/html; charset=ISO-8859-1") },
                        response = "<p>Grüße</p>",
                        cookies = cookies(cookie("cf_clearance", "SECRET-CLEARANCE"), cookie("session", "SECRET-SESSION")),
                    ),
                    message = "Challenge solved!",
                ),
            )
        }
        val request = CrawlRequest("$SITE/start", attributes = ContextAttributes.of(ContextAttribute("caller", "kept-on-request")))
        val response = transport(fake).use { it.execute(request) }
        assertSame(request, response.request)
        assertEquals(200, response.statusCode)
        assertEquals("$SITE/final", response.finalTarget)
        assertEquals(clock.now(), response.receivedAt)
        assertEquals(listOf("text/html; charset=ISO-8859-1"), response.headers["Content-Type"])
        assertContentEquals("<p>Grüße</p>".toByteArray(Charsets.UTF_8), response.body.toByteArray())
        assertEquals(BROWSER_UA, FlareSolverrResponseAttributes.userAgent(response))
        assertEquals(listOf("cf_clearance", "session"), FlareSolverrResponseAttributes.cookieNames(response))
        assertEquals("solved", FlareSolverrResponseAttributes.challengeOutcome(response))
        assertEquals("2", response.attributes[FlareSolverrResponseAttributes.COOKIE_COUNT])
        assertEquals("UTF-8", response.attributes[FlareSolverrResponseAttributes.BODY_CHARSET])
        assertNull(response.attributes["caller"])
        val rendered = response.attributes.toString() + response.toString()
        assertFalse(rendered.contains("SECRET"), rendered)
    }

    @Test
    fun `challenge outcome and optional attributes follow the service message and solution`(): Unit = runBlocking {
        val notDetected = FakeFlareSolverr { replyJson(okRoot(solution(userAgent = null), message = "Challenge not detected!")) }
        val response = transport(notDetected).use { it.execute(CrawlRequest("$SITE/")) }
        assertEquals("not-detected", FlareSolverrResponseAttributes.challengeOutcome(response))
        assertNull(FlareSolverrResponseAttributes.userAgent(response))
        assertEquals(emptyList(), FlareSolverrResponseAttributes.cookieNames(response))
        assertEquals("0", response.attributes[FlareSolverrResponseAttributes.COOKIE_COUNT])
        val unknown = FakeFlareSolverr { replyJson(okRoot(solution(), message = "")) }
        assertEquals("unknown", transport(unknown).use { FlareSolverrResponseAttributes.challengeOutcome(it.execute(CrawlRequest("$SITE/"))) })
        val synthetic = FakeFlareSolverr { replyJson(okRoot(solution(status = null))) }
        assertNull(transport(synthetic).use { it.execute(CrawlRequest("$SITE/")).statusCode })
    }

    @Test
    fun `missing or oversized body and oversized solution members are rejected`(): Unit = runBlocking {
        val missing = FakeFlareSolverr { replyJson(okRoot(solution(response = null))) }
        assertEquals(
            "solution has no response body",
            transport(missing).use { t -> assertFailsWith<FlareSolverrProtocolException> { t.execute(CrawlRequest("$SITE/")) }.reason },
        )
        val config = FlareSolverrTransportConfig(serviceConfig(), maxResponseBodyBytes = 4, maxResponseCookies = 0)
        val exact = FakeFlareSolverr { replyJson(okRoot(solution(response = "ü1"))) }
        assertEquals(3, transport(exact, config).use { it.execute(CrawlRequest("$SITE/")).body.size })
        val big = FakeFlareSolverr { replyJson(okRoot(solution(response = "üüü"))) }
        transport(big, config).use { t ->
            assertEquals("response body bytes", assertFailsWith<FlareSolverrResponseLimitException> { t.execute(CrawlRequest("$SITE/")) }.limitKind)
        }
        val cookie = FakeFlareSolverr { replyJson(okRoot(solution(response = "x", cookies = cookies(cookie("a", "b"))))) }
        transport(cookie, config).use { t ->
            assertEquals("response cookie count", assertFailsWith<FlareSolverrResponseLimitException> { t.execute(CrawlRequest("$SITE/")) }.limitKind)
        }
    }

    @Test
    fun `solved cookies and user agent are stored for the final origin when a store is supplied`(): Unit = runBlocking {
        val store = ClearanceStore(clock = clock)
        val fake = FakeFlareSolverr {
            replyJson(
                okRoot(
                    solution(
                        url = "https://www.example.test/landing",
                        cookies = cookies(
                            cookie("cf_clearance", "token", domain = ".example.test"),
                            cookie("other", "x", domain = "other.test"),
                            cookie("bad", "has;semicolon"),
                        ),
                    ),
                ),
            )
        }
        transport(fake, store = store).use { it.execute(CrawlRequest("$SITE/start")) }
        val stored = store.current(WebOrigin.fromTarget("https://www.example.test/"))!!
        assertEquals(listOf("cf_clearance"), stored.clearance.cookies.map { it.name })
        assertEquals("token", stored.clearance.cookies.single().value)
        assertEquals(BROWSER_UA, stored.clearance.userAgent)
        assertNull(store.current(WebOrigin.fromTarget(SITE)))
        val noUa = FakeFlareSolverr { replyJson(okRoot(solution(userAgent = null, url = "https://ua.example.test/"))) }
        transport(noUa, store = store).use { it.execute(CrawlRequest("https://ua.example.test/")) }
        assertNull(store.current(WebOrigin.fromTarget("https://ua.example.test/")))
    }

    @Test
    fun `service failures propagate unchanged as typed exceptions`(): Unit = runBlocking {
        val fake = FakeFlareSolverr { replyJson(errorRoot("Error: net::ERR_NAME_NOT_RESOLVED"), io.ktor.http.HttpStatusCode.InternalServerError) }
        transport(fake).use { t ->
            assertEquals(
                "Error: net::ERR_NAME_NOT_RESOLVED",
                assertFailsWith<FlareSolverrErrorException> { t.execute(CrawlRequest("$SITE/")) }.serviceMessage,
            )
        }
        assertEquals(1, fake.calls.size)
    }

    @Test
    fun `stateless transport start performs no network work and stop closes it`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        val transport = transport(fake)
        assertEquals("flaresolverr", transport.id)
        transport.start(START_CONTEXT)
        transport.start(START_CONTEXT)
        assertTrue(fake.calls.isEmpty())
        transport.stop(STOP_CONTEXT)
        transport.stop(STOP_CONTEXT)
        transport.close()
        assertFailsWith<IllegalStateException> { transport.execute(CrawlRequest("$SITE/")) }
        assertFailsWith<IllegalStateException> { transport.start(START_CONTEXT) }
        assertTrue(fake.calls.isEmpty())
    }

    @Test
    fun `session pool is created on start used exclusively and destroyed on stop`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        val proxy = FlareSolverrProxy("http://proxy.test:3128", "proxy-user", "proxy-pass")
        val config = FlareSolverrTransportConfig(
            serviceConfig(proxy = proxy),
            sessions = FlareSolverrSessionConfig(count = 2, ttl = Duration.ofMinutes(15)),
        )
        val transport = transport(fake, config)
        assertFailsWith<IllegalStateException> { transport.execute(CrawlRequest("$SITE/")) }
        transport.start(START_CONTEXT)
        transport.start(START_CONTEXT)
        val creates = fake.commands("sessions.create")
        assertEquals(2, creates.size)
        creates.forEach {
            assertEquals(
                buildJsonObject {
                    put("url", "http://proxy.test:3128")
                    put("username", "proxy-user")
                    put("password", "proxy-pass")
                },
                it.json["proxy"],
            )
            assertNull(it.json["session"])
        }
        assertEquals(listOf("session-1", "session-2"), fake.liveSessions)

        repeat(3) { transport.execute(CrawlRequest("$SITE/$it")) }
        val requests = fake.commands("request.get")
        assertEquals(listOf("session-1", "session-2", "session-1"), requests.map { it.string("session") })
        requests.forEach {
            assertEquals(15, it.json["session_ttl_minutes"]!!.jsonPrimitive.content.toInt())
            assertNull(it.json["proxy"], "per-request proxy is ignored by FlareSolverr when a session is used")
        }

        transport.stop(STOP_CONTEXT)
        transport.stop(STOP_CONTEXT)
        assertEquals(listOf("session-1", "session-2"), fake.commands("sessions.destroy").map { it.string("session") })
        assertTrue(fake.liveSessions.isEmpty())
        assertFailsWith<IllegalStateException> { transport.execute(CrawlRequest("$SITE/")) }
    }

    @Test
    fun `sessions without ttl omit session_ttl_minutes`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        val transport = transport(fake, FlareSolverrTransportConfig(serviceConfig(), sessions = FlareSolverrSessionConfig()))
        transport.start(START_CONTEXT)
        transport.execute(CrawlRequest("$SITE/"))
        transport.stop(STOP_CONTEXT)
        val get = fake.commands("request.get").single()
        assertEquals("session-1", get.string("session"))
        assertNull(get.json["session_ttl_minutes"])
        assertNull(fake.commands("sessions.create").single().json["proxy"])
    }

    @Test
    fun `failure during start destroys created sessions and a later start succeeds`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        var creates = 0
        val defaultCreate: suspend io.ktor.client.engine.mock.MockRequestHandleScope.(FakeCall) -> io.ktor.client.request.HttpResponseData = {
            creates++
            if (creates == 2) {
                replyJson(errorRoot("Error: cannot launch browser"), io.ktor.http.HttpStatusCode.InternalServerError)
            } else {
                replyJson(buildJsonObject { put("status", "ok"); put("session", "s-$creates") })
            }
        }
        fake.onSessionCreate = defaultCreate
        val transport = transport(fake, FlareSolverrTransportConfig(serviceConfig(), sessions = FlareSolverrSessionConfig(count = 2)))
        val error = assertFailsWith<FlareSolverrErrorException> { transport.start(START_CONTEXT) }
        assertEquals("Error: cannot launch browser", error.serviceMessage)
        assertEquals(listOf("s-1"), fake.commands("sessions.destroy").map { it.string("session") })
        assertFailsWith<IllegalStateException> { transport.execute(CrawlRequest("$SITE/")) }

        withTimeout(10_000) { transport.start(START_CONTEXT) }
        transport.execute(CrawlRequest("$SITE/a"))
        transport.execute(CrawlRequest("$SITE/b"))
        assertEquals(listOf("s-3", "s-4"), fake.commands("request.get").map { it.string("session") })
        transport.stop(STOP_CONTEXT)
        assertEquals(listOf("s-1", "s-3", "s-4"), fake.commands("sessions.destroy").map { it.string("session") })
    }

    @Test
    fun `start rollback failures are suppressed onto the original failure`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        var creates = 0
        fake.onSessionCreate = {
            creates++
            if (creates == 1) replyJson(buildJsonObject { put("status", "ok"); put("session", "s-1") }) else replyText("garbage")
        }
        fake.onSessionDestroy = { replyJson(errorRoot("Error: The session doesn't exist.")) }
        val transport = transport(fake, FlareSolverrTransportConfig(serviceConfig(), sessions = FlareSolverrSessionConfig(count = 2)))
        val error = assertFailsWith<FlareSolverrProtocolException> { transport.start(START_CONTEXT) }
        assertIs<FlareSolverrErrorException>(error.suppressed.single())
        transport.close()
    }

    @Test
    fun `sessions create without a usable session id is a protocol failure`(): Unit = runBlocking {
        listOf(
            buildJsonObject { put("status", "ok") },
            buildJsonObject { put("status", "ok"); put("session", " ") },
            buildJsonObject { put("status", "ok"); put("session", "s".repeat(257)) },
        ).forEach { reply ->
            val fake = FakeFlareSolverr()
            fake.onSessionCreate = { replyJson(reply) }
            val transport = transport(fake, FlareSolverrTransportConfig(serviceConfig(), sessions = FlareSolverrSessionConfig()))
            assertEquals(
                "sessions.create returned no usable session",
                assertFailsWith<FlareSolverrProtocolException> { transport.start(START_CONTEXT) }.reason,
            )
            transport.close()
        }
    }

    @Test
    fun `stop attempts every destroy and rethrows the first failure with later ones suppressed`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        fake.onSessionDestroy = { replyJson(errorRoot("Error: destroy ${it.string("session")} failed")) }
        val transport = transport(fake, FlareSolverrTransportConfig(serviceConfig(), sessions = FlareSolverrSessionConfig(count = 3)))
        transport.start(START_CONTEXT)
        val error = assertFailsWith<FlareSolverrErrorException> { transport.stop(STOP_CONTEXT) }
        assertEquals(3, fake.commands("sessions.destroy").size)
        assertEquals(2, error.suppressed.size)
        transport.stop(STOP_CONTEXT)
        assertEquals(3, fake.commands("sessions.destroy").size)
        assertFailsWith<IllegalStateException> { transport.execute(CrawlRequest("$SITE/")) }
    }

    @Test
    fun `a session is borrowed exclusively and returned when its caller is cancelled`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        val firstEntered = CompletableDeferred<Unit>()
        val never = CompletableDeferred<Unit>()
        val seen = CopyOnWriteArrayList<String>()
        fake.onRequest = { call ->
            seen += call.string("url")!!
            if (call.string("url") == "$SITE/slow") {
                firstEntered.complete(Unit)
                never.await()
            }
            replyJson(okRoot(solution()))
        }
        val transport = transport(fake, FlareSolverrTransportConfig(serviceConfig(), sessions = FlareSolverrSessionConfig(count = 1)))
        transport.start(START_CONTEXT)
        withTimeout(10_000) {
            val slow = launch(start = CoroutineStart.UNDISPATCHED) { transport.execute(CrawlRequest("$SITE/slow")) }
            firstEntered.await()
            val waiting = async(start = CoroutineStart.UNDISPATCHED) { transport.execute(CrawlRequest("$SITE/next")) }
            assertEquals(listOf("$SITE/slow"), seen, "the second request must wait for the only session")
            slow.cancelAndJoin()
            assertTrue(slow.isCancelled)
            assertEquals(200, waiting.await().statusCode)
        }
        assertEquals(listOf("session-1", "session-1"), fake.commands("request.get").map { it.string("session") })
        transport.stop(STOP_CONTEXT)
        assertTrue(fake.liveSessions.isEmpty())
    }

    @Test
    fun `cancellation of a stateless call propagates without converting to a failure`(): Unit = runBlocking {
        val entered = CompletableDeferred<Unit>()
        val never = CompletableDeferred<Unit>()
        val fake = FakeFlareSolverr {
            entered.complete(Unit)
            never.await()
            replyJson(okRoot(solution()))
        }
        val transport = transport(fake, FlareSolverrTransportConfig(serviceConfig(maxConcurrentCalls = 1)))
        withTimeout(10_000) {
            val job = launch(start = CoroutineStart.UNDISPATCHED) { transport.execute(CrawlRequest("$SITE/")) }
            entered.await()
            job.cancelAndJoin()
            assertTrue(job.isCancelled)
        }
        transport.close()
    }

    private fun JsonObject.str(name: String): String? = (this[name] as? JsonPrimitive)?.content
}
