package com.rmjtromp.trawler.web.transport

import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.extension.ExtensionStopReason
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.RequestBody
import com.rmjtromp.trawler.model.RequestMethod
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.web.policy.AllowAllWebDestinationPolicy
import com.rmjtromp.trawler.web.policy.DefaultHeaderRedactionPolicy
import com.rmjtromp.trawler.web.policy.DefaultHttpFailureClassifier
import com.rmjtromp.trawler.web.policy.DefaultRedirectHeaderPolicy
import com.rmjtromp.trawler.web.policy.DefaultRetryAfterPolicy
import com.rmjtromp.trawler.web.policy.InvalidWebRedirectException
import com.rmjtromp.trawler.web.policy.NoRobotsPolicy
import com.rmjtromp.trawler.web.policy.PublicWebSafetyPolicy
import com.rmjtromp.trawler.web.policy.RobotsDecision
import com.rmjtromp.trawler.web.policy.RobotsFetchResult
import com.rmjtromp.trawler.web.policy.RobotsFetcher
import com.rmjtromp.trawler.web.policy.RobotsPolicy
import com.rmjtromp.trawler.web.policy.WebPolicyRejectedException
import com.rmjtromp.trawler.web.policy.WebResponseTooLargeException
import com.rmjtromp.trawler.web.session.CookieSession
import com.rmjtromp.trawler.web.session.NoCookieSession
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockEngineConfig
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.HttpRequestTimeoutException
import io.ktor.client.request.HttpRequestData
import io.ktor.client.request.HttpResponseData
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.OutgoingContent
import io.ktor.http.headersOf
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.net.Inet6Address
import java.net.InetAddress
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.DeflaterOutputStream
import java.util.zip.ZipException
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

/**
 * Transport-level safety and edge behavior: redirect hop re-checks with the
 * real public-web policy, credential stripping, robots plumbing, response
 * bounds, total timeout, and the public-web factory. All network traffic is
 * MockEngine; addresses come from a handwritten resolver.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class KtorTransportSafetyTest {
    private val publicAddress = v4(93, 184, 216, 34)

    @Test
    fun `every redirect hop reruns public-web safety and blocks internal destinations before sending`(): Unit = runBlocking {
        val resolver = MapResolver(
            "public.test" to listOf(publicAddress),
            "internal.test" to listOf(v4(10, 0, 0, 7)),
            "rebind.test" to listOf(publicAddress, v4(127, 0, 0, 1)),
            "169.254.169.254" to listOf(v4(169, 254, 169, 254)),
            "::ffff:127.0.0.1" to listOf(Inet6Address.getByAddress(null, mappedBytes(127, 0, 0, 1), -1)),
            "64:ff9b::a00:1" to listOf(Inet6Address.getByAddress(null, nat64Bytes(10, 0, 0, 1), -1)),
        )
        listOf(
            "http://internal.test/admin" to "destination-address-blocked:10.0.0.7",
            "https://rebind.test/" to "destination-address-blocked:127.0.0.1",
            "http://169.254.169.254/latest/meta-data/" to "destination-address-blocked:169.254.169.254",
            "http://[::ffff:127.0.0.1]:8080/" to "destination-address-blocked:0:0:0:0:0:ffff:7f00:1",
            "http://[64:ff9b::a00:1]/" to "destination-address-blocked:64:ff9b:0:0:0:0:a00:1",
            "https://user:secret@public.test/" to "userinfo-is-not-allowed",
            "gopher://public.test:70/" to "scheme-not-allowed",
        ).forEach { (location, reason) ->
            val seen = CopyOnWriteArrayList<HttpRequestData>()
            val harness = harness(policies(destination = PublicWebSafetyPolicy(resolver))) { request ->
                seen += request
                reply(status = HttpStatusCode.Found, headers = headersOf(HttpHeaders.Location, location))
            }
            try {
                val failure = assertFailsWith<WebPolicyRejectedException>(location) {
                    harness.transport.execute(CrawlRequest("https://public.test/start"))
                }
                assertEquals("public-web-safety", failure.policyId, location)
                assertEquals(reason, failure.reason, location)
                assertEquals(listOf("https://public.test/start"), seen.map { it.url.toString() }, location)
            } finally {
                harness.close()
            }
        }
    }

    @Test
    fun `redirects to non-web schemes fail before any destination or network call`(): Unit = runBlocking {
        listOf("file:///etc/passwd", "ftp://public.test/file", "mailto:someone@public.test").forEach { location ->
            val destination = RecordingDestination()
            val seen = CopyOnWriteArrayList<HttpRequestData>()
            val harness = harness(policies(destination = destination)) { request ->
                seen += request
                reply(status = HttpStatusCode.MovedPermanently, headers = headersOf(HttpHeaders.Location, location))
            }
            try {
                val failure = assertFailsWith<InvalidWebRedirectException>(location) {
                    harness.transport.execute(CrawlRequest("https://public.test/start"))
                }
                assertEquals("Redirect target has no valid web origin", failure.message, location)
                // Coroutine stack-trace recovery may insert a copy, so search the whole cause chain.
                assertTrue(generateSequence(failure.cause) { it.cause }.any { it is IllegalArgumentException }, location)
                assertEquals(listOf("https://public.test/start"), destination.targets, location)
                assertEquals(1, seen.size, location)
            } finally {
                harness.close()
            }
        }
    }

    @Test
    fun `an allowed target without a web origin is rejected without network`(): Unit = runBlocking {
        val seen = CopyOnWriteArrayList<HttpRequestData>()
        val harness = harness(policies()) { request ->
            seen += request
            reply()
        }
        try {
            val failure = assertFailsWith<WebPolicyRejectedException> {
                harness.transport.execute(CrawlRequest("ftp://example.test/file"))
            }
            assertEquals("allow-all-web-destinations", failure.policyId)
            assertEquals("invalid-web-origin", failure.reason)
            assertIs<IllegalArgumentException>(failure.cause)
            assertTrue(seen.isEmpty())
        } finally {
            harness.close()
        }
    }

    @Test
    fun `credentials are stripped on every kind of cross-origin hop and never restored`(): Unit = runBlocking {
        val sensitive = listOf(
            "Authorization", "proxy-authorization", "Cookie", "X-Api-Key", "x-auth-token",
            "X-Amz-Security-Token", "X-Amz-Credential", "X-Goog-User-Project",
        )
        val start = "https://a.test/start"
        val crossOrigin = listOf(
            "http://a.test/downgrade",
            "https://a.test:8443/port",
            "https://sub.a.test/subdomain",
            "https://other.test/host",
        )
        crossOrigin.forEach { location ->
            val seen = CopyOnWriteArrayList<HttpRequestData>()
            val harness = harness(policies()) { request ->
                seen += request
                when (seen.size) {
                    1 -> reply(status = HttpStatusCode.TemporaryRedirect, headers = headersOf(HttpHeaders.Location, location))
                    2 -> reply(status = HttpStatusCode.Found, headers = headersOf(HttpHeaders.Location, start + "-again"))
                    else -> reply("done".toByteArray())
                }
            }
            try {
                val response = harness.transport.execute(
                    CrawlRequest(start, headers = Headers.from(sensitive.map { Header(it, "secret-$it") } + Header("X-Trace", "keep"))),
                )
                assertEquals(200, response.statusCode, location)
                assertEquals(3, seen.size, location)
                sensitive.forEach { name -> assertEquals(listOf("secret-$name"), seen[0].headers.getAll(name), "$location first hop $name") }
                listOf(1, 2).forEach { hop ->
                    sensitive.forEach { name ->
                        assertNull(seen[hop].headers.getAll(name), "$location hop $hop must not carry $name")
                    }
                    assertEquals(listOf("keep"), seen[hop].headers.getAll("X-Trace"), "$location hop $hop")
                    assertEquals(listOf("TrawlerBot/0.1"), seen[hop].headers.getAll(HttpHeaders.UserAgent), "$location hop $hop")
                }
                assertEquals("2", response.attributes["trawler.web.redirect-count"])
            } finally {
                harness.close()
            }
        }
    }

    @Test
    fun `same-origin redirects keep credentials and body removal drops entity headers`(): Unit = runBlocking {
        val seen = CopyOnWriteArrayList<HttpRequestData>()
        val harness = harness(policies()) { request ->
            seen += request
            if (seen.size == 1) {
                reply(status = HttpStatusCode.SeeOther, headers = headersOf(HttpHeaders.Location, "/after"))
            } else {
                reply()
            }
        }
        try {
            harness.transport.execute(
                CrawlRequest(
                    "https://a.test/form",
                    method = RequestMethod("HEAD"),
                    headers = Headers.of(Header("Authorization", "Bearer same"), Header("Content-Type", "text/plain")),
                    body = RequestBody.from("payload".toByteArray()),
                ),
            )
            assertEquals("HEAD", seen[1].method.value)
            assertEquals(listOf("Bearer same"), seen[1].headers.getAll(HttpHeaders.Authorization))
            assertNull(seen[1].body.bytesOrNull())
            assertNull(seen[1].body.contentType)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `302 keeps non-POST methods and bodies while a lowercase post becomes GET`(): Unit = runBlocking {
        listOf(
            Triple(RequestMethod("PUT"), "PUT", true),
            Triple(RequestMethod("post"), "GET", false),
        ).forEach { (method, expectedMethod, keepsBody) ->
            val seen = CopyOnWriteArrayList<HttpRequestData>()
            val harness = harness(policies()) { request ->
                seen += request
                if (seen.size == 1) reply(status = HttpStatusCode.Found, headers = headersOf(HttpHeaders.Location, "/next")) else reply()
            }
            try {
                harness.transport.execute(CrawlRequest("https://a.test/", method = method, body = RequestBody.from("b".toByteArray())))
                assertEquals(expectedMethod, seen[1].method.value, method.value)
                if (keepsBody) {
                    assertContentEquals("b".toByteArray(), seen[1].body.bytesOrNull(), method.value)
                } else {
                    assertNull(seen[1].body.bytesOrNull(), method.value)
                }
            } finally {
                harness.close()
            }
        }
    }

    @Test
    fun `301 keeps GET and 303 keeps a lowercase head`(): Unit = runBlocking {
        listOf(
            Triple(HttpStatusCode.MovedPermanently, RequestMethod.GET, "GET"),
            Triple(HttpStatusCode.SeeOther, RequestMethod("head"), "head"),
        ).forEach { (status, method, expectedMethod) ->
            val seen = CopyOnWriteArrayList<HttpRequestData>()
            val harness = harness(policies()) { request ->
                seen += request
                if (seen.size == 1) reply(status = status, headers = headersOf(HttpHeaders.Location, "/next")) else reply()
            }
            try {
                harness.transport.execute(CrawlRequest("https://a.test/", method = method))
                assertEquals(expectedMethod, seen[1].method.value, status.toString())
            } finally {
                harness.close()
            }
        }
    }

    @Test
    fun `blank locations and non-redirect 3xx responses are returned without following`(): Unit = runBlocking {
        listOf(
            HttpStatusCode.Found to headersOf(HttpHeaders.Location, "   "),
            HttpStatusCode.NotModified to headersOf(HttpHeaders.Location, "/elsewhere"),
            HttpStatusCode.MultipleChoices to headersOf(HttpHeaders.Location, "/elsewhere"),
        ).forEach { (status, headers) ->
            val seen = CopyOnWriteArrayList<HttpRequestData>()
            val harness = harness(policies()) { request ->
                seen += request
                reply(status = status, headers = headers)
            }
            try {
                val response = harness.transport.execute(CrawlRequest("https://a.test/"))
                assertEquals(status.value, response.statusCode)
                assertEquals(1, seen.size)
                assertEquals("0", response.attributes["trawler.web.redirect-count"])
            } finally {
                harness.close()
            }
        }
    }

    @Test
    fun `robots decisions gate the page and merge crawl delay into pacing`(): Unit = runBlocking {
        val pacing = RecordingPacing()
        val delayed = harness(policies(robots = FixedRobots(RobotsDecision.Allowed(Duration.ofSeconds(2))), pacing = pacing)) { _ ->
            reply("page".toByteArray())
        }
        try {
            delayed.transport.execute(CrawlRequest("https://a.test:8443/page"))
            assertEquals(listOf(WebOrigin("https", "a.test", 8443) to Duration.ofSeconds(2)), pacing.robotsDelays)
        } finally {
            delayed.close()
        }

        val seen = CopyOnWriteArrayList<HttpRequestData>()
        val denied = harness(policies(robots = FixedRobots(RobotsDecision.Disallowed("robots-disallowed")))) { request ->
            seen += request
            reply()
        }
        try {
            val failure = assertFailsWith<WebPolicyRejectedException> {
                denied.transport.execute(CrawlRequest("https://a.test/private"))
            }
            assertEquals("fixed-robots", failure.policyId)
            assertEquals("robots-disallowed", failure.reason)
            assertTrue(seen.isEmpty())
        } finally {
            denied.close()
        }
    }

    @Test
    fun `robots fetch targets keep explicit non-default ports and bracket IPv6 hosts`(): Unit = runBlocking {
        listOf(
            "http://a.test/page" to "http://a.test/robots.txt",
            "https://a.test/page" to "https://a.test/robots.txt",
            "http://a.test:443/page" to "http://a.test:443/robots.txt",
            "https://a.test:80/page" to "https://a.test:80/robots.txt",
            "https://a.test:8443/page" to "https://a.test:8443/robots.txt",
            "http://[2001:db8::1]:8080/page" to "http://[2001:db8::1]:8080/robots.txt",
            "https://[2001:db8::1]/page" to "https://[2001:db8::1]/robots.txt",
        ).forEach { (target, expectedRobots) ->
            val robotsTargets = CopyOnWriteArrayList<String>()
            val robots = FetchingRobots()
            val harness = harness(policies(robots = robots)) { request ->
                if (request.url.encodedPath == "/robots.txt") {
                    robotsTargets += request.url.toString()
                    reply("User-agent: *".toByteArray(), status = HttpStatusCode.ServiceUnavailable)
                } else {
                    reply()
                }
            }
            try {
                harness.transport.execute(CrawlRequest(target))
                assertEquals(listOf(expectedRobots), robotsTargets, target)
                val result = assertIs<RobotsFetchResult.Available>(robots.results.single(), target)
                assertEquals(503, result.statusCode)
                assertEquals("User-agent: *", result.body)
                assertEquals(FIXED_NOW, result.fetchedAt)
            } finally {
                harness.close()
            }
        }
    }

    @Test
    fun `robots fetch failures become unavailable results and always release reservations`(): Unit = runBlocking {
        val robots = FetchingRobots()
        val concurrency = TrackingConcurrency()
        val pacing = RecordingPacing()
        val failing = harness(policies(robots = robots, concurrency = concurrency, pacing = pacing)) { request ->
            if (request.url.encodedPath == "/robots.txt") throw IOException("robots connection reset")
            reply("page".toByteArray())
        }
        try {
            assertContentEquals("page".toByteArray(), failing.transport.execute(CrawlRequest("https://a.test/")).body.toByteArray())
            val unavailable = assertIs<RobotsFetchResult.Unavailable>(robots.results.single())
            assertTrue(unavailable.reason.startsWith("robots-fetch-failed:"), unavailable.reason)
            assertEquals(listOf(WebRequestKind.ROBOTS, WebRequestKind.PAGE), concurrency.kinds)
            assertEquals(0, concurrency.active)
            assertEquals(0, pacing.active)
        } finally {
            failing.close()
        }

        val anonymousRobots = FetchingRobots()
        val anonymous = harness(policies(robots = anonymousRobots)) { request ->
            if (request.url.encodedPath == "/robots.txt") throw object : IOException("anonymous failure") {}
            reply()
        }
        try {
            anonymous.transport.execute(CrawlRequest("https://a.test/"))
            val reason = assertIs<RobotsFetchResult.Unavailable>(anonymousRobots.results.single()).reason
            assertTrue(reason.startsWith("robots-fetch-failed:"), reason)
        } finally {
            anonymous.close()
        }

        val pacingRobots = FetchingRobots()
        val refusingConcurrency = TrackingConcurrency()
        val refusingPacing = RecordingPacing(refuse = setOf(WebRequestKind.ROBOTS))
        val refused = harness(policies(robots = pacingRobots, concurrency = refusingConcurrency, pacing = refusingPacing)) { _ ->
            reply()
        }
        try {
            refused.transport.execute(CrawlRequest("https://a.test/"))
            assertEquals(
                RobotsFetchResult.Unavailable("robots-fetch-failed:PacingWaitExceededException"),
                pacingRobots.results.single(),
            )
            assertEquals(0, refusingConcurrency.active)
            assertEquals(2, refusingConcurrency.released)
        } finally {
            refused.close()
        }
    }

    @Test
    fun `robots fetch cancellation propagates and releases its permit`(): Unit = runBlocking {
        val concurrency = TrackingConcurrency()
        val harness = harness(policies(robots = FetchingRobots(), concurrency = concurrency)) { request ->
            if (request.url.encodedPath == "/robots.txt") throw CancellationException("robots cancelled")
            reply()
        }
        try {
            assertFailsWith<CancellationException> { harness.transport.execute(CrawlRequest("https://a.test/")) }
            assertEquals(listOf(WebRequestKind.ROBOTS), concurrency.kinds)
            assertEquals(0, concurrency.active)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `page pacing refusal releases the concurrency permit without sending`(): Unit = runBlocking {
        val concurrency = TrackingConcurrency()
        val seen = CopyOnWriteArrayList<HttpRequestData>()
        val harness = harness(policies(concurrency = concurrency, pacing = RecordingPacing(refuse = setOf(WebRequestKind.PAGE)))) { request ->
            seen += request
            reply()
        }
        try {
            assertFailsWith<PacingWaitExceededException> { harness.transport.execute(CrawlRequest("https://a.test/")) }
            assertEquals(1, concurrency.released)
            assertEquals(0, concurrency.active)
            assertTrue(seen.isEmpty())
        } finally {
            harness.close()
        }
    }

    @Test
    fun `declared content length above the bound fails before the body is read`(): Unit = runBlocking {
        val harness = harness(policies(), KtorTransportConfig(maxCompressedBodyBytes = 4)) { _ ->
            reply("tiny".toByteArray(), headers = headersOf(HttpHeaders.ContentLength, "5000000000"))
        }
        try {
            val failure = assertFailsWith<WebResponseTooLargeException> { harness.transport.execute(CrawlRequest("https://a.test/")) }
            assertEquals("compressed", failure.limitKind)
            assertEquals(4, failure.limitBytes)
        } finally {
            harness.close()
        }

        val unparsable = harness(policies(), KtorTransportConfig(maxCompressedBodyBytes = 4)) { _ ->
            reply("tiny".toByteArray(), headers = headersOf(HttpHeaders.ContentLength, "not-a-number"))
        }
        try {
            assertContentEquals("tiny".toByteArray(), unparsable.transport.execute(CrawlRequest("https://a.test/")).body.toByteArray())
        } finally {
            unparsable.close()
        }
    }

    @Test
    fun `content encodings are case-insensitive and empty tokens mean identity`(): Unit = runBlocking {
        listOf(" , ", "IDENTITY", "Identity ").forEach { encoding ->
            val harness = harness(policies()) { _ -> reply("plain".toByteArray(), headers = headersOf(HttpHeaders.ContentEncoding, encoding)) }
            try {
                assertContentEquals("plain".toByteArray(), harness.transport.execute(CrawlRequest("https://a.test/")).body.toByteArray(), encoding)
            } finally {
                harness.close()
            }
        }
        val deflated = harness(policies()) { _ -> reply(deflate("inflated".toByteArray()), headers = headersOf(HttpHeaders.ContentEncoding, "DEFLATE")) }
        try {
            assertContentEquals("inflated".toByteArray(), deflated.transport.execute(CrawlRequest("https://a.test/")).body.toByteArray())
        } finally {
            deflated.close()
        }
    }

    @Test
    fun `deflate bombs and malformed deflate bodies fail visibly`(): Unit = runBlocking {
        val bomb = deflate(ByteArray(4 * 1024 * 1024))
        val bounded = harness(policies(), KtorTransportConfig(maxDecompressedBodyBytes = 1_024)) { _ ->
            reply(bomb, headers = headersOf(HttpHeaders.ContentEncoding, "deflate"))
        }
        try {
            val failure = assertFailsWith<WebResponseTooLargeException> { bounded.transport.execute(CrawlRequest("https://a.test/")) }
            assertEquals("decompressed", failure.limitKind)
            assertEquals(1_024, failure.limitBytes)
        } finally {
            bounded.close()
        }
        val malformed = harness(policies()) { _ -> reply(byteArrayOf(1, 2, 3, 4), headers = headersOf(HttpHeaders.ContentEncoding, "deflate")) }
        try {
            assertFailsWith<ZipException> { malformed.transport.execute(CrawlRequest("https://a.test/")) }
        } finally {
            malformed.close()
        }
    }

    @Test
    fun `total request timeout is a classifiable failure rather than coroutine cancellation`() = runTest {
        val started = CompletableDeferred<Unit>()
        val stuckDestination = object : WebDestinationPolicy {
            override val id: String = "stuck-destination"
            override suspend fun evaluate(request: CrawlRequest, history: RedirectHistory): DestinationDecision {
                started.complete(Unit)
                awaitCancellation()
            }
        }
        val harness = harness(policies(destination = stuckDestination), KtorTransportConfig(totalRequestTimeout = Duration.ofSeconds(3))) { _ ->
            reply()
        }
        try {
            val failure = assertFailsWith<HttpRequestTimeoutException> {
                harness.transport.execute(CrawlRequest("https://a.test/slow"))
            }
            assertTrue(started.isCompleted)
            assertEquals(3_000L, testScheduler.currentTime)
            assertEquals(
                "http-request-timeout",
                com.rmjtromp.trawler.web.policy.DefaultWebFailureClassifier.classify(
                    failure,
                    com.rmjtromp.trawler.policy.FailureContext(
                        com.rmjtromp.trawler.model.CrawlerContext("engine", "run", "pipeline"),
                        CrawlRequest("https://a.test/slow"),
                    ),
                ).category,
            )
        } finally {
            harness.close()
        }
    }

    @Test
    fun `caller cancellation during a request still propagates as cancellation`() = runTest {
        val started = CompletableDeferred<Unit>()
        val released = AtomicBoolean(false)
        val stuckDestination = object : WebDestinationPolicy {
            override val id: String = "stuck-destination"
            override suspend fun evaluate(request: CrawlRequest, history: RedirectHistory): DestinationDecision {
                started.complete(Unit)
                try {
                    awaitCancellation()
                } finally {
                    released.set(true)
                }
            }
        }
        val harness = harness(policies(destination = stuckDestination)) { _ -> reply() }
        try {
            var observed: Throwable? = null
            val job = launch(start = CoroutineStart.UNDISPATCHED) {
                try {
                    harness.transport.execute(CrawlRequest("https://a.test/slow"))
                } catch (error: Throwable) {
                    observed = error
                    throw error
                }
            }
            started.await()
            job.cancelAndJoin()
            assertTrue(job.isCancelled)
            assertTrue(released.get())
            assertIs<CancellationException>(observed)
            assertFalse(observed is HttpRequestTimeoutException)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `public-web factory assembles fail-closed policies and blocks loopback literals without network`(): Unit = runBlocking {
        val transport = KtorTransport.publicWeb(clock = FixedClock(FIXED_NOW))
        try {
            assertEquals("ktor-cio", transport.id)
            listOf(
                "http://127.0.0.1:9/" to "destination-address-blocked:127.0.0.1",
                "http://[::1]:9/" to "destination-address-blocked:0:0:0:0:0:0:0:1",
                "http://169.254.169.254/latest/meta-data/" to "destination-address-blocked:169.254.169.254",
                "http://10.1.2.3/" to "destination-address-blocked:10.1.2.3",
            ).forEach { (target, reason) ->
                val failure = assertFailsWith<WebPolicyRejectedException>(target) { transport.execute(CrawlRequest(target)) }
                assertEquals("public-web-safety", failure.policyId, target)
                assertEquals(reason, failure.reason, target)
            }
            assertEquals(
                Headers.of(Header("Cookie", "[REDACTED]"), Header("Accept", "text/html")),
                transport.redactHeadersForDiagnostics(Headers.of(Header("Cookie", "session=secret"), Header("Accept", "text/html"))),
            )
        } finally {
            transport.stop(ExtensionStopContext(ExtensionStopReason.ENGINE_SHUTDOWN))
        }
        assertFailsWith<IllegalStateException> { transport.execute(CrawlRequest("http://127.0.0.1:9/")) }

        val defaults = KtorTransport.publicWeb()
        assertEquals("ktor-cio", defaults.id)
        defaults.close()
        assertFailsWith<IllegalStateException> { defaults.execute(CrawlRequest("http://127.0.0.1:9/")) }

        val created = KtorTransport.create(policies = policies(), clock = FixedClock(FIXED_NOW))
        created.close()
        assertFailsWith<IllegalStateException> { created.execute(CrawlRequest("https://a.test/")) }
    }

    private fun harness(
        policies: WebTransportPolicies,
        configuration: KtorTransportConfig = KtorTransportConfig(),
        handler: suspend MockRequestHandleScope.(HttpRequestData) -> HttpResponseData,
    ): Harness {
        val engine = MockEngine(
            MockEngineConfig().apply {
                reuseHandlers = true
                addHandler { request -> handler(request) }
            },
        )
        val client = HttpClient(engine)
        val transport = KtorTransport.fromClient(client, configuration, policies, FixedClock(FIXED_NOW))
        return Harness(transport, client)
    }

    private fun policies(
        destination: WebDestinationPolicy = AllowAllWebDestinationPolicy,
        robots: RobotsPolicy = NoRobotsPolicy,
        concurrency: OriginConcurrencyPolicy = NoOriginConcurrencyPolicy,
        pacing: OriginPacingPolicy = NoOriginPacingPolicy,
        cookies: CookieSession = NoCookieSession,
    ): WebTransportPolicies = WebTransportPolicies(
        destination = destination,
        robots = robots,
        concurrency = concurrency,
        pacing = pacing,
        cookies = cookies,
        failureClassifier = DefaultHttpFailureClassifier,
        retryAfter = DefaultRetryAfterPolicy,
        redirectHeaders = DefaultRedirectHeaderPolicy(),
        headerRedaction = DefaultHeaderRedactionPolicy(),
    )

    private fun MockRequestHandleScope.reply(
        body: ByteArray = ByteArray(0),
        status: HttpStatusCode = HttpStatusCode.OK,
        headers: io.ktor.http.Headers = headersOf(),
    ): HttpResponseData = respond(body, status, headers)

    private fun OutgoingContent.bytesOrNull(): ByteArray? = (this as? OutgoingContent.ByteArrayContent)?.bytes()

    private fun deflate(bytes: ByteArray): ByteArray = ByteArrayOutputStream().use { output ->
        DeflaterOutputStream(output).use { it.write(bytes) }
        output.toByteArray()
    }

    private class Harness(val transport: KtorTransport, private val client: HttpClient) {
        fun close() {
            transport.close()
            client.close()
        }
    }

    private class FixedClock(private val instant: Instant) : Clock {
        override fun now(): Instant = instant
    }

    private class MapResolver(vararg answers: Pair<String, List<InetAddress>>) : NetworkAddressResolver {
        private val answers = answers.toMap()
        override val id: String = "map-resolver"
        override suspend fun resolve(host: String): List<InetAddress> = answers[host] ?: error("unexpected DNS lookup for $host")
    }

    private class RecordingDestination : WebDestinationPolicy {
        override val id: String = "recording-destination"
        val targets = CopyOnWriteArrayList<String>()
        override suspend fun evaluate(request: CrawlRequest, history: RedirectHistory): DestinationDecision {
            targets += request.target
            return DestinationDecision.Allowed
        }
    }

    private class FixedRobots(private val decision: RobotsDecision) : RobotsPolicy {
        override val id: String = "fixed-robots"
        override suspend fun evaluate(target: String, fetcher: RobotsFetcher): RobotsDecision = decision
    }

    private class FetchingRobots : RobotsPolicy {
        override val id: String = "fetching-robots"
        val results = CopyOnWriteArrayList<RobotsFetchResult>()
        override suspend fun evaluate(target: String, fetcher: RobotsFetcher): RobotsDecision {
            results += fetcher.fetch(WebOrigin.fromTarget(target), "SafetyBot/1")
            return RobotsDecision.Allowed(null)
        }
    }

    private class TrackingConcurrency : OriginConcurrencyPolicy {
        override val id: String = "tracking-concurrency"
        val kinds = CopyOnWriteArrayList<WebRequestKind>()
        @Volatile var active: Int = 0
        @Volatile var released: Int = 0

        override suspend fun acquire(origin: WebOrigin, kind: WebRequestKind): OriginConcurrencyPermit {
            kinds += kind
            active += 1
            val once = AtomicBoolean(false)
            return OriginConcurrencyPermit {
                if (once.compareAndSet(false, true)) {
                    active -= 1
                    released += 1
                }
            }
        }
    }

    private class RecordingPacing(private val refuse: Set<WebRequestKind> = emptySet()) : OriginPacingPolicy {
        override val id: String = "recording-pacing"
        val robotsDelays = CopyOnWriteArrayList<Pair<WebOrigin, Duration>>()
        @Volatile var active: Int = 0

        override suspend fun acquire(origin: WebOrigin, kind: WebRequestKind): OriginPacingReservation {
            if (kind in refuse) throw PacingWaitExceededException(origin, Duration.ofHours(1), Duration.ofSeconds(1))
            active += 1
            val once = AtomicBoolean(false)
            return OriginPacingReservation { if (once.compareAndSet(false, true)) active -= 1 }
        }

        override fun observe(origin: WebOrigin, response: CrawlResponse) = Unit

        override fun observeRobotsDelay(origin: WebOrigin, delay: Duration) {
            robotsDelays += origin to delay
        }
    }

    private companion object {
        val FIXED_NOW: Instant = Instant.parse("2026-01-01T00:00:00Z")

        fun v4(a: Int, b: Int, c: Int, d: Int): InetAddress =
            InetAddress.getByAddress("answer", byteArrayOf(a.toByte(), b.toByte(), c.toByte(), d.toByte()))

        fun mappedBytes(a: Int, b: Int, c: Int, d: Int): ByteArray =
            ByteArray(10) + byteArrayOf(0xff.toByte(), 0xff.toByte(), a.toByte(), b.toByte(), c.toByte(), d.toByte())

        fun nat64Bytes(a: Int, b: Int, c: Int, d: Int): ByteArray =
            byteArrayOf(0x00, 0x64, 0xff.toByte(), 0x9b.toByte()) + ByteArray(8) + byteArrayOf(a.toByte(), b.toByte(), c.toByte(), d.toByte())
    }
}
