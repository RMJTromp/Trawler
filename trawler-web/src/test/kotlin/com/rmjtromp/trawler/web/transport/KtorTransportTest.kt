package com.rmjtromp.trawler.web.transport

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.RequestBody
import com.rmjtromp.trawler.model.RequestMethod
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.web.policy.DefaultHeaderRedactionPolicy
import com.rmjtromp.trawler.web.policy.DefaultHttpFailureClassifier
import com.rmjtromp.trawler.web.policy.DefaultRedirectHeaderPolicy
import com.rmjtromp.trawler.web.policy.DefaultRetryAfterPolicy
import com.rmjtromp.trawler.web.policy.InvalidWebRedirectException
import com.rmjtromp.trawler.web.policy.NoRobotsPolicy
import com.rmjtromp.trawler.web.policy.RobotsDecision
import com.rmjtromp.trawler.web.policy.RobotsFetchResult
import com.rmjtromp.trawler.web.policy.RobotsFetcher
import com.rmjtromp.trawler.web.policy.RobotsPolicy
import com.rmjtromp.trawler.web.policy.RobotsUnavailableException
import com.rmjtromp.trawler.web.policy.UnsupportedWebContentEncodingException
import com.rmjtromp.trawler.web.policy.WebPolicyRejectedException
import com.rmjtromp.trawler.web.policy.WebResponseHeaderNameTooLargeException
import com.rmjtromp.trawler.web.policy.WebResponseHeaderValueTooLargeException
import com.rmjtromp.trawler.web.policy.WebResponseHeadersTooLargeException
import com.rmjtromp.trawler.web.policy.WebResponseTooLargeException
import com.rmjtromp.trawler.web.session.CookieSession
import com.rmjtromp.trawler.web.session.InMemoryCookieSession
import com.rmjtromp.trawler.web.session.NoCookieSession
import com.rmjtromp.trawler.web.policy.AllowAllWebDestinationPolicy
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockEngineConfig
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.HttpRequestTimeoutException
import io.ktor.client.request.get
import io.ktor.client.request.HttpRequestData
import io.ktor.client.request.HttpResponseData
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.headers
import io.ktor.http.headersOf
import io.ktor.http.content.OutgoingContent
import io.ktor.utils.io.ByteReadChannel
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.DeflaterOutputStream
import java.util.zip.GZIPOutputStream
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

@OptIn(ExperimentalCoroutinesApi::class)
class KtorTransportTest {
    @Test
    fun `MockEngine adapts method headers body response metadata and diagnostics`(): Unit = runBlocking {
        val receivedAt = Instant.parse("2026-01-01T00:00:00Z")
        val seen = CopyOnWriteArrayList<HttpRequestData>()
        val harness = mockTransport(clock = FixedClock(receivedAt)) { request ->
            seen += request
            reply(
                body = "created".toByteArray(),
                status = HttpStatusCode.Created,
                responseHeaders = headers {
                    append("X-Duplicate", "one")
                    append("X-Duplicate", "two")
                    append("Content-Type", "text/plain")
                },
            )
        }
        try {
            val target = "https://example.test/submit"
            val body = "payload".toByteArray()
            val response = harness.transport.execute(
                CrawlRequest(
                    target = target,
                    method = RequestMethod("POST"),
                    headers = Headers.of(
                        Header("X-Duplicate", "a"),
                        Header("X-Duplicate", "b"),
                        Header("Host", "caller-host"),
                        Header("Content-Length", "999"),
                        Header("Connection", "close"),
                        Header("Transfer-Encoding", "chunked"),
                        Header("Authorization", "Bearer secret"),
                    ),
                    body = RequestBody.from(body),
                ),
            )

            val request = seen.single()
            assertEquals(HttpMethod.Post, request.method)
            assertEquals(listOf<String>("a", "b"), request.headers.getAll("X-Duplicate"))
            assertEquals(listOf<String>("TrawlerBot/0.1"), request.headers.getAll(HttpHeaders.UserAgent))
            assertEquals(listOf<String>("gzip, deflate"), request.headers.getAll(HttpHeaders.AcceptEncoding))
            assertEquals(ContentType.Application.OctetStream, request.body.contentTypeValue())
            assertContentEquals(body, request.body.asByteArray())
            assertFalse(request.headers.getAll(HttpHeaders.Host).orEmpty().contains("caller-host"))
            assertFalse(request.headers.getAll(HttpHeaders.ContentLength).orEmpty().contains("999"))
            assertFalse(request.headers.getAll(HttpHeaders.Connection).orEmpty().contains("close"))
            assertFalse(request.headers.getAll(HttpHeaders.TransferEncoding).orEmpty().contains("chunked"))

            assertEquals(201, response.statusCode)
            assertEquals(target, response.finalTarget)
            assertEquals(receivedAt, response.receivedAt)
            assertContentEquals("created".toByteArray(), response.body.toByteArray())
            assertEquals(listOf("one", "two"), response.headers["x-duplicate"])
            assertEquals("http-success-or-redirect", response.attributes["trawler.web.http-category"])
            assertEquals("0", response.attributes["trawler.web.redirect-count"])

            val diagnostics = harness.transport.redactHeadersForDiagnostics(
                Headers.of(
                    Header("Authorization", "Bearer secret"),
                    Header("Accept", "text/plain"),
                ),
            )
            assertEquals(listOf("[REDACTED]"), diagnostics["authorization"])
            assertEquals(listOf("Bearer secret"), response.request.headers["Authorization"])
        } finally {
            harness.close()
        }
    }

    @Test
    fun `caller headers override transport defaults and content type`(): Unit = runBlocking {
        val seen = CopyOnWriteArrayList<HttpRequestData>()
        val harness = mockTransport { request ->
            seen += request
            reply()
        }
        try {
            harness.transport.execute(
                CrawlRequest(
                    target = "https://example.test/custom",
                    method = RequestMethod("PUT"),
                    headers = Headers.of(
                        Header(HttpHeaders.UserAgent, "CustomAgent/9"),
                        Header(HttpHeaders.AcceptEncoding, "identity"),
                        Header(HttpHeaders.ContentType, "application/json"),
                    ),
                    body = RequestBody.from("{}".toByteArray()),
                ),
            )
            val request = seen.single()
            assertEquals(listOf<String>("CustomAgent/9"), request.headers.getAll(HttpHeaders.UserAgent))
            assertEquals(listOf<String>("identity"), request.headers.getAll(HttpHeaders.AcceptEncoding))
            assertEquals(ContentType.Application.Json, request.body.contentTypeValue())
        } finally {
            harness.close()
        }
    }

    @Test
    fun `non success responses remain inspectable and do not retry`(): Unit = runBlocking {
        val calls = AtomicInteger()
        val harness = mockTransport { _ ->
            calls.incrementAndGet()
            reply(
                body = "server-failure".toByteArray(),
                status = HttpStatusCode.ServiceUnavailable,
                responseHeaders = headersOf("Retry-After", "3"),
            )
        }
        try {
            val response = harness.transport.execute(CrawlRequest("https://example.test/failure"))
            assertEquals(503, response.statusCode)
            assertContentEquals("server-failure".toByteArray(), response.body.toByteArray())
            assertEquals("http-server-error", response.attributes["trawler.web.http-category"])
            assertEquals("true", response.attributes["trawler.web.http-retryable"])
            assertEquals("3000", response.attributes["trawler.web.retry-after-millis"])
            assertEquals(1, calls.get())
        } finally {
            harness.close()
        }
    }

    @Test
    fun `request bounds reject before MockEngine and exact body bound is accepted`(): Unit = runBlocking {
        val calls = AtomicInteger()
        val exact = mockTransport(
            configuration = KtorTransportConfig(
                maxRequestBodyBytes = 3,
                maxRequestHeaders = 2,
                maxHeaderNameLength = 32,
                maxHeaderValueLength = 32,
            ),
        ) { _ ->
            calls.incrementAndGet()
            reply()
        }
        try {
            exact.transport.execute(
                CrawlRequest(
                    "https://example.test/exact",
                    headers = Headers.empty(),
                    body = RequestBody.from(byteArrayOf(1, 2, 3)),
                ),
            )
            assertEquals(1, calls.get())
        } finally {
            exact.close()
        }

        val oversized = mockTransport(
            configuration = KtorTransportConfig(maxRequestBodyBytes = 3),
        ) { _ ->
            calls.incrementAndGet()
            reply()
        }
        try {
            assertFailsWith<IllegalArgumentException> {
                oversized.transport.execute(
                    CrawlRequest("https://example.test/oversized", body = RequestBody.from(ByteArray(4))),
                )
            }
            assertEquals(1, calls.get())
        } finally {
            oversized.close()
        }

        val tooManyHeaders = mockTransport(
            configuration = KtorTransportConfig(maxRequestHeaders = 2),
        ) { _ ->
            calls.incrementAndGet()
            reply()
        }
        try {
            assertFailsWith<IllegalArgumentException> {
                tooManyHeaders.transport.execute(
                    CrawlRequest("https://example.test/headers", headers = Headers.of(Header("X-Extra", "1"))),
                )
            }
        } finally {
            tooManyHeaders.close()
        }
    }

    @Test
    fun `request target method header-name and header-value limits are enforced`(): Unit = runBlocking {
        val validTarget = "https://e.test/x"
        val targetHarness = mockTransport(
            configuration = KtorTransportConfig(maxTargetLength = validTarget.length),
        ) { _ -> reply() }
        try {
            targetHarness.transport.execute(CrawlRequest(validTarget))
            assertFailsWith<IllegalArgumentException> {
                targetHarness.transport.execute(CrawlRequest("$validTarget-"))
            }
        } finally {
            targetHarness.close()
        }

        val methodHarness = mockTransport(
            configuration = KtorTransportConfig(maxMethodLength = 3),
        ) { _ -> reply() }
        try {
            assertFailsWith<IllegalArgumentException> {
                methodHarness.transport.execute(
                    CrawlRequest("https://example.test/method", method = RequestMethod("LONG")),
                )
            }
        } finally {
            methodHarness.close()
        }

        val nameHarness = mockTransport(
            configuration = KtorTransportConfig(maxHeaderNameLength = 15),
        ) { _ -> reply() }
        try {
            assertFailsWith<IllegalArgumentException> {
                nameHarness.transport.execute(
                    CrawlRequest("https://example.test/name", headers = Headers.of(Header("X-Too-Long-Namee", "v"))),
                )
            }
        } finally {
            nameHarness.close()
        }

        val valueHarness = mockTransport(
            configuration = KtorTransportConfig(maxHeaderValueLength = 15),
        ) { _ -> reply() }
        try {
            assertFailsWith<IllegalArgumentException> {
                valueHarness.transport.execute(
                    CrawlRequest("https://example.test/value", headers = Headers.of(Header("X-Test", "x".repeat(16)))),
                )
            }
        } finally {
            valueHarness.close()
        }
    }

    @Test
    fun `response header bounds reject names values and counts`(): Unit = runBlocking {
        val exactCountHarness = mockTransport(
            configuration = KtorTransportConfig(maxResponseHeaders = 1),
        ) { _ -> reply(responseHeaders = headersOf("X-One", "1")) }
        try {
            assertEquals(
                listOf("1"),
                exactCountHarness.transport.execute(CrawlRequest("https://example.test/exact-count")).headers["X-One"],
            )
        } finally {
            exactCountHarness.close()
        }

        val countHarness = mockTransport(
            configuration = KtorTransportConfig(maxResponseHeaders = 1),
        ) { _ ->
            reply(responseHeaders = headers {
                append("X-One", "1")
                append("X-Two", "2")
            })
        }
        try {
            assertFailsWith<WebResponseHeadersTooLargeException> {
                countHarness.transport.execute(CrawlRequest("https://example.test/headers"))
            }
        } finally {
            countHarness.close()
        }

        val exactNameHarness = mockTransport(
            configuration = KtorTransportConfig(maxHeaderNameLength = 15),
        ) { _ -> reply(responseHeaders = headersOf("X-Response-1234", "1")) }
        try {
            assertEquals(
                listOf("1"),
                exactNameHarness.transport.execute(CrawlRequest("https://example.test/exact-name")).headers["X-Response-1234"],
            )
        } finally {
            exactNameHarness.close()
        }

        val nameHarness = mockTransport(
            configuration = KtorTransportConfig(maxHeaderNameLength = 15),
        ) { _ -> reply(responseHeaders = headersOf("X-Response-Header", "1")) }
        try {
            assertFailsWith<WebResponseHeaderNameTooLargeException> {
                nameHarness.transport.execute(CrawlRequest("https://example.test/header-name"))
            }
        } finally {
            nameHarness.close()
        }

        val exactValueHarness = mockTransport(
            configuration = KtorTransportConfig(maxHeaderValueLength = 15),
        ) { _ -> reply(responseHeaders = headersOf("X-Value", "123456789012345")) }
        try {
            assertEquals(
                listOf("123456789012345"),
                exactValueHarness.transport.execute(CrawlRequest("https://example.test/exact-value")).headers["X-Value"],
            )
        } finally {
            exactValueHarness.close()
        }

        val valueHarness = mockTransport(
            configuration = KtorTransportConfig(maxHeaderValueLength = 15),
        ) { _ -> reply(responseHeaders = headersOf("X-Value", "1234567890123456")) }
        try {
            assertFailsWith<WebResponseHeaderValueTooLargeException> {
                valueHarness.transport.execute(CrawlRequest("https://example.test/header-value"))
            }
        } finally {
            valueHarness.close()
        }
    }

    @Test
    fun `identity gzip and deflate bodies decode within independent bounds`(): Unit = runBlocking {
        val identity = "identity".toByteArray()
        val gzip = "gzip body".toByteArray()
        val deflate = "deflate body".toByteArray()
        val seen = AtomicInteger()
        val harness = mockTransport { _ ->
            when (seen.getAndIncrement()) {
                0 -> reply(identity)
                1 -> reply(
                    gzip(gzip),
                    responseHeaders = headersOf(HttpHeaders.ContentEncoding, "gzip"),
                )
                2 -> reply(
                    deflate(deflate),
                    responseHeaders = headersOf(HttpHeaders.ContentEncoding, "deflate"),
                )
                else -> error("unexpected body request")
            }
        }
        try {
            assertContentEquals(identity, harness.transport.execute(CrawlRequest("https://example.test/identity")).body.toByteArray())
            assertContentEquals(gzip, harness.transport.execute(CrawlRequest("https://example.test/gzip")).body.toByteArray())
            assertContentEquals(deflate, harness.transport.execute(CrawlRequest("https://example.test/deflate")).body.toByteArray())
        } finally {
            harness.close()
        }
    }

    @Test
    fun `compressed decompressed unsupported chained and malformed bodies fail visibly`(): Unit = runBlocking {
        val compressed = gzip("decoded".toByteArray())
        val sequence = AtomicInteger()
        val harness = mockTransport { _ ->
            when (sequence.getAndIncrement()) {
                0 -> reply(
                    byteArrayOf(1, 2, 3),
                    responseHeaders = headersOf(HttpHeaders.ContentEncoding, "br"),
                )
                1 -> reply(
                    compressed,
                    responseHeaders = headersOf(HttpHeaders.ContentEncoding, "gzip, deflate"),
                )
                2 -> reply(
                    "not-gzip".toByteArray(),
                    responseHeaders = headersOf(HttpHeaders.ContentEncoding, "gzip"),
                )
                else -> error("unexpected body request")
            }
        }
        try {
            val decompressed = KtorTransportConfig(maxDecompressedBodyBytes = 3)
            val exact = mockTransport(
                configuration = KtorTransportConfig(maxDecompressedBodyBytes = "decoded".length),
            ) { _ ->
                reply(compressed, responseHeaders = headersOf(HttpHeaders.ContentEncoding, "gzip"))
            }
            try {
                assertContentEquals(
                    "decoded".toByteArray(),
                    exact.transport.execute(CrawlRequest("https://example.test/exact-decompressed")).body.toByteArray(),
                )
            } finally {
                exact.close()
            }

            val bounded = mockTransport(
                configuration = decompressed,
            ) { _ ->
                reply(compressed, responseHeaders = headersOf(HttpHeaders.ContentEncoding, "gzip"))
            }
            try {
                val error = assertFailsWith<WebResponseTooLargeException> {
                    bounded.transport.execute(CrawlRequest("https://example.test/decompressed"))
                }
                assertEquals("decompressed", error.limitKind)
            } finally {
                bounded.close()
            }

            assertFailsWith<UnsupportedWebContentEncodingException> {
                harness.transport.execute(CrawlRequest("https://example.test/unsupported"))
            }
            assertFailsWith<UnsupportedWebContentEncodingException> {
                harness.transport.execute(CrawlRequest("https://example.test/chained"))
            }
            assertFailsWith<IOException> {
                harness.transport.execute(CrawlRequest("https://example.test/malformed"))
            }
        } finally {
            harness.close()
        }

        val raw = byteArrayOf(1, 2, 3)
        val compressedBound = mockTransport(
            configuration = KtorTransportConfig(maxCompressedBodyBytes = raw.size),
        ) { _ -> reply(raw) }
        try {
            assertContentEquals(raw, compressedBound.transport.execute(CrawlRequest("https://example.test/exact-body")).body.toByteArray())
        } finally {
            compressedBound.close()
        }
        val overBound = mockTransport(
            configuration = KtorTransportConfig(maxCompressedBodyBytes = raw.size - 1),
        ) { _ -> reply(raw) }
        try {
            val error = assertFailsWith<WebResponseTooLargeException> {
                overBound.transport.execute(CrawlRequest("https://example.test/over-body"))
            }
            assertEquals("compressed", error.limitKind)
        } finally {
            overBound.close()
        }

        val identityExact = mockTransport(
            configuration = KtorTransportConfig(maxDecompressedBodyBytes = raw.size),
        ) { _ -> reply(raw) }
        try {
            assertContentEquals(
                raw,
                identityExact.transport.execute(CrawlRequest("https://example.test/exact-decompressed-identity")).body.toByteArray(),
            )
        } finally {
            identityExact.close()
        }
        val identityOver = mockTransport(
            configuration = KtorTransportConfig(maxDecompressedBodyBytes = raw.size - 1),
        ) { _ -> reply(raw) }
        try {
            val error = assertFailsWith<WebResponseTooLargeException> {
                identityOver.transport.execute(CrawlRequest("https://example.test/over-decompressed-identity"))
            }
            assertEquals("decompressed", error.limitKind)
        } finally {
            identityOver.close()
        }
    }

    @Test
    fun `redirect methods preserve or convert bodies at documented status boundaries`(): Unit = runBlocking {
        val cases = listOf(
            RedirectCase(301, RequestMethod("POST"), true, "GET", false),
            RedirectCase(302, RequestMethod("POST"), true, "GET", false),
            RedirectCase(303, RequestMethod("HEAD"), false, "HEAD", false),
            RedirectCase(303, RequestMethod("PUT"), true, "GET", false),
            RedirectCase(307, RequestMethod("POST"), true, "POST", true),
            RedirectCase(308, RequestMethod("POST"), true, "POST", true),
        )
        cases.forEach { case ->
            val seen = CopyOnWriteArrayList<HttpRequestData>()
            val harness = mockTransport { request ->
                seen += request
                if (seen.size == 1) {
                    reply(
                        status = HttpStatusCode(case.status, "test"),
                        responseHeaders = headersOf(HttpHeaders.Location, "/final"),
                    )
                } else {
                    reply("done".toByteArray())
                }
            }
            try {
                harness.transport.execute(
                    CrawlRequest(
                        "https://source.test/start",
                        method = case.method,
                        body = if (case.hasBody) RequestBody.from("payload".toByteArray()) else null,
                    ),
                )
                assertEquals(2, seen.size, case.toString())
                assertEquals(case.nextMethod, seen[1].method.value, case.toString())
                if (case.preserveBody) {
                    assertContentEquals("payload".toByteArray(), seen[1].body.asByteArray(), case.toString())
                } else {
                    assertNull(seen[1].bodyBytes(), case.toString())
                }
            } finally {
                harness.close()
            }
        }
    }

    @Test
    fun `redirects resolve relative absolute and fragment targets and expose final metadata`(): Unit = runBlocking {
        val seen = CopyOnWriteArrayList<HttpRequestData>()
        val harness = mockTransport { request ->
            seen += request
            if (seen.size == 1) {
                reply(
                    status = HttpStatusCode.Found,
                    responseHeaders = headersOf(HttpHeaders.Location, "https://dest.test/final#fragment"),
                )
            } else {
                reply("final".toByteArray())
            }
        }
        try {
            val response = harness.transport.execute(CrawlRequest("https://source.test/start"))
            assertEquals("https://dest.test/final", seen[1].url.toString())
            assertEquals("https://dest.test/final", response.finalTarget)
            assertEquals("1", response.attributes["trawler.web.redirect-count"])
            assertContentEquals("final".toByteArray(), response.body.toByteArray())
        } finally {
            harness.close()
        }
    }

    @Test
    fun `redirect bounds loops missing locations and invalid locations are visible`(): Unit = runBlocking {
        val zero = mockTransport(
            configuration = KtorTransportConfig(maxRedirects = 0),
        ) { _ -> reply(status = HttpStatusCode.Found, responseHeaders = headersOf(HttpHeaders.Location, "/next")) }
        try {
            assertFailsWith<com.rmjtromp.trawler.web.policy.RedirectLimitExceededException> {
                zero.transport.execute(CrawlRequest("https://example.test/start"))
            }
            assertEquals(1, zero.engine.requestHistory.size)
        } finally {
            zero.close()
        }

        val exactSeen = AtomicInteger()
        val exact = mockTransport(
            configuration = KtorTransportConfig(maxRedirects = 1),
        ) { _ ->
            if (exactSeen.getAndIncrement() == 0) {
                reply(status = HttpStatusCode.Found, responseHeaders = headersOf(HttpHeaders.Location, "/final"))
            } else {
                reply("done".toByteArray())
            }
        }
        try {
            assertEquals(1, exact.transport.execute(CrawlRequest("https://example.test/start")).attributes["trawler.web.redirect-count"]?.toInt())
        } finally {
            exact.close()
        }

        val exhaustedSeen = AtomicInteger()
        val exhausted = mockTransport(
            configuration = KtorTransportConfig(maxRedirects = 1),
        ) { _ ->
            exhaustedSeen.getAndIncrement()
            reply(status = HttpStatusCode.Found, responseHeaders = headersOf(HttpHeaders.Location, "/again"))
        }
        try {
            assertFailsWith<com.rmjtromp.trawler.web.policy.RedirectLimitExceededException> {
                exhausted.transport.execute(CrawlRequest("https://example.test/start"))
            }
            assertEquals(2, exhaustedSeen.get())
        } finally {
            exhausted.close()
        }

        val loop = mockTransport { _ ->
            reply(status = HttpStatusCode.Found, responseHeaders = headersOf(HttpHeaders.Location, "/same"))
        }
        try {
            assertFailsWith<InvalidWebRedirectException> {
                loop.transport.execute(CrawlRequest("https://example.test/same"))
            }
        } finally {
            loop.close()
        }

        val missing = mockTransport { _ -> reply(status = HttpStatusCode.Found) }
        try {
            assertEquals(302, missing.transport.execute(CrawlRequest("https://example.test/missing")).statusCode)
        } finally {
            missing.close()
        }

        val invalid = mockTransport { _ ->
            reply(status = HttpStatusCode.Found, responseHeaders = headersOf(HttpHeaders.Location, "https://[invalid"))
        }
        try {
            assertFailsWith<InvalidWebRedirectException> {
                invalid.transport.execute(CrawlRequest("https://example.test/invalid"))
            }
        } finally {
            invalid.close()
        }

        val overlong = mockTransport(
            configuration = KtorTransportConfig(maxLocationLength = 5),
        ) { _ -> reply(status = HttpStatusCode.Found, responseHeaders = headersOf(HttpHeaders.Location, "/123456")) }
        try {
            assertFailsWith<InvalidWebRedirectException> {
                overlong.transport.execute(CrawlRequest("https://example.test/overlong"))
            }
        } finally {
            overlong.close()
        }
    }

    @Test
    fun `cross origin redirects strip credentials and use destination cookies`(): Unit = runBlocking {
        val cookies = InMemoryCookieSession(clock = FixedClock(Instant.EPOCH))
        cookies.observe("https://dest.test/bootstrap", listOf("destination=two; Path=/"))
        val seen = CopyOnWriteArrayList<HttpRequestData>()
        val harness = mockTransport(
            policies = testPolicies(cookies = cookies),
        ) { request ->
            seen += request
            if (seen.size == 1) {
                reply(
                    status = HttpStatusCode.Found,
                    responseHeaders = headersOf(HttpHeaders.Location, "https://dest.test/final"),
                )
            } else {
                reply("destination".toByteArray())
            }
        }
        try {
            harness.transport.execute(
                CrawlRequest(
                    "https://source.test/start",
                    headers = Headers.of(
                        Header("Authorization", "source-secret"),
                        Header("Cookie", "source=one"),
                        Header("X-Api-Key", "source-key"),
                    ),
                ),
            )
            assertEquals(listOf<String>("source-secret"), seen[0].headers.getAll(HttpHeaders.Authorization))
            assertEquals(listOf<String>("source=one"), seen[0].headers.getAll(HttpHeaders.Cookie))
            assertTrue(seen[1].headers.getAll(HttpHeaders.Authorization).orEmpty().isEmpty())
            assertTrue(seen[1].headers.getAll("X-Api-Key").orEmpty().isEmpty())
            assertEquals(listOf<String>("destination=two"), seen[1].headers.getAll(HttpHeaders.Cookie))
        } finally {
            harness.close()
        }
    }

    @Test
    fun `every redirect hop reruns destination robots concurrency pacing and session policies`(): Unit = runBlocking {
        val destination = RecordingDestination()
        val robots = RecordingRobots()
        val concurrency = TrackingConcurrency()
        val pacing = TrackingPacing()
        val cookies = RecordingCookies()
        val seen = CopyOnWriteArrayList<HttpRequestData>()
        val harness = mockTransport(
            policies = testPolicies(
                destination = destination,
                robots = robots,
                concurrency = concurrency,
                pacing = pacing,
                cookies = cookies,
            ),
        ) { request ->
            seen += request
            if (seen.size == 1) {
                reply(
                    status = HttpStatusCode.Found,
                    responseHeaders = headersOf(HttpHeaders.Location, "/next"),
                )
            } else {
                reply("ok".toByteArray())
            }
        }
        try {
            harness.transport.execute(CrawlRequest("https://example.test/start"))
            assertEquals(listOf("https://example.test/start", "https://example.test/next"), destination.targets)
            assertEquals(listOf(0, 1), destination.historySizes)
            assertEquals(destination.targets, robots.targets)
            assertEquals(listOf(WebRequestKind.PAGE, WebRequestKind.PAGE), concurrency.kinds)
            assertEquals(2, concurrency.acquired)
            assertEquals(2, concurrency.released)
            assertEquals(0, concurrency.active)
            assertEquals(2, pacing.acquired)
            assertEquals(2, pacing.released)
            assertEquals(2, pacing.observed)
            assertEquals(2, cookies.applied.size)
            assertEquals(2, cookies.observed.size)
        } finally {
            harness.close()
        }
    }

    @Test
    fun `robots uses the shared client without recursive checks and honors its smaller body bound`(): Unit = runBlocking {
        val evaluations = AtomicInteger()
        val robotRequests = CopyOnWriteArrayList<HttpRequestData>()
        val robots = object : RobotsPolicy {
            override val id: String = "controlled-robots"

            override suspend fun evaluate(target: String, fetcher: RobotsFetcher): RobotsDecision {
                evaluations.incrementAndGet()
                val result = fetcher.fetch(WebOrigin.fromTarget(target), "TestBot/1")
                return when (result) {
                    is RobotsFetchResult.Available -> RobotsDecision.Allowed(null)
                    is RobotsFetchResult.Unavailable -> RobotsDecision.Unavailable(result.reason)
                }
            }
        }
        val first = mockTransport(
            configuration = KtorTransportConfig(maxRobotsBodyBytes = 3),
            policies = testPolicies(robots = robots),
        ) { request ->
            robotRequests += request
            when (request.url.encodedPath) {
                "/robots.txt" -> reply("too long".toByteArray())
                else -> reply("page".toByteArray())
            }
        }
        try {
            assertFailsWith<RobotsUnavailableException> {
                first.transport.execute(CrawlRequest("https://example.test/page"))
            }
            assertEquals(1, evaluations.get())
            assertEquals(listOf("/robots.txt"), robotRequests.map { it.url.encodedPath })
            assertEquals(listOf<String>("TestBot/1"), robotRequests.single().headers.getAll(HttpHeaders.UserAgent))
            assertEquals(listOf<String>("text/plain"), robotRequests.single().headers.getAll(HttpHeaders.Accept))
        } finally {
            first.close()
        }

        val successfulRequests = CopyOnWriteArrayList<HttpRequestData>()
        val successful = mockTransport(
            configuration = KtorTransportConfig(maxRobotsBodyBytes = 64),
            policies = testPolicies(
                robots = object : RobotsPolicy {
                    override val id: String = "one-fetch-robots"
                    override suspend fun evaluate(target: String, fetcher: RobotsFetcher): RobotsDecision {
                        val result = fetcher.fetch(WebOrigin.fromTarget(target), "TestBot/1")
                        assertIs<RobotsFetchResult.Available>(result)
                        return RobotsDecision.Allowed(null)
                    }
                },
            ),
        ) { request ->
            successfulRequests += request
            if (request.url.encodedPath == "/robots.txt") reply("Allow: /".toByteArray()) else reply("page".toByteArray())
        }
        try {
            assertContentEquals(
                "page".toByteArray(),
                successful.transport.execute(CrawlRequest("https://example.test/page")).body.toByteArray(),
            )
            assertEquals(listOf("/robots.txt", "/page"), successfulRequests.map { it.url.encodedPath })
        } finally {
            successful.close()
        }
    }

    @Test
    fun `success rejection and exception release reservations`(): Unit = runBlocking {
        val successConcurrency = TrackingConcurrency()
        val successPacing = TrackingPacing()
        val success = mockTransport(
            policies = testPolicies(concurrency = successConcurrency, pacing = successPacing),
        ) { _ -> reply() }
        try {
            success.transport.execute(CrawlRequest("https://example.test/success"))
            assertEquals(1, successConcurrency.acquired)
            assertEquals(1, successConcurrency.released)
            assertEquals(1, successPacing.acquired)
            assertEquals(1, successPacing.released)
            assertEquals(0, successConcurrency.active + successPacing.active)
        } finally {
            success.close()
        }

        val rejectedConcurrency = TrackingConcurrency()
        val rejectedCalls = AtomicInteger()
        val rejected = mockTransport(
            policies = testPolicies(
                destination = object : WebDestinationPolicy {
                    override val id: String = "reject-before-network"
                    override suspend fun evaluate(request: CrawlRequest, history: RedirectHistory): DestinationDecision =
                        DestinationDecision.Rejected("test-rejected")
                },
                concurrency = rejectedConcurrency,
            ),
        ) { _ ->
            rejectedCalls.incrementAndGet()
            reply()
        }
        try {
            assertFailsWith<WebPolicyRejectedException> {
                rejected.transport.execute(CrawlRequest("https://example.test/rejected"))
            }
            assertEquals(0, rejectedCalls.get())
            assertEquals(0, rejectedConcurrency.acquired)
        } finally {
            rejected.close()
        }

        val cause = IllegalStateException("mock failure")
        val exceptionConcurrency = TrackingConcurrency()
        val exceptionPacing = TrackingPacing()
        val exceptional = mockTransport(
            policies = testPolicies(concurrency = exceptionConcurrency, pacing = exceptionPacing),
        ) { _ -> throw cause }
        try {
            val failure = assertFailsWith<IllegalStateException> {
                exceptional.transport.execute(CrawlRequest("https://example.test/exception"))
            }
            assertSame(cause, failure.cause)
            assertEquals(0, exceptionConcurrency.active + exceptionPacing.active)
            assertEquals(1, exceptionConcurrency.released)
            assertEquals(1, exceptionPacing.released)
        } finally {
            exceptional.close()
        }

    }

    @Test
    fun `timeout exceptions and cancellation release reservations`(): Unit = runBlocking {
        val timeoutConcurrency = TrackingConcurrency()
        val timeoutPacing = TrackingPacing()
        val timed = mockTransport(
            configuration = KtorTransportConfig(totalRequestTimeout = Duration.ofSeconds(1)),
            policies = testPolicies(concurrency = timeoutConcurrency, pacing = timeoutPacing),
        ) { request ->
            throw HttpRequestTimeoutException(request)
        }
        try {
            assertFailsWith<HttpRequestTimeoutException> {
                timed.transport.execute(CrawlRequest("https://example.test/timeout"))
            }
            assertEquals(0, timeoutConcurrency.active + timeoutPacing.active)
            assertEquals(1, timeoutConcurrency.released)
            assertEquals(1, timeoutPacing.released)
        } finally {
            timed.close()
        }

        val cancellationConcurrency = TrackingConcurrency()
        val cancellationPacing = TrackingPacing()
        val cancellable = mockTransport(
            policies = testPolicies(concurrency = cancellationConcurrency, pacing = cancellationPacing),
        ) { _ -> throw CancellationException("cancelled") }
        try {
            assertFailsWith<CancellationException> {
                cancellable.transport.execute(CrawlRequest("https://example.test/cancel"))
            }
            assertEquals(0, cancellationConcurrency.active + cancellationPacing.active)
            assertEquals(1, cancellationConcurrency.released)
            assertEquals(1, cancellationPacing.released)
        } finally {
            cancellable.close()
        }
    }

    @Test
    fun `fromClient lifecycle is idempotent and caller ownership is explicit`(): Unit = runBlocking {
        val caller = mockClient { _ -> reply("caller".toByteArray()) }
        val transport = KtorTransport.fromClient(
            caller.client,
            KtorTransportConfig(),
            testPolicies(),
            closeClient = false,
        )
        transport.start(com.rmjtromp.trawler.extension.ExtensionStartContext(this))
        transport.close()
        transport.close()
        transport.stop(
            com.rmjtromp.trawler.extension.ExtensionStopContext(
                com.rmjtromp.trawler.extension.ExtensionStopReason.ENGINE_SHUTDOWN,
            ),
        )
        assertFailsWith<IllegalStateException> { transport.execute(CrawlRequest("https://example.test/closed")) }
        assertFailsWith<IllegalStateException> { transport.start(com.rmjtromp.trawler.extension.ExtensionStartContext(this)) }
        assertEquals("caller", caller.client.get("https://example.test/parent").bodyAsText())
        caller.client.close()

        val ownedCaller = mockClient { _ -> reply("owned".toByteArray()) }
        val ownedTransport = KtorTransport.fromClient(
            ownedCaller.client,
            KtorTransportConfig(),
            testPolicies(),
            closeClient = true,
        )
        ownedTransport.close()
        assertFailsWith<IllegalStateException> {
            ownedCaller.client.get("https://example.test/closed-parent")
        }
    }

    @Test
    fun `owned resources close in reverse order and preserve suppressed failures`(): Unit = runBlocking {
        val closeOrder = CopyOnWriteArrayList<String>()
        val first = CloseProbe("first", closeOrder, throwOnClose = true)
        val second = CloseProbe("second", closeOrder, throwOnClose = true)
        val client = mockClient { _ -> reply() }
        val transport = KtorTransport.fromClient(
            client.client,
            KtorTransportConfig(),
            testPolicies(),
            ownedResources = listOf(first, second),
        )
        val failure = assertFailsWith<IllegalStateException> { transport.close() }
        assertEquals(listOf("second", "first"), closeOrder)
        assertEquals("second", failure.message)
        assertEquals(1, failure.suppressed.size)
        assertEquals("first", failure.suppressed.single().message)
        transport.close()
        client.client.close()
    }

    private fun mockTransport(
        configuration: KtorTransportConfig = KtorTransportConfig(),
        policies: WebTransportPolicies = testPolicies(),
        clock: Clock = FixedClock(Instant.parse("2026-01-01T00:00:00Z")),
        handler: suspend MockRequestHandleScope.(HttpRequestData) -> HttpResponseData,
    ): MockHarness {
        val client = mockClient(handler)
        val transport = KtorTransport.fromClient(
            client.client,
            configuration,
            policies,
            clock,
            closeClient = false,
        )
        return MockHarness(transport, client.client, client.engine)
    }

    private fun mockClient(
        handler: suspend MockRequestHandleScope.(HttpRequestData) -> HttpResponseData,
    ): MockClient {
        val configuration = MockEngineConfig().apply {
            reuseHandlers = true
            addHandler { request -> handler(request) }
        }
        val engine = MockEngine(configuration)
        val client = HttpClient(engine) {
            expectSuccess = false
            followRedirects = false
        }
        return MockClient(client, engine)
    }

    private fun testPolicies(
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
        responseHeaders: io.ktor.http.Headers = headersOf(),
    ): HttpResponseData = respond(body, status, responseHeaders)

    private fun HttpRequestData.bodyBytes(): ByteArray? = (body as? OutgoingContent.ByteArrayContent)?.bytes()

    private fun OutgoingContent?.contentTypeValue(): ContentType? = when (this) {
        is OutgoingContent.ByteArrayContent -> contentType
        is OutgoingContent.ContentWrapper -> contentType
        else -> null
    }

    private fun OutgoingContent?.asByteArray(): ByteArray = when (this) {
        null -> error("request has no body")
        is OutgoingContent.ByteArrayContent -> bytes()
        else -> error("unexpected request body type ${this::class.qualifiedName}")
    }

    private fun gzip(bytes: ByteArray): ByteArray = ByteArrayOutputStream().use { output ->
        GZIPOutputStream(output).use { it.write(bytes) }
        output.toByteArray()
    }

    private fun deflate(bytes: ByteArray): ByteArray = ByteArrayOutputStream().use { output ->
        DeflaterOutputStream(output).use { it.write(bytes) }
        output.toByteArray()
    }

    private data class MockHarness(
        val transport: KtorTransport,
        val client: HttpClient,
        val engine: MockEngine,
    ) {
        fun close() {
            transport.close()
            client.close()
        }
    }

    private data class MockClient(
        val client: HttpClient,
        val engine: MockEngine,
    )

    private data class RedirectCase(
        val status: Int,
        val method: RequestMethod,
        val hasBody: Boolean,
        val nextMethod: String,
        val preserveBody: Boolean,
    )

    private class FixedClock(private val instant: Instant) : Clock {
        override fun now(): Instant = instant
    }

    private class RecordingDestination : WebDestinationPolicy {
        override val id: String = "recording-destination"
        val targets = CopyOnWriteArrayList<String>()
        val historySizes = CopyOnWriteArrayList<Int>()

        override suspend fun evaluate(request: CrawlRequest, history: RedirectHistory): DestinationDecision {
            targets += request.target
            historySizes += history.size
            return DestinationDecision.Allowed
        }
    }

    private class RecordingRobots : RobotsPolicy {
        override val id: String = "recording-robots"
        val targets = CopyOnWriteArrayList<String>()

        override suspend fun evaluate(target: String, fetcher: RobotsFetcher): RobotsDecision {
            targets += target
            return RobotsDecision.Allowed(null)
        }
    }

    private class TrackingConcurrency : OriginConcurrencyPolicy {
        override val id: String = "tracking-concurrency"
        val kinds = CopyOnWriteArrayList<WebRequestKind>()
        var acquired: Int = 0
        var released: Int = 0
        var active: Int = 0

        override suspend fun acquire(origin: WebOrigin, kind: WebRequestKind): OriginConcurrencyPermit {
            kinds += kind
            acquired += 1
            active += 1
            val releasedOnce = AtomicBoolean(false)
            return OriginConcurrencyPermit {
                if (releasedOnce.compareAndSet(false, true)) {
                    released += 1
                    active -= 1
                }
            }
        }
    }

    private class TrackingPacing : OriginPacingPolicy {
        override val id: String = "tracking-pacing"
        var acquired: Int = 0
        var released: Int = 0
        var observed: Int = 0
        var active: Int = 0

        override suspend fun acquire(origin: WebOrigin, kind: WebRequestKind): OriginPacingReservation {
            acquired += 1
            active += 1
            val releasedOnce = AtomicBoolean(false)
            return OriginPacingReservation {
                if (releasedOnce.compareAndSet(false, true)) {
                    released += 1
                    active -= 1
                }
            }
        }

        override fun observe(origin: WebOrigin, response: CrawlResponse) {
            observed += 1
        }

        override fun observeRobotsDelay(origin: WebOrigin, delay: Duration) = Unit
    }

    private class RecordingCookies : CookieSession {
        override val id: String = "recording-cookies"
        val applied = CopyOnWriteArrayList<String>()
        val observed = CopyOnWriteArrayList<String>()

        override suspend fun apply(target: String, headers: Headers): Headers {
            applied += target
            return headers
        }

        override suspend fun observe(target: String, setCookieHeaders: List<String>) {
            observed += target
        }
    }

    private class CloseProbe(
        private val name: String,
        private val order: MutableList<String>,
        private val throwOnClose: Boolean,
    ) : AutoCloseable {
        override fun close() {
            order += name
            if (throwOnClose) throw IllegalStateException(name)
        }
    }
}
