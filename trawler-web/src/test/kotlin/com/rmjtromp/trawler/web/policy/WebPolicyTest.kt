package com.rmjtromp.trawler.web.policy

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.model.ResponseContext
import com.rmjtromp.trawler.policy.AttemptSummary
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.FailureClassification
import com.rmjtromp.trawler.policy.FailureContext
import com.rmjtromp.trawler.policy.RetryDecision
import com.rmjtromp.trawler.transport.RequestExecutor
import com.rmjtromp.trawler.transport.ResponseHandler
import com.rmjtromp.trawler.web.transport.BoundedOriginConcurrencyPolicy
import com.rmjtromp.trawler.web.transport.DestinationDecision
import com.rmjtromp.trawler.web.transport.FixedOriginPacingPolicy
import com.rmjtromp.trawler.web.transport.NetworkAddressResolver
import com.rmjtromp.trawler.web.transport.OriginCapacityExceededException
import com.rmjtromp.trawler.web.transport.OriginPacingPolicy
import com.rmjtromp.trawler.web.transport.OriginPacingReservation
import com.rmjtromp.trawler.web.transport.RedirectHistory
import com.rmjtromp.trawler.web.transport.RedirectHop
import com.rmjtromp.trawler.web.transport.WebOrigin
import com.rmjtromp.trawler.web.transport.WebRequestKind
import java.net.InetAddress
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

@OptIn(ExperimentalCoroutinesApi::class)
class WebPolicyTest {
    @Test
    fun `web origins normalize scheme host default ports and IPv6`() {
        assertEquals(WebOrigin("http", "example.test", 80), WebOrigin.fromTarget("HTTP://EXAMPLE.TEST/path"))
        assertEquals(WebOrigin("https", "example.test", 443), WebOrigin.fromTarget("https://EXAMPLE.TEST/path"))
        assertEquals(WebOrigin("https", "example.test", 8443), WebOrigin.fromTarget("https://EXAMPLE.TEST:8443/path"))
        assertEquals(WebOrigin("https", "2001:db8::1", 443), WebOrigin.fromTarget("https://[2001:DB8::1]/path"))
    }

    @Test
    fun `web origin rejects relative custom and malformed targets`() {
        listOf(
            "relative",
            "https:///missing-host",
            "https://example.test:0/",
            "https://example.test:65536/",
            "ftp://example.test/",
            "https://[2001:db8::1",
        ).forEach { target ->
            assertFailsWith<IllegalArgumentException> { WebOrigin.fromTarget(target) }
        }
        assertFailsWith<IllegalArgumentException> { WebOrigin("", "example.test", 80) }
        assertFailsWith<IllegalArgumentException> { WebOrigin("https", "example.test", 0) }
    }

    @Test
    fun `redirect history is immutable and enforces exact bound`() {
        val hop = RedirectHop("https://a.test/", "https://b.test/", 302, crossOrigin = true)
        val empty = RedirectHistory.empty(0)
        assertEquals(0, empty.size)
        assertFailsWith<IllegalArgumentException> { empty.plus(hop) }

        val history = RedirectHistory.empty(2).plus(hop)
        assertEquals(1, history.size)
        assertTrue(history.containsTarget("https://b.test/"))
        assertFalse(history.containsTarget("https://a.test/"))
        val second = history.plus(hop.copy(toTarget = "https://c.test/", statusCode = 307))
        assertEquals(2, second.size)
        assertFailsWith<IllegalArgumentException> {
            second.plus(hop.copy(toTarget = "https://d.test/", statusCode = 308))
        }
        assertEquals(1, history.size)
        assertFailsWith<IllegalArgumentException> { RedirectHistory.empty(-1) }
    }

    @Test
    fun `public safety rejects malformed targets before resolving`() = runTest {
        val resolver = RecordingResolver { listOf(ipv4(8, 8, 8, 8)) }
        val policy = PublicWebSafetyPolicy(resolver, PublicWebSafetyConfig(maxTargetLength = 64))
        val rejected = listOf(
            ("https://a.test/" + "x".repeat(64)) to "target-length-bound",
            "https://a b" to "target-contains-control-or-whitespace",
            "not a uri" to "target-contains-control-or-whitespace",
            "https://[bad" to "target-is-not-a-uri",
            "ftp://example.test/" to "scheme-not-allowed",
            "https://user:password@example.test/" to "userinfo-is-not-allowed",
            "https:///missing-host" to "host-required",
            "https://example.test:0/" to "invalid-port",
        )
        rejected.forEach { (target, reason) ->
            val decision = policy.evaluate(CrawlRequest(target), RedirectHistory.empty(2))
            assertEquals(DestinationDecision.Rejected(reason), decision, target)
        }
        assertTrue(resolver.hosts.isEmpty())

        val ipv6Resolver = RecordingResolver { listOf(ipv4(8, 8, 8, 8)) }
        PublicWebSafetyPolicy(ipv6Resolver).evaluate(
            CrawlRequest("https://[2001:db8::1]/"),
            RedirectHistory.empty(1),
        )
        assertEquals(listOf("2001:db8::1"), ipv6Resolver.hosts)
        assertEquals(
            DestinationDecision.Rejected("host-resolved-to-no-address"),
            PublicWebSafetyPolicy(RecordingResolver { emptyList() })
                .evaluate(CrawlRequest("https://empty.test/"), RedirectHistory.empty(1)),
        )
    }

    @Test
    fun `public safety accepts public addresses and rejects every blocked address class`() = runTest {
        val blocked = listOf(
            "IPv4 private 10" to ipv4(10, 1, 2, 3),
            "IPv4 private 172" to ipv4(172, 16, 2, 3),
            "IPv4 private 192" to ipv4(192, 168, 2, 3),
            "IPv4 unspecified" to ipv4(0, 0, 0, 0),
            "IPv4 loopback" to ipv4(127, 0, 0, 1),
            "IPv4 link-local" to ipv4(169, 254, 1, 1),
            "IPv4 metadata" to ipv4(169, 254, 169, 254),
            "IPv4 metadata alternate" to ipv4(100, 100, 100, 200),
            "IPv4 CGNAT" to ipv4(100, 64, 0, 1),
            "IPv4 benchmark" to ipv4(198, 18, 0, 1),
            "IPv4 multicast" to ipv4(224, 0, 0, 1),
            "IPv4 broadcast" to ipv4(255, 255, 255, 255),
            "IPv4-mapped private" to ipv4Mapped(192, 168, 1, 1),
            "IPv6 loopback" to ipv6(0, 0, 0, 0, 0, 0, 0, 1),
            "IPv6 unspecified" to ipv6(0, 0, 0, 0, 0, 0, 0, 0),
            "IPv6 unique-local" to ipv6(0xfc00, 0, 0, 0, 0, 0, 0, 1),
            "IPv6 link-local" to ipv6(0xfe80, 0, 0, 0, 0, 0, 0, 1),
            "IPv6 multicast" to ipv6(0xff02, 0, 0, 0, 0, 0, 0, 1),
            "IPv6 metadata" to ipv6(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x00fe),
            "IPv6 link-local metadata" to ipv6(0xfe80, 0, 0, 0, 0, 0, 0xa9fe, 0xa9fe),
        )
        blocked.forEach { (label, address) ->
            val policy = PublicWebSafetyPolicy(RecordingResolver { listOf(address) })
            val decision = policy.evaluate(
                CrawlRequest("https://example.test/${label.replace(' ', '-')}/"),
                RedirectHistory.empty(1),
            )
            assertIs<DestinationDecision.Rejected>(decision, label)
            assertTrue(decision.reason.contains("destination-address-blocked"), label)
        }

        val resolver = RecordingResolver { listOf(ipv4(8, 8, 8, 8)) }
        assertEquals(
            DestinationDecision.Allowed,
            PublicWebSafetyPolicy(resolver).evaluate(CrawlRequest("HTTPS://example.test/"), RedirectHistory.empty(1)),
        )
    }

    @Test
    fun `public safety fails closed when any DNS answer is blocked and preserves resolver cause`() = runTest {
        val resolver = RecordingResolver { listOf(ipv4(8, 8, 8, 8), ipv4(10, 0, 0, 1)) }
        val decision = PublicWebSafetyPolicy(resolver)
            .evaluate(CrawlRequest("https://example.test/"), RedirectHistory.empty(1))
        assertIs<DestinationDecision.Rejected>(decision)

        val cause = IllegalStateException("resolver failed")
        val failing = RecordingResolver { throw cause }
        val error = assertFailsWith<WebAddressResolutionException> {
            PublicWebSafetyPolicy(failing).evaluate(CrawlRequest("https://example.test/"), RedirectHistory.empty(1))
        }
        assertEquals("example.test", error.host)
        assertEquals(cause, error.cause)

        val cancelling = RecordingResolver { throw CancellationException("cancelled") }
        assertFailsWith<CancellationException> {
            PublicWebSafetyPolicy(cancelling).evaluate(CrawlRequest("https://example.test/"), RedirectHistory.empty(1))
        }
    }

    @Test
    fun `public safety configuration can selectively allow address classes`() = runTest {
        val policy = PublicWebSafetyPolicy(
            RecordingResolver { listOf(ipv4(10, 0, 0, 1)) },
            PublicWebSafetyConfig(blockPrivateAddresses = false),
        )
        assertEquals(
            DestinationDecision.Allowed,
            policy.evaluate(CrawlRequest("https://example.test/"), RedirectHistory.empty(1)),
        )
        assertFailsWith<IllegalArgumentException> { PublicWebSafetyConfig(allowedSchemes = emptySet()) }
        assertFailsWith<IllegalArgumentException> { PublicWebSafetyConfig(allowedSchemes = setOf(" ")) }
        assertFailsWith<IllegalArgumentException> { PublicWebSafetyConfig(maxTargetLength = 0) }
    }

    @Test
    fun `public safety configuration snapshots and protects allowed schemes`() {
        val supplied = mutableSetOf("http")
        val configuration = PublicWebSafetyConfig(allowedSchemes = supplied)
        supplied += "ftp"

        assertEquals(setOf("http"), configuration.allowedSchemes)
        @Suppress("UNCHECKED_CAST")
        val mutableView = configuration.allowedSchemes as MutableSet<String>
        assertFailsWith<UnsupportedOperationException> { mutableView += "https" }
    }

    @Test
    fun `HTTP status and retry-after policies classify boundaries without retrying`() {
        val statuses = mapOf(
            null to "transport-unknown",
            200 to "http-success-or-redirect",
            399 to "http-success-or-redirect",
            400 to "http-client-error",
            408 to "http-request-timeout",
            425 to "http-too-early",
            429 to "http-rate-limited",
            500 to "http-server-error",
            599 to "http-server-error",
            600 to "http-unclassified",
        )
        statuses.forEach { (status, category) ->
            val classification = DefaultHttpFailureClassifier.classify(response(status))
            assertEquals(category, classification.category)
            assertEquals(status, classification.statusCode)
        }
        assertTrue(DefaultHttpFailureClassifier.classify(response(null)).retryableByDefault)
        assertFalse(DefaultHttpFailureClassifier.classify(response(400)).retryableByDefault)

        val now = Instant.parse("2026-01-01T00:00:00Z")
        assertEquals(Duration.ofSeconds(12), DefaultRetryAfterPolicy.delay(response(429, "12"), now))
        assertNull(DefaultRetryAfterPolicy.delay(response(429, "-1"), now))
        assertNull(DefaultRetryAfterPolicy.delay(response(429, "not-a-date"), now))
        assertNull(DefaultRetryAfterPolicy.delay(response(429, " "), now))
        assertNull(DefaultRetryAfterPolicy.delay(response(429, "1".repeat(129)), now))
        assertEquals(
            Duration.ofSeconds(10),
            DefaultRetryAfterPolicy.delay(response(429, "Thu, 01 Jan 2026 00:00:10 GMT"), now),
        )
        assertEquals(
            Duration.ZERO,
            DefaultRetryAfterPolicy.delay(response(429, "Wed, 31 Dec 2025 23:59:59 GMT"), now),
        )
    }

    @Test
    fun `HTTP retry policy applies retryable attempt and delay bounds`() {
        val now = Instant.parse("2026-01-01T00:00:00Z")
        val clock = MutableClock(now)
        val policy = DefaultHttpRetryPolicy(
            maxAttempts = 4,
            initialDelay = Duration.ofSeconds(2),
            maximumDelay = Duration.ofSeconds(5),
            clock = clock,
        )
        val context = FailureContext(CrawlerContext("e", "r", "p"), CrawlRequest("x"))
        val retryable = FailureClassification("temporary", retryableByDefault = true)
        assertEquals(RetryDecision.RetryAt(now.plusSeconds(2)), policy.decide(retryable, AttemptSummary(1, null, 0), context))
        assertEquals(RetryDecision.RetryAt(now.plusSeconds(4)), policy.decide(retryable, AttemptSummary(2, null, 1), context))
        assertEquals(RetryDecision.RetryAt(now.plusSeconds(5)), policy.decide(retryable, AttemptSummary(3, null, 2), context))
        assertEquals(RetryDecision.DoNotRetry, policy.decide(retryable, AttemptSummary(4, null, 3), context))
        assertEquals(
            RetryDecision.DoNotRetry,
            policy.decide(FailureClassification("permanent", false), AttemptSummary(1, null, 0), context),
        )
        assertFailsWith<IllegalArgumentException> { DefaultHttpRetryPolicy(maxAttempts = 0) }
        assertFailsWith<IllegalArgumentException> { DefaultHttpRetryPolicy(initialDelay = Duration.ofSeconds(-1)) }
        assertFailsWith<IllegalArgumentException> { DefaultHttpRetryPolicy(maximumDelay = Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> {
            DefaultHttpRetryPolicy(initialDelay = Duration.ofSeconds(2), maximumDelay = Duration.ofSeconds(1))
        }
    }

    @Test
    fun `web failure classifier preserves stable categories for policy and transport failures`() {
        val context = FailureContext(CrawlerContext("e", "r", "p"), CrawlRequest("x"))
        val origin = WebOrigin("https", "example.test", 443)
        val failures = listOf(
            WebPolicyRejectedException("policy", "unsafe") to "web-policy-rejected",
            RobotsUnavailableException("robots") to "robots-unavailable",
            WebAddressResolutionException("example.test", IllegalStateException("dns")) to "web-dns-failure",
            WebResponseTooLargeException("compressed", 1) to "web-body-limit",
            WebResponseHeadersTooLargeException(1) to "web-header-limit",
            WebResponseHeaderValueTooLargeException(1) to "web-header-value-limit",
            WebResponseHeaderNameTooLargeException(1) to "web-header-name-limit",
            UnsupportedWebContentEncodingException("br") to "web-content-encoding",
            CookieHeaderLimitException(1) to "web-cookie-header-limit",
            com.rmjtromp.trawler.web.transport.NetworkAddressLimitExceededException("host", 1) to "web-dns-answer-limit",
            com.rmjtromp.trawler.web.transport.OriginCapacityExceededException("full") to "web-origin-capacity",
            com.rmjtromp.trawler.web.transport.PacingWaitExceededException(origin, Duration.ofSeconds(1), Duration.ZERO) to "web-pacing-bound",
            RedirectLimitExceededException(1) to "web-redirect-limit",
            InvalidWebRedirectException("loop") to "web-invalid-redirect",
            java.net.SocketTimeoutException("socket") to "http-socket-timeout",
            IllegalStateException("other") to "web-transport-failure",
        )
        failures.forEach { (error, category) ->
            val classification = DefaultWebFailureClassifier.classify(error, context)
            assertEquals(category, classification.category, error::class.simpleName)
        }
    }

    @Test
    fun `header redaction and redirect policy preserve safe metadata and strip credentials`() {
        val headers = Headers.of(
            Header("Authorization", "Bearer secret"),
            Header("X-Api-Key", "key"),
            Header("Cookie", "session=value"),
            Header("Accept", "text/html"),
        )
        val redacted = DefaultHeaderRedactionPolicy().redact(headers)
        assertEquals(listOf("[REDACTED]"), redacted["authorization"])
        assertEquals(listOf("[REDACTED]"), redacted["cookie"])
        assertEquals(listOf("text/html"), redacted["accept"])
        assertEquals(listOf("Bearer secret"), headers["authorization"])

        val redirectHeaders = Headers.of(
            Header("Authorization", "secret"),
            Header("Proxy-Authorization", "proxy"),
            Header("Cookie", "cookie"),
            Header("X-Api-Key", "api"),
            Header("Host", "old.test"),
            Header("Content-Length", "5"),
            Header("Transfer-Encoding", "chunked"),
            Header("Content-Type", "text/plain"),
            Header("Accept", "text/plain"),
        )
        val crossOrigin = DefaultRedirectHeaderPolicy().headersForRedirect(
            redirectHeaders,
            crossOrigin = true,
            bodyRemoved = true,
        )
        assertEquals(listOf("text/plain"), crossOrigin["accept"])
        listOf("authorization", "proxy-authorization", "cookie", "x-api-key", "host", "content-length", "transfer-encoding", "content-type")
            .forEach { assertTrue(crossOrigin[it].isEmpty(), it) }
        val sameOrigin = DefaultRedirectHeaderPolicy().headersForRedirect(redirectHeaders, false, false)
        assertEquals(listOf("secret"), sameOrigin["authorization"])
        assertTrue(sameOrigin["host"].isEmpty())
        assertTrue(sameOrigin["content-type"].isNotEmpty())

        assertFailsWith<IllegalArgumentException> { DefaultHeaderRedactionPolicy(sensitiveHeaderNames = setOf(" ")) }
        assertFailsWith<IllegalArgumentException> { DefaultHeaderRedactionPolicy(redactedValue = " ") }
    }

    @Test
    fun `request and response middleware preserve order and immutable copies`() = runTest {
        val requestOrder = mutableListOf<String>()
        val responseOrder = mutableListOf<String>()
        val baseRequest = CrawlRequest("https://example.test")
        val crawler = CrawlerContext("e", "r", "p")
        val requestContext = RequestContext(crawler, baseRequest)
        val middleware = listOf(
            AddRequestHeaderMiddleware("first", Header("X-First", "1")),
            AddRequestHeaderMiddleware("second", Header("X-Second", "2")),
        )
        val transport: RequestExecutor = RequestExecutor { request, _ ->
            requestOrder += "transport:${request.headers.entries.map { it.name }.joinToString(",")}"
            response(200)
        }
        val second: RequestExecutor = RequestExecutor { request, context ->
            requestOrder += middleware[1].id
            middleware[1].intercept(request, context, transport)
        }
        val first: RequestExecutor = RequestExecutor { request, context ->
            requestOrder += middleware[0].id
            middleware[0].intercept(request, context, second)
        }
        first.execute(baseRequest, requestContext)
        assertEquals(listOf("first", "second", "transport:X-First,X-Second"), requestOrder)

        val response = response(200)
        val responseContext = ResponseContext(crawler, baseRequest, response)
        val responseMiddleware = listOf(
            AddResponseAttributeMiddleware("first", com.rmjtromp.trawler.model.ContextAttribute("first", "1")),
            AddResponseAttributeMiddleware("second", com.rmjtromp.trawler.model.ContextAttribute("second", "2")),
        )
        val terminal: ResponseHandler = ResponseHandler { value, _ ->
            responseOrder += "handler"
            value
        }
        val secondResponse: ResponseHandler = ResponseHandler { value, context ->
            responseOrder += responseMiddleware[1].id
            responseMiddleware[1].intercept(value, context, terminal)
        }
        val firstResponse: ResponseHandler = ResponseHandler { value, context ->
            responseOrder += responseMiddleware[0].id
            responseMiddleware[0].intercept(value, context, secondResponse)
        }
        val decorated = firstResponse.handle(response, responseContext)
        assertEquals(listOf("first", "second", "handler"), responseOrder)
        assertEquals("1", decorated.attributes["first"])
        assertEquals("2", decorated.attributes["second"])
        assertFailsWith<IllegalArgumentException> { AddRequestHeaderMiddleware(" ", Header("X", "v")) }
        assertFailsWith<IllegalArgumentException> {
            AddResponseAttributeMiddleware(" ", com.rmjtromp.trawler.model.ContextAttribute("x", "v"))
        }
    }

    @Test
    fun `origin concurrency enforces capacity releases once and evicts only idle entries`() = runTest {
        val origin = WebOrigin("https", "a.test", 443)
        val policy = BoundedOriginConcurrencyPolicy(permitsPerOrigin = 1, maxOrigins = 2)
        val first = policy.acquire(origin, WebRequestKind.PAGE)
        val waiting = async(start = CoroutineStart.UNDISPATCHED) { policy.acquire(origin, WebRequestKind.PAGE) }
        runCurrent()
        assertFalse(waiting.isCompleted)
        first.release()
        first.release()
        val second = waiting.await()
        second.release()

        val a = policy.acquire(WebOrigin("https", "a.test", 443), WebRequestKind.PAGE)
        a.release()
        val b = policy.acquire(WebOrigin("https", "b.test", 443), WebRequestKind.PAGE)
        val c = policy.acquire(WebOrigin("https", "c.test", 443), WebRequestKind.PAGE)
        assertFailsWith<OriginCapacityExceededException> {
            policy.acquire(WebOrigin("https", "a.test", 443), WebRequestKind.PAGE)
        }
        b.release()
        c.release()
    }

    @Test
    fun `origin concurrency cancellation releases a waiting reservation`() = runTest {
        val origin = WebOrigin("https", "cancel.test", 443)
        val policy = BoundedOriginConcurrencyPolicy(permitsPerOrigin = 1)
        val first = policy.acquire(origin, WebRequestKind.PAGE)
        val cancelled = async(start = CoroutineStart.UNDISPATCHED) {
            policy.acquire(origin, WebRequestKind.PAGE)
        }
        runCurrent()
        assertFalse(cancelled.isCompleted)
        cancelled.cancelAndJoin()
        first.release()
        val next = policy.acquire(origin, WebRequestKind.PAGE)
        next.release()
    }

    @Test
    fun `JDK resolver is bounded and closes its owned dispatcher`() = runTest {
        val resolver = com.rmjtromp.trawler.web.transport.JdkNetworkAddressResolver(
            maxParallelLookups = 1,
            maxResolvedAddresses = 16,
        )
        assertTrue(resolver.resolve("localhost").isNotEmpty())
        resolver.close()
        resolver.close()
        assertFailsWith<IllegalStateException> { resolver.resolve("localhost") }
        assertFailsWith<IllegalArgumentException> {
            com.rmjtromp.trawler.web.transport.JdkNetworkAddressResolver(maxParallelLookups = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            com.rmjtromp.trawler.web.transport.JdkNetworkAddressResolver(maxResolvedAddresses = 0)
        }
    }

    @Test
    fun `fixed origin pacing is delayed per origin and observes retry-after and robots delay`() = runTest {
        val now = Instant.parse("2026-01-01T00:00:00Z")
        val clock = MutableClock(now)
        val origin = WebOrigin("https", "a.test", 443)
        val policy = FixedOriginPacingPolicy(
            minimumDelay = Duration.ofMillis(100),
            maxWait = Duration.ofSeconds(20),
            clock = clock,
            retryAfterPolicy = DefaultRetryAfterPolicy,
        )
        val first = policy.acquire(origin, WebRequestKind.PAGE)
        first.release()
        val delayed = async(start = CoroutineStart.UNDISPATCHED) { policy.acquire(origin, WebRequestKind.PAGE) }
        runCurrent()
        assertFalse(delayed.isCompleted)
        advanceTimeBy(99)
        runCurrent()
        assertFalse(delayed.isCompleted)
        advanceTimeBy(1)
        val second = delayed.await()
        second.release()

        val bounded = FixedOriginPacingPolicy(
            minimumDelay = Duration.ZERO,
            maxWait = Duration.ofSeconds(5),
            clock = clock,
            retryAfterPolicy = DefaultRetryAfterPolicy,
        )
        bounded.acquire(origin, WebRequestKind.PAGE).release()
        bounded.observe(origin, response(429, "10"))
        assertFailsWith<com.rmjtromp.trawler.web.transport.PacingWaitExceededException> {
            bounded.acquire(origin, WebRequestKind.PAGE)
        }

        val isolated = WebOrigin("https", "isolated.test", 443)
        val isolatedFirst = policy.acquire(isolated, WebRequestKind.PAGE)
        isolatedFirst.release()
        policy.observeRobotsDelay(isolated, Duration.ofSeconds(3))
        val robotsDelayed = async(start = CoroutineStart.UNDISPATCHED) {
            policy.acquire(isolated, WebRequestKind.ROBOTS)
        }
        runCurrent()
        assertFalse(robotsDelayed.isCompleted)
        advanceTimeBy(3_000)
        robotsDelayed.await().release()
    }

    @Test
    fun `fixed origin pacing cancellation leaves a safe reserved gap and evicts idle origins`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val policy = FixedOriginPacingPolicy(
            minimumDelay = Duration.ofMillis(50),
            maxWait = Duration.ofSeconds(1),
            maxOrigins = 2,
            clock = clock,
            retryAfterPolicy = DefaultRetryAfterPolicy,
        )
        val a = WebOrigin("https", "a.test", 443)
        val b = WebOrigin("https", "b.test", 443)
        val c = WebOrigin("https", "c.test", 443)
        policy.acquire(a, WebRequestKind.PAGE).release()
        val waiting = async(start = CoroutineStart.UNDISPATCHED) { policy.acquire(a, WebRequestKind.PAGE) }
        runCurrent()
        waiting.cancelAndJoin()
        policy.acquire(b, WebRequestKind.PAGE).release()
        policy.acquire(c, WebRequestKind.PAGE).release()
        val active = policy.acquire(b, WebRequestKind.PAGE)
        val activeC = policy.acquire(c, WebRequestKind.PAGE)
        assertFailsWith<OriginCapacityExceededException> { policy.acquire(a, WebRequestKind.PAGE) }
        active.release()
        activeC.release()
        advanceTimeBy(50)
    }

    @Test
    fun `robots policy selects user agent groups and resolves rules on path and query`() = runTest {
        val clock = MutableClock(Instant.parse("2026-01-01T00:00:00Z"))
        val fetcher = RecordingRobotsFetcher(
            RobotsFetchResult.Available(
                200,
                """
                # A comment and an inline comment are ignored.
                User-agent: *
                Disallow: /private
                Allow: /private/public
                User-agent: TrawlerBot
                Disallow: /special
                Allow: /special/open
                Disallow: /wild/*/end$
                Disallow: /search?secret=1$
                Disallow: /tie # the allow rule wins an exact tie
                Allow: /tie
                Crawl-delay: 0.5
                """.trimIndent(),
                clock.now(),
            ),
        )
        val policy = CachingRobotsPolicy(
            RobotsPolicyConfig(userAgent = "TrawlerBot/0.1"),
            clock,
        )
        assertIs<RobotsDecision.Allowed>(policy.evaluate("https://example.test/", fetcher))
        assertIs<RobotsDecision.Disallowed>(policy.evaluate("https://example.test/special", fetcher))
        val allowed = assertIs<RobotsDecision.Allowed>(policy.evaluate("https://example.test/special/open", fetcher))
        assertEquals(Duration.ofMillis(500), allowed.crawlDelay)
        assertIs<RobotsDecision.Disallowed>(policy.evaluate("https://example.test/wild/x/end", fetcher))
        assertIs<RobotsDecision.Allowed>(policy.evaluate("https://example.test/wild/x/end-more", fetcher))
        assertIs<RobotsDecision.Disallowed>(policy.evaluate("https://example.test/search?secret=1", fetcher))
        assertIs<RobotsDecision.Allowed>(policy.evaluate("https://example.test/search?secret=2", fetcher))
        assertIs<RobotsDecision.Allowed>(policy.evaluate("https://example.test/tie", fetcher))
        assertEquals(1, fetcher.calls.size)

        val genericPolicy = CachingRobotsPolicy(RobotsPolicyConfig(userAgent = "OtherBot"), clock)
        val genericFetcher = RecordingRobotsFetcher(fetcherResponse())
        assertIs<RobotsDecision.Disallowed>(genericPolicy.evaluate("https://example.test/private", genericFetcher))
        assertIs<RobotsDecision.Allowed>(genericPolicy.evaluate("https://example.test/private/public", genericFetcher))
    }

    @Test
    fun `robots policy handles status body delay unavailable behavior TTL and eviction`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val good = RobotsFetchResult.Available(200, "User-agent: *\nAllow: /", clock.now())
        val fetcher = RecordingRobotsFetcher(good)
        val policy = CachingRobotsPolicy(
            RobotsPolicyConfig(cacheTtl = Duration.ofSeconds(10), maxEntries = 1),
            clock,
        )
        assertIs<RobotsDecision.Allowed>(policy.evaluate("https://a.test/", fetcher))
        clock.advance(Duration.ofSeconds(10))
        assertIs<RobotsDecision.Allowed>(policy.evaluate("https://a.test/", fetcher))
        assertEquals(1, fetcher.calls.size)
        clock.advance(Duration.ofNanos(1))
        assertIs<RobotsDecision.Allowed>(policy.evaluate("https://a.test/", fetcher))
        assertEquals(2, fetcher.calls.size)
        assertIs<RobotsDecision.Allowed>(policy.evaluate("https://b.test/", fetcher))
        assertIs<RobotsDecision.Allowed>(policy.evaluate("https://a.test/", fetcher))
        assertEquals(4, fetcher.calls.size)

        val notFound = CachingRobotsPolicy().evaluate(
            "https://empty.test/private",
            RecordingRobotsFetcher(RobotsFetchResult.Available(404, "", clock.now())),
        )
        assertIs<RobotsDecision.Allowed>(notFound)

        val tooLarge = CachingRobotsPolicy(
            RobotsPolicyConfig(maxBodyBytes = 3),
        ).evaluate(
            "https://large.test/",
            RecordingRobotsFetcher(RobotsFetchResult.Available(200, "Allow: /", clock.now())),
        )
        assertIs<RobotsDecision.Disallowed>(tooLarge)

        val failedFetcher = RecordingRobotsFetcher(RobotsFetchResult.Unavailable("network"))
        assertIs<RobotsDecision.Allowed>(
            CachingRobotsPolicy(RobotsPolicyConfig(unavailableBehavior = RobotsUnavailableBehavior.ALLOW))
                .evaluate("https://fail.test/", failedFetcher),
        )
        assertIs<RobotsDecision.Disallowed>(
            CachingRobotsPolicy(RobotsPolicyConfig(unavailableBehavior = RobotsUnavailableBehavior.DENY))
                .evaluate("https://fail.test/", failedFetcher),
        )
        assertIs<RobotsDecision.Unavailable>(
            CachingRobotsPolicy(RobotsPolicyConfig(unavailableBehavior = RobotsUnavailableBehavior.FAIL))
                .evaluate("https://fail.test/", failedFetcher),
        )

        val longDelay = "User-agent: *\nCrawl-delay: 10\nAllow: /"
        listOf(
            RobotsUnavailableBehavior.ALLOW to RobotsDecision.Allowed(null)::class,
            RobotsUnavailableBehavior.DENY to RobotsDecision.Disallowed::class,
            RobotsUnavailableBehavior.FAIL to RobotsDecision.Unavailable::class,
        ).forEach { (behavior, expected) ->
            val decision = CachingRobotsPolicy(
                RobotsPolicyConfig(maxCrawlDelay = Duration.ofSeconds(5), longCrawlDelayBehavior = behavior),
            ).evaluate("https://delay-$behavior.test/", RecordingRobotsFetcher(RobotsFetchResult.Available(200, longDelay, clock.now())))
            assertEquals(expected, decision::class)
        }

        assertFailsWith<IllegalArgumentException> { RobotsPolicyConfig(userAgent = " ") }
        assertFailsWith<IllegalArgumentException> { RobotsPolicyConfig(cacheTtl = Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { RobotsPolicyConfig(maxEntries = 0) }
        assertFailsWith<IllegalArgumentException> { RobotsPolicyConfig(maxBodyBytes = 0) }
        assertFailsWith<IllegalArgumentException> { RobotsPolicyConfig(maxCrawlDelay = Duration.ZERO) }
    }

    @Test
    fun `robots fetch cancellation and invalid targets are visible and uncached`() = runTest {
        val started = kotlinx.coroutines.CompletableDeferred<Unit>()
        val cancelled = kotlinx.coroutines.CompletableDeferred<Unit>()
        val fetcher = object : RobotsFetcher {
            override suspend fun fetch(origin: WebOrigin, userAgent: String): RobotsFetchResult {
                started.complete(Unit)
                try {
                    awaitCancellation()
                } finally {
                    cancelled.complete(Unit)
                }
            }
        }
        val job = launch(start = CoroutineStart.UNDISPATCHED) {
            CachingRobotsPolicy().evaluate("https://example.test/", fetcher)
        }
        started.await()
        job.cancelAndJoin()
        assertTrue(cancelled.isCompleted)

        val calls = AtomicInteger()
        val noCall = object : RobotsFetcher {
            override suspend fun fetch(origin: WebOrigin, userAgent: String): RobotsFetchResult {
                calls.incrementAndGet()
                return RobotsFetchResult.Available(200, "Allow: /", Instant.EPOCH)
            }
        }
        assertIs<RobotsDecision.Disallowed>(CachingRobotsPolicy().evaluate("relative", noCall))
        assertEquals(0, calls.get())
    }

    @Test
    fun `web transport configuration and policy bundle reject every invalid bound`() {
        assertFailsWith<IllegalArgumentException> { com.rmjtromp.trawler.web.transport.KtorTransportConfig(id = " ") }
        assertFailsWith<IllegalArgumentException> {
            com.rmjtromp.trawler.web.transport.KtorTransportConfig(connectTimeout = Duration.ZERO)
        }
        assertFailsWith<IllegalArgumentException> {
            com.rmjtromp.trawler.web.transport.KtorTransportConfig(connectTimeout = Duration.ofNanos(1))
        }
        assertFailsWith<IllegalArgumentException> {
            com.rmjtromp.trawler.web.transport.KtorTransportConfig(maxRequestBodyBytes = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            com.rmjtromp.trawler.web.transport.KtorTransportConfig(maxRedirects = -1)
        }
        assertFailsWith<IllegalArgumentException> {
            com.rmjtromp.trawler.web.transport.KtorTransportConfig(userAgent = " ")
        }
        assertFailsWith<IllegalArgumentException> {
            com.rmjtromp.trawler.web.transport.PublicWebPresetConfig(permitsPerOrigin = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            com.rmjtromp.trawler.web.transport.PublicWebPresetConfig(maxOrigins = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            com.rmjtromp.trawler.web.transport.PublicWebPresetConfig(maximumPacingWait = Duration.ZERO)
        }
        assertFailsWith<IllegalArgumentException> {
            com.rmjtromp.trawler.web.transport.PublicWebPresetConfig(minimumOriginDelay = Duration.ofSeconds(-1))
        }
        val preset = com.rmjtromp.trawler.web.transport.WebPresets.publicWebPolicies(
            RecordingResolver { listOf(ipv4(8, 8, 8, 8)) },
            clock = MutableClock(Instant.EPOCH),
        )
        assertEquals("public-web-safety", preset.destination.id)
        assertEquals("cached-robots-policy", preset.robots.id)
        assertEquals("bounded-origin-concurrency", preset.concurrency.id)
        assertEquals("fixed-origin-pacing", preset.pacing.id)
        assertEquals("memory-cookie-session", preset.cookies.id)
        val blankPolicies = listOf<() -> Unit>(
            { preset.copy(destination = blankDestination) },
            {
                preset.copy(robots = object : com.rmjtromp.trawler.web.policy.RobotsPolicy {
                    override val id: String = ""
                    override suspend fun evaluate(target: String, fetcher: RobotsFetcher): RobotsDecision = RobotsDecision.Allowed(null)
                })
            },
            {
                preset.copy(concurrency = object : com.rmjtromp.trawler.web.transport.OriginConcurrencyPolicy {
                    override val id: String = ""
                    override suspend fun acquire(
                        origin: WebOrigin,
                        kind: WebRequestKind,
                    ) = com.rmjtromp.trawler.web.transport.OriginConcurrencyPermit { }
                })
            },
            {
                preset.copy(pacing = object : OriginPacingPolicy {
                    override val id: String = ""
                    override suspend fun acquire(origin: WebOrigin, kind: WebRequestKind) = OriginPacingReservation { }
                    override fun observe(origin: WebOrigin, response: CrawlResponse) = Unit
                    override fun observeRobotsDelay(origin: WebOrigin, delay: Duration) = Unit
                })
            },
            {
                preset.copy(cookies = object : com.rmjtromp.trawler.web.session.CookieSession {
                    override val id: String = ""
                    override suspend fun apply(target: String, headers: Headers) = headers
                    override suspend fun observe(target: String, setCookieHeaders: List<String>) = Unit
                })
            },
            {
                preset.copy(failureClassifier = object : HttpFailureClassifier {
                    override val id: String = ""
                    override fun classify(response: CrawlResponse) = DefaultHttpFailureClassifier.classify(response)
                })
            },
            {
                preset.copy(retryAfter = object : RetryAfterPolicy {
                    override val id: String = ""
                    override fun delay(response: CrawlResponse, now: Instant): Duration? = null
                })
            },
            {
                preset.copy(redirectHeaders = object : RedirectHeaderPolicy {
                    override val id: String = ""
                    override fun headersForRedirect(headers: Headers, crossOrigin: Boolean, bodyRemoved: Boolean) = headers
                })
            },
            {
                preset.copy(headerRedaction = object : HeaderRedactionPolicy {
                    override val id: String = ""
                    override fun redact(headers: Headers) = headers
                })
            },
        )
        blankPolicies.forEach { invalid -> assertFailsWith<IllegalArgumentException> { invalid() } }
        val base = com.rmjtromp.trawler.web.transport.KtorTransportConfig()
        listOf<() -> Unit>(
            { base.copy(requestTimeout = Duration.ZERO) },
            { base.copy(socketTimeout = Duration.ZERO) },
            { base.copy(totalRequestTimeout = Duration.ZERO) },
            { base.copy(maxCompressedBodyBytes = 0) },
            { base.copy(maxDecompressedBodyBytes = 0) },
            { base.copy(maxRobotsBodyBytes = 0) },
            { base.copy(maxResponseHeaders = 0) },
            { base.copy(maxRequestHeaders = 0) },
            { base.copy(maxTargetLength = 0) },
            { base.copy(maxMethodLength = 0) },
            { base.copy(maxHeaderNameLength = 0) },
            { base.copy(maxHeaderValueLength = 0) },
            { base.copy(maxLocationLength = 0) },
            { base.copy(maxClientConnections = 0) },
            { base.copy(maxConnectionsPerOrigin = 0) },
        ).forEach { invalid -> assertFailsWith<IllegalArgumentException> { invalid() } }

        assertFailsWith<IllegalArgumentException> { BoundedOriginConcurrencyPolicy(permitsPerOrigin = 0) }
        assertFailsWith<IllegalArgumentException> { BoundedOriginConcurrencyPolicy(maxOrigins = 0) }
        assertFailsWith<IllegalArgumentException> {
            FixedOriginPacingPolicy(
                minimumDelay = Duration.ofSeconds(-1),
                retryAfterPolicy = DefaultRetryAfterPolicy,
            )
        }
        assertFailsWith<IllegalArgumentException> {
            FixedOriginPacingPolicy(maxWait = Duration.ZERO, retryAfterPolicy = DefaultRetryAfterPolicy)
        }
        assertFailsWith<IllegalArgumentException> {
            FixedOriginPacingPolicy(maxOrigins = 0, retryAfterPolicy = DefaultRetryAfterPolicy)
        }
        assertFailsWith<IllegalArgumentException> { RedirectHop(" ", "https://b.test", 302, true) }
        assertFailsWith<IllegalArgumentException> { RedirectHop("https://a.test", " ", 302, true) }
        assertFailsWith<IllegalArgumentException> { RedirectHop("https://a.test", "https://b.test", 200, true) }
        assertFailsWith<IllegalArgumentException> { DestinationDecision.Rejected("") }
        assertFailsWith<IllegalArgumentException> { HttpFailureClassification("", null, false) }
        assertFailsWith<IllegalArgumentException> { RobotsFetchResult.Unavailable("") }
        assertFailsWith<IllegalArgumentException> { RobotsDecision.Disallowed("") }
        assertFailsWith<IllegalArgumentException> { RobotsDecision.Unavailable("") }
        assertFailsWith<IllegalArgumentException> {
            com.rmjtromp.trawler.web.transport.WebTransportPolicies(
                destination = blankDestination,
                robots = NoRobotsPolicy,
                concurrency = BoundedOriginConcurrencyPolicy(),
                pacing = NoPacing,
                cookies = com.rmjtromp.trawler.web.session.NoCookieSession,
                failureClassifier = DefaultHttpFailureClassifier,
                retryAfter = DefaultRetryAfterPolicy,
                redirectHeaders = DefaultRedirectHeaderPolicy(),
                headerRedaction = DefaultHeaderRedactionPolicy(),
            )
        }
    }

    private fun response(status: Int?, retryAfter: String? = null): CrawlResponse = CrawlResponse(
        request = CrawlRequest("https://example.test/"),
        statusCode = status,
        headers = retryAfter?.let { Headers.of(Header("Retry-After", it)) } ?: Headers.empty(),
        body = ResponseBody.empty(),
        receivedAt = Instant.EPOCH,
    )

    private fun ipv4(vararg octets: Int): InetAddress =
        InetAddress.getByAddress("answer", octets.map(Int::toByte).toByteArray())

    private fun ipv4Mapped(a: Int, b: Int, c: Int, d: Int): InetAddress =
        InetAddress.getByAddress("answer", ByteArray(10) + byteArrayOf(0xff.toByte(), 0xff.toByte(), a.toByte(), b.toByte(), c.toByte(), d.toByte()))

    private fun ipv6(vararg words: Int): InetAddress {
        require(words.size == 8)
        val bytes = ByteArray(16)
        words.forEachIndexed { index, word ->
            bytes[index * 2] = (word ushr 8).toByte()
            bytes[index * 2 + 1] = word.toByte()
        }
        return InetAddress.getByAddress("answer", bytes)
    }

    private class RecordingResolver(
        private val answer: suspend (String) -> List<InetAddress>,
    ) : NetworkAddressResolver {
        override val id: String = "test-resolver"
        val hosts = CopyOnWriteArrayList<String>()

        override suspend fun resolve(host: String): List<InetAddress> {
            hosts += host
            return answer(host)
        }
    }

    private class MutableClock(initial: Instant) : Clock {
        @Volatile
        var current: Instant = initial
        override fun now(): Instant = current
        fun advance(duration: Duration) {
            current = current.plus(duration)
        }
    }

    private class RecordingRobotsFetcher(
        private val response: RobotsFetchResult,
    ) : RobotsFetcher {
        val calls = CopyOnWriteArrayList<Pair<WebOrigin, String>>()
        override suspend fun fetch(origin: WebOrigin, userAgent: String): RobotsFetchResult {
            calls += origin to userAgent
            return response
        }
    }

    private fun fetcherResponse(): RobotsFetchResult =
        RobotsFetchResult.Available(
            200,
            "User-agent: *\nDisallow: /private\nAllow: /private/public",
            Instant.EPOCH,
        )

    private object NoPacing : OriginPacingPolicy {
        override val id: String = "no-pacing-test"
        override suspend fun acquire(origin: WebOrigin, kind: WebRequestKind): OriginPacingReservation = OriginPacingReservation { }
        override fun observe(origin: WebOrigin, response: CrawlResponse) = Unit
        override fun observeRobotsDelay(origin: WebOrigin, delay: Duration) = Unit
    }

    private val blankDestination = object : com.rmjtromp.trawler.web.transport.WebDestinationPolicy {
        override val id: String = " "
        override suspend fun evaluate(request: CrawlRequest, history: RedirectHistory): DestinationDecision = DestinationDecision.Allowed
    }

}
