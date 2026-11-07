package com.rmjtromp.trawler.web.policy

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.policy.AttemptSummary
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.FailureClassification
import com.rmjtromp.trawler.policy.FailureContext
import com.rmjtromp.trawler.policy.RetryDecision
import com.rmjtromp.trawler.web.transport.KtorTransportConfig
import com.rmjtromp.trawler.web.transport.NetworkAddressResolver
import com.rmjtromp.trawler.web.transport.WebPresets
import io.ktor.client.network.sockets.ConnectTimeoutException
import io.ktor.client.plugins.HttpRequestTimeoutException
import java.net.InetAddress
import java.net.SocketTimeoutException
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class HttpOutcomePolicyTest {
    private val context = FailureContext(CrawlerContext("engine", "run", "pipeline"), CrawlRequest("https://example.test/"))

    @Test
    fun `web failure classifier keeps retryability and safe detail for every adapter failure`() {
        data class Expected(val category: String, val retryable: Boolean, val detail: String?)
        val origin = com.rmjtromp.trawler.web.transport.WebOrigin("https", "example.test", 443)
        val cases = listOf(
            WebPolicyRejectedException("policy", "unsafe") to Expected("web-policy-rejected", false, "unsafe"),
            RobotsUnavailableException("robots-down") to Expected("robots-unavailable", true, "robots-down"),
            WebAddressResolutionException("dns.test", IllegalStateException("dns")) to Expected("web-dns-failure", true, "dns.test"),
            WebResponseTooLargeException("decompressed", 1) to Expected("web-body-limit", false, "decompressed"),
            UnsupportedWebContentEncodingException("br") to Expected("web-content-encoding", false, "br"),
            ConnectTimeoutException("connect", null) to Expected("http-connect-timeout", true, null),
            SocketTimeoutException("socket") to Expected("http-socket-timeout", true, null),
            HttpRequestTimeoutException("https://example.test/", 5L, null) to Expected("http-request-timeout", true, null),
            com.rmjtromp.trawler.web.transport.OriginCapacityExceededException("full") to Expected("web-origin-capacity", true, null),
            com.rmjtromp.trawler.web.transport.PacingWaitExceededException(origin, Duration.ofSeconds(9), Duration.ofSeconds(1)) to
                Expected("web-pacing-bound", true, null),
            com.rmjtromp.trawler.web.transport.NetworkAddressLimitExceededException("wide.test", 2) to
                Expected("web-dns-answer-limit", false, null),
            IllegalArgumentException("other") to Expected("web-transport-failure", true, "IllegalArgumentException"),
        )
        cases.forEach { (error, expected) ->
            val classification = DefaultWebFailureClassifier.classify(error, context)
            assertEquals(expected.category, classification.category, error::class.simpleName)
            assertEquals(expected.retryable, classification.retryableByDefault, error::class.simpleName)
            assertEquals(expected.detail, classification.detail, error::class.simpleName)
        }
        assertEquals("default-web-failure-classifier", DefaultWebFailureClassifier.id)
    }

    @Test
    fun `retry policy starts at the initial delay for unnumbered attempts and caps overflow`() {
        val now = Instant.parse("2026-01-01T00:00:00Z")
        val clock = FixedClock(now)
        val retryable = FailureClassification("temporary", retryableByDefault = true)

        val zero = DefaultHttpRetryPolicy(maxAttempts = 2, initialDelay = Duration.ZERO, maximumDelay = Duration.ofSeconds(1), clock = clock)
        assertEquals("default-http-retry", zero.id)
        assertEquals(RetryDecision.RetryAt(now), zero.decide(retryable, AttemptSummary(0, null, 0), context))
        assertEquals(RetryDecision.RetryAt(now), zero.decide(retryable, AttemptSummary(1, null, 0), context))
        assertEquals(RetryDecision.DoNotRetry, zero.decide(retryable, AttemptSummary(2, null, 1), context))

        val huge = Duration.ofSeconds(10_000_000_000_000_000L)
        val overflowing = DefaultHttpRetryPolicy(maxAttempts = 64, initialDelay = huge, maximumDelay = huge, clock = clock)
        assertEquals(RetryDecision.RetryAt(now.plus(huge)), overflowing.decide(retryable, AttemptSummary(11, null, 10), context))
        assertEquals(RetryDecision.RetryAt(now.plus(huge)), overflowing.decide(retryable, AttemptSummary(63, null, 62), context))
    }

    @Test
    fun `retry policy defaults use one second exponential backoff on the system clock`() {
        val policy = DefaultHttpRetryPolicy()
        val retryable = FailureClassification("temporary", retryableByDefault = true)
        val before = Instant.now()
        val first = assertIs<RetryDecision.RetryAt>(policy.decide(retryable, AttemptSummary(1, null, 0), context))
        val second = assertIs<RetryDecision.RetryAt>(policy.decide(retryable, AttemptSummary(2, null, 1), context))
        val after = Instant.now()
        assertTrue(first.at >= before.plusSeconds(1) && first.at <= after.plusSeconds(1), first.toString())
        assertTrue(second.at >= before.plusSeconds(2) && second.at <= after.plusSeconds(2), second.toString())
        assertEquals(RetryDecision.DoNotRetry, policy.decide(retryable, AttemptSummary(3, null, 2), context))
        assertFailsWith<IllegalArgumentException> { DefaultHttpRetryPolicy(maximumDelay = Duration.ofSeconds(-1)) }
    }

    @Test
    fun `status classification covers informational and every documented boundary`() {
        mapOf(
            100 to ("http-unclassified" to false),
            199 to ("http-unclassified" to false),
            200 to ("http-success-or-redirect" to false),
            304 to ("http-success-or-redirect" to false),
            404 to ("http-client-error" to false),
            499 to ("http-client-error" to false),
            503 to ("http-server-error" to true),
        ).forEach { (status, expected) ->
            val classification = DefaultHttpFailureClassifier.classify(response(status))
            assertEquals(expected.first, classification.category, status.toString())
            assertEquals(expected.second, classification.retryableByDefault, status.toString())
        }
        assertEquals("default-http-failure-classifier", DefaultHttpFailureClassifier.id)
    }

    @Test
    fun `retry-after tolerates surrounding whitespace and rejects overflowing numbers`() {
        val now = Instant.parse("2026-01-01T00:00:00Z")
        assertEquals(Duration.ofSeconds(5), DefaultRetryAfterPolicy.delay(response(503, " 5 "), now))
        assertEquals(Duration.ZERO, DefaultRetryAfterPolicy.delay(response(503, "0"), now))
        assertEquals(Duration.ofSeconds(Long.MAX_VALUE), DefaultRetryAfterPolicy.delay(response(503, Long.MAX_VALUE.toString()), now))
        assertNull(DefaultRetryAfterPolicy.delay(response(503, "9223372036854775808"), now))
        assertNull(DefaultRetryAfterPolicy.delay(response(503, null), now))
        assertEquals(
            Duration.ofSeconds(1),
            DefaultRetryAfterPolicy.delay(response(503, "Thu, 01 Jan 2026 00:00:01 GMT"), now),
        )
        assertEquals("default-retry-after", DefaultRetryAfterPolicy.id)
    }

    @Test
    fun `adapter exceptions expose their bounded diagnostic fields`() {
        assertEquals(7, WebResponseTooLargeException("compressed", 7).limitBytes)
        assertEquals(3, WebResponseHeadersTooLargeException(3).limit)
        assertEquals(4, WebResponseHeaderValueTooLargeException(4).limit)
        assertEquals(5, WebResponseHeaderNameTooLargeException(5).limit)
        assertEquals("zstd", UnsupportedWebContentEncodingException("zstd").encoding)
        val rejected = WebPolicyRejectedException("policy-id", "why")
        assertEquals("policy-id", rejected.policyId)
        assertEquals("why", rejected.reason)
        assertEquals(2, RedirectLimitExceededException(2).limit)
        assertEquals(9, CookieHeaderLimitException(9).limit)
        assertEquals("Redirect loop detected", InvalidWebRedirectException("Redirect loop detected").message)
    }

    @Test
    fun `header policies reject blank configuration and match names case-insensitively`() {
        assertFailsWith<IllegalArgumentException> { DefaultRedirectHeaderPolicy(setOf(" ")) }
        assertFailsWith<IllegalArgumentException> { DefaultHeaderRedactionPolicy(setOf("")) }
        assertFailsWith<IllegalArgumentException> { DefaultHeaderRedactionPolicy(redactedValue = " ") }

        val headers = Headers.of(
            Header("X-Custom-Secret", "secret"),
            Header("authorization", "Bearer token"),
            Header("Content-Type", "text/plain"),
            Header("transfer-encoding", "chunked"),
            Header("X-Trace", "keep"),
        )
        val custom = DefaultRedirectHeaderPolicy(setOf("X-CUSTOM-SECRET"))
        assertEquals("safe-redirect-headers", custom.id)
        assertEquals(
            listOf("authorization", "Content-Type", "X-Trace"),
            custom.headersForRedirect(headers, crossOrigin = true, bodyRemoved = false).entries.map { it.name },
        )
        assertEquals(
            listOf("X-Custom-Secret", "authorization", "X-Trace"),
            custom.headersForRedirect(headers, crossOrigin = false, bodyRemoved = true).entries.map { it.name },
        )
        val redacted = DefaultHeaderRedactionPolicy(setOf("X-CUSTOM-SECRET"), redactedValue = "***").redact(headers)
        assertEquals(listOf("***", "Bearer token"), redacted.entries.take(2).map { it.value })
        assertEquals("sensitive-header-redaction", DefaultHeaderRedactionPolicy().id)
    }

    @Test
    fun `transport configuration rejects sub-millisecond timeouts and the preset defaults are explicit`() {
        listOf<() -> Unit>(
            { KtorTransportConfig(requestTimeout = Duration.ofNanos(999_999)) },
            { KtorTransportConfig(socketTimeout = Duration.ofNanos(999_999)) },
            { KtorTransportConfig(totalRequestTimeout = Duration.ofNanos(999_999)) },
            { KtorTransportConfig(requestTimeout = Duration.ofSeconds(-1)) },
        ).forEach { invalid -> assertFailsWith<IllegalArgumentException> { invalid() } }
        assertEquals(Duration.ofMillis(1), KtorTransportConfig(totalRequestTimeout = Duration.ofMillis(1)).totalRequestTimeout)

        val resolver = object : NetworkAddressResolver {
            override val id: String = "unused"
            override suspend fun resolve(host: String): List<InetAddress> = error("not called")
        }
        val policies = WebPresets.publicWebPolicies(resolver)
        assertEquals("public-web-safety", policies.destination.id)
        assertEquals("default-http-failure-classifier", policies.failureClassifier.id)
        assertEquals("default-retry-after", policies.retryAfter.id)
        assertEquals("safe-redirect-headers", policies.redirectHeaders.id)
        assertEquals("sensitive-header-redaction", policies.headerRedaction.id)
    }

    private fun response(status: Int?, retryAfter: String? = null): CrawlResponse = CrawlResponse(
        request = CrawlRequest("https://example.test/"),
        statusCode = status,
        headers = retryAfter?.let { Headers.of(Header("Retry-After", it)) } ?: Headers.empty(),
        body = ResponseBody.empty(),
        receivedAt = Instant.EPOCH,
    )

    private class FixedClock(private val instant: Instant) : Clock {
        override fun now(): Instant = instant
    }
}
