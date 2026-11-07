package com.rmjtromp.trawler.web.transport

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.web.policy.DefaultRetryAfterPolicy
import java.net.Inet6Address
import java.net.InetAddress
import java.net.URISyntaxException
import java.time.Duration
import java.time.Instant
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

@OptIn(ExperimentalCoroutinesApi::class)
class WebTransportContractsTest {
    @Test
    fun `web origin parsing rejects missing parts and preserves the URI cause`() {
        assertFailsWith<IllegalArgumentException> { WebOrigin("https", " ", 443) }
        assertFailsWith<IllegalArgumentException> { WebOrigin("https", "example.test", 65_536) }
        assertEquals(WebOrigin("https", "example.test", 65_535), WebOrigin.fromTarget("https://example.test:65535/"))
        assertEquals("Target scheme is required", assertFailsWith<IllegalArgumentException> { WebOrigin.fromTarget("//example.test/") }.message)
        assertEquals("Target host is required", assertFailsWith<IllegalArgumentException> { WebOrigin.fromTarget("mailto:a@example.test") }.message)
        assertEquals(
            "A port is required for scheme 'ws'",
            assertFailsWith<IllegalArgumentException> { WebOrigin.fromTarget("ws://example.test/") }.message,
        )
        assertEquals(WebOrigin("ws", "example.test", 9000), WebOrigin.fromTarget("WS://example.test:9000/"))
        val invalid = assertFailsWith<IllegalArgumentException> { WebOrigin.fromTarget("https://exa mple.test/") }
        assertIs<URISyntaxException>(invalid.cause)
    }

    @Test
    fun `redirect hops and history expose immutable bounded entries`() {
        val hop = RedirectHop("https://a.test/", "https://b.test/", 399, crossOrigin = true)
        assertEquals("https://a.test/", hop.fromTarget)
        assertEquals("https://b.test/", hop.toTarget)
        assertEquals(399, hop.statusCode)
        assertTrue(hop.crossOrigin)
        assertFailsWith<IllegalArgumentException> { RedirectHop("https://a.test/", "https://b.test/", 299, false) }
        assertFailsWith<IllegalArgumentException> { RedirectHop("https://a.test/", "https://b.test/", 400, false) }

        val history = RedirectHistory.empty(3).plus(hop).plus(hop.copy(statusCode = 300, crossOrigin = false))
        assertEquals(listOf(hop, hop.copy(statusCode = 300, crossOrigin = false)), history.entries)
        @Suppress("UNCHECKED_CAST")
        val mutable = history.entries as MutableList<RedirectHop>
        assertFailsWith<UnsupportedOperationException> { mutable.add(hop) }
        assertEquals(2, history.size)
    }

    @Test
    fun `JDK resolver handles literals without DNS validates input and refuses use after close`(): Unit = runBlocking {
        val resolver = JdkNetworkAddressResolver()
        try {
            assertEquals("jdk-dns", resolver.id)
            assertEquals(listOf(InetAddress.getByAddress(byteArrayOf(127, 0, 0, 1))), resolver.resolve("127.0.0.1"))
            val ipv6 = resolver.resolve("::1").single()
            assertIs<Inet6Address>(ipv6)
            assertTrue(ipv6.isLoopbackAddress)
            assertFailsWith<IllegalArgumentException> { resolver.resolve(" ") }
        } finally {
            resolver.close()
        }
        resolver.close()
        assertEquals("DNS resolver is closed", assertFailsWith<IllegalStateException> { resolver.resolve("127.0.0.1") }.message)
        assertFailsWith<IllegalArgumentException> { JdkNetworkAddressResolver(maxParallelLookups = 0) }
        assertFailsWith<IllegalArgumentException> { JdkNetworkAddressResolver(maxResolvedAddresses = 0) }
    }

    @Test
    fun `origin concurrency fails rather than grows when every origin slot is active`() = runTest {
        val policy = BoundedOriginConcurrencyPolicy(permitsPerOrigin = 1, maxOrigins = 1)
        assertEquals("bounded-origin-concurrency", policy.id)
        val held = policy.acquire(origin("a.test"), WebRequestKind.PAGE)
        val failure = assertFailsWith<OriginCapacityExceededException> { policy.acquire(origin("b.test"), WebRequestKind.ROBOTS) }
        assertContains(failure.message.orEmpty(), "b.test")
        held.release()
        held.release()
        val replacement = policy.acquire(origin("b.test"), WebRequestKind.PAGE)
        replacement.release()

        val none = NoOriginConcurrencyPolicy.acquire(origin("c.test"), WebRequestKind.PAGE)
        none.release()
        none.release()
        assertEquals("no-origin-concurrency", NoOriginConcurrencyPolicy.id)
    }

    @Test
    fun `origin concurrency evicts one idle origin when several are idle and keeps admitting work`() = runTest {
        val policy = BoundedOriginConcurrencyPolicy(permitsPerOrigin = 1, maxOrigins = 2)
        policy.acquire(origin("a.test"), WebRequestKind.PAGE).release()
        policy.acquire(origin("b.test"), WebRequestKind.PAGE).release()
        policy.acquire(origin("a.test"), WebRequestKind.PAGE).release()
        val c = policy.acquire(origin("c.test"), WebRequestKind.PAGE)
        val a = policy.acquire(origin("a.test"), WebRequestKind.PAGE)
        assertFailsWith<OriginCapacityExceededException>("b was the least recently used idle origin and was evicted") {
            policy.acquire(origin("b.test"), WebRequestKind.PAGE)
        }
        c.release()
        a.release()
        policy.acquire(origin("b.test"), WebRequestKind.PAGE).release()
    }

    @Test
    fun `pacing observes retry-after only for known origins and never moves the next start earlier`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val policy = FixedOriginPacingPolicy(
            minimumDelay = Duration.ofSeconds(1),
            maxWait = Duration.ofSeconds(30),
            clock = clock,
            retryAfterPolicy = DefaultRetryAfterPolicy,
        )
        assertEquals("fixed-origin-pacing", policy.id)
        val known = origin("known.test")
        policy.observe(origin("unknown.test"), response(retryAfter = "20"))
        val unknownStart = currentTime
        policy.acquire(origin("unknown.test"), WebRequestKind.PAGE).release()
        assertEquals(unknownStart, currentTime, "an observation for an unseen origin creates no pacing state")

        val first = policy.acquire(known, WebRequestKind.PAGE)
        first.release()
        first.release()
        policy.observe(known, response(retryAfter = null))
        policy.observe(known, response(retryAfter = "0"))
        val beforeSecond = currentTime
        policy.acquire(known, WebRequestKind.PAGE).release()
        assertEquals(1_000L, currentTime - beforeSecond, "no hint and an earlier hint keep the minimum delay")

        policy.observe(known, response(retryAfter = "10"))
        val beforeThird = currentTime
        policy.acquire(known, WebRequestKind.PAGE).release()
        assertEquals(10_000L, currentTime - beforeThird)
    }

    @Test
    fun `robots crawl delay creates pacing state only grows the delay and rejects negatives`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val policy = FixedOriginPacingPolicy(
            minimumDelay = Duration.ofSeconds(1),
            maxWait = Duration.ofSeconds(30),
            clock = clock,
            retryAfterPolicy = DefaultRetryAfterPolicy,
        )
        val target = origin("robots.test")
        policy.observeRobotsDelay(target, Duration.ofSeconds(4))
        policy.observeRobotsDelay(target, Duration.ofSeconds(2))
        policy.acquire(target, WebRequestKind.ROBOTS).release()
        val before = currentTime
        policy.acquire(target, WebRequestKind.PAGE).release()
        assertEquals(4_000L, currentTime - before)
        assertFailsWith<IllegalArgumentException> { policy.observeRobotsDelay(target, Duration.ofMillis(-1)) }
        assertFailsWith<IllegalArgumentException> {
            FixedOriginPacingPolicy(maxWait = Duration.ofSeconds(-1), retryAfterPolicy = DefaultRetryAfterPolicy)
        }

        val full = FixedOriginPacingPolicy(maxOrigins = 1, clock = clock, retryAfterPolicy = DefaultRetryAfterPolicy)
        val active = full.acquire(origin("a.test"), WebRequestKind.PAGE)
        assertFailsWith<OriginCapacityExceededException> { full.observeRobotsDelay(origin("b.test"), Duration.ofSeconds(1)) }
        active.release()
        full.observeRobotsDelay(origin("b.test"), Duration.ofSeconds(1))
    }

    @Test
    fun `no-op pacing performs no waiting and ignores hints`() = runTest {
        val start = currentTime
        val reservation = NoOriginPacingPolicy.acquire(origin("a.test"), WebRequestKind.PAGE)
        reservation.release()
        reservation.release()
        NoOriginPacingPolicy.observe(origin("a.test"), response(retryAfter = "100"))
        NoOriginPacingPolicy.observeRobotsDelay(origin("a.test"), Duration.ofHours(1))
        NoOriginPacingPolicy.acquire(origin("a.test"), WebRequestKind.PAGE).release()
        assertEquals(start, currentTime)
        assertEquals("no-origin-pacing", NoOriginPacingPolicy.id)
    }

    @Test
    fun `bound exceptions carry the host origin and limits in their messages`() {
        assertEquals(
            "DNS answer for 'wide.test' exceeds configured address bound of 16",
            NetworkAddressLimitExceededException("wide.test", 16).message,
        )
        val pacing = PacingWaitExceededException(origin("a.test"), Duration.ofSeconds(9), Duration.ofSeconds(1)).message.orEmpty()
        assertContains(pacing, "a.test")
        assertContains(pacing, "PT9S")
        assertContains(pacing, "PT1S")
    }

    private fun origin(host: String): WebOrigin = WebOrigin("https", host, 443)

    private fun response(retryAfter: String?): CrawlResponse = CrawlResponse(
        request = CrawlRequest("https://known.test/"),
        statusCode = 429,
        headers = retryAfter?.let { Headers.of(Header("Retry-After", it)) } ?: Headers.empty(),
        body = ResponseBody.empty(),
        receivedAt = Instant.EPOCH,
    )

    private class MutableClock(initial: Instant) : Clock {
        @Volatile
        private var current: Instant = initial
        override fun now(): Instant = current
    }
}
