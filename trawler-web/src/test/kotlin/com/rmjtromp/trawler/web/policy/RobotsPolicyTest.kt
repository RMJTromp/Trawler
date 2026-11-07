package com.rmjtromp.trawler.web.policy

import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.web.transport.WebOrigin
import java.io.IOException
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

@OptIn(ExperimentalCoroutinesApi::class)
class RobotsPolicyTest {
    private val allowedWithoutDelay = RobotsDecision.Allowed(null)
    private val disallowed = RobotsDecision.Disallowed("robots-disallowed")

    @Test
    fun `directives before any user-agent form an implicit wildcard group`() = runTest {
        val policy = policyFor(
            """
            Disallow: /early
            Crawl-delay: 2
            User-agent: SomeoneElse
            Disallow: /
            """.trimIndent(),
        )
        assertEquals(disallowed, policy.evaluate("https://example.test/early/page"))
        assertEquals(RobotsDecision.Allowed(Duration.ofSeconds(2)), policy.evaluate("https://example.test/late"))
    }

    @Test
    fun `malformed lines unknown keys and empty user agents are ignored`() = runTest {
        val policy = policyFor(
            """
            Disallow /no-colon
            : /empty-key
            Sitemap: https://example.test/sitemap.xml
            Host: example.test
            User-agent:
            Disallow: /orphan
            User-agent: *
            Disallow: /blocked # trailing comment
            Disallow:
            """.trimIndent(),
        )
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test/no-colon"))
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test/empty-key"))
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test/orphan"))
        assertEquals(disallowed, policy.evaluate("https://example.test/blocked"))
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test/anything-else"))
    }

    @Test
    fun `consecutive user-agent lines share a group and the most specific agent wins`() = runTest {
        val body = """
            User-agent: *
            Disallow: /wildcard

            User-agent: bot
            Disallow: /contains

            User-agent: Other
            User-agent: TRAWLER
            Disallow: /prefix
        """.trimIndent()
        val specific = policyFor(body, userAgent = "TrawlerBot/0.1")
        assertEquals(disallowed, specific.evaluate("https://example.test/prefix"))
        assertEquals(allowedWithoutDelay, specific.evaluate("https://example.test/contains"))
        assertEquals(allowedWithoutDelay, specific.evaluate("https://example.test/wildcard"))

        val substring = policyFor(body, userAgent = "SuperBot")
        assertEquals(disallowed, substring.evaluate("https://example.test/contains"))
        assertEquals(allowedWithoutDelay, substring.evaluate("https://example.test/prefix"))

        val unmatched = policyFor(body, userAgent = "Crawler")
        assertEquals(disallowed, unmatched.evaluate("https://example.test/wildcard"))
        assertEquals(allowedWithoutDelay, unmatched.evaluate("https://example.test/contains"))
    }

    @Test
    fun `equally specific groups merge rules and keep the longest crawl delay`() = runTest {
        val policy = policyFor(
            """
            User-agent: *
            Disallow: /one
            Crawl-delay: 1

            User-agent: *
            Disallow: /two
            Crawl-delay: 3
            """.trimIndent(),
        )
        assertEquals(disallowed, policy.evaluate("https://example.test/one"))
        assertEquals(disallowed, policy.evaluate("https://example.test/two"))
        assertEquals(RobotsDecision.Allowed(Duration.ofSeconds(3)), policy.evaluate("https://example.test/three"))
    }

    @Test
    fun `documents without groups and the empty allow rule allow every path`() = runTest {
        assertEquals(allowedWithoutDelay, policyFor("# only a comment\n\n").evaluate("https://example.test/x"))
        assertEquals(allowedWithoutDelay, policyFor("").evaluate("https://example.test/x"))

        val emptyAllow = policyFor("User-agent: *\nAllow:\nDisallow: /private")
        assertEquals(allowedWithoutDelay, emptyAllow.evaluate("https://example.test/public"))
        assertEquals(disallowed, emptyAllow.evaluate("https://example.test/private"))
    }

    @Test
    fun `equally specific rules of the same kind resolve deterministically`() = runTest {
        val policy = policyFor("User-agent: *\nDisallow: /a*\nDisallow: /*a\nAllow: /b*\nAllow: /*b")
        assertEquals(disallowed, policy.evaluate("https://example.test/aa"))
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test/bb"))
    }

    @Test
    fun `wildcards anchors and regex metacharacters match literally and linearly`() = runTest {
        val policy = policyFor(
            """
            User-agent: *
            Disallow: /*.php$
            Disallow: /a*b*c
            Disallow: /x${'$'}y
            Disallow: /dot.html
            Disallow: /(group)+
            Disallow: /**double
            Disallow: /exact$
            """.trimIndent(),
        )
        assertEquals(disallowed, policy.evaluate("https://example.test/index.php"))
        assertEquals(disallowed, policy.evaluate("https://example.test/dir/index.php"))
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test/index.php?x=1"))
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test/index.phps"))
        assertEquals(disallowed, policy.evaluate("https://example.test/a-b-c-tail"))
        assertEquals(disallowed, policy.evaluate("https://example.test/abc"))
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test/acb"))
        assertEquals(disallowed, policy.evaluate("https://example.test/x\$y/z"))
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test/xy"))
        assertEquals(disallowed, policy.evaluate("https://example.test/dot.html"))
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test/dotXhtml"))
        assertEquals(disallowed, policy.evaluate("https://example.test/(group)+"))
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test/groupgroup"))
        assertEquals(disallowed, policy.evaluate("https://example.test/double"))
        assertEquals(disallowed, policy.evaluate("https://example.test/zz/double"))
        assertEquals(disallowed, policy.evaluate("https://example.test/exact"))
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test/exact/more"))
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    fun `hostile wildcard patterns cannot cause catastrophic backtracking`() = runTest {
        val hostile = "/" + "*a".repeat(40) + "*b"
        val policy = policyFor("User-agent: *\nDisallow: $hostile")
        val path = "/" + "a".repeat(4_000)
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test$path"))
        assertEquals(disallowed, policy.evaluate("https://example.test${path}b"))
    }

    @Test
    fun `paths default to root and keep raw query and percent encoding`() = runTest {
        val policy = policyFor(
            """
            User-agent: *
            Disallow: /?
            Disallow: /caf%C3%A9
            """.trimIndent(),
        )
        assertEquals(disallowed, policy.evaluate("https://example.test?q=1"))
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test"))
        assertEquals(disallowed, policy.evaluate("https://example.test/caf%C3%A9/menu"))
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test/cafe"))
    }

    @Test
    fun `crawl delays parse decimals ignore invalid values and never shorten huge values`() = runTest {
        val maximum = Duration.ofSeconds(5)
        fun config() = RobotsPolicyConfig(maxCrawlDelay = maximum, longCrawlDelayBehavior = RobotsUnavailableBehavior.DENY)
        listOf(
            "1.5" to RobotsDecision.Allowed(Duration.ofMillis(1_500)),
            "0" to RobotsDecision.Allowed(Duration.ZERO),
            "0.0001" to RobotsDecision.Allowed(Duration.ZERO),
            "5" to RobotsDecision.Allowed(maximum),
            "-1" to allowedWithoutDelay,
            "soon" to allowedWithoutDelay,
            "NaN" to allowedWithoutDelay,
            "1e999999999999" to allowedWithoutDelay,
            "5.001" to RobotsDecision.Disallowed("robots-crawl-delay-too-long"),
            "1e400" to RobotsDecision.Disallowed("robots-crawl-delay-too-long"),
            // 2^64 ms + 1 ms would truncate to 1 ms and 2^63 ms to a negative duration.
            "18446744073709551.617" to RobotsDecision.Disallowed("robots-crawl-delay-too-long"),
            "9223372036854775.808" to RobotsDecision.Disallowed("robots-crawl-delay-too-long"),
        ).forEach { (value, expected) ->
            val policy = CachingRobotsPolicy(config(), MutableClock(Instant.EPOCH))
            val fetcher = RecordingFetcher(RobotsFetchResult.Available(200, "User-agent: *\nCrawl-delay: $value", Instant.EPOCH))
            assertEquals(expected, policy.evaluate("https://example.test/", fetcher), value)
        }
    }

    @Test
    fun `non-success robots statuses are unavailable while 404 and 2xx are rule sets`() = runTest {
        listOf(199, 300, 301, 304, 400, 401, 403, 410, 429, 500, 503, 599).forEach { status ->
            val decision = CachingRobotsPolicy(clock = MutableClock(Instant.EPOCH)).evaluate(
                "https://example.test/page",
                RecordingFetcher(RobotsFetchResult.Available(status, "User-agent: *\nAllow: /", Instant.EPOCH)),
            )
            assertEquals(RobotsDecision.Disallowed("robots-unavailable:robots-status-$status"), decision, status.toString())
        }
        listOf(200, 204, 299, 404).forEach { status ->
            val decision = CachingRobotsPolicy(clock = MutableClock(Instant.EPOCH)).evaluate(
                "https://example.test/private",
                RecordingFetcher(
                    RobotsFetchResult.Available(status, if (status == 404) "" else "User-agent: *\nDisallow: /private", Instant.EPOCH),
                ),
            )
            assertEquals(if (status == 404) allowedWithoutDelay else disallowed, decision, status.toString())
        }
        val failing = CachingRobotsPolicy(
            RobotsPolicyConfig(unavailableBehavior = RobotsUnavailableBehavior.FAIL),
            MutableClock(Instant.EPOCH),
        ).evaluate("https://example.test/", RecordingFetcher(RobotsFetchResult.Available(500, "", Instant.EPOCH)))
        assertEquals(RobotsDecision.Unavailable("robots-unavailable:robots-status-500"), failing)
    }

    @Test
    fun `robots body bound counts UTF-8 bytes and is checked before the status`() = runTest {
        val twoByteChars = "éé"
        val exact = CachingRobotsPolicy(RobotsPolicyConfig(maxBodyBytes = 4), MutableClock(Instant.EPOCH))
            .evaluate("https://example.test/", RecordingFetcher(RobotsFetchResult.Available(200, twoByteChars, Instant.EPOCH)))
        assertEquals(allowedWithoutDelay, exact)
        val over = CachingRobotsPolicy(RobotsPolicyConfig(maxBodyBytes = 4), MutableClock(Instant.EPOCH))
            .evaluate("https://example.test/", RecordingFetcher(RobotsFetchResult.Available(500, "${twoByteChars}x", Instant.EPOCH)))
        assertEquals(RobotsDecision.Disallowed("robots-unavailable:robots-body-limit"), over)
    }

    @Test
    fun `fetch exceptions become cached unavailable results and expire with the TTL`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val policy = CachingRobotsPolicy(
            RobotsPolicyConfig(cacheTtl = Duration.ofSeconds(30), unavailableBehavior = RobotsUnavailableBehavior.FAIL),
            clock,
        )
        var failure: Exception? = IOException("connection reset")
        val calls = CopyOnWriteArrayList<WebOrigin>()
        val fetcher = RobotsFetcher { origin, _ ->
            calls += origin
            failure?.let { throw it }
            RobotsFetchResult.Available(200, "User-agent: *\nAllow: /", clock.now())
        }
        val expected = RobotsDecision.Unavailable("robots-unavailable:robots-fetch-failed:IOException")
        assertEquals(expected, policy.evaluate("https://example.test/", fetcher))
        clock.advance(Duration.ofSeconds(30))
        assertEquals(expected, policy.evaluate("https://example.test/other", fetcher))
        assertEquals(1, calls.size)

        failure = null
        clock.advance(Duration.ofMillis(1))
        assertEquals(allowedWithoutDelay, policy.evaluate("https://example.test/", fetcher))
        assertEquals(2, calls.size)

        val anonymous = CachingRobotsPolicy(clock = clock).evaluate(
            "https://anonymous.test/",
            RobotsFetcher { _, _ -> throw object : RuntimeException("anonymous") {} },
        )
        assertEquals(RobotsDecision.Disallowed("robots-unavailable:robots-fetch-failed:failure"), anonymous)
    }

    @Test
    fun `cache is keyed by origin evicts the oldest insertion and passes the configured agent`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val fetcher = RecordingFetcher(RobotsFetchResult.Available(200, "User-agent: *\nAllow: /", Instant.EPOCH))
        val policy = CachingRobotsPolicy(RobotsPolicyConfig(userAgent = "AgentUnderTest/2", maxEntries = 2), clock)

        policy.evaluate("https://a.test/1", fetcher)
        policy.evaluate("http://a.test/1", fetcher)
        policy.evaluate("https://A.TEST:443/2", fetcher)
        assertEquals(
            listOf(WebOrigin("https", "a.test", 443), WebOrigin("http", "a.test", 80)),
            fetcher.calls.map { it.first },
        )
        assertTrue(fetcher.calls.all { it.second == "AgentUnderTest/2" })

        policy.evaluate("https://b.test/", fetcher)
        assertEquals(3, fetcher.calls.size)
        policy.evaluate("http://a.test/again", fetcher)
        assertEquals(3, fetcher.calls.size, "the newer origin remains cached")
        policy.evaluate("https://a.test/again", fetcher)
        assertEquals(4, fetcher.calls.size, "the oldest origin was evicted")
    }

    @Test
    fun `concurrent misses for one origin refresh a full cache without evicting another origin`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val gate = CompletableDeferred<Unit>()
        val calls = CopyOnWriteArrayList<WebOrigin>()
        val fetcher = RobotsFetcher { origin, _ ->
            calls += origin
            gate.await()
            RobotsFetchResult.Available(200, "User-agent: *\nDisallow: /private", clock.now())
        }
        val policy = CachingRobotsPolicy(RobotsPolicyConfig(maxEntries = 1), clock)
        val first = async(start = CoroutineStart.UNDISPATCHED) { policy.evaluate("https://same.test/private", fetcher) }
        val second = async(start = CoroutineStart.UNDISPATCHED) { policy.evaluate("https://same.test/public", fetcher) }
        runCurrent()
        assertEquals(2, calls.size, "both misses fetch because the cache lock is not held across network work")
        gate.complete(Unit)
        assertEquals(disallowed, first.await())
        assertEquals(allowedWithoutDelay, second.await())
        assertEquals(disallowed, policy.evaluate("https://same.test/private", fetcher))
        assertEquals(2, calls.size)
    }

    @Test
    fun `invalid targets are disallowed without fetching`() = runTest {
        val fetcher = RecordingFetcher(RobotsFetchResult.Available(200, "", Instant.EPOCH))
        val policy = CachingRobotsPolicy(clock = MutableClock(Instant.EPOCH))
        assertEquals("cached-robots-policy", policy.id)
        listOf(
            "https://exa mple.test/" to "target-is-not-a-uri",
            "https://example.test/%zz" to "target-is-not-a-uri",
            "mailto:someone@example.test" to "target-origin-is-invalid",
            "ftp://example.test/file" to "target-origin-is-invalid",
            "/relative" to "target-origin-is-invalid",
        ).forEach { (target, reason) ->
            assertEquals(RobotsDecision.Disallowed(reason), policy.evaluate(target, fetcher), target)
        }
        assertTrue(fetcher.calls.isEmpty())
    }

    @Test
    fun `explicit opt-out and decision validation`() = runTest {
        val fetcher = RecordingFetcher(RobotsFetchResult.Unavailable("never"))
        assertEquals("no-robots-policy", NoRobotsPolicy.id)
        assertEquals(allowedWithoutDelay, NoRobotsPolicy.evaluate("https://example.test/", fetcher))
        assertTrue(fetcher.calls.isEmpty())

        assertFailsWith<IllegalArgumentException> { RobotsDecision.Allowed(Duration.ofMillis(-1)) }
        assertEquals(Duration.ZERO, RobotsDecision.Allowed(Duration.ZERO).crawlDelay)
        assertFailsWith<IllegalArgumentException> { RobotsPolicyConfig(cacheTtl = Duration.ofSeconds(-1)) }
        assertFailsWith<IllegalArgumentException> { RobotsPolicyConfig(maxCrawlDelay = Duration.ofSeconds(-1)) }
        assertEquals("network", RobotsFetchResult.Unavailable("network").reason)
        assertEquals("robots", RobotsUnavailableException("robots").message)
    }

    private fun policyFor(body: String, userAgent: String = "TrawlerBot/0.1"): BoundPolicy =
        BoundPolicy(
            CachingRobotsPolicy(RobotsPolicyConfig(userAgent = userAgent), MutableClock(Instant.EPOCH)),
            RecordingFetcher(RobotsFetchResult.Available(200, body, Instant.EPOCH)),
        )

    private class BoundPolicy(private val policy: CachingRobotsPolicy, private val fetcher: RecordingFetcher) {
        suspend fun evaluate(target: String): RobotsDecision = policy.evaluate(target, fetcher)
    }

    private class RecordingFetcher(private val result: RobotsFetchResult) : RobotsFetcher {
        val calls = CopyOnWriteArrayList<Pair<WebOrigin, String>>()

        override suspend fun fetch(origin: WebOrigin, userAgent: String): RobotsFetchResult {
            calls += origin to userAgent
            return result
        }
    }

    private class MutableClock(initial: Instant) : Clock {
        @Volatile
        private var current: Instant = initial
        override fun now(): Instant = current
        fun advance(duration: Duration) {
            current = current.plus(duration)
        }
    }
}
