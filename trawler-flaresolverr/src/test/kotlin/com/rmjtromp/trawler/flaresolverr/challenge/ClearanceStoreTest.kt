package com.rmjtromp.trawler.flaresolverr.challenge

import com.rmjtromp.trawler.flaresolverr.BROWSER_UA
import com.rmjtromp.trawler.flaresolverr.MutableClock
import com.rmjtromp.trawler.flaresolverr.SITE
import com.rmjtromp.trawler.web.transport.WebOrigin
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class ClearanceStoreTest {
    private val clock = MutableClock()
    private val origin = WebOrigin.fromTarget(SITE)

    private fun clearance(
        origin: WebOrigin = this.origin,
        vararg cookies: ClearanceCookie = arrayOf(ClearanceCookie("cf_clearance", "token")),
    ) = Clearance(origin, cookies.toList(), BROWSER_UA, clock.now())

    @Test
    fun `clearance cookie validates header-safe names values and paths`() {
        ClearanceCookie("cf_clearance", "abc.DEF-123_~!#$%&'*+^`|")
        ClearanceCookie("empty", "")
        listOf(
            { ClearanceCookie("", "v") },
            { ClearanceCookie("bad name", "v") },
            { ClearanceCookie("bad;name", "v") },
            { ClearanceCookie("x".repeat(1_025), "v") },
            { ClearanceCookie("n", "has space") },
            { ClearanceCookie("n", "semi;colon") },
            { ClearanceCookie("n", "com,ma") },
            { ClearanceCookie("n", "\"quoted\"") },
            { ClearanceCookie("n", "back\\slash") },
            { ClearanceCookie("n", "line\r\nInjected: yes") },
            { ClearanceCookie("n", "v", path = "relative") },
        ).forEach { assertFailsWith<IllegalArgumentException> { it() } }
    }

    @Test
    fun `clearance cookie applies by expiry secure flag and path prefix`() {
        val now = Instant.parse("2026-01-01T00:00:00Z")
        val root = ClearanceCookie("a", "1")
        assertTrue(root.appliesTo("", https = false, now))
        assertTrue(root.appliesTo("/any/path", https = false, now))
        val secure = ClearanceCookie("s", "1", secure = true)
        assertTrue(secure.appliesTo("/", https = true, now))
        assertFalse(secure.appliesTo("/", https = false, now))
        val scoped = ClearanceCookie("p", "1", path = "/app/")
        assertTrue(scoped.appliesTo("/app", https = true, now).not())
        assertTrue(scoped.appliesTo("/app/", https = true, now))
        assertTrue(scoped.appliesTo("/app/x", https = true, now))
        val noSlash = ClearanceCookie("p", "1", path = "/app")
        assertTrue(noSlash.appliesTo("/app", https = true, now))
        assertTrue(noSlash.appliesTo("/app/deeper", https = true, now))
        assertFalse(noSlash.appliesTo("/application", https = true, now))
        val expiring = ClearanceCookie("e", "1", expiresAt = now.plusSeconds(1))
        assertTrue(expiring.appliesTo("/", https = true, now))
        assertFalse(expiring.appliesTo("/", https = true, now.plusSeconds(1)))
    }

    @Test
    fun `clearance validates user agent and unique cookie keys and redacts values`() {
        val cookie = ClearanceCookie("cf_clearance", "SECRET-VALUE", path = "/", secure = true)
        val clearance = Clearance(origin, listOf(cookie, ClearanceCookie("cf_clearance", "other", path = "/x")), BROWSER_UA, clock.now())
        assertEquals(2, clearance.cookies.size)
        assertFailsWith<UnsupportedOperationException> { (clearance.cookies as MutableList<ClearanceCookie>).clear() }
        assertFalse(clearance.toString().contains("SECRET"))
        assertTrue(clearance.toString().contains("cf_clearance"))
        assertEquals("ClearanceCookie(name=cf_clearance, value=[REDACTED], path=/, secure=true)", cookie.toString())
        listOf(
            { Clearance(origin, emptyList(), " ", clock.now()) },
            { Clearance(origin, emptyList(), "agent\n", clock.now()) },
            { Clearance(origin, listOf(cookie, cookie), BROWSER_UA, clock.now()) },
        ).forEach { assertFailsWith<IllegalArgumentException> { it() } }
    }

    @Test
    fun `store config validates bounds`() {
        ClearanceStoreConfig(resolutionFailureCooldown = Duration.ZERO)
        listOf(
            { ClearanceStoreConfig(maxOrigins = 0) },
            { ClearanceStoreConfig(maxClearanceLifetime = Duration.ZERO) },
            { ClearanceStoreConfig(challengeMarkerLifetime = Duration.ZERO) },
            { ClearanceStoreConfig(resolutionFailureCooldown = Duration.ofSeconds(-1)) },
            { ClearanceStoreConfig(maxCookiesPerOrigin = 0) },
            { ClearanceStoreConfig(maxCookieHeaderLength = 0) },
        ).forEach { assertFailsWith<IllegalArgumentException> { it() } }
    }

    @Test
    fun `stored clearance expires by lifetime and is keyed by exact origin`() {
        val store = ClearanceStore(ClearanceStoreConfig(maxClearanceLifetime = Duration.ofMinutes(30)), clock)
        val stored = store.store(clearance())
        assertEquals(1, stored.generation)
        assertEquals(clock.now().plus(Duration.ofMinutes(30)), stored.expiresAt)
        assertSame(stored, store.current(origin))
        assertNull(store.current(WebOrigin.fromTarget("https://www.example.test/")))
        assertNull(store.current(WebOrigin.fromTarget("http://example.test/")))
        assertNull(store.current(WebOrigin.fromTarget("https://example.test:8443/")))
        assertFalse(stored.toString().contains("token"))
        clock.advance(Duration.ofMinutes(30).minusMillis(1))
        assertNotNull(store.current(origin))
        clock.advance(Duration.ofMillis(1))
        assertNull(store.current(origin))
        assertNull(store.current(origin))
    }

    @Test
    fun `storing drops already expired cookies bounds the cookie count and advances generations`() {
        val store = ClearanceStore(ClearanceStoreConfig(maxCookiesPerOrigin = 2), clock)
        val expired = ClearanceCookie("old", "1", expiresAt = clock.now())
        val stored = store.store(
            clearance(
                origin,
                expired,
                ClearanceCookie("a", "1"),
                ClearanceCookie("b", "2", expiresAt = clock.now().plusSeconds(60)),
                ClearanceCookie("c", "3"),
            ),
        )
        assertEquals(listOf("a", "b"), stored.clearance.cookies.map { it.name })
        val untouched = clearance(origin, ClearanceCookie("a", "1"))
        val second = store.store(untouched)
        assertSame(untouched, second.clearance)
        assertEquals(2, second.generation)
        assertEquals(3, store.store(clearance(WebOrigin.fromTarget("https://other.test/"))).generation)
    }

    @Test
    fun `least recently used origins are evicted at the origin bound`() {
        val store = ClearanceStore(ClearanceStoreConfig(maxOrigins = 2), clock)
        val a = WebOrigin.fromTarget("https://a.test/")
        val b = WebOrigin.fromTarget("https://b.test/")
        val c = WebOrigin.fromTarget("https://c.test/")
        store.store(clearance(a))
        store.store(clearance(b))
        assertNotNull(store.current(a))
        store.store(clearance(c))
        assertNotNull(store.current(a))
        assertNull(store.current(b))
        assertNotNull(store.current(c))
    }

    @Test
    fun `expired entries are evicted before live ones when the bound is reached`() {
        val store = ClearanceStore(
            ClearanceStoreConfig(maxOrigins = 2, maxClearanceLifetime = Duration.ofMinutes(1)),
            clock,
        )
        val a = WebOrigin.fromTarget("https://a.test/")
        val b = WebOrigin.fromTarget("https://b.test/")
        val c = WebOrigin.fromTarget("https://c.test/")
        store.store(clearance(a))
        clock.advance(Duration.ofSeconds(30))
        store.store(clearance(b))
        clock.advance(Duration.ofSeconds(10))
        assertNotNull(store.current(a), "touch a so plain LRU would evict b")
        clock.advance(Duration.ofSeconds(21))
        store.store(clearance(c))
        assertNull(store.current(a))
        assertNotNull(store.current(b))
        assertNotNull(store.current(c))
    }

    @Test
    fun `challenge markers expire and are cleared by a new clearance`() {
        val store = ClearanceStore(ClearanceStoreConfig(challengeMarkerLifetime = Duration.ofMinutes(10)), clock)
        assertNull(store.pendingChallenge(origin))
        store.recordChallenge(origin, ChallengeKind.CLOUDFLARE, sentGeneration = null)
        assertEquals(ChallengeKind.CLOUDFLARE, store.pendingChallenge(origin)!!.kind)
        clock.advance(Duration.ofMinutes(10))
        assertNull(store.pendingChallenge(origin))
        store.recordChallenge(origin, ChallengeKind("other"), sentGeneration = null)
        store.store(clearance())
        assertNull(store.pendingChallenge(origin))
    }

    @Test
    fun `only a challenge on the current generation invalidates the clearance`() {
        val store = ClearanceStore(clock = clock)
        val first = store.store(clearance())
        store.recordChallenge(origin, ChallengeKind.CLOUDFLARE, sentGeneration = null)
        assertSame(first, store.current(origin), "a request without clearance must not invalidate it")
        assertNull(store.pendingChallenge(origin))
        store.recordChallenge(origin, ChallengeKind.CLOUDFLARE, sentGeneration = first.generation - 1)
        assertSame(first, store.current(origin), "an older generation must not invalidate the newer clearance")
        store.recordChallenge(origin, ChallengeKind.CLOUDFLARE, sentGeneration = first.generation)
        assertNull(store.current(origin))
        assertNotNull(store.pendingChallenge(origin))
    }

    @Test
    fun `a challenge after clearance expiry marks the origin`() {
        val store = ClearanceStore(ClearanceStoreConfig(maxClearanceLifetime = Duration.ofSeconds(1)), clock)
        store.store(clearance())
        clock.advance(Duration.ofSeconds(1))
        store.recordChallenge(origin, ChallengeKind.CLOUDFLARE, sentGeneration = null)
        assertNotNull(store.pendingChallenge(origin))
    }

    @Test
    fun `resolution failure cooldown is bounded and can be disabled`() {
        val store = ClearanceStore(ClearanceStoreConfig(resolutionFailureCooldown = Duration.ofSeconds(30)), clock)
        assertFalse(store.inResolutionCooldown(origin))
        store.recordResolutionFailure(origin)
        assertTrue(store.inResolutionCooldown(origin))
        clock.advance(Duration.ofSeconds(30))
        assertFalse(store.inResolutionCooldown(origin))
        store.recordResolutionFailure(origin)
        store.store(clearance())
        assertFalse(store.inResolutionCooldown(origin))

        val disabled = ClearanceStore(ClearanceStoreConfig(resolutionFailureCooldown = Duration.ZERO), clock)
        disabled.recordResolutionFailure(origin)
        assertFalse(disabled.inResolutionCooldown(origin))
    }

    @Test
    fun `live cooldown and marker entries count toward the bound and are evicted LRU`() {
        val store = ClearanceStore(ClearanceStoreConfig(maxOrigins = 2), clock)
        val a = WebOrigin.fromTarget("https://a.test/")
        val b = WebOrigin.fromTarget("https://b.test/")
        store.recordResolutionFailure(a)
        store.recordChallenge(b, ChallengeKind.CLOUDFLARE, null)
        store.store(clearance(WebOrigin.fromTarget("https://c.test/")))
        assertFalse(store.inResolutionCooldown(a), "LRU eviction removed the oldest live entry")
        assertNotNull(store.pendingChallenge(b))
    }

    @Test
    fun `clear and clearAll remove state`() {
        val store = ClearanceStore(clock = clock)
        val other = WebOrigin.fromTarget("https://other.test/")
        store.store(clearance())
        store.store(clearance(other))
        store.clear(origin)
        assertNull(store.current(origin))
        assertNotNull(store.current(other))
        store.clearAll()
        assertNull(store.current(other))
        assertEquals(clock.now(), store.now())
        assertEquals(ClearanceStoreConfig(), ClearanceStore().configuration)
    }
}
