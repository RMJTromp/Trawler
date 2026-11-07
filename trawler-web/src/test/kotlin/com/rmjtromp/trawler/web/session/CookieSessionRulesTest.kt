package com.rmjtromp.trawler.web.session

import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.policy.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

/** RFC 6265 domain, path, secure, and expiry rules for the process-local jar. */
class CookieSessionRulesTest {
    @Test
    fun `domain attribute must domain-match on a label boundary and never widen to unrelated hosts`() = runTest {
        val session = InMemoryCookieSession(clock = MutableClock(Instant.EPOCH))
        session.observe(
            "https://www.example.test/",
            listOf(
                "exact=1; Domain=www.example.test",
                "parent=2; Domain=EXAMPLE.test",
                "partial=3; Domain=ample.test",
                "sibling=4; Domain=api.example.test",
                "blank=5; Domain=",
                "dot=6; Domain=.",
            ),
        )
        assertEquals("exact=1; parent=2", cookieFor(session, "https://www.example.test/"))
        assertEquals("exact=1; parent=2", cookieFor(session, "https://deep.www.example.test/"))
        assertEquals("parent=2", cookieFor(session, "https://api.example.test/"))
        assertEquals(null, cookieFor(session, "https://ample.test/"))
        assertEquals(null, cookieFor(session, "https://notexample.test/"))
        assertEquals(null, cookieFor(session, "https://example.test.evil/"))
    }

    @Test
    fun `host-only cookies are not sent to subdomains`() = runTest {
        val session = InMemoryCookieSession(clock = MutableClock(Instant.EPOCH))
        session.observe("https://example.test/", listOf("host=1"))
        assertEquals("host=1", cookieFor(session, "https://EXAMPLE.test/"))
        assertEquals(null, cookieFor(session, "https://sub.example.test/"))
    }

    @Test
    fun `IP address hosts only accept identical domains and never suffix-match other addresses`() = runTest {
        val session = InMemoryCookieSession(clock = MutableClock(Instant.EPOCH))
        session.observe(
            "http://10.0.0.1/",
            listOf("suffix=1; Domain=0.0.1", "same=2; Domain=10.0.0.1"),
        )
        assertEquals("same=2", cookieFor(session, "http://10.0.0.1/"))
        assertEquals(null, cookieFor(session, "http://192.10.0.0.1/"))
        assertEquals(null, cookieFor(session, "http://110.0.0.1/"))

        val other = InMemoryCookieSession(clock = MutableClock(Instant.EPOCH))
        other.observe("http://10.0.0.1/", listOf("suffix=1; Domain=0.0.1"))
        assertEquals(null, cookieFor(other, "http://20.0.0.1/"))
        assertEquals(null, cookieFor(other, "http://10.0.0.1/"))
    }

    @Test
    fun `IPv6 literal hosts accept only host-only or identical domain cookies`() = runTest {
        val session = InMemoryCookieSession(clock = MutableClock(Instant.EPOCH))
        session.observe(
            "http://[2001:db8::1]/",
            listOf("hostonly=1", "same=2; Domain=[2001:db8::1]", "suffix=3; Domain=db8::1]"),
        )
        assertEquals("hostonly=1; same=2", cookieFor(session, "http://[2001:db8::1]/"))
        assertEquals(null, cookieFor(session, "http://[2001:db8::2]/"))
    }

    @Test
    fun `path attribute and default path follow RFC prefix rules`() = runTest {
        val session = InMemoryCookieSession(clock = MutableClock(Instant.EPOCH))
        session.observe(
            "https://example.test/docs/guide/page",
            listOf(
                "defaulted=1",
                "slashed=2; Path=/docs/",
                "relative=3; Path=docs",
                "exact=4; Path=/docs/guide/page",
            ),
        )
        // A Path attribute without a leading slash is ignored, so "relative" uses the default path.
        assertEquals("exact=4; defaulted=1; relative=3; slashed=2", cookieFor(session, "https://example.test/docs/guide/page"))
        assertEquals("defaulted=1; relative=3; slashed=2", cookieFor(session, "https://example.test/docs/guide"))
        assertEquals("defaulted=1; relative=3; slashed=2", cookieFor(session, "https://example.test/docs/guide/other"))
        assertEquals("slashed=2", cookieFor(session, "https://example.test/docs/"))
        assertEquals(null, cookieFor(session, "https://example.test/docs"))
        assertEquals(null, cookieFor(session, "https://example.test/docs-extra/guide"))
        assertEquals(null, cookieFor(session, "https://example.test/guide/page"))

        val rootDefaults = InMemoryCookieSession(clock = MutableClock(Instant.EPOCH))
        rootDefaults.observe("https://example.test/file", listOf("fromfile=1"))
        rootDefaults.observe("https://example.test", listOf("fromempty=2"))
        assertEquals("fromfile=1; fromempty=2", cookieFor(rootDefaults, "https://example.test/any/where"))
    }

    @Test
    fun `secure cookies are never sent over plain HTTP`() = runTest {
        val session = InMemoryCookieSession(clock = MutableClock(Instant.EPOCH))
        session.observe("https://example.test/", listOf("token=1; Secure; HttpOnly; SameSite=Strict", "plain=2; secure"))
        assertEquals("token=1; plain=2", cookieFor(session, "HTTPS://example.test/"))
        assertEquals(null, cookieFor(session, "http://example.test/"))
        assertEquals(null, cookieFor(session, "ws://example.test/"))
    }

    @Test
    fun `expires bounds lifetime and invalid dates fall back to the configured maximum`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val session = InMemoryCookieSession(CookieSessionConfig(maxCookieLifetime = Duration.ofSeconds(100)), clock)
        session.observe(
            "https://example.test/",
            listOf(
                "dated=1; Expires=${httpDate(Instant.EPOCH.plusSeconds(10))}",
                "capped=2; Expires=${httpDate(Instant.EPOCH.plusSeconds(1_000))}",
                "invalid=3; Expires=Thu, 01-Jan-1970 00:00:10 GMT",
                "session=4",
            ),
        )
        clock.set(Instant.EPOCH.plusSeconds(9))
        assertEquals("dated=1; capped=2; invalid=3; session=4", cookieFor(session, "https://example.test/"))
        clock.set(Instant.EPOCH.plusSeconds(10))
        assertEquals("capped=2; invalid=3; session=4", cookieFor(session, "https://example.test/"))
        clock.set(Instant.EPOCH.plusSeconds(99))
        assertEquals("capped=2; invalid=3; session=4", cookieFor(session, "https://example.test/"))
        clock.set(Instant.EPOCH.plusSeconds(100))
        assertEquals(null, cookieFor(session, "https://example.test/"))
    }

    @Test
    fun `max-age takes precedence over expires regardless of attribute order`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val session = InMemoryCookieSession(CookieSessionConfig(maxCookieLifetime = Duration.ofSeconds(1_000)), clock)
        val farFuture = httpDate(Instant.EPOCH.plusSeconds(900))
        val past = httpDate(Instant.EPOCH.minusSeconds(10))
        session.observe(
            "https://example.test/",
            listOf(
                "ageFirst=1; Max-Age=5; Expires=$farFuture",
                "expiresFirst=2; Expires=$farFuture; Max-Age=5",
                "keptDespitePast=3; Max-Age=50; Expires=$past",
                "huge=4; Max-Age=999999999999",
                "lastAgeWins=5; Max-Age=0; Max-Age=50",
            ),
        )
        assertEquals(
            "ageFirst=1; expiresFirst=2; keptDespitePast=3; huge=4; lastAgeWins=5",
            cookieFor(session, "https://example.test/"),
        )
        clock.set(Instant.EPOCH.plusSeconds(5))
        assertEquals("keptDespitePast=3; huge=4; lastAgeWins=5", cookieFor(session, "https://example.test/"))
        clock.set(Instant.EPOCH.plusSeconds(50))
        assertEquals("huge=4", cookieFor(session, "https://example.test/"))
        clock.set(Instant.EPOCH.plusSeconds(1_000))
        assertEquals(null, cookieFor(session, "https://example.test/"))
    }

    @Test
    fun `deletion removes only the cookie with the same domain path and name`() = runTest {
        val session = InMemoryCookieSession(clock = MutableClock(Instant.EPOCH))
        session.observe("https://example.test/", listOf("id=root; Path=/", "id=docs; Path=/docs", "other=1; Path=/"))
        session.observe("https://example.test/", listOf("id=gone; Path=/docs; Max-Age=-1"))
        assertEquals("id=root; other=1", cookieFor(session, "https://example.test/docs/page"))
        session.observe("https://example.test/", listOf("id=gone; Expires=${httpDate(Instant.EPOCH)}"))
        assertEquals("other=1", cookieFor(session, "https://example.test/docs/page"))
    }

    @Test
    fun `cookie names must be RFC tokens and empty names are rejected`() = runTest {
        val session = InMemoryCookieSession(clock = MutableClock(Instant.EPOCH))
        session.observe(
            "https://example.test/",
            listOf(
                "=no-name",
                "no-separator",
                " =blank",
                "tab\tname=1",
                "café=1",
                "(paren)=1",
                "semi:colon=1",
                "ctl\u0001=1",
                "ok_name-1.2=value=with=equals",
                "empty-value=",
            ),
        )
        assertEquals("ok_name-1.2=value=with=equals; empty-value=", cookieFor(session, "https://example.test/"))
    }

    @Test
    fun `one response cannot process more Set-Cookie values than the jar bound`() = runTest {
        val session = InMemoryCookieSession(CookieSessionConfig(maxCookies = 2), MutableClock(Instant.EPOCH))
        session.observe("https://example.test/", listOf("a=1", "b=2", "c=3"))
        assertEquals("a=1; b=2", cookieFor(session, "https://example.test/"))
    }

    @Test
    fun `cookie header bound is inclusive and oversized Set-Cookie lines are ignored`() = runTest {
        val session = InMemoryCookieSession(
            CookieSessionConfig(maxCookieHeaderLength = "a=1; b=2".length),
            MutableClock(Instant.EPOCH),
        )
        session.observe("https://example.test/", listOf("a=1", "b=2", "c=" + "x".repeat(20)))
        assertEquals("a=1; b=2", cookieFor(session, "https://example.test/"))
    }

    @Test
    fun `hostless targets neither receive nor store cookies`() = runTest {
        val session = InMemoryCookieSession(clock = MutableClock(Instant.EPOCH))
        val headers = Headers.empty()
        session.observe("mailto:someone@example.test", listOf("a=1"))
        assertSame(headers, session.apply("mailto:someone@example.test", headers))
        session.observe("https://example.test/", emptyList())
        assertEquals(null, cookieFor(session, "https://example.test/"))
        assertEquals("memory-cookie-session", session.id)
        assertEquals("no-cookie-session", NoCookieSession.id)
    }

    @Test
    fun `cookies with equal path length keep least recently sent order`() = runTest {
        val session = InMemoryCookieSession(clock = MutableClock(Instant.EPOCH))
        session.observe("https://example.test/", listOf("first=1", "second=2"))
        assertEquals("first=1; second=2", cookieFor(session, "https://example.test/"))
        session.observe("https://example.test/", listOf("first=updated"))
        assertEquals("second=2; first=updated", cookieFor(session, "https://example.test/"))
        assertTrue(session.apply("https://example.test/", Headers.empty())["Cookie"].single().startsWith("second=2"))
    }

    private suspend fun cookieFor(session: CookieSession, target: String): String? =
        session.apply(target, Headers.empty())["Cookie"].singleOrNull()

    private fun httpDate(instant: Instant): String =
        DateTimeFormatter.RFC_1123_DATE_TIME.format(instant.atOffset(ZoneOffset.UTC))

    private class MutableClock(initial: Instant) : Clock {
        @Volatile
        private var current: Instant = initial
        override fun now(): Instant = current
        fun set(instant: Instant) {
            current = instant
        }
    }
}
