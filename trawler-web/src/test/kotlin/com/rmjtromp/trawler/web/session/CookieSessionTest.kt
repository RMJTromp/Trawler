package com.rmjtromp.trawler.web.session

import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.web.policy.CookieHeaderLimitException
import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class CookieSessionTest {
    @Test
    fun `cookie configuration validates every positive bound`() {
        assertFailsWith<IllegalArgumentException> { CookieSessionConfig(maxCookies = 0) }
        assertFailsWith<IllegalArgumentException> { CookieSessionConfig(maxCookieValueLength = 0) }
        assertFailsWith<IllegalArgumentException> { CookieSessionConfig(maxCookieHeaderLength = 0) }
        assertFailsWith<IllegalArgumentException> { CookieSessionConfig(maxCookieLifetime = Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { CookieSessionConfig(maxCookieLifetime = Duration.ofSeconds(-1)) }
    }

    @Test
    fun `cookies honor host domain path secure and caller precedence`() = runTest {
        val session = InMemoryCookieSession(clock = MutableClock(Instant.EPOCH))
        session.observe(
            "https://example.test/account/login",
            listOf(
                "host=one; Path=/account",
                "wide=two; Domain=.example.test; Path=/",
                "secure=three; Secure; Path=/",
            ),
        )

        assertEquals(
            "host=one; wide=two; secure=three",
            session.apply("https://example.test/account/view", Headers.empty())["Cookie"].single(),
        )
        assertEquals(
            "host=one; wide=two",
            session.apply("http://example.test/account/view", Headers.empty())["Cookie"].single(),
        )
        assertEquals(
            "wide=two",
            session.apply("https://sub.example.test/account/view", Headers.empty())["Cookie"].single(),
        )
        val boundarySession = InMemoryCookieSession()
        boundarySession.observe(
            "https://example.test/account/login",
            listOf("host=one; Path=/account", "wide=two; Domain=.example.test; Path=/", "secure=three; Secure; Path=/"),
        )
        assertEquals(
            "wide=two; secure=three",
            boundarySession.apply("https://example.test/accountant", Headers.empty())["Cookie"].single(),
        )
        assertEquals(
            listOf("caller=value"),
            session.apply(
                "https://example.test/account/view",
                Headers.of(Header("Cookie", "caller=value")),
            )["Cookie"],
        )
        assertEquals(
            listOf("caller=value"),
            session.apply(
                "https://example.test/account/view",
                Headers.of(Header("cOoKiE", "caller=value")),
            )["Cookie"],
        )
    }

    @Test
    fun `cookie expiry max-age overwrite delete and invalid input are bounded`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val session = InMemoryCookieSession(
            CookieSessionConfig(maxCookieLifetime = Duration.ofSeconds(30), maxCookieValueLength = 5),
            clock,
        )
        session.observe("https://example.test/path/login", listOf("name=old; Path=/path"))
        session.observe("https://example.test/path/login", listOf("name=new; Path=/path"))
        assertEquals("name=new", session.apply("https://example.test/path", Headers.empty())["Cookie"].single())

        session.observe("https://example.test/path/login", listOf("short=value; Max-Age=1"))
        assertEquals("name=new; short=value", session.apply("https://example.test/path", Headers.empty())["Cookie"].single())
        clock.advance(Duration.ofSeconds(1))
        assertEquals("name=new", session.apply("https://example.test/path", Headers.empty())["Cookie"].single())

        session.observe("https://example.test/path/login", listOf("name=x; Path=/path; Max-Age=0"))
        assertTrue(session.apply("https://example.test/path", Headers.empty())["Cookie"].isEmpty())

        session.observe(
            "https://example.test/path/login",
            listOf(
                "past=value; Expires=Thu, 01 Jan 1970 00:00:00 GMT",
                "foreign=value; Domain=other.test",
                "bad name=value",
                "too-long=123456",
                "bad-age=value; Max-Age=not-a-number",
            ),
        )
        assertTrue(session.apply("https://example.test/path", Headers.empty())["Cookie"].isEmpty())
    }

    @Test
    fun `cookie eviction is oldest first and ordering prefers longer paths`() = runTest {
        val session = InMemoryCookieSession(CookieSessionConfig(maxCookies = 2))
        session.observe("https://example.test/", listOf("first=1", "second=2"))
        session.observe("https://example.test/", listOf("third=3"))
        assertEquals("second=2; third=3", session.apply("https://example.test/", Headers.empty())["Cookie"].single())

        val ordered = InMemoryCookieSession()
        ordered.observe("https://example.test/account/login", listOf("root=1; Path=/", "account=2; Path=/account"))
        assertEquals(
            "account=2; root=1",
            ordered.apply("https://example.test/account/view", Headers.empty())["Cookie"].single(),
        )
    }

    @Test
    fun `cookie header and value bounds reject emitted headers but ignore oversized input`() = runTest {
        val session = InMemoryCookieSession(
            CookieSessionConfig(maxCookieValueLength = 4, maxCookieHeaderLength = 10),
        )
        session.observe("https://example.test/", listOf("a=1234", "b=1234"))
        val error = assertFailsWith<CookieHeaderLimitException> {
            session.apply("https://example.test/", Headers.empty())
        }
        assertEquals(10, error.limit)

        session.observe("https://example.test/", listOf("too-big=12345", "also-too-big=12345"))
        assertFailsWith<CookieHeaderLimitException> {
            session.apply("https://example.test/", Headers.empty())
        }

        val caller = session.apply(
            "https://example.test/",
            Headers.of(Header("Cookie", "caller".repeat(100))),
        )
        assertEquals(1, caller["Cookie"].size)
    }

    @Test
    fun `cookie observation and clear are safe under concurrent updates`() = runTest {
        val session = InMemoryCookieSession()
        val values = (1..20).map { value ->
            async {
                session.observe("https://example.test/", listOf("cookie$value=$value"))
                session.apply("https://example.test/", Headers.empty())
            }
        }.awaitAll()
        assertTrue(values.all { it["Cookie"].isNotEmpty() })
        session.clear()
        assertTrue(session.apply("https://example.test/", Headers.empty())["Cookie"].isEmpty())
        assertEquals(Headers.empty(), NoCookieSession.apply("https://example.test/", Headers.empty()))
        NoCookieSession.observe("https://example.test/", listOf("ignored=value"))
    }

    private class MutableClock(initial: Instant) : Clock {
        private var current = initial
        override fun now(): Instant = current
        fun advance(duration: Duration) {
            current = current.plus(duration)
        }
    }
}
