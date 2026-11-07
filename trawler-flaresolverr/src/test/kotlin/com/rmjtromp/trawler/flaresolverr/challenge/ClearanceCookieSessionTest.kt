package com.rmjtromp.trawler.flaresolverr.challenge

import com.rmjtromp.trawler.flaresolverr.BROWSER_UA
import com.rmjtromp.trawler.flaresolverr.MutableClock
import com.rmjtromp.trawler.flaresolverr.SITE
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.web.policy.AllowAllWebDestinationPolicy
import com.rmjtromp.trawler.web.policy.CookieHeaderLimitException
import com.rmjtromp.trawler.web.policy.DefaultHeaderRedactionPolicy
import com.rmjtromp.trawler.web.policy.DefaultHttpFailureClassifier
import com.rmjtromp.trawler.web.policy.DefaultRedirectHeaderPolicy
import com.rmjtromp.trawler.web.policy.DefaultRetryAfterPolicy
import com.rmjtromp.trawler.web.policy.NoRobotsPolicy
import com.rmjtromp.trawler.web.session.CookieSession
import com.rmjtromp.trawler.web.session.InMemoryCookieSession
import com.rmjtromp.trawler.web.session.NoCookieSession
import com.rmjtromp.trawler.web.transport.KtorTransport
import com.rmjtromp.trawler.web.transport.KtorTransportConfig
import com.rmjtromp.trawler.web.transport.NoOriginConcurrencyPolicy
import com.rmjtromp.trawler.web.transport.NoOriginPacingPolicy
import com.rmjtromp.trawler.web.transport.WebOrigin
import com.rmjtromp.trawler.web.transport.WebTransportPolicies
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.request.HttpRequestData
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import kotlinx.coroutines.runBlocking
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class ClearanceCookieSessionTest {
    private val clock = MutableClock()
    private val origin = WebOrigin.fromTarget(SITE)
    private val store = ClearanceStore(clock = clock)
    private val coordinator = ChallengeClearance(
        ScriptedResolver { error("not used") },
        CloudflareChallengeDetector(),
        store,
    )
    private val jar = InMemoryCookieSession(clock = clock)
    private val session = coordinator.cookieSession(jar)

    private fun storeClearance(origin: WebOrigin = this.origin, vararg cookies: ClearanceCookie) =
        store.store(Clearance(origin, cookies.toList(), BROWSER_UA, clock.now()))

    private fun pairs(headers: Headers): Map<String, String> {
        val cookie = headers["Cookie"]
        assertEquals(1, cookie.size, "exactly one Cookie header")
        return cookie.single().split("; ").associate { it.substringBefore('=') to it.substringAfter('=') }
    }

    @Test
    fun `identifier composes the delegate identifier`() {
        assertEquals("flaresolverr-clearance+memory-cookie-session", session.id)
    }

    @Test
    fun `clearance cookies merge with jar cookies and replace same-name jar values`(): Unit = runBlocking {
        session.observe("$SITE/", listOf("jar=1; Path=/", "cf_clearance=stale-jar-value; Path=/"))
        storeClearance(
            origin,
            ClearanceCookie("cf_clearance", "fresh"),
            ClearanceCookie("__cf_bm", "bm", path = "/app"),
            ClearanceCookie("secure_only", "s", secure = true),
        )
        val merged = pairs(session.apply("$SITE/app/page", Headers.of(Header("Accept", "*/*"))))
        assertEquals(mapOf("jar" to "1", "cf_clearance" to "fresh", "__cf_bm" to "bm", "secure_only" to "s"), merged)
        val root = pairs(session.apply("$SITE/", Headers.empty()))
        assertEquals(mapOf("jar" to "1", "cf_clearance" to "fresh", "secure_only" to "s"), root)
        val noPath = pairs(session.apply(SITE, Headers.empty()))
        assertEquals("fresh", noPath["cf_clearance"])
    }

    @Test
    fun `clearance applies only to its exact origin and honors secure and expiry`(): Unit = runBlocking {
        storeClearance(origin, ClearanceCookie("cf_clearance", "fresh", expiresAt = clock.now().plusSeconds(10)))
        listOf("https://www.example.test/", "http://example.test/", "https://example.test:8443/", "https://other.test/").forEach {
            assertTrue(session.apply(it, Headers.empty())["Cookie"].isEmpty(), it)
        }
        val plain = WebOrigin.fromTarget("http://plain.test/")
        storeClearance(plain, ClearanceCookie("secure_only", "s", secure = true))
        assertTrue(session.apply("http://plain.test/", Headers.empty())["Cookie"].isEmpty())
        clock.advance(Duration.ofSeconds(10))
        assertTrue(session.apply("$SITE/", Headers.empty())["Cookie"].isEmpty())
    }

    @Test
    fun `a caller supplied Cookie header is honored and receives no clearance`(): Unit = runBlocking {
        storeClearance(origin, ClearanceCookie("cf_clearance", "fresh"))
        val headers = Headers.of(Header("cookie", "mine=1"))
        val result = session.apply("$SITE/", headers)
        assertEquals(listOf("mine=1"), result["Cookie"])
    }

    @Test
    fun `targets without clearance or a web origin get only the delegate result`(): Unit = runBlocking {
        session.observe("$SITE/", listOf("jar=1; Path=/"))
        assertEquals(mapOf("jar" to "1"), pairs(session.apply("$SITE/", Headers.empty())))
        val delegateOnly = coordinator.cookieSession(NoCookieSession)
        val headers = Headers.of(Header("Accept", "x"))
        assertSame(headers, delegateOnly.apply("custom-scheme:thing", headers))
        storeClearance(origin, ClearanceCookie("cf_clearance", "fresh"))
        assertEquals(mapOf("cf_clearance" to "fresh"), pairs(delegateOnly.apply("$SITE/", headers)))
    }

    @Test
    fun `observe is forwarded to the delegate and never writes clearance into the jar`(): Unit = runBlocking {
        val observed = CopyOnWriteArrayList<Pair<String, List<String>>>()
        val delegate = object : CookieSession {
            override val id: String = "recording"
            override suspend fun apply(target: String, headers: Headers): Headers = headers
            override suspend fun observe(target: String, setCookieHeaders: List<String>) {
                observed += target to setCookieHeaders
            }
        }
        val decorated = coordinator.cookieSession(delegate)
        decorated.observe("$SITE/", listOf("a=1"))
        assertEquals(listOf("$SITE/" to listOf("a=1")), observed)
        storeClearance(origin, ClearanceCookie("cf_clearance", "fresh"))
        decorated.apply("$SITE/", Headers.empty())
        assertEquals(1, observed.size)
    }

    @Test
    fun `empty segments in a delegate Cookie header are dropped while merging`(): Unit = runBlocking {
        val delegate = object : CookieSession {
            override val id: String = "sloppy"
            override suspend fun apply(target: String, headers: Headers): Headers =
                headers.plus(Header("Cookie", "jar=1; ;  ;cf_clearance=old"))
            override suspend fun observe(target: String, setCookieHeaders: List<String>) = Unit
        }
        storeClearance(origin, ClearanceCookie("cf_clearance", "fresh"))
        val merged = coordinator.cookieSession(delegate).apply("$SITE/", Headers.empty())
        assertEquals(listOf("jar=1; cf_clearance=fresh"), merged["Cookie"])
    }

    @Test
    fun `the merged header is bounded`(): Unit = runBlocking {
        val small = ClearanceStore(ClearanceStoreConfig(maxCookieHeaderLength = 20), clock)
        val bounded = ChallengeClearance(ScriptedResolver { error("unused") }, CloudflareChallengeDetector(), small)
            .cookieSession(NoCookieSession)
        small.store(Clearance(origin, listOf(ClearanceCookie("cf_clearance", "1234567")), BROWSER_UA, clock.now()))
        assertEquals(listOf("cf_clearance=1234567"), bounded.apply("$SITE/", Headers.empty())["Cookie"])
        small.store(Clearance(origin, listOf(ClearanceCookie("cf_clearance", "12345678")), BROWSER_UA, clock.now()))
        assertEquals(20, assertFailsWith<CookieHeaderLimitException> { bounded.apply("$SITE/", Headers.empty()) }.limit)
    }

    @Test
    fun `KtorTransport applies clearance per hop and never forwards it across origins`(): Unit = runBlocking {
        storeClearance(origin, ClearanceCookie("cf_clearance", "fresh"))
        val seen = CopyOnWriteArrayList<HttpRequestData>()
        val engine = MockEngine { request ->
            seen += request
            when (request.url.toString()) {
                "$SITE/start" -> respond("", HttpStatusCode.Found, headersOf(HttpHeaders.Location, "$SITE/same-origin"))
                "$SITE/same-origin" -> respond("", HttpStatusCode.Found, headersOf(HttpHeaders.Location, "https://other.test/landing"))
                else -> respond("landed", HttpStatusCode.OK)
            }
        }
        val transport = KtorTransport.fromClient(
            client = HttpClient(engine),
            configuration = KtorTransportConfig(),
            policies = WebTransportPolicies(
                destination = AllowAllWebDestinationPolicy,
                robots = NoRobotsPolicy,
                concurrency = NoOriginConcurrencyPolicy,
                pacing = NoOriginPacingPolicy,
                cookies = coordinator.cookieSession(InMemoryCookieSession(clock = clock)),
                failureClassifier = DefaultHttpFailureClassifier,
                retryAfter = DefaultRetryAfterPolicy,
                redirectHeaders = DefaultRedirectHeaderPolicy(),
                headerRedaction = DefaultHeaderRedactionPolicy(),
            ),
            closeClient = true,
        )
        transport.use { it.execute(CrawlRequest("$SITE/start")) }
        assertEquals(3, seen.size)
        assertEquals(listOf("cf_clearance=fresh"), seen[0].headers.getAll(HttpHeaders.Cookie))
        assertEquals(listOf("cf_clearance=fresh"), seen[1].headers.getAll(HttpHeaders.Cookie))
        assertEquals(null, seen[2].headers.getAll(HttpHeaders.Cookie))
    }
}
