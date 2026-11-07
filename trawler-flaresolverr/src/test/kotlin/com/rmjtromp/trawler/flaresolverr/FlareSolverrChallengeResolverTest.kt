package com.rmjtromp.trawler.flaresolverr

import com.rmjtromp.trawler.flaresolverr.challenge.ChallengeKind
import com.rmjtromp.trawler.flaresolverr.challenge.ChallengeResolutionRequest
import com.rmjtromp.trawler.web.policy.WebPolicyRejectedException
import com.rmjtromp.trawler.web.transport.WebOrigin
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class FlareSolverrChallengeResolverTest {
    private val clock = MutableClock(Instant.parse("2026-05-06T07:08:09Z"))
    private val origin = WebOrigin.fromTarget(SITE)

    private fun resolver(
        fake: FakeFlareSolverr,
        config: FlareSolverrResolverConfig = FlareSolverrResolverConfig(serviceConfig()),
        destination: RecordingDestinationPolicy = RecordingDestinationPolicy(),
    ) = FlareSolverrChallengeResolver.createWithEngine(config, destination, fake.engine, clock)

    private fun request(target: String = "$SITE/protected?x=1") =
        ChallengeResolutionRequest(origin, target, ChallengeKind.CLOUDFLARE)

    @Test
    fun `resolves once with returnOnlyCookies and returns origin-scoped clearance`(): Unit = runBlocking {
        val fake = FakeFlareSolverr {
            replyJson(
                okRoot(
                    solution(
                        response = null,
                        cookies = cookies(
                            cookie("cf_clearance", "clear-token", domain = ".example.test", secure = true, expires = 1_900_000_000.0),
                            cookie("__cf_bm", "bm", domain = "example.test", path = "/app"),
                            cookie("third", "party", domain = "challenges.cloudflare.test"),
                            cookie("quoted", "\"q\""),
                            cookie("cf_clearance", "duplicate"),
                        ),
                    ),
                    message = "Challenge solved!",
                ),
            )
        }
        val proxy = FlareSolverrProxy("http://proxy.test:8080")
        val resolver = resolver(fake, FlareSolverrResolverConfig(serviceConfig(proxy = proxy)))
        assertEquals("flaresolverr-challenge-resolver", resolver.id)
        resolver.start(START_CONTEXT)
        val clearance = resolver.resolve(request())
        resolver.stop(STOP_CONTEXT)

        val json = fake.calls.single().json
        assertEquals("request.get", fake.calls.single().cmd)
        assertEquals("$SITE/protected?x=1", (json["url"] as JsonPrimitive).content)
        assertEquals(JsonPrimitive(true), json["returnOnlyCookies"])
        assertEquals(JsonPrimitive(5_000L), json["maxTimeout"])
        assertEquals(buildJsonObject { put("url", "http://proxy.test:8080") }, json["proxy"])
        assertNull(json["session"])

        assertEquals(origin, clearance.origin)
        assertEquals(BROWSER_UA, clearance.userAgent)
        assertEquals(clock.now(), clearance.obtainedAt)
        assertEquals(listOf("cf_clearance", "__cf_bm"), clearance.cookies.map { it.name })
        assertEquals("clear-token", clearance.cookies[0].value)
        assertTrue(clearance.cookies[0].secure)
        assertEquals(Instant.ofEpochSecond(1_900_000_000), clearance.cookies[0].expiresAt)
        assertEquals("/app", clearance.cookies[1].path)
        assertFalse(clearance.toString().contains("clear-token"))
        assertFalse(clearance.cookies.toString().contains("clear-token"))
    }

    @Test
    fun `retained cookies are bounded by maxCookies`(): Unit = runBlocking {
        val fake = FakeFlareSolverr {
            replyJson(okRoot(solution(cookies = cookies(cookie("a", "1"), cookie("b", "2"), cookie("c", "3")))))
        }
        val resolver = resolver(fake, FlareSolverrResolverConfig(serviceConfig(), maxCookies = 2))
        assertEquals(listOf("a", "b"), resolver.resolve(request()).cookies.map { it.name })
        resolver.close()
    }

    @Test
    fun `target must be HTTP on the requested origin before any call`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        val resolver = resolver(fake)
        assertEquals(
            "target-origin-mismatch",
            assertFailsWith<UnsupportedFlareSolverrRequestException> {
                resolver.resolve(request("https://other.test/"))
            }.reason,
        )
        assertEquals(
            "target-is-not-an-absolute-web-url",
            assertFailsWith<UnsupportedFlareSolverrRequestException> {
                resolver.resolve(ChallengeResolutionRequest(origin, "file:///etc/passwd", ChallengeKind.CLOUDFLARE))
            }.reason,
        )
        val short = resolver(fake, FlareSolverrResolverConfig(serviceConfig(), maxTargetLength = 10))
        assertEquals(
            "target-length-bound",
            assertFailsWith<UnsupportedFlareSolverrRequestException> { short.resolve(request()) }.reason,
        )
        assertTrue(fake.calls.isEmpty())
        resolver.close()
        short.close()
    }

    @Test
    fun `destination policy is applied to the initial and the differing final URL`(): Unit = runBlocking {
        val blockedInitial = RecordingDestinationPolicy(rejectMarker = "protected")
        val fake = FakeFlareSolverr()
        assertFailsWith<WebPolicyRejectedException> { resolver(fake, destination = blockedInitial).resolve(request()) }
        assertTrue(fake.calls.isEmpty())

        val redirecting = FakeFlareSolverr { replyJson(okRoot(solution(url = "$SITE/blocked-landing"))) }
        val blockedFinal = RecordingDestinationPolicy(rejectMarker = "blocked")
        assertFailsWith<WebPolicyRejectedException> { resolver(redirecting, destination = blockedFinal).resolve(request()) }
        assertEquals(listOf("$SITE/protected?x=1", "$SITE/blocked-landing"), blockedFinal.evaluated)
    }

    @Test
    fun `missing user agent and service failures surface as typed exceptions`(): Unit = runBlocking {
        val noUa = FakeFlareSolverr { replyJson(okRoot(solution(userAgent = null))) }
        assertEquals(
            "solution has no userAgent",
            assertFailsWith<FlareSolverrProtocolException> { resolver(noUa).resolve(request()) }.reason,
        )
        val timeout = FakeFlareSolverr {
            replyJson(errorRoot("Error: Error solving the challenge. Timeout after 5.0 seconds."), io.ktor.http.HttpStatusCode.InternalServerError)
        }
        assertFailsWith<FlareSolverrErrorException> { resolver(timeout).resolve(request()) }
        val limit = FakeFlareSolverr { replyJson(okRoot(solution(cookies = cookies(*Array(257) { cookie("c$it", "v") })))) }
        assertEquals(
            "response cookie count",
            assertFailsWith<FlareSolverrResponseLimitException> { resolver(limit).resolve(request()) }.limitKind,
        )
    }

    @Test
    fun `closed resolver rejects start and resolution`(): Unit = runBlocking {
        val fake = FakeFlareSolverr()
        val resolver = resolver(fake)
        resolver.stop(STOP_CONTEXT)
        resolver.close()
        assertFailsWith<IllegalStateException> { resolver.start(START_CONTEXT) }
        assertFailsWith<IllegalStateException> { resolver.resolve(request()) }
        assertTrue(fake.calls.isEmpty())
    }
}
