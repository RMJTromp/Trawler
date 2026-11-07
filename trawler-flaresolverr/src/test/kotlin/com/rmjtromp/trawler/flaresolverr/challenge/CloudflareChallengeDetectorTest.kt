package com.rmjtromp.trawler.flaresolverr.challenge

import com.rmjtromp.trawler.flaresolverr.SITE
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.ResponseBody
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.junit.jupiter.api.Test

class CloudflareChallengeDetectorTest {
    private val detector = CloudflareChallengeDetector()

    private fun response(status: Int?, body: String = "", vararg headers: Pair<String, String>) = CrawlResponse(
        request = CrawlRequest("$SITE/"),
        statusCode = status,
        headers = Headers.from(headers.map { Header(it.first, it.second) }),
        body = ResponseBody.from(body.toByteArray(Charsets.UTF_8)),
        receivedAt = Instant.EPOCH,
    )

    private fun detected(signal: String) = ChallengeDetection.Detected(ChallengeKind.CLOUDFLARE, signal)

    @Test
    fun `cf-mitigated challenge header is detected on 403 and 503 case-insensitively`() {
        assertEquals("cloudflare-challenge-detector", detector.id)
        assertEquals(detected("cf-mitigated-header"), detector.detect(response(403, "", "cf-mitigated" to "challenge")))
        assertEquals(detected("cf-mitigated-header"), detector.detect(response(503, "", "CF-Mitigated" to "Managed-Challenge")))
    }

    @Test
    fun `challenge page markers require Cloudflare evidence`() {
        val page = "<html><head><title>Just a moment...</title></head></html>"
        assertEquals(detected("challenge-page-marker"), detector.detect(response(503, page, "Server" to " Cloudflare ")))
        assertEquals(
            detected("challenge-page-marker"),
            detector.detect(response(403, "<script src=\"/cdn-cgi/challenge-platform/h/b\"></script>", "CF-RAY" to "8a-AMS")),
        )
        assertEquals(
            detected("challenge-page-marker"),
            detector.detect(response(403, "WINDOW._CF_CHL_OPT={}", "Server" to "cloudflare")),
        )
        assertEquals(ChallengeDetection.NotDetected, detector.detect(response(403, page)))
        assertEquals(ChallengeDetection.NotDetected, detector.detect(response(403, page, "Server" to "nginx")))
        assertEquals(ChallengeDetection.NotDetected, detector.detect(response(403, "Forbidden", "Server" to "cloudflare")))
    }

    @Test
    fun `non-challenge statuses and ordinary errors are never challenges`() {
        assertEquals(ChallengeDetection.NotDetected, detector.detect(response(200, "", "cf-mitigated" to "challenge")))
        assertEquals(ChallengeDetection.NotDetected, detector.detect(response(429, "", "cf-mitigated" to "challenge")))
        assertEquals(ChallengeDetection.NotDetected, detector.detect(response(null, "", "cf-mitigated" to "challenge")))
        assertEquals(ChallengeDetection.NotDetected, detector.detect(response(403, "", "cf-mitigated" to "block")))
        assertEquals(ChallengeDetection.NotDetected, detector.detect(response(503, "Service Unavailable")))
    }

    @Test
    fun `body inspection is bounded to the configured prefix`() {
        val marker = "window._cf_chl_opt"
        val bounded = CloudflareChallengeDetector(maxInspectedBodyBytes = 10 + marker.length)
        val exactlyInside = "x".repeat(10) + marker
        val oneBeyond = "x".repeat(11) + marker
        assertEquals(detected("challenge-page-marker"), bounded.detect(response(403, exactlyInside, "Server" to "cloudflare")))
        assertEquals(ChallengeDetection.NotDetected, bounded.detect(response(403, oneBeyond, "Server" to "cloudflare")))
    }

    @Test
    fun `statuses and markers are replaceable and validated`() {
        val custom = CloudflareChallengeDetector(statusCodes = setOf(429), bodyMarkers = listOf("captcha-here"))
        assertEquals(detected("challenge-page-marker"), custom.detect(response(429, "..captcha-here..", "Server" to "cloudflare")))
        assertEquals(ChallengeDetection.NotDetected, custom.detect(response(403, "", "cf-mitigated" to "challenge")))
        val headerOnly = CloudflareChallengeDetector(bodyMarkers = emptyList())
        assertEquals(ChallengeDetection.NotDetected, headerOnly.detect(response(403, "<title>Just a moment...</title>", "Server" to "cloudflare")))
        assertEquals(detected("cf-mitigated-header"), headerOnly.detect(response(403, "", "cf-mitigated" to "challenge")))
        assertEquals(3, CloudflareChallengeDetector.DEFAULT_BODY_MARKERS.size)
        listOf(
            { CloudflareChallengeDetector(statusCodes = emptySet()) },
            { CloudflareChallengeDetector(statusCodes = setOf(99)) },
            { CloudflareChallengeDetector(statusCodes = setOf(600)) },
            { CloudflareChallengeDetector(maxInspectedBodyBytes = 0) },
            { CloudflareChallengeDetector(bodyMarkers = listOf(" ")) },
        ).forEach { assertFailsWith<IllegalArgumentException> { it() } }
    }

    @Test
    fun `challenge value types validate their inputs`() {
        assertEquals("custom", ChallengeKind("custom").value)
        listOf(
            { ChallengeKind(" ") },
            { ChallengeKind("x".repeat(129)) },
            { ChallengeKind("bad\nkind") },
            { ChallengeDetection.Detected(ChallengeKind.CLOUDFLARE, " ") },
            { ChallengeResolutionRequest(com.rmjtromp.trawler.web.transport.WebOrigin.fromTarget(SITE), " ", ChallengeKind.CLOUDFLARE) },
        ).forEach { assertFailsWith<IllegalArgumentException> { it() } }
        val request = ChallengeResolutionRequest(
            com.rmjtromp.trawler.web.transport.WebOrigin.fromTarget(SITE),
            "$SITE/?token=secret",
            ChallengeKind.CLOUDFLARE,
        )
        assertEquals("ChallengeResolutionRequest(origin=WebOrigin(scheme=https, host=example.test, port=443), kind=cloudflare)", request.toString())
    }
}
