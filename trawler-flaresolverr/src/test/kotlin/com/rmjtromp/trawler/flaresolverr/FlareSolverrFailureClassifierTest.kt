package com.rmjtromp.trawler.flaresolverr

import com.rmjtromp.trawler.flaresolverr.challenge.ChallengeDetectedException
import com.rmjtromp.trawler.flaresolverr.challenge.ChallengeKind
import com.rmjtromp.trawler.flaresolverr.challenge.ChallengeResolutionException
import com.rmjtromp.trawler.policy.FailureClassification
import com.rmjtromp.trawler.policy.FailureClassifier
import com.rmjtromp.trawler.policy.FailureContext
import com.rmjtromp.trawler.web.policy.DefaultWebFailureClassifier
import com.rmjtromp.trawler.web.policy.WebPolicyRejectedException
import com.rmjtromp.trawler.web.transport.WebOrigin
import java.io.IOException
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class FlareSolverrFailureClassifierTest {
    private class RecordingDelegate : FailureClassifier {
        override val id: String = "delegate"
        val seen = CopyOnWriteArrayList<Throwable>()

        override fun classify(error: Throwable, context: FailureContext): FailureClassification {
            seen += error
            return FailureClassification("delegated", retryableByDefault = false)
        }
    }

    private val origin = WebOrigin.fromTarget(SITE)
    private val delegate = RecordingDelegate()
    private val classifier = FlareSolverrFailureClassifier(delegate)

    private fun classify(error: Throwable) = classifier.classify(error, FAILURE_CONTEXT)

    @Test
    fun `identifier composes the delegate identifier`() {
        assertEquals("flaresolverr-failure-classifier+delegate", classifier.id)
    }

    @Test
    fun `every adapter failure maps to its category retryability and safe detail`() {
        val cases = listOf(
            ChallengeDetectedException(ChallengeKind.CLOUDFLARE, "cf-mitigated-header", origin) to
                FailureClassification(FlareSolverrFailureCategories.CHALLENGE_DETECTED, true, "cloudflare"),
            FlareSolverrUnavailableException(IOException("refused")) to
                FailureClassification(FlareSolverrFailureCategories.UNAVAILABLE, true),
            FlareSolverrErrorException("Error: https://example.test/?token=secret timed out") to
                FailureClassification(FlareSolverrFailureCategories.SERVICE_ERROR, true),
            FlareSolverrProtocolException("bad") to
                FailureClassification(FlareSolverrFailureCategories.PROTOCOL, false),
            FlareSolverrResponseLimitException("response body bytes", 1) to
                FailureClassification(FlareSolverrFailureCategories.RESPONSE_LIMIT, false, "response body bytes"),
            UnsupportedFlareSolverrRequestException("method-not-supported") to
                FailureClassification(FlareSolverrFailureCategories.UNSUPPORTED_REQUEST, false, "method-not-supported"),
        )
        cases.forEach { (error, expected) -> assertEquals(expected, classify(error), error.toString()) }
        assertTrue(delegate.seen.isEmpty())
        assertFalse(classify(FlareSolverrErrorException("token=secret")).toString().contains("secret"))
    }

    @Test
    fun `resolution failures are retryable only for transient causes`() {
        fun resolution(reason: String, cause: Throwable? = null) =
            classify(ChallengeResolutionException(ChallengeKind.CLOUDFLARE, reason, cause))
        assertEquals(
            FailureClassification(FlareSolverrFailureCategories.CHALLENGE_RESOLUTION_FAILED, true, "resolution-cooldown"),
            resolution("resolution-cooldown"),
        )
        assertTrue(resolution("resolver-failed", FlareSolverrUnavailableException(IOException())).retryableByDefault)
        assertTrue(resolution("resolver-failed", FlareSolverrErrorException("x")).retryableByDefault)
        assertFalse(resolution("resolver-failed", FlareSolverrProtocolException("x")).retryableByDefault)
        assertFalse(resolution("resolver-failed", WebPolicyRejectedException("p", "r")).retryableByDefault)
        assertFalse(resolution("origin-mismatch").retryableByDefault)
        assertEquals("origin-mismatch", resolution("origin-mismatch").detail)
    }

    @Test
    fun `wrapped adapter failures are recognized within the cause depth bound`() {
        var wrapped: Throwable = FlareSolverrProtocolException("deep")
        repeat(8) { wrapped = RuntimeException("wrapper $it", wrapped) }
        assertEquals(FlareSolverrFailureCategories.PROTOCOL, classify(wrapped).category)
        val tooDeep = RuntimeException("one more", wrapped)
        assertEquals("delegated", classify(tooDeep).category)
        assertSame(tooDeep, delegate.seen.single())
    }

    @Test
    fun `destination rejections and foreign failures are delegated with the original error`() {
        val rejected = WebPolicyRejectedException("public-web-safety", "destination-address-blocked")
        assertEquals("delegated", classify(rejected).category)
        val rejectedWrappingOwn = WebPolicyRejectedException("p", "r").also { it.initCause(FlareSolverrProtocolException("hidden")) }
        assertEquals("delegated", classify(rejectedWrappingOwn).category)
        val foreign = IllegalArgumentException("other")
        assertEquals("delegated", classify(foreign).category)
        assertEquals(listOf(rejected, rejectedWrappingOwn, foreign), delegate.seen.toList())
    }

    @Test
    fun `default web classifier remains the natural delegate`() {
        val real = FlareSolverrFailureClassifier(DefaultWebFailureClassifier)
        assertEquals(
            "web-policy-rejected",
            real.classify(WebPolicyRejectedException("p", "reason"), FAILURE_CONTEXT).category,
        )
        assertEquals("web-transport-failure", real.classify(IllegalStateException(), FAILURE_CONTEXT).category)
    }
}
