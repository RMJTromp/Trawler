package com.rmjtromp.trawler.web.policy

import com.rmjtromp.trawler.policy.AttemptSummary
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.FailureClassification
import com.rmjtromp.trawler.policy.FailureClassifier
import com.rmjtromp.trawler.policy.FailureContext
import com.rmjtromp.trawler.policy.RetryDecision
import com.rmjtromp.trawler.policy.RetryPolicy
import com.rmjtromp.trawler.policy.SystemClock
import com.rmjtromp.trawler.web.transport.NetworkAddressLimitExceededException
import com.rmjtromp.trawler.web.transport.OriginCapacityExceededException
import com.rmjtromp.trawler.web.transport.PacingWaitExceededException
import io.ktor.client.network.sockets.ConnectTimeoutException
import io.ktor.client.plugins.HttpRequestTimeoutException
import java.net.SocketTimeoutException
import java.time.Duration

/**
 * Maps adapter failures into core's extensible failure categories.
 *
 * A response status is classified separately by [HttpFailureClassifier] and
 * remains inspectable. This classifier handles failures that prevent a
 * response from reaching extraction; it never performs a retry.
 */
public object DefaultWebFailureClassifier : FailureClassifier {
    override val id: String = "default-web-failure-classifier"

    override fun classify(error: Throwable, context: FailureContext): FailureClassification = when (error) {
        is WebPolicyRejectedException -> FailureClassification("web-policy-rejected", false, error.reason)
        is RobotsUnavailableException -> FailureClassification("robots-unavailable", true, error.message)
        is WebAddressResolutionException -> if (error.cause is NetworkAddressLimitExceededException) {
            FailureClassification("web-dns-answer-limit", false)
        } else {
            FailureClassification("web-dns-failure", true, error.host)
        }
        is WebResponseTooLargeException -> FailureClassification("web-body-limit", false, error.limitKind)
        is WebResponseHeadersTooLargeException -> FailureClassification("web-header-limit", false)
        is WebResponseHeaderNameTooLargeException -> FailureClassification("web-header-name-limit", false)
        is WebResponseHeaderValueTooLargeException -> FailureClassification("web-header-value-limit", false)
        is UnsupportedWebContentEncodingException -> FailureClassification("web-content-encoding", false, error.encoding)
        is CookieHeaderLimitException -> FailureClassification("web-cookie-header-limit", false)
        is NetworkAddressLimitExceededException -> FailureClassification("web-dns-answer-limit", false)
        is OriginCapacityExceededException -> FailureClassification("web-origin-capacity", true)
        is PacingWaitExceededException -> FailureClassification("web-pacing-bound", true)
        is RedirectLimitExceededException -> FailureClassification("web-redirect-limit", false)
        is InvalidWebRedirectException -> FailureClassification("web-invalid-redirect", false)
        is ConnectTimeoutException -> FailureClassification("http-connect-timeout", true)
        is SocketTimeoutException -> FailureClassification("http-socket-timeout", true)
        is HttpRequestTimeoutException -> FailureClassification("http-request-timeout", true)
        else -> FailureClassification("web-transport-failure", true, error::class.simpleName)
    }
}

/**
 * Visible exponential backoff policy for failures classified by the web
 * adapter. It returns a core retry decision; it never retries inside Ktor.
 */
public class DefaultHttpRetryPolicy(
    private val maxAttempts: Int = 3,
    private val initialDelay: Duration = Duration.ofSeconds(1),
    private val maximumDelay: Duration = Duration.ofSeconds(30),
    private val clock: Clock = SystemClock,
) : RetryPolicy {
    override val id: String = "default-http-retry"

    init {
        require(maxAttempts > 0) { "HTTP retry attempt bound must be positive" }
        require(!initialDelay.isNegative) { "HTTP retry initial delay must not be negative" }
        require(!maximumDelay.isNegative && !maximumDelay.isZero) {
            "HTTP retry maximum delay must be positive"
        }
        require(initialDelay <= maximumDelay) { "HTTP retry initial delay must not exceed maximum delay" }
    }

    override fun decide(
        failure: FailureClassification,
        attempt: AttemptSummary,
        context: FailureContext,
    ): RetryDecision {
        if (!failure.retryableByDefault || attempt.attemptNumber >= maxAttempts) {
            return RetryDecision.DoNotRetry
        }
        val exponent = (attempt.attemptNumber - 1).coerceIn(0, 30)
        val multiplier = 1L shl exponent
        val delay = try {
            initialDelay.multipliedBy(multiplier).coerceAtMost(maximumDelay)
        } catch (_: ArithmeticException) {
            maximumDelay
        }
        return RetryDecision.RetryAt(clock.now().plus(delay))
    }
}

private fun Duration.coerceAtMost(maximum: Duration): Duration = if (this > maximum) maximum else this
