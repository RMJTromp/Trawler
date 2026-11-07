package com.rmjtromp.trawler.flaresolverr

import com.rmjtromp.trawler.flaresolverr.challenge.ChallengeDetectedException
import com.rmjtromp.trawler.flaresolverr.challenge.ChallengeResolutionException
import com.rmjtromp.trawler.policy.FailureClassification
import com.rmjtromp.trawler.policy.FailureClassifier
import com.rmjtromp.trawler.policy.FailureContext
import com.rmjtromp.trawler.web.policy.WebPolicyRejectedException

/** Extensible core failure categories produced by [FlareSolverrFailureClassifier]. */
public object FlareSolverrFailureCategories {
    /** A challenge page was detected on an ordinary response; retry after resolution. */
    public const val CHALLENGE_DETECTED: String = "flaresolverr-challenge-detected"

    /** Clearance could not be obtained; the detail carries the non-sensitive reason. */
    public const val CHALLENGE_RESOLUTION_FAILED: String = "flaresolverr-challenge-resolution-failed"

    /** The FlareSolverr service was unreachable or timed out. */
    public const val UNAVAILABLE: String = "flaresolverr-unavailable"

    /** FlareSolverr answered `status: error`, for example a solve timeout. */
    public const val SERVICE_ERROR: String = "flaresolverr-service-error"

    /** FlareSolverr answered with an unexpected response shape. */
    public const val PROTOCOL: String = "flaresolverr-protocol"

    /** A FlareSolverr response exceeded a configured bound. */
    public const val RESPONSE_LIMIT: String = "flaresolverr-response-limit"

    /** The request cannot be expressed through FlareSolverr. */
    public const val UNSUPPORTED_REQUEST: String = "flaresolverr-unsupported-request"
}

/**
 * Classifies FlareSolverr transport and challenge-clearance failures and
 * delegates every other failure to [delegate] (normally
 * `DefaultWebFailureClassifier`).
 *
 * The error and up to [MAX_CAUSE_DEPTH] causes are inspected so a wrapped
 * adapter failure is still recognized. Destination-policy rejections raised
 * by this module use `trawler-web`'s `WebPolicyRejectedException` and are left
 * to the delegate. Classification never retries; retry belongs to the
 * selected `RetryPolicy`, for example `ChallengeRetryPolicy`.
 *
 * Details contain only categories, limit kinds, and reason codes, never the
 * FlareSolverr service message, target URLs, cookies, or credentials.
 */
public class FlareSolverrFailureClassifier(
    private val delegate: FailureClassifier,
) : FailureClassifier {
    override val id: String = "flaresolverr-failure-classifier+${delegate.id}"

    override fun classify(error: Throwable, context: FailureContext): FailureClassification {
        var current: Throwable = error
        for (depth in 0..MAX_CAUSE_DEPTH) {
            if (current is WebPolicyRejectedException) break
            classifyOwn(current)?.let { return it }
            val cause = current.cause
            if (cause == null || cause === current) break
            current = cause
        }
        return delegate.classify(error, context)
    }

    private fun classifyOwn(error: Throwable): FailureClassification? = when (error) {
        is ChallengeDetectedException -> FailureClassification(
            FlareSolverrFailureCategories.CHALLENGE_DETECTED,
            retryableByDefault = true,
            detail = error.kind.value,
        )

        is ChallengeResolutionException -> FailureClassification(
            FlareSolverrFailureCategories.CHALLENGE_RESOLUTION_FAILED,
            retryableByDefault = error.reason == "resolution-cooldown" ||
                error.cause is FlareSolverrUnavailableException ||
                error.cause is FlareSolverrErrorException,
            detail = error.reason,
        )

        is FlareSolverrUnavailableException ->
            FailureClassification(FlareSolverrFailureCategories.UNAVAILABLE, retryableByDefault = true)

        is FlareSolverrErrorException ->
            FailureClassification(FlareSolverrFailureCategories.SERVICE_ERROR, retryableByDefault = true)

        is FlareSolverrProtocolException ->
            FailureClassification(FlareSolverrFailureCategories.PROTOCOL, retryableByDefault = false)

        is FlareSolverrResponseLimitException -> FailureClassification(
            FlareSolverrFailureCategories.RESPONSE_LIMIT,
            retryableByDefault = false,
            detail = error.limitKind,
        )

        is UnsupportedFlareSolverrRequestException -> FailureClassification(
            FlareSolverrFailureCategories.UNSUPPORTED_REQUEST,
            retryableByDefault = false,
            detail = error.reason,
        )

        else -> null
    }

    private companion object {
        const val MAX_CAUSE_DEPTH: Int = 8
    }
}
