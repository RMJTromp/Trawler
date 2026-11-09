package com.rmjtromp.trawler.web.policy

import com.rmjtromp.trawler.model.CrawlResponse
import java.io.IOException
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.Locale

/** Classification of an HTTP response without hiding the response itself. */
public data class HttpFailureClassification(
    public val category: String,
    public val statusCode: Int?,
    public val retryableByDefault: Boolean,
) {
    init {
        require(category.isNotBlank()) { "HTTP failure category must not be blank" }
    }
}

/** Classifies HTTP responses; it never schedules or performs a retry. */
public interface HttpFailureClassifier {
    /** Stable identifier used by transport diagnostics and configuration review. */
    public val id: String

    /** Returns a classification while leaving [response] inspectable. */
    public fun classify(response: CrawlResponse): HttpFailureClassification
}

/** Conservative status-only HTTP classification suitable for ordinary runs. */
public object DefaultHttpFailureClassifier : HttpFailureClassifier {
    override val id: String = "default-http-failure-classifier"

    override fun classify(response: CrawlResponse): HttpFailureClassification {
        val status = response.statusCode
        return when {
            status == null -> HttpFailureClassification("transport-unknown", null, retryableByDefault = true)
            status in 200..399 -> HttpFailureClassification("http-success-or-redirect", status, false)
            status == 408 -> HttpFailureClassification("http-request-timeout", status, true)
            status == 425 -> HttpFailureClassification("http-too-early", status, true)
            status == 429 -> HttpFailureClassification("http-rate-limited", status, true)
            status in 500..599 -> HttpFailureClassification("http-server-error", status, true)
            status in 400..499 -> HttpFailureClassification("http-client-error", status, false)
            else -> HttpFailureClassification("http-unclassified", status, false)
        }
    }
}

/** Reads a server Retry-After hint without making a retry decision. */
public interface RetryAfterPolicy {
    /** Stable identifier used by pacing diagnostics. */
    public val id: String

    /** Returns a non-negative delay, or `null` when the response has no valid hint. */
    public fun delay(response: CrawlResponse, now: Instant): Duration?
}

/**
 * Parses the standard delta-seconds and HTTP-date Retry-After forms.
 *
 * Invalid, negative, or overlong header values are ignored. A valid large
 * value is returned unchanged; the selected pacing policy decides whether a
 * caller should wait, defer, or reject it under its own bound.
 */
public object DefaultRetryAfterPolicy : RetryAfterPolicy {
    override val id: String = "default-retry-after"

    override fun delay(response: CrawlResponse, now: Instant): Duration? {
        val value = response.headers["Retry-After"].firstOrNull() ?: return null
        if (value.length > 128) return null
        val trimmed = value.trim()
        if (trimmed.isEmpty()) return null
        trimmed.toLongOrNull()?.let { seconds ->
            if (seconds < 0) return null
            return try {
                Duration.ofSeconds(seconds)
            } catch (_: ArithmeticException) {
                null
            }
        }
        return try {
            val retryAt = ZonedDateTime.parse(
                trimmed,
                DateTimeFormatter.RFC_1123_DATE_TIME.withLocale(Locale.US),
            ).withZoneSameInstant(ZoneOffset.UTC).toInstant()
            if (retryAt.isAfter(now)) Duration.between(now, retryAt) else Duration.ZERO
        } catch (_: DateTimeParseException) {
            null
        }
    }
}

/** Raised when an HTTP response body exceeds a configured compressed/decompressed bound. */
public class WebResponseTooLargeException(
    public val limitKind: String,
    public val limitBytes: Int,
) : IllegalStateException("HTTP response exceeds $limitKind body limit of $limitBytes bytes")

/** Raised when response headers exceed the configured adapter bound. */
public class WebResponseHeadersTooLargeException(
    public val limit: Int,
) : IllegalStateException("HTTP response headers exceed configured limit of $limit entries")

/** Raised when one response header value exceeds the configured bound. */
public class WebResponseHeaderValueTooLargeException(
    public val limit: Int,
) : IllegalStateException("HTTP response header value exceeds configured limit of $limit characters")

/** Raised when one response header name exceeds the configured bound. */
public class WebResponseHeaderNameTooLargeException(
    public val limit: Int,
) : IllegalStateException("HTTP response header name exceeds configured limit of $limit characters")

/** Raised when a response uses an encoding this adapter deliberately rejects. */
public class UnsupportedWebContentEncodingException(
    public val encoding: String,
) : IllegalStateException("Unsupported HTTP content encoding '$encoding'")

/**
 * Raised when the configured forward proxy cannot carry a request: it refused
 * or timed out the connection, closed or stalled it before a tunnel existed, or
 * rejected the request. The request never falls back to a direct connection.
 *
 * @property connectStatus Status the proxy answered with, or `null` when it gave none.
 */
public class WebProxyFailureException(
    public val connectStatus: Int?,
    cause: Throwable?,
) : IOException(
    if (connectStatus == null) "Forward proxy failed" else "Forward proxy answered with status $connectStatus",
    cause,
)

/** Raised when a web destination or redirect is rejected by a policy. */
public class WebPolicyRejectedException(
    public val policyId: String,
    public val reason: String,
) : IllegalStateException("Web policy '$policyId' rejected the target: $reason")

/** Raised when the redirect limit is reached before a final response exists. */
public class RedirectLimitExceededException(
    public val limit: Int,
) : IllegalStateException("HTTP redirect limit of $limit was reached")

/** Raised when a redirect target cannot be resolved or would loop. */
public class InvalidWebRedirectException(message: String) : IllegalStateException(message)

/** Raised when a configured cookie header cannot remain within its bound. */
public class CookieHeaderLimitException(
    public val limit: Int,
) : IllegalStateException("Cookie request header exceeds configured limit of $limit characters")
