package com.rmjtromp.trawler.flaresolverr.challenge

import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.web.transport.WebOrigin
import java.util.Locale

/**
 * Extensible challenge identifier, for example `cloudflare`.
 *
 * Challenge kinds are open values rather than a vendor-shaped closed enum, so
 * a host-supplied detector can introduce its own kinds without changing this
 * module. These contracts are deliberately local to `trawler-flaresolverr`;
 * core defines no challenge abstraction.
 */
@JvmInline
public value class ChallengeKind(public val value: String) {
    init {
        require(value.isNotBlank()) { "Challenge kind must not be blank" }
        require(value.length <= 128 && value.none(Char::isISOControl)) {
            "Challenge kind must be at most 128 printable characters"
        }
    }

    public companion object {
        /** Cloudflare managed/interactive challenge or "I'm Under Attack" interstitial. */
        public val CLOUDFLARE: ChallengeKind = ChallengeKind("cloudflare")
    }
}

/** Expected detection outcomes. */
public sealed interface ChallengeDetection {
    /** The response is an ordinary response. */
    public data object NotDetected : ChallengeDetection

    /** The response is a challenge page; [signal] names the non-sensitive evidence. */
    public data class Detected(
        public val kind: ChallengeKind,
        public val signal: String,
    ) : ChallengeDetection {
        init {
            require(signal.isNotBlank()) { "Challenge detection signal must not be blank" }
        }
    }
}

/**
 * Decides whether a transport response is an anti-bot challenge.
 *
 * Detection is a pure, non-suspending policy over an already bounded core
 * response. It must not perform network work, mutate state, or retain the
 * response. Implementations are replaceable; [CloudflareChallengeDetector]
 * is the bundled conservative default.
 */
public interface ChallengeDetector {
    /** Stable identifier used for diagnostics and configuration review. */
    public val id: String

    /** Classifies [response] without changing it. */
    public fun detect(response: CrawlResponse): ChallengeDetection
}

/**
 * Conservative Cloudflare challenge detector.
 *
 * A response is a challenge only when its status is one of [statusCodes]
 * (403 and 503 by default) and either:
 *
 * - a `cf-mitigated` header contains `challenge`; or
 * - the response comes from Cloudflare (`Server: cloudflare` or a `CF-RAY`
 *   header) and the first [maxInspectedBodyBytes] bytes contain a known
 *   challenge-page marker.
 *
 * Ordinary 403/503 responses without Cloudflare evidence are never reported
 * as challenges, so origin errors keep their HTTP classification.
 */
public class CloudflareChallengeDetector(
    statusCodes: Set<Int> = setOf(403, 503),
    private val maxInspectedBodyBytes: Int = 64 * 1024,
    bodyMarkers: List<String> = DEFAULT_BODY_MARKERS,
) : ChallengeDetector {
    override val id: String = "cloudflare-challenge-detector"

    private val statuses: Set<Int> = statusCodes.toSet()
    private val markers: List<String> = bodyMarkers.toList()

    init {
        require(statuses.isNotEmpty()) { "Challenge status set must not be empty" }
        require(statuses.all { it in 100..599 }) { "Challenge statuses must be HTTP status codes" }
        require(maxInspectedBodyBytes > 0) { "Inspected body bound must be positive" }
        require(markers.all { it.isNotBlank() }) { "Challenge body markers must not be blank" }
    }

    override fun detect(response: CrawlResponse): ChallengeDetection {
        val status = response.statusCode ?: return ChallengeDetection.NotDetected
        if (status !in statuses) return ChallengeDetection.NotDetected
        if (response.headers["cf-mitigated"].any { it.lowercase(Locale.ROOT).contains("challenge") }) {
            return ChallengeDetection.Detected(ChallengeKind.CLOUDFLARE, "cf-mitigated-header")
        }
        val fromCloudflare = response.headers["Server"].any { it.trim().equals("cloudflare", ignoreCase = true) } ||
            response.headers["CF-RAY"].isNotEmpty()
        if (!fromCloudflare || markers.isEmpty()) return ChallengeDetection.NotDetected
        val bytes = response.body.toByteArray()
        val prefix = String(bytes, 0, minOf(bytes.size, maxInspectedBodyBytes), Charsets.ISO_8859_1)
        return if (markers.any { prefix.contains(it, ignoreCase = true) }) {
            ChallengeDetection.Detected(ChallengeKind.CLOUDFLARE, "challenge-page-marker")
        } else {
            ChallengeDetection.NotDetected
        }
    }

    public companion object {
        /** Markers present on Cloudflare challenge interstitials. */
        public val DEFAULT_BODY_MARKERS: List<String> = listOf(
            "/cdn-cgi/challenge-platform/",
            "window._cf_chl_opt",
            "<title>Just a moment...</title>",
        )
    }
}

/** Input to one challenge resolution for [origin]. [target] is the request target that triggered it. */
public data class ChallengeResolutionRequest(
    public val origin: WebOrigin,
    public val target: String,
    public val kind: ChallengeKind,
) {
    init {
        require(target.isNotBlank()) { "Challenge resolution target must not be blank" }
    }

    override fun toString(): String = "ChallengeResolutionRequest(origin=$origin, kind=${kind.value})"
}

/**
 * Obtains clearance for an origin after a challenge was detected.
 *
 * A resolver performs at most one resolution per call and must not loop or
 * retry internally; [ChallengeClearance] owns single-flight coordination and
 * the engine owns retry. Implementations must honor cancellation, bound all
 * waits, keep clearance values out of diagnostics, and return clearance for
 * exactly [ChallengeResolutionRequest.origin].
 */
public interface ChallengeResolver {
    /** Stable identifier used for diagnostics and configuration review. */
    public val id: String

    /** Resolves one challenge or throws; cancellation must propagate. */
    public suspend fun resolve(request: ChallengeResolutionRequest): Clearance
}

/**
 * Raised by [ChallengeClearance.responseMiddleware] when a challenge page is
 * detected. It turns the challenge into an ordinary engine-visible attempt
 * failure so retry and attempt accounting stay with the engine. The message
 * contains only the challenge kind and detection signal.
 */
public class ChallengeDetectedException(
    public val kind: ChallengeKind,
    public val signal: String,
    public val origin: WebOrigin,
) : IllegalStateException("Anti-bot challenge '${kind.value}' detected ($signal)")

/**
 * Raised when clearance could not be obtained before a request.
 *
 * [reason] is a non-sensitive code: `resolver-failed` (the original resolver
 * failure is the [cause]), `resolution-cooldown` (a recent resolution for the
 * origin failed and the configured cooldown has not elapsed), or
 * `origin-mismatch` (the resolver returned clearance for another origin).
 */
public class ChallengeResolutionException(
    public val kind: ChallengeKind,
    public val reason: String,
    cause: Throwable? = null,
) : IllegalStateException("Challenge '${kind.value}' resolution failed: $reason", cause)
