package com.rmjtromp.trawler.flaresolverr

import com.rmjtromp.trawler.flaresolverr.challenge.Clearance
import com.rmjtromp.trawler.flaresolverr.challenge.ClearanceCookie
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.web.policy.WebPolicyRejectedException
import com.rmjtromp.trawler.web.transport.DestinationDecision
import com.rmjtromp.trawler.web.transport.RedirectHistory
import com.rmjtromp.trawler.web.transport.WebDestinationPolicy
import com.rmjtromp.trawler.web.transport.WebOrigin
import java.time.Instant

/**
 * Validates that [target] is a bounded absolute HTTP(S) URL and returns its
 * origin. Non-HTTP schemes are rejected regardless of the destination policy,
 * because FlareSolverr's browser would otherwise load `file:`, `data:`, or
 * other local schemes on the FlareSolverr host.
 */
internal fun requireHttpTarget(target: String, maxTargetLength: Int): WebOrigin {
    if (target.length > maxTargetLength) throw UnsupportedFlareSolverrRequestException("target-length-bound")
    if (target.any { it.isISOControl() || it.isWhitespace() }) {
        throw UnsupportedFlareSolverrRequestException("target-contains-control-or-whitespace")
    }
    val origin = try {
        WebOrigin.fromTarget(target)
    } catch (_: IllegalArgumentException) {
        throw UnsupportedFlareSolverrRequestException("target-is-not-an-absolute-web-url")
    }
    if (origin.scheme != "http" && origin.scheme != "https") {
        throw UnsupportedFlareSolverrRequestException("target-scheme-not-http")
    }
    return origin
}

/**
 * Runs [target] through the caller-supplied destination policy before or
 * after a FlareSolverr call. Redirect hops inside FlareSolverr's browser are
 * invisible to Trawler, so the history is always empty.
 */
internal suspend fun admitDestination(policy: WebDestinationPolicy, request: CrawlRequest) {
    when (val decision = policy.evaluate(request, RedirectHistory.empty(0))) {
        DestinationDecision.Allowed -> Unit
        is DestinationDecision.Rejected -> throw WebPolicyRejectedException(policy.id, decision.reason)
    }
}

/**
 * Validates the solution's final URL: it must be HTTP(S), within bounds, and
 * admitted by [policy] when it differs from [requestTarget]. Returns the
 * final target and its origin. A rejected final URL discards the solution.
 */
internal suspend fun admitFinalTarget(
    policy: WebDestinationPolicy,
    request: CrawlRequest,
    solutionUrl: String?,
    maxTargetLength: Int,
): Pair<String, WebOrigin> {
    val finalTarget = solutionUrl ?: request.target
    val origin = try {
        requireHttpTarget(finalTarget, maxTargetLength)
    } catch (error: UnsupportedFlareSolverrRequestException) {
        throw FlareSolverrProtocolException("final url rejected: ${error.reason}", error)
    }
    if (finalTarget != request.target) {
        admitDestination(policy, request.copy(target = finalTarget))
    }
    return finalTarget to origin
}

/**
 * Builds origin-scoped clearance from solved browser cookies. Cookies whose
 * domain does not match the origin host, or whose name/value cannot be sent
 * safely in a `Cookie` header, are dropped. Duplicate name/path pairs keep
 * the first occurrence.
 */
internal fun clearanceFrom(
    origin: WebOrigin,
    cookies: List<SolvedCookie>,
    userAgent: String,
    obtainedAt: Instant,
    maxCookies: Int,
): Clearance {
    val seen = HashSet<Pair<String, String>>()
    val retained = cookies.asSequence()
        .filter { it.appliesToHost(origin.host) }
        .filter { ClearanceCookie.isCookieToken(it.name) && ClearanceCookie.isCookieValue(it.value) }
        .filter { seen.add(it.name to it.path) }
        .take(maxCookies)
        .map { ClearanceCookie(it.name, it.value, it.path, it.secure, it.expiresAt) }
        .toList()
    return Clearance(origin, retained, userAgent, obtainedAt)
}
