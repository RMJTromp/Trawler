package com.rmjtromp.trawler.flaresolverr.challenge

import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.SystemClock
import com.rmjtromp.trawler.web.transport.WebOrigin
import java.time.Duration
import java.time.Instant
import java.util.Collections
import java.util.LinkedHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * One clearance cookie obtained by a challenge resolver.
 *
 * The value is a credential: it is never included in [toString], response
 * attributes, events, or exception messages. Names and values are validated
 * so they can be placed in a `Cookie` request header without header injection.
 * [expiresAt] is `null` for a browser session cookie, which is then bounded
 * only by the owning store's clearance lifetime.
 */
public class ClearanceCookie(
    public val name: String,
    public val value: String,
    public val path: String = "/",
    public val secure: Boolean = false,
    public val expiresAt: Instant? = null,
) {
    init {
        require(isCookieToken(name)) { "Clearance cookie name is not a valid cookie token" }
        require(isCookieValue(value)) { "Clearance cookie value contains characters not allowed in a Cookie header" }
        require(path.startsWith('/')) { "Clearance cookie path must start with '/'" }
    }

    /** Whether this cookie may be sent to [requestPath] over [https] at [now]. */
    public fun appliesTo(requestPath: String, https: Boolean, now: Instant): Boolean {
        if (expiresAt != null && !expiresAt.isAfter(now)) return false
        if (secure && !https) return false
        val normalizedPath = requestPath.ifEmpty { "/" }
        return path == "/" || normalizedPath == path || normalizedPath.startsWith(path.trimEnd('/') + "/")
    }

    override fun toString(): String = "ClearanceCookie(name=$name, value=[REDACTED], path=$path, secure=$secure)"

    internal companion object {
        private val SEPARATORS: Set<Char> =
            setOf('(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']', '?', '=', '{', '}', ' ', '\t')

        fun isCookieToken(value: String): Boolean =
            value.isNotEmpty() && value.length <= 1_024 && value.all { it.code in 0x21..0x7e && it !in SEPARATORS }

        fun isCookieValue(value: String): Boolean =
            value.all { it.code in 0x21..0x7e && it != ';' && it != ',' && it != '"' && it != '\\' }
    }
}

/**
 * Challenge clearance for exactly one [origin].
 *
 * Clearance cookies are bound by the protecting service to the [userAgent]
 * and normally to the egress IP address that solved the challenge. Reuse
 * therefore requires the reusing transport to send this exact User-Agent and
 * to share the solver's egress path/proxy. The object is immutable; its cookie
 * values are sensitive and are redacted from [toString].
 */
public class Clearance(
    public val origin: WebOrigin,
    cookies: List<ClearanceCookie>,
    public val userAgent: String,
    public val obtainedAt: Instant,
) {
    public val cookies: List<ClearanceCookie> = Collections.unmodifiableList(cookies.toList())

    init {
        require(userAgent.isNotBlank()) { "Clearance User-Agent must not be blank" }
        require(userAgent.none(Char::isISOControl)) { "Clearance User-Agent must not contain control characters" }
        require(this.cookies.map { it.name to it.path }.toSet().size == this.cookies.size) {
            "Clearance cookies must have unique name/path pairs"
        }
    }

    override fun toString(): String =
        "Clearance(origin=$origin, cookies=${cookies.map { it.name }}, userAgent=$userAgent, obtainedAt=$obtainedAt)"
}

/**
 * A clearance currently held by a [ClearanceStore].
 *
 * [generation] increases every time any origin receives a new clearance in
 * the owning store. It is a non-sensitive token attached to requests so a
 * challenge observed on a request that carried an older or no clearance does
 * not invalidate a newer one.
 */
public class StoredClearance internal constructor(
    public val clearance: Clearance,
    public val generation: Long,
    public val expiresAt: Instant,
) {
    override fun toString(): String =
        "StoredClearance(origin=${clearance.origin}, generation=$generation, expiresAt=$expiresAt)"
}

/** Bounds and timing for [ClearanceStore]. */
public data class ClearanceStoreConfig(
    public val maxOrigins: Int = 256,
    public val maxClearanceLifetime: Duration = Duration.ofMinutes(30),
    public val challengeMarkerLifetime: Duration = Duration.ofMinutes(10),
    public val resolutionFailureCooldown: Duration = Duration.ofSeconds(30),
    public val maxCookiesPerOrigin: Int = 64,
    public val maxCookieHeaderLength: Int = 16_384,
) {
    init {
        require(maxOrigins > 0) { "Clearance origin bound must be positive" }
        require(maxClearanceLifetime.toMillis() > 0) { "Clearance lifetime must be positive" }
        require(challengeMarkerLifetime.toMillis() > 0) { "Challenge marker lifetime must be positive" }
        require(!resolutionFailureCooldown.isNegative) { "Resolution failure cooldown must not be negative" }
        require(maxCookiesPerOrigin > 0) { "Clearance cookie bound must be positive" }
        require(maxCookieHeaderLength > 0) { "Clearance Cookie header bound must be positive" }
    }
}

/**
 * Bounded, expiring, process-local clearance and challenge-marker state.
 *
 * The store is keyed by exact [WebOrigin]; clearance for one origin is never
 * applied to another origin, including sibling subdomains. Each origin entry
 * holds at most one clearance, one pending challenge marker, and one recent
 * resolution failure timestamp. Entries expire by the injected [clock] and
 * the least recently used origin is evicted when [ClearanceStoreConfig.maxOrigins]
 * is reached. Nothing is persisted; clearance values are lost on process exit.
 *
 * All operations are non-suspending, short critical sections under one lock
 * and are safe to call from any coroutine. The store performs no network work.
 */
public class ClearanceStore(
    public val configuration: ClearanceStoreConfig = ClearanceStoreConfig(),
    private val clock: Clock = SystemClock,
) {
    private class Entry(
        var stored: StoredClearance? = null,
        var pending: PendingChallenge? = null,
        var lastFailureAt: Instant? = null,
    )

    private val lock = ReentrantLock()
    private val entries = LinkedHashMap<WebOrigin, Entry>(16, 0.75f, true)
    private var generationSequence: Long = 0

    /** Returns the unexpired clearance for [origin], or `null`. */
    public fun current(origin: WebOrigin): StoredClearance? = lock.withLock {
        val entry = entries[origin] ?: return null
        val now = clock.now()
        val stored = entry.stored ?: return null
        if (!stored.expiresAt.isAfter(now)) {
            entry.stored = null
            removeIfEmpty(origin, entry, now)
            return null
        }
        stored
    }

    /**
     * Stores [clearance] for its origin, replacing any older clearance and
     * clearing the origin's challenge marker and failure cooldown. Cookies
     * already expired at storage time are dropped; at most
     * [ClearanceStoreConfig.maxCookiesPerOrigin] cookies are retained.
     */
    public fun store(clearance: Clearance): StoredClearance = lock.withLock {
        val now = clock.now()
        val retained = clearance.cookies
            .filter { it.expiresAt == null || it.expiresAt.isAfter(now) }
            .take(configuration.maxCookiesPerOrigin)
        val bounded = if (retained.size == clearance.cookies.size) {
            clearance
        } else {
            Clearance(clearance.origin, retained, clearance.userAgent, clearance.obtainedAt)
        }
        val stored = StoredClearance(
            clearance = bounded,
            generation = ++generationSequence,
            expiresAt = now.plus(configuration.maxClearanceLifetime),
        )
        val entry = entryFor(clearance.origin, now)
        entry.stored = stored
        entry.pending = null
        entry.lastFailureAt = null
        stored
    }

    /** Removes all state for [origin]. */
    public fun clear(origin: WebOrigin) {
        lock.withLock { entries.remove(origin) }
    }

    /** Removes all state held by this store. */
    public fun clearAll() {
        lock.withLock { entries.clear() }
    }

    /**
     * Records a challenge observed for [origin]. When the challenged request
     * carried [sentGeneration] and that is still the current clearance, the
     * clearance is invalidated because the protecting service rejected it.
     * A challenge on a request without the current clearance only marks the
     * origin when no valid clearance exists.
     */
    internal fun recordChallenge(origin: WebOrigin, kind: ChallengeKind, sentGeneration: Long?) {
        lock.withLock {
            val now = clock.now()
            val entry = entryFor(origin, now)
            val stored = entry.stored?.takeIf { it.expiresAt.isAfter(now) }
            if (stored != null && sentGeneration != stored.generation) return
            entry.stored = null
            entry.pending = PendingChallenge(kind, now.plus(configuration.challengeMarkerLifetime))
        }
    }

    /** Returns the unexpired pending challenge for [origin], or `null`. */
    internal fun pendingChallenge(origin: WebOrigin): PendingChallenge? = lock.withLock {
        val entry = entries[origin] ?: return null
        val now = clock.now()
        val pending = entry.pending ?: return null
        if (!pending.expiresAt.isAfter(now)) {
            entry.pending = null
            removeIfEmpty(origin, entry, now)
            return null
        }
        pending
    }

    /** Records a failed resolution so concurrent and immediate retries fail fast during the cooldown. */
    internal fun recordResolutionFailure(origin: WebOrigin) {
        lock.withLock { entryFor(origin, clock.now()).lastFailureAt = clock.now() }
    }

    /** Returns whether [origin] is inside its resolution-failure cooldown. */
    internal fun inResolutionCooldown(origin: WebOrigin): Boolean = lock.withLock {
        val failedAt = entries[origin]?.lastFailureAt ?: return false
        val cooldown = configuration.resolutionFailureCooldown
        !cooldown.isZero && failedAt.plus(cooldown).isAfter(clock.now())
    }

    internal fun now(): Instant = clock.now()

    private fun entryFor(origin: WebOrigin, now: Instant): Entry {
        entries[origin]?.let { return it }
        if (entries.size >= configuration.maxOrigins) {
            evictExpired(now)
        }
        while (entries.size >= configuration.maxOrigins) {
            val eldest = entries.keys.firstOrNull() ?: break
            entries.remove(eldest)
        }
        return Entry().also { entries[origin] = it }
    }

    private fun evictExpired(now: Instant) {
        val iterator = entries.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next().value
            if (entry.isExpired(now)) iterator.remove()
        }
    }

    private fun removeIfEmpty(origin: WebOrigin, entry: Entry, now: Instant) {
        if (entry.isExpired(now)) entries.remove(origin)
    }

    private fun Entry.isExpired(now: Instant): Boolean {
        val clearanceLive = stored?.expiresAt?.isAfter(now) == true
        val pendingLive = pending?.expiresAt?.isAfter(now) == true
        val cooldown = configuration.resolutionFailureCooldown
        val failureLive = lastFailureAt?.let { !cooldown.isZero && it.plus(cooldown).isAfter(now) } == true
        return !clearanceLive && !pendingLive && !failureLive
    }
}

/** A challenge observed for an origin that has no usable clearance yet. */
internal class PendingChallenge(
    val kind: ChallengeKind,
    val expiresAt: Instant,
)
