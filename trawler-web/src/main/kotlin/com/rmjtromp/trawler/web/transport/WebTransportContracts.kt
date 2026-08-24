package com.rmjtromp.trawler.web.transport

import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.SystemClock
import com.rmjtromp.trawler.web.policy.RetryAfterPolicy
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.sync.Semaphore
import kotlin.concurrent.withLock
import java.net.InetAddress
import java.time.Duration
import java.time.Instant
import java.util.Collections
import java.util.LinkedHashMap
import java.util.Locale
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

/** The logical origin used for per-origin web concurrency and pacing. */
public data class WebOrigin(
    public val scheme: String,
    public val host: String,
    public val port: Int,
) {
    init {
        require(scheme.isNotBlank()) { "Web origin scheme must not be blank" }
        require(host.isNotBlank()) { "Web origin host must not be blank" }
        require(port in 1..65_535) { "Web origin port must be between 1 and 65535" }
    }

    /** Returns a normalized origin for an absolute HTTP-like target. */
    public companion object {
        public fun fromTarget(target: String): WebOrigin {
            val uri = try {
                java.net.URI(target)
            } catch (error: Exception) {
                throw IllegalArgumentException("Target is not a valid URI", error)
            }
            val scheme = uri.scheme?.lowercase(Locale.ROOT)
                ?: throw IllegalArgumentException("Target scheme is required")
            val host = uri.host?.lowercase(Locale.ROOT)?.removePrefix("[")?.removeSuffix("]")
                ?: throw IllegalArgumentException("Target host is required")
            val port = if (uri.port == -1) {
                when (scheme) {
                    "http" -> 80
                    "https" -> 443
                    else -> throw IllegalArgumentException("A port is required for scheme '$scheme'")
                }
            } else {
                uri.port
            }
            return WebOrigin(scheme, host, port)
        }
    }
}

/** Distinguishes ordinary page requests from engine-controlled robots fetches. */
public enum class WebRequestKind {
    PAGE,
    ROBOTS,
}

/** One bounded redirect transition retained while a request is executing. */
public data class RedirectHop(
    public val fromTarget: String,
    public val toTarget: String,
    public val statusCode: Int,
    public val crossOrigin: Boolean,
) {
    init {
        require(fromTarget.isNotBlank()) { "Redirect source must not be blank" }
        require(toTarget.isNotBlank()) { "Redirect target must not be blank" }
        require(statusCode in 300..399) { "Redirect status must be in the 3xx range" }
    }
}

/** Immutable, bounded redirect history supplied to destination policies. */
public class RedirectHistory private constructor(
    entries: List<RedirectHop>,
    private val maximumEntries: Int,
) {
    public val entries: List<RedirectHop> =
        Collections.unmodifiableList(entries.toList())

    public val size: Int
        get() = entries.size

    /** Returns whether the exact target has already appeared in this history. */
    public fun containsTarget(target: String): Boolean =
        entries.any { it.toTarget == target }

    /** Appends one hop while preserving the configured history bound. */
    public fun plus(hop: RedirectHop): RedirectHistory {
        require(entries.size < maximumEntries) {
            "Redirect history limit of $maximumEntries has been reached"
        }
        return RedirectHistory(entries + hop, maximumEntries)
    }

    public companion object {
        /** Creates an empty history whose maximum size is [maximumEntries]. */
        public fun empty(maximumEntries: Int): RedirectHistory {
            require(maximumEntries >= 0) { "Redirect history bound must not be negative" }
            return RedirectHistory(emptyList(), maximumEntries)
        }
    }
}

/** Authorizes each target before it is sent, including every redirect hop. */
public interface WebDestinationPolicy {
    /** Stable identifier used by transport diagnostics and configuration review. */
    public val id: String

    /**
     * Evaluates [request] before any network call. The call is suspending so a
     * policy may perform bounded DNS/address checks; cancellation must abort
     * that work and failures reject the current request.
     */
    public suspend fun evaluate(request: com.rmjtromp.trawler.model.CrawlRequest, history: RedirectHistory): DestinationDecision
}

/** Expected destination admission outcomes. */
public sealed interface DestinationDecision {
    /** The target may proceed to robots and transport processing. */
    public data object Allowed : DestinationDecision

    /** The target is rejected without invoking the HTTP client. */
    public data class Rejected(public val reason: String) : DestinationDecision {
        init {
            require(reason.isNotBlank()) { "Destination rejection reason must not be blank" }
        }
    }
}

/** Resolves host names for safety policies without owning an HTTP client. */
public interface NetworkAddressResolver {
    /** Stable identifier used by diagnostics. */
    public val id: String

    /** Resolves at most the implementation's configured address bound. */
    public suspend fun resolve(host: String): List<InetAddress>
}

/**
 * JDK DNS resolver using an adapter-owned bounded dispatcher.
 *
 * DNS lookup is blocking on the JDK API. The resolver owns its fixed-size
 * executor and must be closed by the transport or extension that created it.
 * Cancellation stops waiting for the lookup; a resolver close interrupts and
 * shuts down its worker threads. No HTTP client is created here.
 */
public class JdkNetworkAddressResolver(
    private val maxParallelLookups: Int = 4,
    private val maxResolvedAddresses: Int = 16,
) : NetworkAddressResolver, AutoCloseable {
    override val id: String = "jdk-dns"

    private val closed = AtomicBoolean(false)
    private val executor: ExecutorService
    private val dispatcher: ExecutorCoroutineDispatcher

    init {
        require(maxParallelLookups > 0) { "DNS parallelism must be positive" }
        require(maxResolvedAddresses > 0) { "DNS address bound must be positive" }
        executor = Executors.newFixedThreadPool(
            maxParallelLookups,
            DaemonThreadFactory("trawler-web-dns"),
        )
        dispatcher = executor.asCoroutineDispatcher()
    }

    override suspend fun resolve(host: String): List<InetAddress> {
        require(host.isNotBlank()) { "DNS host must not be blank" }
        check(!closed.get()) { "DNS resolver is closed" }
        return withContext(dispatcher) {
            val addresses = InetAddress.getAllByName(host)
                .asSequence()
                .distinctBy { it.hostAddress }
                .toList()
            if (addresses.size > maxResolvedAddresses) {
                throw NetworkAddressLimitExceededException(host, maxResolvedAddresses)
            }
            addresses
        }
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            dispatcher.close()
            executor.shutdownNow()
        }
    }
}

/** A releasable per-origin concurrency reservation. */
public fun interface OriginConcurrencyPermit {
    /** Releases the permit; repeated calls are harmless. */
    public fun release()
}

/** Bounds concurrent page and robots requests for each logical origin. */
public interface OriginConcurrencyPolicy {
    /** Stable identifier used by transport diagnostics. */
    public val id: String

    /** Acquires one bounded origin permit, cooperatively respecting cancellation. */
    public suspend fun acquire(origin: WebOrigin, kind: WebRequestKind): OriginConcurrencyPermit
}

/**
 * Process-local per-origin semaphore policy with bounded origin cardinality.
 *
 * Idle origin entries may be evicted when the configured origin bound is full.
 * An origin with active permits is never evicted; if every entry is active,
 * acquisition fails rather than growing the map without bound.
 */
public class BoundedOriginConcurrencyPolicy(
    private val permitsPerOrigin: Int = 2,
    private val maxOrigins: Int = 256,
) : OriginConcurrencyPolicy {
    override val id: String = "bounded-origin-concurrency"

    private data class Entry(
        val semaphore: Semaphore,
        var active: Int,
        var lastUsed: Long,
    )

    private val stateLock = ReentrantLock()
    private val entries = LinkedHashMap<WebOrigin, Entry>()
    private var sequence: Long = 0

    init {
        require(permitsPerOrigin > 0) { "Origin permits must be positive" }
        require(maxOrigins > 0) { "Origin bound must be positive" }
    }

    override suspend fun acquire(origin: WebOrigin, kind: WebRequestKind): OriginConcurrencyPermit {
        val entry = stateLock.withLock {
            val current = entries[origin] ?: createEntry(origin)
            current.active += 1
            current.lastUsed = ++sequence
            current
        }
        try {
            entry.semaphore.acquire()
        } catch (cancelled: Throwable) {
            releaseEntry(origin, entry)
            throw cancelled
        }
        val released = AtomicBoolean(false)
        return OriginConcurrencyPermit {
            if (released.compareAndSet(false, true)) {
                entry.semaphore.release()
                releaseEntry(origin, entry)
            }
        }
    }

    private fun createEntry(origin: WebOrigin): Entry {
        if (entries.size >= maxOrigins) {
            val evictable = entries.entries
                .filter { it.value.active == 0 }
                .minByOrNull { it.value.lastUsed }
                ?: throw OriginCapacityExceededException("No idle origin slot is available for $origin")
            entries.remove(evictable.key)
        }
        return Entry(Semaphore(permitsPerOrigin), active = 0, lastUsed = ++sequence)
            .also { entries[origin] = it }
    }

    private fun releaseEntry(origin: WebOrigin, entry: Entry) {
        stateLock.withLock {
            if (entries[origin] === entry) {
                entry.active = (entry.active - 1).coerceAtLeast(0)
                entry.lastUsed = ++sequence
            }
        }
    }

}

/** No-op origin concurrency for a caller that supplies its own limit. */
public object NoOriginConcurrencyPolicy : OriginConcurrencyPolicy {
    override val id: String = "no-origin-concurrency"

    override suspend fun acquire(origin: WebOrigin, kind: WebRequestKind): OriginConcurrencyPermit =
        OriginConcurrencyPermit { }
}

/** A releasable pacing reservation. */
public fun interface OriginPacingReservation {
    /** Releases the reservation; repeated calls are harmless. */
    public fun release()
}

/** Origin pacing contract, including response and robots-delay observation. */
public interface OriginPacingPolicy {
    /** Stable identifier used by transport diagnostics. */
    public val id: String

    /** Reserves the next bounded request start for [origin]. */
    public suspend fun acquire(origin: WebOrigin, kind: WebRequestKind): OriginPacingReservation

    /** Observes server pacing hints without retrying the response. */
    public fun observe(origin: WebOrigin, response: CrawlResponse)

    /** Merges a robots crawl delay with the configured origin delay. */
    public fun observeRobotsDelay(origin: WebOrigin, delay: Duration)
}

/**
 * Fixed-delay origin pacing with bounded state and explicit long-wait failure.
 *
 * A reservation reserves a start slot and is intentionally not held across
 * redirect processing. Cancellation during the suspending delay leaves a safe
 * gap before the next request; it never creates an extra request.
 */
public class FixedOriginPacingPolicy(
    private val minimumDelay: Duration = Duration.ofMillis(250),
    private val maxWait: Duration = Duration.ofMinutes(5),
    private val maxOrigins: Int = 256,
    private val clock: Clock = SystemClock,
    private val retryAfterPolicy: RetryAfterPolicy,
) : OriginPacingPolicy {
    override val id: String = "fixed-origin-pacing"

    private data class Entry(
        var nextAllowed: Instant,
        var effectiveDelay: Duration,
        var active: Int,
        var lastUsed: Long,
    )

    private val stateLock = ReentrantLock()
    private val entries = LinkedHashMap<WebOrigin, Entry>()
    private var sequence: Long = 0

    init {
        require(!minimumDelay.isNegative) { "Origin minimum delay must not be negative" }
        require(!maxWait.isNegative && !maxWait.isZero) { "Maximum pacing wait must be positive" }
        require(maxOrigins > 0) { "Origin pacing bound must be positive" }
    }

    override suspend fun acquire(origin: WebOrigin, kind: WebRequestKind): OriginPacingReservation {
        val now = clock.now()
        val (entry, waitUntil) = stateLock.withLock {
            val entry = entries[origin] ?: createEntry(origin, now)
            val start = if (entry.nextAllowed.isAfter(now)) entry.nextAllowed else now
            val wait = Duration.between(now, start)
            if (wait > maxWait) {
                throw PacingWaitExceededException(origin, wait, maxWait)
            }
            entry.nextAllowed = start.plus(entry.effectiveDelay)
            entry.active += 1
            entry.lastUsed = ++sequence
            entry to start
        }
        val wait = Duration.between(now, waitUntil)
        try {
            if (!wait.isZero) {
                delay(wait.toMillis().coerceAtLeast(1))
            }
        } catch (cancelled: Throwable) {
            release(origin, entry)
            throw cancelled
        }
        val released = AtomicBoolean(false)
        return OriginPacingReservation {
            if (released.compareAndSet(false, true)) release(origin, entry)
        }
    }

    override fun observe(origin: WebOrigin, response: CrawlResponse) {
        val delay = retryAfterPolicy.delay(response, clock.now()) ?: return
        stateLock.withLock {
            val entry = entries[origin] ?: return@withLock
            val retryUntil = clock.now().plus(delay)
            if (retryUntil.isAfter(entry.nextAllowed)) entry.nextAllowed = retryUntil
            entry.lastUsed = ++sequence
        }
    }

    override fun observeRobotsDelay(origin: WebOrigin, delay: Duration) {
        require(!delay.isNegative) { "Robots crawl delay must not be negative" }
        stateLock.withLock {
            val entry = entries[origin] ?: createEntry(origin, clock.now())
            if (delay > entry.effectiveDelay) entry.effectiveDelay = delay
            entry.lastUsed = ++sequence
        }
    }

    private fun release(origin: WebOrigin, entry: Entry) {
        stateLock.withLock {
            if (entries[origin] === entry) {
                entry.active = (entry.active - 1).coerceAtLeast(0)
                entry.lastUsed = ++sequence
            }
        }
    }

    private fun createEntry(origin: WebOrigin, now: Instant): Entry {
        if (entries.size >= maxOrigins) {
            val evictable = entries.entries
                .filter { it.value.active == 0 }
                .minByOrNull { it.value.lastUsed }
                ?: throw OriginCapacityExceededException("No origin pacing slot is available for $origin")
            entries.remove(evictable.key)
        }
        return Entry(now, minimumDelay, active = 0, lastUsed = ++sequence)
            .also { entries[origin] = it }
    }
}

/** No-op origin pacing for a caller that supplies its own timing policy. */
public object NoOriginPacingPolicy : OriginPacingPolicy {
    override val id: String = "no-origin-pacing"

    override suspend fun acquire(origin: WebOrigin, kind: WebRequestKind): OriginPacingReservation =
        OriginPacingReservation { }

    override fun observe(origin: WebOrigin, response: CrawlResponse) = Unit

    override fun observeRobotsDelay(origin: WebOrigin, delay: Duration) = Unit
}

/** Raised when a bounded origin map cannot admit another active origin. */
public class OriginCapacityExceededException(message: String) : IllegalStateException(message)

/** Raised when a DNS answer exceeds the resolver's bounded address set. */
public class NetworkAddressLimitExceededException(
    host: String,
    limit: Int,
) : IllegalStateException("DNS answer for '$host' exceeds configured address bound of $limit")

/** Raised instead of suspending for an unbounded server or robots delay. */
public class PacingWaitExceededException(
    origin: WebOrigin,
    wait: Duration,
    maximum: Duration,
) : IllegalStateException("Pacing wait for $origin ($wait) exceeds configured bound $maximum")

private class DaemonThreadFactory(
    private val name: String,
) : ThreadFactory {
    private var counter: Int = 0

    override fun newThread(runnable: Runnable): Thread =
        Thread(runnable, "$name-${++counter}").apply { isDaemon = true }
}
