package com.rmjtromp.trawler.web.policy

import com.rmjtromp.trawler.web.transport.RedirectHistory
import com.rmjtromp.trawler.web.transport.WebDestinationPolicy
import com.rmjtromp.trawler.web.transport.WebOrigin
import com.rmjtromp.trawler.web.transport.DestinationDecision
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.net.URI
import java.net.URISyntaxException
import java.time.Duration
import java.time.Instant
import java.util.Collections
import java.util.LinkedHashMap
import java.util.Locale

/** Defines what happens when robots.txt cannot be obtained or interpreted. */
public enum class RobotsUnavailableBehavior {
    ALLOW,
    DENY,
    FAIL,
}

/** Result of one bounded, engine-controlled robots request. */
public sealed interface RobotsFetchResult {
    /** A bounded body was obtained; 404 is represented as an empty rule set. */
    public data class Available(
        public val statusCode: Int,
        public val body: String,
        public val fetchedAt: Instant,
    ) : RobotsFetchResult

    /** The origin could not provide a usable robots document. */
    public data class Unavailable(public val reason: String) : RobotsFetchResult {
        init {
            require(reason.isNotBlank()) { "Robots unavailability reason must not be blank" }
        }
    }
}

/** Callback supplied by the transport for one robots fetch on its shared client. */
public fun interface RobotsFetcher {
    /** Fetches one origin's robots document without recursively checking robots. */
    public suspend fun fetch(origin: WebOrigin, userAgent: String): RobotsFetchResult
}

/** Expected robots admission outcomes for the current target. */
public sealed interface RobotsDecision {
    /** The request may proceed; an optional delay is merged into origin pacing. */
    public data class Allowed(public val crawlDelay: Duration?) : RobotsDecision {
        init {
            require(crawlDelay == null || !crawlDelay.isNegative) {
                "Robots crawl delay must not be negative"
            }
        }
    }

    /** The target is disallowed without making the page request. */
    public data class Disallowed(public val reason: String) : RobotsDecision {
        init {
            require(reason.isNotBlank()) { "Robots rejection reason must not be blank" }
        }
    }

    /** The configured unavailable behavior is `FAIL`; no page request is made. */
    public data class Unavailable(public val reason: String) : RobotsDecision {
        init {
            require(reason.isNotBlank()) { "Robots failure reason must not be blank" }
        }
    }
}

/** Robots contract evaluated before every page redirect hop. */
public interface RobotsPolicy {
    /** Stable identifier used by transport diagnostics. */
    public val id: String

    /** Evaluates the target through the supplied shared-client fetcher. */
    public suspend fun evaluate(target: String, fetcher: RobotsFetcher): RobotsDecision
}

/** Explicit opt-out policy for trusted callers that do not select robots enforcement. */
public object NoRobotsPolicy : RobotsPolicy {
    override val id: String = "no-robots-policy"

    override suspend fun evaluate(target: String, fetcher: RobotsFetcher): RobotsDecision =
        RobotsDecision.Allowed(crawlDelay = null)
}

/** Configuration for bounded robots caching and rule handling. */
public data class RobotsPolicyConfig(
    public val userAgent: String = "TrawlerBot/0.1",
    public val cacheTtl: Duration = Duration.ofHours(1),
    public val maxEntries: Int = 256,
    public val maxBodyBytes: Int = 128 * 1024,
    public val unavailableBehavior: RobotsUnavailableBehavior = RobotsUnavailableBehavior.DENY,
    public val maxCrawlDelay: Duration = Duration.ofMinutes(5),
    public val longCrawlDelayBehavior: RobotsUnavailableBehavior = RobotsUnavailableBehavior.FAIL,
) {
    init {
        require(userAgent.isNotBlank()) { "Robots user agent must not be blank" }
        require(!cacheTtl.isNegative && !cacheTtl.isZero) { "Robots cache TTL must be positive" }
        require(maxEntries > 0) { "Robots cache entry bound must be positive" }
        require(maxBodyBytes > 0) { "Robots body bound must be positive" }
        require(!maxCrawlDelay.isNegative && !maxCrawlDelay.isZero) {
            "Maximum robots crawl delay must be positive"
        }
    }
}

/**
 * Bounded, process-local robots policy with a small parser for user-agent,
 * allow/disallow, and crawl-delay directives.
 *
 * The cache owns only rules and timestamps. It never owns an HTTP client; the
 * fetcher is supplied by [com.rmjtromp.trawler.web.transport.KtorTransport],
 * which applies ordinary origin concurrency and pacing to robots requests.
 */
public class CachingRobotsPolicy(
    private val configuration: RobotsPolicyConfig = RobotsPolicyConfig(),
    private val clock: com.rmjtromp.trawler.policy.Clock = com.rmjtromp.trawler.policy.SystemClock,
) : RobotsPolicy {
    override val id: String = "cached-robots-policy"

    private data class CacheEntry(
        val loadedAt: Instant,
        val rules: ParsedRules?,
        val unavailableReason: String?,
    )

    private val mutex = Mutex()
    private val cache = LinkedHashMap<WebOrigin, CacheEntry>()

    override suspend fun evaluate(target: String, fetcher: RobotsFetcher): RobotsDecision {
        val uri = try {
            URI(target)
        } catch (_: URISyntaxException) {
            return RobotsDecision.Disallowed("target-is-not-a-uri")
        }
        val origin = try {
            WebOrigin.fromTarget(target)
        } catch (_: IllegalArgumentException) {
            return RobotsDecision.Disallowed("target-origin-is-invalid")
        }
        val entry = load(origin, fetcher)
        val unavailable = entry.unavailableReason
        if (unavailable != null) return unavailable(unavailable)
        val rules = requireNotNull(entry.rules)
        val path = buildString {
            append(uri.rawPath.takeIf { it.isNotEmpty() } ?: "/")
            uri.rawQuery?.let { append('?').append(it) }
        }
        if (!rules.allows(path)) {
            return RobotsDecision.Disallowed("robots-disallowed")
        }
        val delay = rules.crawlDelay
        if (delay != null && delay > configuration.maxCrawlDelay) {
            return when (configuration.longCrawlDelayBehavior) {
                RobotsUnavailableBehavior.ALLOW -> RobotsDecision.Allowed(null)
                RobotsUnavailableBehavior.DENY -> RobotsDecision.Disallowed("robots-crawl-delay-too-long")
                RobotsUnavailableBehavior.FAIL -> RobotsDecision.Unavailable("robots-crawl-delay-too-long")
            }
        }
        return RobotsDecision.Allowed(delay)
    }

    private suspend fun load(origin: WebOrigin, fetcher: RobotsFetcher): CacheEntry {
        val now = clock.now()
        mutex.withLock {
            cache[origin]?.let { cached ->
                if (Duration.between(cached.loadedAt, now) <= configuration.cacheTtl) return cached
                cache.remove(origin)
            }
        }
        val result = try {
            fetcher.fetch(origin, configuration.userAgent)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (proxyFailure: WebProxyFailureException) {
            // A proxy outage says nothing about the site's rules and must not be cached as if it did.
            throw proxyFailure
        } catch (error: Exception) {
            RobotsFetchResult.Unavailable("robots-fetch-failed:${error::class.simpleName ?: "failure"}")
        }
        val entry = when (result) {
            is RobotsFetchResult.Available -> {
                val bodyBytes = result.body.toByteArray(Charsets.UTF_8)
                if (bodyBytes.size > configuration.maxBodyBytes) {
                    CacheEntry(result.fetchedAt, null, "robots-body-limit")
                } else if (result.statusCode == 404 || result.statusCode in 200..299) {
                    CacheEntry(result.fetchedAt, RobotsParser.parse(result.body, configuration.userAgent), null)
                } else {
                    CacheEntry(result.fetchedAt, null, "robots-status-${result.statusCode}")
                }
            }

            is RobotsFetchResult.Unavailable -> CacheEntry(clock.now(), null, result.reason)
        }
        mutex.withLock {
            if (cache.size >= configuration.maxEntries && origin !in cache) {
                cache.entries.firstOrNull()?.let { cache.remove(it.key) }
            }
            cache[origin] = entry
        }
        return entry
    }

    private fun unavailable(reason: String): RobotsDecision = when (configuration.unavailableBehavior) {
        RobotsUnavailableBehavior.ALLOW -> RobotsDecision.Allowed(null)
        RobotsUnavailableBehavior.DENY -> RobotsDecision.Disallowed("robots-unavailable:$reason")
        RobotsUnavailableBehavior.FAIL -> RobotsDecision.Unavailable("robots-unavailable:$reason")
    }
}

private data class ParsedRule(
    val allow: Boolean,
    val pattern: String,
    val order: Int,
)

private data class ParsedRules(
    val rules: List<ParsedRule>,
    val crawlDelay: Duration?,
) {
    fun allows(path: String): Boolean {
        val matching = rules.filter { it.matches(path) }
        if (matching.isEmpty()) return true
        return matching.maxWithOrNull(
            compareBy<ParsedRule>({ it.pattern.replace("*", "").length }, { if (it.allow) 1 else 0 }, { it.order }),
        )?.allow == true
    }
}

private fun ParsedRule.matches(path: String): Boolean {
    if (pattern.isEmpty()) return true
    val endAnchored = pattern.endsWith('$')
    val body = if (endAnchored) pattern.dropLast(1) else pattern
    return wildcardMatches(if (endAnchored) body else "$body*", path)
}

/**
 * Whole-string match where `*` matches any run of characters and every other
 * character is literal. It keeps only the most recent `*` as a backtracking
 * point, so the worst case is O(pattern x path) rather than the exponential
 * backtracking a generated regex allows for hostile patterns such as `*a*a*a*b`.
 */
private fun wildcardMatches(pattern: String, path: String): Boolean {
    var patternIndex = 0
    var pathIndex = 0
    var starIndex = -1
    var starPathIndex = 0
    while (pathIndex < path.length) {
        if (patternIndex < pattern.length && pattern[patternIndex] == '*') {
            starIndex = patternIndex++
            starPathIndex = pathIndex
        } else if (patternIndex < pattern.length && pattern[patternIndex] == path[pathIndex]) {
            patternIndex++
            pathIndex++
        } else if (starIndex >= 0) {
            patternIndex = starIndex + 1
            pathIndex = ++starPathIndex
        } else {
            return false
        }
    }
    while (patternIndex < pattern.length && pattern[patternIndex] == '*') patternIndex++
    return patternIndex == pattern.length
}

private object RobotsParser {
    private data class Group(
        val agents: MutableList<String> = mutableListOf(),
        val rules: MutableList<ParsedRule> = mutableListOf(),
        var crawlDelay: Duration? = null,
        var hasDirective: Boolean = false,
    )

    fun parse(body: String, userAgent: String): ParsedRules {
        val groups = mutableListOf<Group>()
        var current: Group? = null
        var order = 0
        body.lineSequence().forEach { rawLine ->
            val line = rawLine.substringBefore('#').trim()
            if (line.isEmpty()) return@forEach
            val separator = line.indexOf(':')
            if (separator <= 0) return@forEach
            val key = line.substring(0, separator).trim().lowercase(Locale.ROOT)
            val value = line.substring(separator + 1).trim()
            when (key) {
                "user-agent" -> {
                    if (current == null || current.hasDirective) {
                        current = Group().also(groups::add)
                    }
                    if (value.isNotEmpty()) current.agents += value.lowercase(Locale.ROOT)
                }

                "allow", "disallow", "crawl-delay" -> {
                    if (current == null) current = Group(agents = mutableListOf("*"), hasDirective = true).also(groups::add)
                    current.hasDirective = true
                    when (key) {
                        "allow", "disallow" -> if (value.isNotEmpty() || key == "allow") {
                            current.rules += ParsedRule(key == "allow", value, order++)
                        }

                        "crawl-delay" -> parseDelay(value)?.let { current.crawlDelay = it }
                    }
                }
            }
        }

        val normalizedAgent = userAgent.lowercase(Locale.ROOT)
        val scored = groups.mapNotNull { group ->
            val score = group.agents.maxOfOrNull { agent ->
                when {
                    agent == "*" -> 0
                    normalizedAgent.startsWith(agent) -> agent.length
                    normalizedAgent.contains(agent) -> agent.length
                    else -> -1
                }
            } ?: -1
            if (score >= 0) group to score else null
        }
        val selectedScore = scored.maxOfOrNull { it.second } ?: 0
        val selected = scored.filter { it.second == selectedScore }.map { it.first }
        return ParsedRules(
            rules = Collections.unmodifiableList(selected.flatMap { it.rules }),
            crawlDelay = selected.mapNotNull { it.crawlDelay }.maxOrNull(),
        )
    }

    private fun parseDelay(value: String): Duration? = try {
        val seconds = value.toBigDecimal()
        val millis = seconds.multiply(java.math.BigDecimal.valueOf(1_000L))
        when {
            seconds.signum() < 0 -> null
            // Saturate instead of letting BigDecimal.toLong() keep only the low 64 bits.
            millis > java.math.BigDecimal.valueOf(Long.MAX_VALUE) -> Duration.ofMillis(Long.MAX_VALUE)
            else -> Duration.ofMillis(millis.toLong())
        }
    } catch (_: ArithmeticException) {
        null
    } catch (_: NumberFormatException) {
        null
    }
}

/** Raised when a fail-closed robots decision prevents the page request. */
public class RobotsUnavailableException(reason: String) : IllegalStateException(reason)
