package com.rmjtromp.trawler.policy

import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

/**
 * Time source used by policies and configuration-sensitive lifecycle code.
 *
 * The engine owns the reference for one configuration; implementations must
 * be thread-safe or document a narrower ownership scope. Calls are
 * non-suspending and do not acquire resources, so cancellation is handled by
 * the surrounding caller rather than by this interface.
 */
public fun interface Clock {
    /** Returns the current instant according to the selected clock. */
    public fun now(): Instant
}

/** JDK-backed wall clock for configurations that explicitly select it. */
public object SystemClock : Clock {
    override fun now(): Instant = Instant.now()
}

/**
 * Injectable source of jitter and other policy randomness.
 *
 * Randomness is an extension/policy concern, never a global engine singleton.
 * Implementations must be safe for their configured concurrency or be scoped
 * to one extension. Callers that need deterministic verification should
 * supply a fixed implementation.
 */
public interface RandomSource {
    /** Returns a random integer in `[0, boundExclusive)`. */
    public fun nextInt(boundExclusive: Int): Int

    /** Returns a random long in `[0, boundExclusive)`. */
    public fun nextLong(boundExclusive: Long): Long

    /** Returns a random double in `[0.0, 1.0)`. */
    public fun nextDouble(): Double
}

/** JDK-backed random source for configurations that explicitly select it. */
public object DefaultRandomSource : RandomSource {
    override fun nextInt(boundExclusive: Int): Int {
        require(boundExclusive > 0) { "Random integer bound must be positive" }
        return ThreadLocalRandom.current().nextInt(boundExclusive)
    }

    override fun nextLong(boundExclusive: Long): Long {
        require(boundExclusive > 0) { "Random long bound must be positive" }
        return ThreadLocalRandom.current().nextLong(boundExclusive)
    }

    override fun nextDouble(): Double = ThreadLocalRandom.current().nextDouble()
}
