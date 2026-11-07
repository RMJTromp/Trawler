package com.rmjtromp.trawler.postgres

import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * Asserts that [actual] is [expected] or the copy kotlinx.coroutines creates
 * during stack-trace recovery (enabled when JVM assertions are on), which keeps
 * the original exception as its cause. Anything else fails, so a wrapped or
 * replaced failure is still detected.
 */
internal fun assertOriginalFailure(expected: Throwable, actual: Throwable) {
    if (actual === expected) return
    assertTrue(actual::class == expected::class, "Expected ${expected::class.simpleName} but was ${actual::class.simpleName}")
    assertSame(expected, actual.cause, "Failure is neither the original nor its coroutine-recovered copy")
}
