package com.rmjtromp.trawler.policy

import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class TimeAndRandomTest {
    @Test
    fun `default random source validates positive bounds without nondeterministic assertions`() {
        assertEquals(0, DefaultRandomSource.nextInt(1))
        assertEquals(0L, DefaultRandomSource.nextLong(1))
        assertFailsWith<IllegalArgumentException> { DefaultRandomSource.nextInt(0) }
        assertFailsWith<IllegalArgumentException> { DefaultRandomSource.nextInt(-1) }
        assertFailsWith<IllegalArgumentException> { DefaultRandomSource.nextLong(0) }
        assertFailsWith<IllegalArgumentException> { DefaultRandomSource.nextLong(-1) }
    }

    @Test
    fun `default double and system clock stay within their documented ranges`() {
        repeat(16) {
            val value = DefaultRandomSource.nextDouble()
            assertTrue(value >= 0.0 && value < 1.0)
        }
        val before = java.time.Instant.now()
        val observed = SystemClock.now()
        assertTrue(!observed.isBefore(before))
        assertEquals(java.time.Instant.EPOCH, Clock { java.time.Instant.EPOCH }.now())
    }
}
