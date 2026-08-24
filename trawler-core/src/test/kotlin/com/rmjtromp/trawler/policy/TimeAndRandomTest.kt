package com.rmjtromp.trawler.policy

import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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
}
