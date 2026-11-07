package com.rmjtromp.trawler.retention

import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.junit.jupiter.api.Test

class RetentionTest {
    @Test
    fun `retention windows require a positive batch bound`() {
        val window = RetentionWindow(Instant.EPOCH, maxEntries = 1)
        assertEquals(1, window.maxEntries)
        assertEquals(Instant.EPOCH, window.cutoff)
        listOf(0, -1).forEach { invalid ->
            val failure = assertFailsWith<IllegalArgumentException> { RetentionWindow(Instant.EPOCH, invalid) }
            assertEquals("Retention batch must be positive", failure.message)
        }
    }

    @Test
    fun `retention summaries default protected entries to zero per independent category`() {
        val summary = RetentionSummary(RetentionCategory.RESPONSES, deleted = 3)
        assertEquals(0L, summary.protectedEntries)
        assertEquals(RetentionCategory.RESPONSES, summary.category)
        assertEquals(2L, RetentionSummary(RetentionCategory.OUTBOX_HISTORY, 1, 2).protectedEntries)
        assertEquals(7, RetentionCategory.entries.size)
    }
}
