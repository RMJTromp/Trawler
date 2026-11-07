package com.rmjtromp.trawler.event

import com.rmjtromp.trawler.model.ContextAttributes
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame
import org.junit.jupiter.api.Test

class EventsTest {
    @Test
    fun `event context identifiers are optional but never blank when present`() {
        val empty = EventContext(Instant.EPOCH)
        assertNull(empty.engineRunId)
        assertNull(empty.pipelineRunId)
        assertNull(empty.pipelineId)
        assertNull(empty.requestId)
        assertNull(empty.workItemId)
        assertEquals(ContextAttributes.empty(), empty.attributes)

        val full = EventContext(
            emittedAt = Instant.EPOCH,
            engineRunId = "engine",
            pipelineRunId = "pipeline-run",
            pipelineId = "pipeline",
            requestId = "request",
            workItemId = "work",
        )
        assertEquals("work", full.workItemId)

        val fields = listOf<Pair<String, (String) -> EventContext>>(
            "engine run ID" to { value -> EventContext(Instant.EPOCH, engineRunId = value) },
            "pipeline run ID" to { value -> EventContext(Instant.EPOCH, pipelineRunId = value) },
            "pipeline ID" to { value -> EventContext(Instant.EPOCH, pipelineId = value) },
            "request ID" to { value -> EventContext(Instant.EPOCH, requestId = value) },
            "work item ID" to { value -> EventContext(Instant.EPOCH, workItemId = value) },
        )
        fields.forEach { (label, construct) ->
            listOf("", " ", "\t\n").forEach { blank ->
                val failure = assertFailsWith<IllegalArgumentException> { construct(blank) }
                assertEquals("Event $label must not be blank when present", failure.message)
            }
        }
    }

    @Test
    fun `crawler events carry an optional bounded detail that must not be blank`() {
        val context = EventContext(Instant.EPOCH, pipelineId = "pipeline")
        val event = CrawlerEvent(CrawlerEventType.ENGINE_RUN_STARTED, context)
        assertNull(event.detail)
        assertSame(context, event.context)
        assertEquals("safe detail", CrawlerEvent(CrawlerEventType.ENGINE_RUN_FAILED, context, "safe detail").detail)
        listOf("", "  ").forEach { blank ->
            val failure = assertFailsWith<IllegalArgumentException> {
                CrawlerEvent(CrawlerEventType.ENGINE_RUN_FAILED, context, blank)
            }
            assertEquals("Event detail must not be blank when present", failure.message)
        }
    }

    @Test
    fun `observer failure results preserve the original cause`() {
        val cause = IllegalStateException("observer broke")
        assertSame(cause, ObserverResult.Failed(cause).error)
    }
}
