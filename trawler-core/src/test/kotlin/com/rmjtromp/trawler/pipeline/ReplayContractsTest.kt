package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.response.ResponseReplayQuery
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import org.junit.jupiter.api.Test

class ReplayContractsTest {
    private val clock = SettableClock(Instant.parse("2026-09-01T00:00:00Z"))
    private val pipeline = testPipeline("replayable", bodyExtractor(), clock)

    @Test
    fun `replay selections bind the exact pipeline and require a positive limit`() {
        val query = ResponseReplayQuery.ByPipelineId("replayable")
        val selection = pipeline.replay(query)
        assertSame(pipeline, selection.pipeline)
        assertSame(query, selection.query)
        assertEquals(1_024, selection.limit)
        assertEquals(1, pipeline.replay(query, limit = 1).limit)
        listOf(0, -1).forEach { limit ->
            val error = assertFailsWith<IllegalArgumentException> { ReplaySelection(pipeline, query, limit) }
            assertEquals("Replay response limit must be positive", error.message)
        }
    }

    @Test
    fun `replay results are terminal, non-negative, and snapshot their failures`() {
        val failures = mutableListOf(RunFailure("replay", IllegalStateException("boom")))
        val result = result(failures = failures)
        failures.clear()
        assertEquals(1, result.failures.size, "The failure list is an immutable snapshot")
        assertFailsWith<UnsupportedOperationException> { (result.failures as MutableList<RunFailure>).clear() }

        val invalid = listOf(
            { result(pipelineId = " ") } to "Replay pipeline ID must not be blank",
            { result(responsesRead = -1) } to "Replay response count must not be negative",
            { result(producedValues = -1) } to "Replay produced count must not be negative",
            { result(deliveredValues = -1) } to "Replay delivered count must not be negative",
            { result(status = RunStatus.RUNNING) } to "Replay result must be terminal",
        )
        invalid.forEach { (build, message) ->
            assertEquals(message, assertFailsWith<IllegalArgumentException> { build() }.message)
        }
        RunStatus.entries.filterNot { it == RunStatus.RUNNING }.forEach { status ->
            assertEquals(status, result(status = status).status)
        }
        assertEquals(0, result(responsesRead = 0, producedValues = 0, deliveredValues = 0).responsesRead)
    }

    private fun result(
        pipelineId: String = "replayable",
        responsesRead: Long = 1,
        producedValues: Long = 1,
        deliveredValues: Long = 1,
        status: RunStatus = RunStatus.COMPLETED,
        failures: List<RunFailure> = emptyList(),
    ): ReplayResult = ReplayResult(
        pipelineId = pipelineId,
        responsesRead = responsesRead,
        producedValues = producedValues,
        deliveredValues = deliveredValues,
        status = status,
        failures = failures,
        startedAt = clock.current,
        completedAt = clock.current,
    )
}
