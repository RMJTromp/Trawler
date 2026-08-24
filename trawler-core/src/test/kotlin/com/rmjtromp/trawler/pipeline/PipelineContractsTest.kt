package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertSame
import org.junit.jupiter.api.Test

class PipelineContractsTest {
    @Test
    fun `typed decisions preserve values and distinguish expected drops`() {
        val accept = TransformDecision.Accept(7)
        val mutate = TransformDecision.Mutate(8)
        val drop = TransformDecision.Drop("filtered")
        val rejected = ValidationResult.Rejected("invalid")
        val cause = IllegalStateException("extractor failed")
        val failure = ExtractionResult.Failure(cause)
        val outputFailure = OutputResult.Failed(cause)

        assertEquals(7, accept.value)
        assertEquals(8, mutate.value)
        assertEquals("filtered", drop.reason)
        assertEquals("invalid", rejected.reason)
        assertSame(cause, failure.error)
        assertSame(cause, outputFailure.error)
        val accepted = assertIs<OutputResult.Accepted>(OutputResult.Accepted(2))
        val skipped = assertIs<OutputResult.Skipped>(OutputResult.Skipped("not selected"))
        assertEquals(2, accepted.itemCount)
        assertEquals("not selected", skipped.reason)
    }

    @Test
    fun `produced context retains the caller type and immutable attributes`() {
        val attributes = ContextAttributes.of(ContextAttribute("source", "unit"))
        val context = ProductionContext(
            pipelineId = "pipeline",
            engineRunId = "engine",
            pipelineRunId = "pipeline-run",
            requestId = "request",
            responseReference = null,
            originalTarget = "original",
            finalTarget = "final",
            producedAt = Instant.EPOCH,
            attributes = attributes,
        )
        val produced: Produced<Int> = Produced(42, context)

        assertEquals(42, produced.value)
        assertEquals("unit", produced.context.attributes["source"])
        @Suppress("UNCHECKED_CAST")
        val mutable = produced.context.attributes.entries as MutableList<com.rmjtromp.trawler.model.ContextAttribute>
        assertFailsWith<UnsupportedOperationException> {
            mutable += ContextAttribute("forbidden", "value")
        }
    }

    @Test
    fun `contract constructors validate failure reasons and counts`() {
        assertFailsWith<IllegalArgumentException> { ValidationResult.Rejected(" ") }
        assertFailsWith<IllegalArgumentException> { OutputResult.Accepted(-1) }
        assertFailsWith<IllegalArgumentException> { PipelineDefinition(" ") }
        assertFailsWith<IllegalArgumentException> { PipelineDefinition("id", " ") }
        assertFailsWith<IllegalArgumentException> {
            ProductionContext(" ", "engine", "run", null, null, null, null, Instant.EPOCH)
        }
        assertFailsWith<IllegalArgumentException> { RunFailure(" ", IllegalStateException()) }
        assertFailsWith<IllegalArgumentException> { com.rmjtromp.trawler.policy.RetryHint(true, Duration.ofNanos(-1)) }
    }

    @Test
    fun `successful extraction retains values and bounded follow-ups`() {
        val result = ExtractionResult.Success(
            values = listOf("one", "two"),
            followUps = listOf(com.rmjtromp.trawler.model.CrawlRequest("follow-up")),
        )
        assertIs<ExtractionResult.Success<String>>(result)
        assertEquals(listOf("one", "two"), result.values)
        assertEquals("follow-up", result.followUps.single().target)
    }
}
