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

    @Test
    fun `production context rejects each blank identifier and accepts absent optional provenance`() {
        fun context(
            pipelineId: String = "pipeline",
            engineRunId: String = "engine",
            pipelineRunId: String = "run",
            requestId: String? = null,
            originalTarget: String? = null,
            finalTarget: String? = null,
        ) = ProductionContext(
            pipelineId = pipelineId,
            engineRunId = engineRunId,
            pipelineRunId = pipelineRunId,
            requestId = requestId,
            responseReference = null,
            originalTarget = originalTarget,
            finalTarget = finalTarget,
            producedAt = Instant.EPOCH,
        )

        val minimal = context()
        assertEquals(null, minimal.requestId)
        assertEquals(null, minimal.originalTarget)
        assertEquals(null, minimal.finalTarget)
        assertEquals(ContextAttributes.empty(), minimal.attributes)
        assertEquals("request", context(requestId = "request").requestId)

        assertEquals("Engine run ID must not be blank", assertFailsWith<IllegalArgumentException> {
            context(engineRunId = " ")
        }.message)
        assertEquals("Pipeline run ID must not be blank", assertFailsWith<IllegalArgumentException> {
            context(pipelineRunId = "")
        }.message)
        assertEquals("Request ID must not be blank when present", assertFailsWith<IllegalArgumentException> {
            context(requestId = " ")
        }.message)
        assertEquals("Original target must not be blank when present", assertFailsWith<IllegalArgumentException> {
            context(originalTarget = " ")
        }.message)
        assertEquals("Final target must not be blank when present", assertFailsWith<IllegalArgumentException> {
            context(finalTarget = " ")
        }.message)
    }

    @Test
    fun `contract defaults are explicit and optional reasons stay absent`() {
        val output = object : Output<Int> {
            override val id: String = "output"
            override suspend fun write(
                items: List<Produced<Int>>,
                context: com.rmjtromp.trawler.model.OutputContext,
            ): OutputResult = OutputResult.Accepted(items.size)
        }
        val binding = OutputBinding(output)
        assertSame(output, binding.output)
        assertEquals(OutputRequirement.REQUIRED, binding.requirement)
        assertEquals(OutputRequirement.BEST_EFFORT, OutputBinding(output, OutputRequirement.BEST_EFFORT).requirement)

        assertEquals(null, TransformDecision.Drop().reason)
        assertEquals(null, OutputResult.Skipped().reason)
        assertEquals(null, ExtractionResult.Ignore().reason)
        assertEquals(null, OutputResult.Failed(IllegalStateException()).retryHint)
        assertEquals(emptyList(), ExtractionResult.Success(listOf(1)).followUps)
        assertEquals(OutputResult.Accepted(0), OutputResult.Accepted(0))

        val definition = PipelineDefinition("pipeline")
        assertEquals(null, definition.displayName)
        assertEquals("Pipeline", PipelineDefinition("pipeline", "Pipeline").displayName)
    }
}
