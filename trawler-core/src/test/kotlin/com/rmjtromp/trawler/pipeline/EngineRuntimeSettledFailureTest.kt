package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.DiscoveryCheckpointCandidate
import com.rmjtromp.trawler.discovery.DiscoveryCheckpointCodec
import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.discovery.FixedOverlapBoundaryPolicy
import com.rmjtromp.trawler.discovery.HarvestCompletion
import com.rmjtromp.trawler.discovery.HarvestCompletionContext
import com.rmjtromp.trawler.discovery.HarvestRunOutcome
import com.rmjtromp.trawler.discovery.HarvestStrategy
import com.rmjtromp.trawler.discovery.SettledItemFailurePolicy
import com.rmjtromp.trawler.discovery.WatermarkHarvestStrategy
import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.policy.BuiltInPolicies
import com.rmjtromp.trawler.policy.FailureClassification
import com.rmjtromp.trawler.policy.FailureClassifier
import com.rmjtromp.trawler.policy.FailureContext
import com.rmjtromp.trawler.state.CheckpointPayload
import com.rmjtromp.trawler.state.DiscoveryCheckpointCodecDescriptor
import java.nio.ByteBuffer
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

/**
 * Engine classification of settled item failures for harvest progress, and
 * watermark boundaries that follow the searched window rather than run time.
 */
class EngineRuntimeSettledFailureTest {
    private val clock = SettableClock(Instant.parse("2026-08-01T06:00:00Z"))
    private val codec = object : DiscoveryCheckpointCodec {
        override val descriptor: DiscoveryCheckpointCodecDescriptor = DiscoveryCheckpointCodecDescriptor("feed-cursor", 1)
        override val strategyId: String = "feed"
    }

    @Test
    fun `a permanent item failure commits the watermark only when the strategy allows settled failures`() = runTest {
        val allowed = settledCase(SettledItemFailurePolicy.ALLOWS_COMPLETION, BadItem.PERMANENT_EXTRACTION)
        assertEquals(RunStatus.FAILED, allowed.status, "The run still reports the failed item")
        assertEquals("extraction", allowed.run.failures.single().stage)
        val completion = allowed.completions.single()
        assertEquals(HarvestRunOutcome.COMPLETED_WITH_SETTLED_ITEM_FAILURES, completion.runOutcome)
        assertEquals(1, completion.settledItemFailures)
        assertEquals(false, completion.completionPredicatePassed)
        assertNotNull(allowed.state.delegate.loadHarvestState("settled", "watermark"))
        assertTrue(assertNotNull(allowed.state.delegate.load("settled", "feed")).completed)

        val blocked = settledCase(SettledItemFailurePolicy.BLOCKS_COMPLETION, BadItem.PERMANENT_EXTRACTION)
        assertEquals(RunStatus.FAILED, blocked.status)
        assertEquals(HarvestRunOutcome.COMPLETED_WITH_SETTLED_ITEM_FAILURES, blocked.completions.single().runOutcome)
        assertNull(blocked.state.delegate.loadHarvestState("settled", "watermark"))
        assertEquals(false, assertNotNull(blocked.state.delegate.load("settled", "feed")).completed)
        assertTrue("harvest-incomplete:watermark boundary is not settled" in blocked.warnings, "${blocked.warnings}")
    }

    @Test
    fun `exhausted transient failures and required output failures never become settled`() = runTest {
        listOf(BadItem.TRANSIENT_TRANSPORT, BadItem.REQUIRED_OUTPUT).forEach { bad ->
            val case = settledCase(SettledItemFailurePolicy.ALLOWS_COMPLETION, bad)
            assertEquals(RunStatus.FAILED, case.status, "$bad")
            val completion = case.completions.single()
            assertEquals(HarvestRunOutcome.FAILED, completion.runOutcome, "$bad")
            assertEquals(0, completion.settledItemFailures, "$bad")
            assertNull(case.state.delegate.loadHarvestState("settled", "watermark"), "$bad")
            assertEquals(false, assertNotNull(case.state.delegate.load("settled", "feed")).completed, "$bad")
        }
    }

    @Test
    fun `the engine still rejects a commit for a failed run even when a strategy returns one`() = runTest {
        val alwaysCommit = object : HarvestStrategy by WatermarkHarvestStrategy(FixedOverlapBoundaryPolicy(Duration.ZERO)) {
            override fun complete(context: HarvestCompletionContext): HarvestCompletion = HarvestCompletion.Commit(null)
        }
        val case = settledCase(SettledItemFailurePolicy.ALLOWS_COMPLETION, BadItem.TRANSIENT_TRANSPORT, alwaysCommit)
        assertEquals(RunStatus.FAILED, case.status)
        assertTrue("harvest-commit-rejected-by-engine" in case.warnings, "${case.warnings}")
        assertEquals(false, assertNotNull(case.state.delegate.load("settled", "feed")).completed)
    }

    @Test
    fun `the committed watermark is the searched window end even when work outlasts the overlap`() = runTest {
        val overlap = Duration.ofMinutes(1)
        val workDuration = Duration.ofMinutes(3)
        val state = AtomicHookedStateStore(clock)
        val transport = ScriptedTransport { request ->
            clock.current = clock.current.plus(workDuration)
            okResponse(request)
        }
        val discovery = ResumableTestDiscovery("feed", codec) {
            listOf(DiscoveryEmission.Request(CrawlRequest("item-${it.incrementalWindow?.startInclusive}"), candidate()))
        }
        val pipeline = testPipeline(
            "slow",
            bodyExtractor(),
            clock,
            transport = transport,
            discoveries = listOf(discovery),
            harvest = WatermarkHarvestStrategy(FixedOverlapBoundaryPolicy(overlap)),
        )
        val engine = testEngine(listOf(pipeline), clock, state = state)
        try {
            val searchedUntil = clock.current
            assertEquals(RunStatus.COMPLETED, engine.runOnce().status)
            assertEquals(searchedUntil.plus(workDuration), clock.current, "The run outlasted the overlap")
            val committed = assertNotNull(state.delegate.loadHarvestState("slow", "watermark"))
            assertEquals(searchedUntil.toEpochMilli(), ByteBuffer.wrap(committed.payload.toByteArray()).long)

            assertEquals(RunStatus.COMPLETED, engine.runOnce().status)
            val nextWindow = assertNotNull(discovery.contexts.last().incrementalWindow)
            assertEquals(
                searchedUntil.minus(overlap),
                nextWindow.startInclusive,
                "Changes made while the first run processed work are inside the next window",
            )
        } finally {
            engine.shutdown()
        }
    }

    private enum class BadItem { PERMANENT_EXTRACTION, TRANSIENT_TRANSPORT, REQUIRED_OUTPUT }

    private class TransientTestException : RuntimeException("temporarily unavailable")

    private class SettledCase(
        val status: RunStatus,
        val run: PipelineRunResult,
        val completions: List<HarvestCompletionContext>,
        val state: AtomicHookedStateStore,
        val warnings: List<String?>,
    )

    private class RecordingStrategy(private val delegate: HarvestStrategy) : HarvestStrategy by delegate {
        val completions: MutableList<HarvestCompletionContext> = mutableListOf()

        override fun complete(context: HarvestCompletionContext): HarvestCompletion {
            completions += context
            return delegate.complete(context)
        }
    }

    private suspend fun settledCase(
        policy: SettledItemFailurePolicy,
        bad: BadItem,
        strategy: HarvestStrategy = WatermarkHarvestStrategy(
            FixedOverlapBoundaryPolicy(Duration.ZERO),
            settledItemFailures = policy,
        ),
    ): SettledCase {
        val state = AtomicHookedStateStore(clock)
        val recording = RecordingStrategy(strategy)
        val classifier = object : FailureClassifier {
            override val id: String = "test-classifier"
            override fun classify(error: Throwable, context: FailureContext): FailureClassification =
                FailureClassification(error::class.simpleName ?: "failure", retryableByDefault = error is TransientTestException)
        }
        val transport = ScriptedTransport { request ->
            if (bad == BadItem.TRANSIENT_TRANSPORT && request.target == "bad") throw TransientTestException()
            okResponse(request)
        }
        val extractor = LambdaExtractor<String>("extractor") { response, _ ->
            if (bad == BadItem.PERMANENT_EXTRACTION && response.request.target == "bad") {
                ExtractionResult.Failure(IllegalArgumentException("unparseable item"))
            } else {
                ExtractionResult.Success(listOf(response.body.utf8()))
            }
        }
        val output = CollectingOutput<String>("output") { items ->
            if (bad == BadItem.REQUIRED_OUTPUT && items.any { it.value == "bad" }) {
                OutputResult.Failed(IllegalStateException("database unavailable"))
            } else {
                OutputResult.Accepted(items.size)
            }
        }
        val discovery = ResumableTestDiscovery("feed", codec) {
            listOf(
                DiscoveryEmission.Request(CrawlRequest("good"), candidate()),
                DiscoveryEmission.Request(CrawlRequest("bad"), candidate()),
            )
        }
        val pipeline = testPipeline(
            "settled",
            extractor,
            clock,
            transport = transport,
            policies = BuiltInPolicies.ordinary(clock = clock).copy(failureClassifier = classifier),
            discoveries = listOf(discovery),
            outputs = listOf(output to OutputRequirement.REQUIRED),
            harvest = recording,
        )
        val events = EventLog()
        val engine = testEngine(listOf(pipeline), clock, state = state, observers = listOf(events))
        try {
            val result = engine.runOnce()
            return SettledCase(
                status = result.status,
                run = result.pipelineRuns.single(),
                completions = recording.completions.toList(),
                state = state,
                warnings = events.details(CrawlerEventType.CAPABILITY_WARNING),
            )
        } finally {
            engine.shutdown()
        }
    }

    private fun candidate(): DiscoveryCheckpointCandidate =
        DiscoveryCheckpointCandidate(codec.descriptor, codec.strategyId, CheckpointPayload.from("c".toByteArray()))
}
