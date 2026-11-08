package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.CursorHarvestStrategy
import com.rmjtromp.trawler.discovery.DiscoveryCheckpointCandidate
import com.rmjtromp.trawler.discovery.DiscoveryCheckpointCodec
import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.policy.BudgetDecision
import com.rmjtromp.trawler.policy.BudgetObservation
import com.rmjtromp.trawler.policy.BudgetSnapshot
import com.rmjtromp.trawler.policy.BuiltInPolicies
import com.rmjtromp.trawler.policy.CrawlBudgetPolicy
import com.rmjtromp.trawler.state.CheckpointPayload
import com.rmjtromp.trawler.state.DiscoveryCheckpointCodecDescriptor
import com.rmjtromp.trawler.state.EnqueueResult
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

/** Checkpoint-only discovery emissions: progress without work, ordering, bounds, and failure isolation. */
class EngineRuntimeCheckpointMarkerTest {
    private val clock = SettableClock(Instant.parse("2026-07-01T00:00:00Z"))
    private val descriptor = DiscoveryCheckpointCodecDescriptor("marker-cursor", 1)
    private val codec = object : DiscoveryCheckpointCodec {
        override val descriptor: DiscoveryCheckpointCodecDescriptor = this@EngineRuntimeCheckpointMarkerTest.descriptor
        override val strategyId: String = "pages"
    }

    @Test
    fun `markers advance the checkpoint without creating work or budget observations on both store paths`() = runTest {
        listOf(HookedStateStore(clock), AtomicHookedStateStore(clock)).forEach { state ->
            val observations = mutableListOf<BudgetObservation>()
            val budget = object : CrawlBudgetPolicy {
                override val id: String = "recording-budget"
                override fun evaluate(snapshot: BudgetSnapshot, observation: BudgetObservation): BudgetDecision {
                    observations += observation
                    return BudgetDecision.Continue
                }
            }
            val transport = ScriptedTransport()
            val output = CollectingOutput<String>("output")
            val discovery = ResumableTestDiscovery("pages", codec) { context ->
                if (context.checkpoint == null) {
                    listOf(
                        DiscoveryEmission.Request(CrawlRequest("page-1-item"), candidate("page-1")),
                        DiscoveryEmission.Checkpoint(candidate("page-2")),
                        DiscoveryEmission.Checkpoint(candidate("page-3")),
                    )
                } else {
                    emptyList()
                }
            }
            val pipeline = testPipeline(
                "markers",
                bodyExtractor(),
                clock,
                transport = transport,
                policies = BuiltInPolicies.ordinary(clock = clock).copy(budget = budget),
                discoveries = listOf(discovery),
                outputs = listOf(output to OutputRequirement.REQUIRED),
                harvest = CursorHarvestStrategy(),
            )
            val events = EventLog()
            val engine = testEngine(listOf(pipeline), clock, state = state, observers = listOf(events))
            try {
                val result = engine.runOnce()
                assertEquals(RunStatus.COMPLETED, result.status, state.id)
                val run = result.pipelineRuns.single()
                assertEquals(1, run.discoveryEmissions, "Markers are not work emissions")
                assertEquals(1, run.acceptedWork)
                assertEquals(listOf("page-1-item"), transport.calls.map { it.target })
                assertEquals(listOf("page-1-item"), output.values())
                assertEquals(
                    1,
                    observations.count { it is BudgetObservation.DiscoveryEmitted },
                    "Budget policies only observe work candidates",
                )
                assertEquals(
                    listOf("page-1", "page-2", "page-3", "page-3"),
                    state.checkpointWrites.map { String(it.payload.toByteArray()) },
                )
                assertEquals(listOf(0L, 1L, 2L, 3L), state.checkpointWrites.map { it.revision })
                val stored = assertNotNull(state.delegate.load("markers", "pages"))
                assertEquals(CheckpointPayload.from("page-3".toByteArray()), stored.payload)
                assertTrue(stored.completed, "A settled walk that ended with markers still completes")
                assertEquals(
                    listOf("pages", "pages", "pages", "completed:pages"),
                    events.details(CrawlerEventType.DISCOVERY_CHECKPOINTED),
                )

                assertEquals(RunStatus.COMPLETED, engine.runOnce().status)
                assertEquals(stored, discovery.contexts.last().checkpoint, "The next walk resumes after the marker")
            } finally {
                engine.shutdown()
            }
        }
    }

    @Test
    fun `a marker after a rejected emission is not applied so the resumed walk revisits the rejected item`() = runTest {
        val state = HookedStateStore(clock)
        state.enqueueHook = { request, _, _ ->
            if (request.target == "rejected") EnqueueResult.Rejected("adapter full") else null
        }
        val events = EventLog()
        val discovery = ResumableTestDiscovery("pages", codec) {
            listOf(
                DiscoveryEmission.Request(CrawlRequest("accepted"), candidate("page-1")),
                DiscoveryEmission.Request(CrawlRequest("rejected")),
                DiscoveryEmission.Checkpoint(candidate("page-2")),
            )
        }
        val pipeline = testPipeline("rejection", bodyExtractor(), clock, discoveries = listOf(discovery))
        val engine = testEngine(listOf(pipeline), clock, state = state, observers = listOf(events))
        try {
            val result = engine.runOnce()
            assertEquals(RunStatus.ABANDONED, result.status)
            val stored = assertNotNull(state.delegate.load("rejection", "pages"))
            assertEquals(CheckpointPayload.from("page-1".toByteArray()), stored.payload)
            assertEquals(false, stored.completed)
            assertEquals(listOf("page-1"), state.checkpointWrites.map { String(it.payload.toByteArray()) })
            val warnings = events.details(CrawlerEventType.CAPABILITY_WARNING)
            assertTrue("checkpoint-marker-after-rejection:pages" in warnings, "$warnings")
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `a lost marker compare-and-set reloads progress and abandons the run`() = runTest {
        val state = HookedStateStore(clock)
        state.checkpointCasHook = { _, _, _, next ->
            if (String(next.payload.toByteArray()) == "page-2") false else null
        }
        val events = EventLog()
        val discovery = ResumableTestDiscovery("pages", codec) {
            listOf(
                DiscoveryEmission.Request(CrawlRequest("item"), candidate("page-1")),
                DiscoveryEmission.Checkpoint(candidate("page-2")),
            )
        }
        val pipeline = testPipeline("conflict", bodyExtractor(), clock, discoveries = listOf(discovery))
        val engine = testEngine(listOf(pipeline), clock, state = state, observers = listOf(events))
        try {
            assertEquals(RunStatus.ABANDONED, engine.runOnce().status)
            val stored = assertNotNull(state.delegate.load("conflict", "pages"))
            assertEquals(CheckpointPayload.from("page-1".toByteArray()), stored.payload)
            assertTrue("checkpoint-revision-conflict:pages" in events.details(CrawlerEventType.CAPABILITY_WARNING))
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `markers from a non-resumable discovery or with a foreign codec fail only that discovery`() = runTest {
        val state = HookedStateStore(clock)
        val plain = testPipeline(
            "plain",
            bodyExtractor(),
            clock,
            discoveries = listOf(ListDiscovery("plain") { listOf(DiscoveryEmission.Checkpoint(candidate("x"))) }),
        )
        val foreign = testPipeline(
            "foreign",
            bodyExtractor(),
            clock,
            discoveries = listOf(
                ResumableTestDiscovery("pages", codec) {
                    listOf(
                        DiscoveryEmission.Checkpoint(
                            DiscoveryCheckpointCandidate(
                                DiscoveryCheckpointCodecDescriptor("other", 1),
                                "pages",
                                CheckpointPayload.from("x".toByteArray()),
                            ),
                        ),
                    )
                },
            ),
        )
        val healthy = testPipeline("healthy", bodyExtractor(), clock, discoveries = listOf(seeds("ok")))
        val engine = testEngine(listOf(plain, foreign, healthy), clock, state = state)
        try {
            val runs = engine.runOnce().pipelineRuns.associateBy { it.pipelineId }
            val plainRun = assertNotNull(runs["plain"])
            assertEquals(RunStatus.FAILED, plainRun.status)
            assertEquals("discovery:plain", plainRun.failures.single().stage)
            assertEquals("Checkpoint marker requires a resumable discovery", plainRun.failures.single().error.message)
            val foreignRun = assertNotNull(runs["foreign"])
            assertEquals(RunStatus.FAILED, foreignRun.status)
            assertEquals(
                "Discovery checkpoint candidate codec does not match its discovery codec",
                foreignRun.failures.single().error.message,
            )
            assertNull(state.delegate.load("foreign", "pages"))
            assertEquals(RunStatus.COMPLETED, assertNotNull(runs["healthy"]).status)
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `markers count toward the per-discovery emission bound`() = runTest {
        val state = HookedStateStore(clock)
        val events = EventLog()
        val discovery = ResumableTestDiscovery("pages", codec) {
            (1..3).map { DiscoveryEmission.Checkpoint(candidate("page-$it")) }
        }
        val pipeline = testPipeline("bounded", bodyExtractor(), clock, discoveries = listOf(discovery))
        val engine = testEngine(
            listOf(pipeline),
            clock,
            state = state,
            observers = listOf(events),
            options = EngineRuntimeOptions(maxDiscoveryEmissionsPerDiscovery = 2),
        )
        try {
            assertEquals(RunStatus.ABANDONED, engine.runOnce().status)
            val stored = assertNotNull(state.delegate.load("bounded", "pages"))
            assertEquals(CheckpointPayload.from("page-2".toByteArray()), stored.payload)
            assertEquals(false, stored.completed)
            assertTrue(
                "budget:discovery-per-discovery-bound" in events.details(CrawlerEventType.CAPABILITY_WARNING),
            )
        } finally {
            engine.shutdown()
        }
    }

    private fun candidate(value: String): DiscoveryCheckpointCandidate =
        DiscoveryCheckpointCandidate(descriptor, "pages", CheckpointPayload.from(value.toByteArray()))
}
