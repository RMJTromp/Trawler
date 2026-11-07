package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.CursorHarvestStrategy
import com.rmjtromp.trawler.discovery.DiscoveryCheckpointCandidate
import com.rmjtromp.trawler.discovery.DiscoveryCheckpointCodec
import com.rmjtromp.trawler.discovery.DiscoveryContext
import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.discovery.ResumableDiscovery
import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.state.AtomicEnqueueCheckpointResult
import com.rmjtromp.trawler.state.CheckpointPayload
import com.rmjtromp.trawler.state.CheckpointResolution
import com.rmjtromp.trawler.state.DiscoveryCheckpoint
import com.rmjtromp.trawler.state.DiscoveryCheckpointCodecDescriptor
import com.rmjtromp.trawler.state.EnqueueResult
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

/** Engine-level resumable discovery: checkpoint admission, resolution, and completion. */
class EngineRuntimeCheckpointTest {
    private val clock = SettableClock(Instant.parse("2026-05-01T00:00:00Z"))
    private val descriptor = DiscoveryCheckpointCodecDescriptor("test-cursor", 2)

    @Test
    fun `atomic admission advances checkpoints only with accepted work and completion is committed after the walk settles`() = runTest {
        val state = AtomicHookedStateStore(clock)
        val events = EventLog()
        val output = CollectingOutput<String>("output")
        val discovery = ResumableTestDiscovery("pages", codec()) { context ->
            if (context.checkpoint == null) {
                listOf(
                    DiscoveryEmission.Request(CrawlRequest("page-1"), candidate("cursor-1")),
                    DiscoveryEmission.Request(CrawlRequest("page-2"), candidate("cursor-2")),
                )
            } else {
                emptyList()
            }
        }
        val pipeline = testPipeline(
            "resumable",
            bodyExtractor(),
            clock,
            discoveries = listOf(discovery),
            outputs = listOf(output to OutputRequirement.REQUIRED),
            harvest = CursorHarvestStrategy(),
        )
        val engine = testEngine(listOf(pipeline), clock, state = state, observers = listOf(events))
        try {
            val result = engine.runOnce()
            assertEquals(RunStatus.COMPLETED, result.status)
            assertEquals(listOf("page-1", "page-2"), output.values())
            assertEquals(listOf(0L, 1L, 2L), state.checkpointWrites.map { it.revision })
            assertEquals(listOf(false, false, true), state.checkpointWrites.map { it.completed })
            val stored = assertNotNull(state.delegate.load("resumable", "pages"))
            assertEquals(2, stored.revision)
            assertTrue(stored.completed)
            assertEquals(CheckpointPayload.from("cursor-2".toByteArray()), stored.payload)
            assertEquals("test-cursor", stored.codecId)
            assertEquals(2, stored.codecVersion)
            assertEquals("cursor-mode", stored.strategyId)
            assertEquals("generation-1", stored.generationId)
            assertEquals(
                listOf("pages", "pages", "completed:pages"),
                events.details(CrawlerEventType.DISCOVERY_CHECKPOINTED),
            )

            val second = engine.runOnce()
            assertEquals(RunStatus.COMPLETED, second.status)
            assertEquals(stored, discovery.contexts.last().checkpoint, "The next walk resumes from the stored checkpoint")
            assertEquals(3, state.checkpointWrites.size, "An already-completed checkpoint is not rewritten")
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `atomic revision conflicts and capacity rejections leave work and checkpoint untouched`() = runTest {
        val state = AtomicHookedStateStore(clock)
        val outcomes = ArrayDeque(
            listOf(
                AtomicEnqueueCheckpointResult.RevisionConflict,
                AtomicEnqueueCheckpointResult.Rejected("queue is full"),
            ),
        )
        state.atomicHook = { _, _ -> outcomes.removeFirst() }
        val events = EventLog()
        val transport = ScriptedTransport()
        val discovery = ResumableTestDiscovery("pages", codec()) {
            listOf(
                DiscoveryEmission.Request(CrawlRequest("conflict"), candidate("c1")),
                DiscoveryEmission.Request(CrawlRequest("rejected"), candidate("c2")),
            )
        }
        val pipeline = testPipeline(
            "atomic",
            bodyExtractor(),
            clock,
            transport = transport,
            discoveries = listOf(discovery),
            harvest = CursorHarvestStrategy(),
        )
        val engine = testEngine(listOf(pipeline), clock, state = state, observers = listOf(events))
        try {
            val result = engine.runOnce()
            assertEquals(RunStatus.ABANDONED, result.status)
            val run = result.pipelineRuns.single()
            assertEquals(0, run.acceptedWork)
            assertEquals(2, run.discoveryEmissions)
            assertTrue(transport.calls.isEmpty())
            assertNull(state.delegate.load("atomic", "pages"))
            val warnings = events.details(CrawlerEventType.CAPABILITY_WARNING)
            assertTrue("checkpoint-revision-conflict:pages" in warnings, "$warnings")
            assertTrue("work-rejected:queue is full" in warnings, "$warnings")
            assertTrue("harvest-incomplete:cursor walk is not settled" in warnings, "$warnings")
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `non-atomic checkpoint stores advance after enqueue, reload after a lost compare-and-set, and skip rejected work`() = runTest {
        val state = HookedStateStore(clock)
        var casCalls = 0
        state.checkpointCasHook = { _, _, _, _ ->
            casCalls++
            if (casCalls == 2) false else null
        }
        var enqueueCalls = 0
        state.enqueueHook = { _, _, _ ->
            enqueueCalls++
            if (enqueueCalls == 5) EnqueueResult.Rejected("adapter full") else null
        }
        val events = EventLog()
        val transport = ScriptedTransport()
        val output = CollectingOutput<String>("output")
        val discovery = ResumableTestDiscovery("pages", codec()) {
            listOf(
                DiscoveryEmission.Request(CrawlRequest("first"), candidate("c1")),
                DiscoveryEmission.Request(CrawlRequest("second"), candidate("c2")),
                DiscoveryEmission.ExistingResponse(
                    CrawlRequest("prefetched"),
                    okResponse(CrawlRequest("prefetched"), body = "prefetched-body"),
                    candidate("c3"),
                ),
                DiscoveryEmission.Request(CrawlRequest("first"), candidate("c4")),
                DiscoveryEmission.Request(CrawlRequest("rejected"), candidate("c5")),
            )
        }
        val pipeline = testPipeline(
            "direct",
            bodyExtractor(),
            clock,
            transport = transport,
            discoveries = listOf(discovery),
            outputs = listOf(output to OutputRequirement.REQUIRED),
        )
        val engine = testEngine(listOf(pipeline), clock, state = state, observers = listOf(events))
        try {
            val result = engine.runOnce()
            assertEquals(RunStatus.ABANDONED, result.status, "A lost checkpoint race abandons the run as resumable")
            val run = result.pipelineRuns.single()
            assertEquals(3, run.acceptedWork)
            assertEquals(1, run.suppressedWork, "The duplicate active request is suppressed")
            assertEquals(listOf("first", "second"), transport.calls.map { it.target }, "Existing responses are not fetched")
            assertEquals(setOf("first", "second", "prefetched-body"), output.values().toSet())
            assertEquals(
                listOf(0L, 1L, 1L, 2L),
                state.checkpointWrites.map { it.revision },
                "After the lost race the engine reloads revision 0 and continues from it; rejected work never checkpoints",
            )
            val stored = assertNotNull(state.delegate.load("direct", "pages"))
            assertEquals(CheckpointPayload.from("c4".toByteArray()), stored.payload)
            assertTrue(!stored.completed, "An abandoned run must not complete its checkpoint")
            val warnings = events.details(CrawlerEventType.CAPABILITY_WARNING)
            assertTrue("checkpoint-revision-conflict:pages" in warnings, "$warnings")
            assertTrue("work-rejected:adapter full" in warnings, "$warnings")
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `checkpoint candidates must match the discovery codec, strategy, payload bound, and a resumable source`() = runTest {
        val state = AtomicHookedStateStore(clock)
        fun failingPipeline(id: String, discovery: com.rmjtromp.trawler.discovery.Discovery) =
            testPipeline(id, bodyExtractor(), clock, discoveries = listOf(discovery))
        val oversized = failingPipeline(
            "oversized",
            ResumableTestDiscovery("d", codec()) {
                listOf(DiscoveryEmission.Request(CrawlRequest("x"), candidate("0123456789abcdef-too-large")))
            },
        )
        val wrongCodec = failingPipeline(
            "wrong-codec",
            ResumableTestDiscovery("d", codec()) {
                listOf(
                    DiscoveryEmission.Request(
                        CrawlRequest("x"),
                        DiscoveryCheckpointCandidate(DiscoveryCheckpointCodecDescriptor("other", 1), "cursor-mode", payload("p")),
                    ),
                )
            },
        )
        val wrongStrategy = failingPipeline(
            "wrong-strategy",
            ResumableTestDiscovery("d", codec()) {
                listOf(DiscoveryEmission.Request(CrawlRequest("x"), DiscoveryCheckpointCandidate(descriptor, "other-mode", payload("p"))))
            },
        )
        val notResumable = failingPipeline(
            "not-resumable",
            ListDiscovery("d") { listOf(DiscoveryEmission.Request(CrawlRequest("x"), candidate("p"))) },
        )
        val engine = testEngine(
            listOf(oversized, wrongCodec, wrongStrategy, notResumable),
            clock,
            state = state,
            options = EngineRuntimeOptions(maxCheckpointPayloadBytes = 16),
        )
        try {
            val runs = engine.runOnce().pipelineRuns.associateBy { it.pipelineId }
            val expectations = mapOf(
                "oversized" to "Discovery checkpoint exceeds the configured payload bound",
                "wrong-codec" to "Discovery checkpoint candidate codec does not match its discovery codec",
                "wrong-strategy" to "Discovery checkpoint candidate strategy does not match its discovery codec",
                "not-resumable" to "Checkpoint candidate requires a resumable discovery",
            )
            expectations.forEach { (pipelineId, message) ->
                val run = runs.getValue(pipelineId)
                assertEquals(RunStatus.FAILED, run.status, pipelineId)
                assertEquals("discovery:d", run.failures.single().stage, pipelineId)
                assertEquals(message, run.failures.single().error.message, pipelineId)
                assertEquals(0, run.acceptedWork, "Rejected candidates release their reserved work slot: $pipelineId")
            }
            assertTrue(state.checkpointWrites.isEmpty())
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `stored checkpoints are used, migrated, reset, or quarantined explicitly before discovery`() = runTest {
        val stored = checkpoint(revision = 4, payload = "old", codecId = "legacy", version = 1)

        // Migration rewrites the checkpoint under the current codec with the next revision.
        run {
            val case = checkpointCase(stored) { CheckpointResolution.Migrated(it.copy(payload = payload("migrated"))) }
            assertEquals(RunStatus.COMPLETED, case.status)
            val migrated = assertNotNull(case.discovery.contexts.single().checkpoint)
            assertEquals(5, migrated.revision)
            assertEquals("test-cursor", migrated.codecId)
            assertEquals(2, migrated.codecVersion)
            assertEquals("cursor-mode", migrated.strategyId)
            assertEquals(payload("migrated"), migrated.payload)
            assertTrue("migrated:pages" in case.events.details(CrawlerEventType.DISCOVERY_CHECKPOINTED))
        }
        // A reset stores an empty current-codec payload.
        run {
            val case = checkpointCase(stored) { CheckpointResolution.Reset("format retired") }
            assertEquals(RunStatus.COMPLETED, case.status)
            val reset = assertNotNull(case.discovery.contexts.single().checkpoint)
            assertEquals(5, reset.revision)
            assertEquals(0, reset.payload.size)
            assertEquals("test-cursor", reset.codecId)
            assertTrue("checkpoint-reset:format retired" in case.events.details(CrawlerEventType.CAPABILITY_WARNING))
        }
        // Quarantine retains the incompatible bytes before resetting.
        run {
            val case = checkpointCase(stored, atomic = true) { CheckpointResolution.Quarantine("unknown layout") }
            assertEquals(RunStatus.COMPLETED, case.status)
            val atomicState = case.state as AtomicHookedStateStore
            assertEquals(listOf(stored to "unknown layout"), atomicState.quarantinedCheckpoints)
            assertEquals(0, assertNotNull(case.discovery.contexts.single().checkpoint).payload.size)
            assertTrue("checkpoint-quarantined:unknown layout" in case.events.details(CrawlerEventType.CAPABILITY_WARNING))
        }
        // A compatible checkpoint is passed through unchanged.
        run {
            val compatible = checkpoint(revision = 2, payload = "fine")
            val case = checkpointCase(compatible) { CheckpointResolution.Use(it) }
            assertEquals(RunStatus.COMPLETED, case.status)
            assertEquals(compatible, case.discovery.contexts.single().checkpoint)
        }

        val failures = listOf<Pair<Triple<DiscoveryCheckpoint, Boolean, ((DiscoveryCheckpoint) -> CheckpointResolution)?>, String>>(
            Triple(checkpoint(revision = 0, payload = "x".repeat(20)), false, null) to
                "Discovery checkpoint exceeds the configured payload bound",
            Triple(stored, false, { _: DiscoveryCheckpoint -> CheckpointResolution.Use(stored) }) to
                "Checkpoint codec returned an incompatible usable state",
            Triple(stored, false, { _: DiscoveryCheckpoint -> CheckpointResolution.Use(checkpoint(1, "v", version = 7)) }) to
                "Checkpoint codec returned an incompatible usable state",
            Triple(stored, false, { _: DiscoveryCheckpoint ->
                CheckpointResolution.Use(checkpoint(1, "s").copy(strategyId = "other-mode"))
            }) to "Checkpoint codec returned an incompatible usable state",
            Triple(
                checkpoint(revision = 0, payload = "ok"),
                false,
                { current: DiscoveryCheckpoint -> CheckpointResolution.Use(current.copy(payload = payload("y".repeat(20)))) },
            ) to "Resolved discovery checkpoint exceeds the configured payload bound",
            Triple(stored, false, { current: DiscoveryCheckpoint ->
                CheckpointResolution.Migrated(current.copy(payload = payload("z".repeat(20))))
            }) to "Migrated discovery checkpoint exceeds the configured payload bound",
            Triple(stored, false, { _: DiscoveryCheckpoint -> CheckpointResolution.Quarantine("no store") }) to
                "Checkpoint codec mismatch requires a quarantine-capable state store",
        )
        failures.forEach { (setup, message) ->
            val (seed, atomic, resolver) = setup
            val case = checkpointCase(seed, atomic = atomic, resolver = resolver)
            assertDiscoveryCheckpointFailure(case, message)
        }

        val races = listOf<Pair<(DiscoveryCheckpoint) -> CheckpointResolution, String>>(
            { current: DiscoveryCheckpoint -> CheckpointResolution.Migrated(current) } to
                "Checkpoint migration lost a compare-and-set race",
            { _: DiscoveryCheckpoint -> CheckpointResolution.Reset("reset") } to
                "Checkpoint reset lost a compare-and-set race",
            { _: DiscoveryCheckpoint -> CheckpointResolution.Quarantine("quarantine") } to
                "Checkpoint quarantine reset lost a compare-and-set race",
        )
        races.forEach { (resolver, message) ->
            val case = checkpointCase(stored, atomic = true, loseCas = true, resolver = resolver)
            assertDiscoveryCheckpointFailure(case, message)
        }
    }

    @Test
    fun `checkpoint completion conflicts stop later completions and keep the run resumable`() = runTest {
        val state = AtomicHookedStateStore(clock)
        state.checkpointCasHook = { _, discoveryId, _, next -> if (next.completed && discoveryId == "first") false else null }
        val events = EventLog()
        val first = ResumableTestDiscovery("first", codec()) {
            listOf(DiscoveryEmission.Request(CrawlRequest("a"), candidate("a")))
        }
        val second = ResumableTestDiscovery("second", codec()) {
            listOf(DiscoveryEmission.Request(CrawlRequest("b"), candidate("b")))
        }
        val pipeline = testPipeline("conflicts", bodyExtractor(), clock, discoveries = listOf(first, second))
        val engine = testEngine(listOf(pipeline), clock, state = state, observers = listOf(events))
        try {
            val result = engine.runOnce()
            assertEquals(RunStatus.ABANDONED, result.status)
            assertTrue("checkpoint-completion-conflict:first" in events.details(CrawlerEventType.CAPABILITY_WARNING))
            assertEquals(1, state.checkpointWrites.count { it.completed }, "The second discovery is not completed after a conflict")
            assertEquals(false, state.delegate.load("conflicts", "second")?.completed)
        } finally {
            engine.shutdown()
        }
    }

    private class CheckpointCase(
        val status: RunStatus,
        val run: PipelineRunResult,
        val discovery: ResumableTestDiscovery,
        val state: HookedStateStore,
        val events: EventLog,
    )

    private suspend fun checkpointCase(
        seed: DiscoveryCheckpoint,
        atomic: Boolean = false,
        loseCas: Boolean = false,
        resolver: ((DiscoveryCheckpoint) -> CheckpointResolution)?,
    ): CheckpointCase {
        val state = if (atomic) AtomicHookedStateStore(clock) else HookedStateStore(clock)
        assertTrue(state.delegate.compareAndSet("checkpointed", "pages", null, seed.copy(revision = 0)))
        if (seed.revision > 0) {
            var revision = 0L
            while (revision < seed.revision) {
                assertTrue(state.delegate.compareAndSet("checkpointed", "pages", revision, seed.copy(revision = revision + 1)))
                revision++
            }
        }
        if (loseCas) state.checkpointCasHook = { _, _, _, _ -> false }
        val events = EventLog()
        val discovery = ResumableTestDiscovery("pages", codec(resolver)) { emptyList() }
        val pipeline = testPipeline("checkpointed", bodyExtractor(), clock, discoveries = listOf(discovery))
        val engine = testEngine(
            listOf(pipeline),
            clock,
            state = state,
            observers = listOf(events),
            options = EngineRuntimeOptions(maxCheckpointPayloadBytes = 16),
        )
        try {
            val result = engine.runOnce()
            return CheckpointCase(result.status, result.pipelineRuns.single(), discovery, state, events)
        } finally {
            engine.shutdown()
        }
    }

    private fun assertDiscoveryCheckpointFailure(case: CheckpointCase, message: String) {
        assertEquals(RunStatus.FAILED, case.status, message)
        assertEquals("discovery-checkpoint:pages", case.run.failures.single().stage, message)
        assertEquals(message, case.run.failures.single().error.message)
        assertTrue(case.discovery.contexts.isEmpty(), "Discovery must not start with an unresolved checkpoint: $message")
        assertEquals(listOf("pages"), case.events.details(CrawlerEventType.DISCOVERY_FAILED))
    }

    private fun payload(value: String): CheckpointPayload = CheckpointPayload.from(value.toByteArray())

    private fun candidate(value: String): DiscoveryCheckpointCandidate =
        DiscoveryCheckpointCandidate(descriptor, "cursor-mode", payload(value), generationId = "generation-1")

    private fun checkpoint(
        revision: Long,
        payload: String,
        codecId: String = descriptor.id,
        version: Int = descriptor.version,
    ): DiscoveryCheckpoint = DiscoveryCheckpoint(
        revision = revision,
        codecId = codecId,
        codecVersion = version,
        payload = payload(payload),
        strategyId = "cursor-mode",
    )

    private fun codec(resolver: ((DiscoveryCheckpoint) -> CheckpointResolution)? = null): DiscoveryCheckpointCodec =
        object : DiscoveryCheckpointCodec {
            override val descriptor: DiscoveryCheckpointCodecDescriptor = this@EngineRuntimeCheckpointTest.descriptor
            override val strategyId: String = "cursor-mode"
            override fun resolve(checkpoint: DiscoveryCheckpoint): CheckpointResolution =
                resolver?.invoke(checkpoint) ?: super.resolve(checkpoint)
        }
}

internal class ResumableTestDiscovery(
    override val id: String,
    override val checkpointCodec: DiscoveryCheckpointCodec,
    private val emissions: suspend (DiscoveryContext) -> List<DiscoveryEmission>,
) : ResumableDiscovery {
    val contexts: MutableList<DiscoveryContext> = mutableListOf()

    override fun discover(context: DiscoveryContext): Flow<DiscoveryEmission> = flow {
        contexts += context
        emissions(context).forEach { emit(it) }
    }
}
