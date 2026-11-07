package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.DiscoveryCheckpointCandidate
import com.rmjtromp.trawler.discovery.DiscoveryCheckpointCodec
import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.discovery.FixedOverlapBoundaryPolicy
import com.rmjtromp.trawler.discovery.HarvestAdmission
import com.rmjtromp.trawler.discovery.HarvestCompletion
import com.rmjtromp.trawler.discovery.HarvestCompletionContext
import com.rmjtromp.trawler.discovery.HarvestContext
import com.rmjtromp.trawler.discovery.HarvestMode
import com.rmjtromp.trawler.discovery.HarvestRunOutcome
import com.rmjtromp.trawler.discovery.HarvestStateCandidate
import com.rmjtromp.trawler.discovery.HarvestStateCodec
import com.rmjtromp.trawler.discovery.HarvestStrategy
import com.rmjtromp.trawler.discovery.WatermarkHarvestStrategy
import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.policy.BuiltInPolicies
import com.rmjtromp.trawler.policy.CircuitBreakerDecision
import com.rmjtromp.trawler.policy.CircuitBreakerPolicy
import com.rmjtromp.trawler.policy.ConsecutiveFailureCircuitBreaker
import com.rmjtromp.trawler.policy.DiscoveryAdmissionContext
import com.rmjtromp.trawler.policy.DiscoveryAdmissionDecision
import com.rmjtromp.trawler.policy.DiscoveryAdmissionPolicy
import com.rmjtromp.trawler.policy.IncrementalBoundaryPolicy
import com.rmjtromp.trawler.policy.PipelineEligibilityContext
import com.rmjtromp.trawler.policy.PipelineEligibilityDecision
import com.rmjtromp.trawler.policy.PipelineEligibilityPolicy
import com.rmjtromp.trawler.state.CheckpointPayload
import com.rmjtromp.trawler.state.DiscoveryCheckpointCodecDescriptor
import com.rmjtromp.trawler.state.HarvestState
import com.rmjtromp.trawler.state.HarvestStateCodecDescriptor
import com.rmjtromp.trawler.state.HarvestStateResolution
import com.rmjtromp.trawler.state.PipelineEligibilityOverride
import com.rmjtromp.trawler.state.PipelineHealthSnapshot
import com.rmjtromp.trawler.state.WorkQueueMetrics
import java.nio.ByteBuffer
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

/** Harvest state, admission, eligibility, circuit, and health coordination in the engine runtime. */
class EngineRuntimeRecurringTest {
    private val clock = SettableClock(Instant.parse("2026-06-01T12:00:00Z"))
    private val checkpointCodec = object : DiscoveryCheckpointCodec {
        override val descriptor: DiscoveryCheckpointCodecDescriptor = DiscoveryCheckpointCodecDescriptor("cp", 1)
        override val strategyId: String = "walk"
    }
    private val harvestDescriptor = HarvestStateCodecDescriptor("harvest-codec", 3)

    @Test
    fun `watermark harvests commit boundary state after a settled walk and feed the next incremental window`() = runTest {
        val state = AtomicHookedStateStore(clock)
        val events = EventLog()
        val discovery = ResumableTestDiscovery("feed", checkpointCodec) { context ->
            listOf(DiscoveryEmission.Request(CrawlRequest("item-${context.incrementalWindow?.startInclusive}"), candidate()))
        }
        val strategy = WatermarkHarvestStrategy(FixedOverlapBoundaryPolicy(Duration.ofMinutes(5)))
        val pipeline = testPipeline("feed", bodyExtractor(), clock, discoveries = listOf(discovery), harvest = strategy)
        val engine = testEngine(listOf(pipeline), clock, state = state, observers = listOf(events))
        try {
            assertEquals(RunStatus.COMPLETED, engine.runOnce().status)
            val firstWindow = assertNotNull(discovery.contexts.single().incrementalWindow)
            assertNull(firstWindow.startInclusive)
            assertEquals(clock.current, firstWindow.endExclusive)
            val committed = assertNotNull(state.delegate.loadHarvestState("feed", "watermark"))
            assertEquals(0, committed.revision)
            assertEquals(clock.current.toEpochMilli(), ByteBuffer.wrap(committed.payload.toByteArray()).long)
            assertTrue(state.delegate.load("feed", "feed")!!.completed)

            val firstBoundary = clock.current
            clock.current = clock.current.plus(Duration.ofHours(1))
            assertEquals(RunStatus.COMPLETED, engine.runOnce().status)
            val secondWindow = assertNotNull(discovery.contexts.last().incrementalWindow)
            assertEquals(firstBoundary.minus(Duration.ofMinutes(5)), secondWindow.startInclusive)
            assertEquals(1, state.delegate.loadHarvestState("feed", "watermark")!!.revision)
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `stored harvest state is used, migrated, reset, or quarantined before discovery`() = runTest {
        val stored = harvestState(revision = 0, payload = "old", codecVersion = 1)

        harvestCase(stored) { HarvestStateResolution.Migrated(it.copy(payload = payload("new"))) }.also { case ->
            assertEquals(RunStatus.COMPLETED, case.status)
            val migrated = assertNotNull(case.strategy.contexts.first().state)
            assertEquals(1, migrated.revision)
            assertEquals(harvestDescriptor.version, migrated.codecVersion)
            assertEquals(payload("new"), migrated.payload)
            assertTrue("harvest-state-migrated" in case.events.details(CrawlerEventType.DISCOVERY_CHECKPOINTED))
        }
        harvestCase(stored) { HarvestStateResolution.Reset("schema retired") }.also { case ->
            assertEquals(RunStatus.COMPLETED, case.status)
            val reset = assertNotNull(case.strategy.contexts.first().state)
            assertEquals(1, reset.revision)
            assertEquals(0, reset.payload.size)
            assertEquals(false, reset.historicalComplete)
            assertTrue("harvest-state-reset:schema retired" in case.events.details(CrawlerEventType.CAPABILITY_WARNING))
        }
        harvestCase(stored, atomic = true) { HarvestStateResolution.Quarantine("unknown") }.also { case ->
            assertEquals(RunStatus.COMPLETED, case.status)
            assertEquals(listOf(stored to "unknown"), (case.state as AtomicHookedStateStore).quarantinedHarvestStates)
            assertEquals(0, assertNotNull(case.strategy.contexts.first().state).payload.size)
            assertTrue("harvest-state-quarantined:unknown" in case.events.details(CrawlerEventType.CAPABILITY_WARNING))
        }
        val compatible = harvestState(revision = 0, payload = "fine", codecVersion = harvestDescriptor.version)
        harvestCase(compatible, resolver = null).also { case ->
            assertEquals(RunStatus.COMPLETED, case.status)
            assertEquals(compatible, case.strategy.contexts.first().state)
        }

        val failures = listOf<Triple<HarvestState, ((HarvestState) -> HarvestStateResolution)?, String>>(
            Triple(harvestState(0, "x".repeat(20), harvestDescriptor.version), null, "Harvest state exceeds the configured payload bound"),
            Triple(stored, { HarvestStateResolution.Use(stored) }, "Harvest-state codec returned an incompatible usable state"),
            Triple(stored, { HarvestStateResolution.Use(compatible.copy(strategyId = "other")) }, "Harvest-state codec returned an incompatible usable state"),
            Triple(stored, { HarvestStateResolution.Use(compatible.copy(codecId = "other-codec")) }, "Harvest-state codec returned an incompatible usable state"),
            Triple(
                compatible,
                { current -> HarvestStateResolution.Use(current.copy(payload = payload("y".repeat(20)))) },
                "Resolved harvest state exceeds the configured payload bound",
            ),
            Triple(
                stored,
                { current -> HarvestStateResolution.Migrated(current.copy(payload = payload("z".repeat(20)))) },
                "Migrated harvest state exceeds the configured payload bound",
            ),
            Triple(stored, { HarvestStateResolution.Quarantine("no store") }, "Harvest-state codec mismatch requires a quarantine-capable state store"),
        )
        failures.forEach { (seed, resolver, message) ->
            assertHarvestFailure(harvestCase(seed, resolver = resolver), message)
        }
        listOf<Pair<(HarvestState) -> HarvestStateResolution, String>>(
            { current: HarvestState -> HarvestStateResolution.Migrated(current) } to "Harvest-state migration lost a compare-and-set race",
            { _: HarvestState -> HarvestStateResolution.Reset("r") } to "Harvest-state reset lost a compare-and-set race",
            { _: HarvestState -> HarvestStateResolution.Quarantine("q") } to "Harvest-state quarantine reset lost a compare-and-set race",
        ).forEach { (resolver, message) ->
            assertHarvestFailure(harvestCase(stored, atomic = true, loseCas = true, resolver = resolver), message)
        }
    }

    @Test
    fun `harvest and discovery admission deferrals keep work resumable without running discovery`() = runTest {
        val state = AtomicHookedStateStore(clock)
        state.metricsHook = { WorkQueueMetrics(openWorkCount = 3, oldestOpenWorkAt = Instant.EPOCH) }
        val events = EventLog()
        val deferredDiscovery = ResumableTestDiscovery("deferred", checkpointCodec) { error("must not run") }
        val deferringStrategy = RecordingHarvestStrategy(
            codec = null,
            admit = { HarvestAdmission.Defer("strategy waiting") },
        )
        val harvestDeferred = testPipeline(
            "harvest-deferred",
            bodyExtractor(),
            clock,
            discoveries = listOf(deferredDiscovery),
            harvest = deferringStrategy,
        )
        val admissions = mutableListOf<DiscoveryAdmissionContext>()
        val deferringAdmission = object : DiscoveryAdmissionPolicy {
            override val id: String = "deferring-admission"
            override fun decide(context: DiscoveryAdmissionContext): DiscoveryAdmissionDecision {
                admissions += context
                return DiscoveryAdmissionDecision.Defer("backlog")
            }
        }
        val admissionDiscovery = ListDiscovery("plain") { error("must not run") }
        val admissionDeferred = testPipeline(
            "admission-deferred",
            bodyExtractor(),
            clock,
            policies = BuiltInPolicies.ordinary(clock = clock).copy(admission = deferringAdmission),
            discoveries = listOf(admissionDiscovery),
        )
        val engine = testEngine(listOf(harvestDeferred, admissionDeferred), clock, state = state, observers = listOf(events))
        try {
            val result = engine.runOnce()
            assertEquals(RunStatus.ABANDONED, result.status)
            assertTrue(result.pipelineRuns.all { it.status == RunStatus.ABANDONED })
            assertTrue(deferredDiscovery.contexts.isEmpty())
            assertTrue(admissionDiscovery.contexts.isEmpty())
            val warnings = events.details(CrawlerEventType.CAPABILITY_WARNING)
            assertTrue("discovery-deferred:strategy waiting" in warnings, "$warnings")
            assertTrue("discovery-deferred:backlog" in warnings, "$warnings")
            assertEquals(3, deferringStrategy.contexts.single().openWorkCount, "Queue metrics feed the harvest context")
            val admission = admissions.single()
            assertEquals(3, admission.openWorkCount)
            assertEquals(Instant.EPOCH, admission.oldestOpenWorkAt)
            assertEquals("plain", admission.discoveryId)
            val completion = deferringStrategy.completions.single()
            assertEquals(false, completion.discoveryReachedNaturalEnd, "A deferred discovery never reaches its natural end")
            assertEquals(HarvestRunOutcome.ABANDONED, completion.runOutcome)
        } finally {
            engine.shutdown()
        }

        val plainState = HookedStateStore(clock)
        val metricsFree = mutableListOf<DiscoveryAdmissionContext>()
        val recordingAdmission = object : DiscoveryAdmissionPolicy {
            override val id: String = "recording-admission"
            override fun decide(context: DiscoveryAdmissionContext): DiscoveryAdmissionDecision {
                metricsFree += context
                return DiscoveryAdmissionDecision.Admit
            }
        }
        val plain = testPipeline(
            "plain",
            bodyExtractor(),
            clock,
            policies = BuiltInPolicies.ordinary(clock = clock).copy(admission = recordingAdmission),
            discoveries = listOf(seeds("a")),
        )
        val plainEngine = testEngine(listOf(plain), clock, state = plainState)
        try {
            assertEquals(RunStatus.COMPLETED, plainEngine.runOnce().status)
            assertEquals(0, metricsFree.single().openWorkCount, "Without queue metrics the active-work count is used")
            assertNull(metricsFree.single().oldestOpenWorkAt)
        } finally {
            plainEngine.shutdown()
        }
    }

    @Test
    fun `harvest commits are rejected for unsettled runs, missing codecs, mismatched candidates, and conflicts`() = runTest {
        val goodCandidate = HarvestStateCandidate("custom", harvestDescriptor.id, harvestDescriptor.version, payload("s"), historicalComplete = true)

        // A commit after an abandoned run is refused by the engine regardless of the strategy decision.
        harvestCommitCase(
            codec = harvestCodec(null),
            commit = goodCandidate,
            abandonRun = true,
        ).also { case ->
            assertEquals(RunStatus.ABANDONED, case.status)
            assertTrue("harvest-commit-rejected-by-engine" in case.warnings)
            assertTrue(case.state.harvestWrites.isEmpty())
            assertEquals(false, case.state.delegate.load("commit", "walk")?.completed)
        }
        harvestCommitCase(codec = null, commit = goodCandidate).also { case ->
            assertEquals(RunStatus.ABANDONED, case.status)
            assertTrue("harvest-state-codec-missing" in case.warnings)
            assertTrue(case.state.harvestWrites.isEmpty())
        }
        harvestCommitCase(codec = harvestCodec(null), commit = goodCandidate, harvestConflict = true).also { case ->
            assertEquals(RunStatus.ABANDONED, case.status)
            assertTrue("harvest-state-conflict" in case.warnings)
            assertEquals(true, case.state.delegate.load("commit", "walk")?.completed, "Checkpoints commit before harvest state")
            assertNull(case.state.delegate.loadHarvestState("commit", "custom"))
        }
        harvestCommitCase(codec = harvestCodec(null), commit = goodCandidate).also { case ->
            assertEquals(RunStatus.COMPLETED, case.status)
            val written = assertNotNull(case.state.delegate.loadHarvestState("commit", "custom"))
            assertEquals(0, written.revision)
            assertTrue(written.historicalComplete)
            assertEquals(payload("s"), written.payload)
        }

        val invalid = listOf(
            goodCandidate.copy(strategyId = "someone-else") to "Harvest candidate strategy does not match its strategy codec",
            goodCandidate.copy(codecVersion = 9) to "Harvest candidate codec does not match its strategy codec",
            goodCandidate.copy(codecId = "other-codec") to "Harvest candidate codec does not match its strategy codec",
            goodCandidate.copy(payload = payload("p".repeat(20))) to "Harvest state exceeds the configured payload bound",
        )
        invalid.forEach { (candidate, message) ->
            val case = harvestCommitCase(codec = harvestCodec(null), commit = candidate)
            assertEquals(RunStatus.FAILED, case.status, message)
            assertEquals("pipeline", case.run.failures.single().stage)
            assertEquals(message, case.run.failures.single().error.message)
            assertTrue(case.state.harvestWrites.isEmpty())
        }
    }

    @Test
    fun `operator overrides and eligibility policies disable or defer a pipeline before any work`() = runTest {
        val state = HookedStateStore(clock)
        val events = EventLog()
        val log = mutableListOf<String>()
        val transport = ScriptedTransport()
        val disabled = testPipeline(
            "disabled",
            bodyExtractor(),
            clock,
            transport = transport,
            discoveries = listOf(seeds("d")),
            extensions = listOf(LoggingExtension("disabled-ext", log)),
        )
        val contexts = mutableListOf<PipelineEligibilityContext>()
        val deferring = object : PipelineEligibilityPolicy {
            override val id: String = "deferring"
            override fun decide(context: PipelineEligibilityContext): PipelineEligibilityDecision {
                contexts += context
                return PipelineEligibilityDecision.Deferred(context.now.plusSeconds(60), "maintenance window")
            }
        }
        val deferred = testPipeline(
            "deferred",
            bodyExtractor(),
            clock,
            transport = transport,
            policies = BuiltInPolicies.ordinary(clock = clock).copy(eligibility = deferring),
            discoveries = listOf(seeds("e")),
        )
        val engine = testEngine(listOf(disabled, deferred), clock, state = state, observers = listOf(events))
        try {
            state.setEligibilityOverride("disabled", PipelineEligibilityOverride(enabled = false, reason = "operator pause"))
            state.setEligibilityOverride("deferred", PipelineEligibilityOverride(enabled = true, expiresAt = clock.current.minusSeconds(1)))
            val result = engine.runOnce()
            assertEquals(RunStatus.ABANDONED, result.status)
            assertTrue(transport.calls.isEmpty())
            assertTrue(log.isEmpty(), "Blocked pipelines do not start their extensions")
            val warnings = events.details(CrawlerEventType.CAPABILITY_WARNING)
            assertTrue("pipeline-disabled:operator pause" in warnings, "$warnings")
            assertTrue("pipeline-deferred:maintenance window" in warnings, "$warnings")
            assertNull(contexts.single().override, "An expired override is not supplied to eligibility policy")

            state.setEligibilityOverride("disabled", PipelineEligibilityOverride(enabled = false, expiresAt = clock.current.minusSeconds(1)))
            val reopened = engine.runOnce(RunRequest(pipelineIds = setOf("disabled")))
            assertEquals(RunStatus.COMPLETED, reopened.status, "An expired disable override no longer blocks the pipeline")
            assertEquals(listOf("d"), transport.calls.map { it.target })
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `pipeline health is recorded per run, opens the circuit after failures, and reports compare-and-set conflicts`() = runTest {
        val state = HookedStateStore(clock)
        val events = EventLog()
        var failNext = true
        val transport = ScriptedTransport { request ->
            if (failNext) throw IllegalStateException("upstream down") else okResponse(request)
        }
        val circuit = ConsecutiveFailureCircuitBreaker(failureThreshold = 2, coolDown = Duration.ofMinutes(10))
        val pipeline = testPipeline(
            "health",
            bodyExtractor(),
            clock,
            transport = transport,
            policies = BuiltInPolicies.ordinary(clock = clock).copy(circuitBreaker = circuit),
            discoveries = listOf(seeds("h")),
        )
        val engine = testEngine(listOf(pipeline), clock, state = state, observers = listOf(events))
        try {
            assertEquals(RunStatus.FAILED, engine.runOnce().status)
            val afterFailure = assertNotNull(state.delegate.loadPipelineHealth("health"))
            assertEquals(0, afterFailure.revision)
            assertEquals(1, afterFailure.consecutiveFailures)
            assertEquals(1, afterFailure.totalFailures)
            assertEquals(clock.current, afterFailure.lastObservedAt)

            failNext = false
            assertEquals(RunStatus.COMPLETED, engine.runOnce().status)
            val afterSuccess = assertNotNull(state.delegate.loadPipelineHealth("health"))
            assertEquals(1, afterSuccess.revision)
            assertEquals(0, afterSuccess.consecutiveFailures)
            assertEquals(1, afterSuccess.totalSuccesses)

            failNext = true
            engine.runOnce()
            engine.runOnce()
            val calls = transport.calls.size
            val blocked = engine.runOnce()
            assertEquals(RunStatus.ABANDONED, blocked.status)
            assertEquals(calls, transport.calls.size, "An open circuit prevents transport calls")
            assertTrue(
                "circuit-open:Consecutive failure threshold reached" in events.details(CrawlerEventType.CAPABILITY_WARNING),
            )
            val health = assertNotNull(state.delegate.loadPipelineHealth("health"))
            assertEquals(4, health.revision, "The blocked run is still observed by the health policy")
            assertEquals(3, health.totalFailures, "An abandoned blocked run is not counted as a failure")

            state.healthCasHook = { _, _, _ -> false }
            engine.runOnce()
            assertTrue("pipeline-health-conflict" in events.details(CrawlerEventType.CAPABILITY_WARNING))
            assertEquals(health, state.delegate.loadPipelineHealth("health"), "A conflicting health write is not applied")
        } finally {
            engine.shutdown()
        }

        val persisted = HookedStateStore(clock)
        assertTrue(
            persisted.delegate.compareAndSetPipelineHealth(
                "persisted",
                null,
                PipelineHealthSnapshot(revision = 0, circuitOpenUntil = clock.current.plusSeconds(30)),
            ),
        )
        val customCircuit = object : CircuitBreakerPolicy {
            override val id: String = "custom-circuit"
            override fun decide(snapshot: PipelineHealthSnapshot, now: Instant): CircuitBreakerDecision =
                if (snapshot.circuitOpenUntil?.isAfter(now) == true) CircuitBreakerDecision.Open(snapshot.circuitOpenUntil, "cooling") else CircuitBreakerDecision.Closed
        }
        val persistedPipeline = testPipeline(
            "persisted",
            bodyExtractor(),
            clock,
            policies = BuiltInPolicies.ordinary(clock = clock).copy(circuitBreaker = customCircuit),
        )
        val persistedEvents = EventLog()
        val persistedEngine = testEngine(listOf(persistedPipeline), clock, state = persisted, observers = listOf(persistedEvents))
        try {
            assertEquals(RunStatus.ABANDONED, persistedEngine.runOnce().status)
            assertTrue("circuit-open:cooling" in persistedEvents.details(CrawlerEventType.CAPABILITY_WARNING))
        } finally {
            persistedEngine.shutdown()
        }
    }

    @Test
    fun `a failed resumable discovery is reported to the harvest strategy as not naturally ended`() = runTest {
        val state = HookedStateStore(clock)
        val strategy = RecordingHarvestStrategy(codec = null)
        val failing = ResumableTestDiscovery("failing", checkpointCodec) { throw IllegalStateException("feed broke") }
        val pipeline = testPipeline("failed-walk", bodyExtractor(), clock, discoveries = listOf(failing), harvest = strategy)
        val engine = testEngine(listOf(pipeline), clock, state = state)
        try {
            val run = engine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.FAILED, run.status)
            assertEquals("discovery:failing", run.failures.single().stage)
            val completion = strategy.completions.single()
            assertEquals(false, completion.discoveryReachedNaturalEnd)
            assertEquals(HarvestRunOutcome.FAILED, completion.runOutcome)
            assertEquals(false, completion.completionPredicatePassed)
            assertTrue(state.checkpointWrites.isEmpty(), "A failed walk never completes its checkpoint")
        } finally {
            engine.shutdown()
        }
    }

    private class HarvestCase(
        val status: RunStatus,
        val run: PipelineRunResult,
        val strategy: RecordingHarvestStrategy,
        val state: HookedStateStore,
        val events: EventLog,
    ) {
        val warnings: List<String?> get() = events.details(CrawlerEventType.CAPABILITY_WARNING)
    }

    private suspend fun harvestCase(
        seed: HarvestState,
        atomic: Boolean = false,
        loseCas: Boolean = false,
        resolver: ((HarvestState) -> HarvestStateResolution)?,
    ): HarvestCase {
        val state = if (atomic) AtomicHookedStateStore(clock) else HookedStateStore(clock)
        assertTrue(state.delegate.compareAndSetHarvestState("harvested", "custom", null, seed))
        if (loseCas) state.harvestCasHook = { _, _, _, _ -> false }
        val strategy = RecordingHarvestStrategy(codec = harvestCodec(resolver))
        val discovery = ResumableTestDiscovery("walk", checkpointCodec) { emptyList() }
        val pipeline = testPipeline("harvested", bodyExtractor(), clock, discoveries = listOf(discovery), harvest = strategy)
        val events = EventLog()
        val engine = testEngine(
            listOf(pipeline),
            clock,
            state = state,
            observers = listOf(events),
            options = EngineRuntimeOptions(maxCheckpointPayloadBytes = 16),
        )
        try {
            val result = engine.runOnce()
            return HarvestCase(result.status, result.pipelineRuns.single(), strategy, state, events)
        } finally {
            engine.shutdown()
        }
    }

    private fun assertHarvestFailure(case: HarvestCase, message: String) {
        assertEquals(RunStatus.FAILED, case.status, message)
        assertEquals("pipeline", case.run.failures.single().stage, message)
        assertEquals(message, case.run.failures.single().error.message)
        assertTrue(case.strategy.contexts.isEmpty(), "No discovery is admitted with unresolved harvest state: $message")
    }

    private suspend fun harvestCommitCase(
        codec: HarvestStateCodec?,
        commit: HarvestStateCandidate,
        abandonRun: Boolean = false,
        harvestConflict: Boolean = false,
    ): HarvestCase {
        val state = AtomicHookedStateStore(clock)
        if (harvestConflict) state.harvestCasHook = { _, _, _, _ -> false }
        val strategy = RecordingHarvestStrategy(codec = codec, completion = { HarvestCompletion.Commit(commit) })
        val discovery = ResumableTestDiscovery("walk", checkpointCodec) {
            buildList {
                add(DiscoveryEmission.Request(CrawlRequest("page"), candidate()))
                if (abandonRun) add(DiscoveryEmission.Request(CrawlRequest("x".repeat(64))))
            }
        }
        val pipeline = testPipeline("commit", bodyExtractor(), clock, discoveries = listOf(discovery), harvest = strategy)
        val events = EventLog()
        val engine = testEngine(
            listOf(pipeline),
            clock,
            state = state,
            observers = listOf(events),
            options = EngineRuntimeOptions(maxCheckpointPayloadBytes = 16, maxTargetBytes = 32),
        )
        try {
            val result = engine.runOnce()
            return HarvestCase(result.status, result.pipelineRuns.single(), strategy, state, events)
        } finally {
            engine.shutdown()
        }
    }

    private fun payload(value: String): CheckpointPayload = CheckpointPayload.from(value.toByteArray())

    private fun candidate(): DiscoveryCheckpointCandidate =
        DiscoveryCheckpointCandidate(checkpointCodec.descriptor, checkpointCodec.strategyId, payload("c"))

    private fun harvestState(revision: Long, payload: String, codecVersion: Int): HarvestState = HarvestState(
        revision = revision,
        strategyId = "custom",
        codecId = harvestDescriptor.id,
        codecVersion = codecVersion,
        payload = payload(payload),
    )

    private fun harvestCodec(resolver: ((HarvestState) -> HarvestStateResolution)?): HarvestStateCodec =
        object : HarvestStateCodec {
            override val descriptor: HarvestStateCodecDescriptor = harvestDescriptor
            override val strategyId: String = "custom"
            override fun resolve(state: HarvestState): HarvestStateResolution =
                resolver?.invoke(state) ?: super.resolve(state)
        }

    private class RecordingHarvestStrategy(
        private val codec: HarvestStateCodec?,
        private val admit: (HarvestContext) -> HarvestAdmission = { HarvestAdmission.Run },
        private val completion: (HarvestCompletionContext) -> HarvestCompletion = { HarvestCompletion.Commit(null) },
    ) : HarvestStrategy {
        override val id: String = "custom"
        override val mode: HarvestMode = HarvestMode.CUSTOM
        override val stateCodec: HarvestStateCodec? get() = codec
        val contexts: MutableList<HarvestContext> = mutableListOf()
        val completions: MutableList<HarvestCompletionContext> = mutableListOf()

        override fun admit(context: HarvestContext): HarvestAdmission {
            contexts += context
            return admit.invoke(context)
        }

        override fun window(context: HarvestContext): IncrementalBoundaryPolicy.Window? = null

        override fun complete(context: HarvestCompletionContext): HarvestCompletion {
            completions += context
            return completion(context)
        }
    }
}
