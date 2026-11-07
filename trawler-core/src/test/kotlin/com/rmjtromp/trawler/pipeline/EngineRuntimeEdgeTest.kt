package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.event.CrawlerEvent
import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.event.Observer
import com.rmjtromp.trawler.event.ObserverResult
import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.model.RequestBody
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.policy.BudgetDecision
import com.rmjtromp.trawler.policy.BudgetObservation
import com.rmjtromp.trawler.policy.BudgetSnapshot
import com.rmjtromp.trawler.policy.BuiltInPolicies
import com.rmjtromp.trawler.policy.CrawlBudgetPolicy
import com.rmjtromp.trawler.policy.PacingReservation
import com.rmjtromp.trawler.policy.PipelineEligibilityContext
import com.rmjtromp.trawler.policy.PipelineEligibilityDecision
import com.rmjtromp.trawler.policy.PipelineEligibilityPolicy
import com.rmjtromp.trawler.policy.RequestPacingPolicy
import com.rmjtromp.trawler.response.InMemoryResponseStore
import com.rmjtromp.trawler.state.AttemptOutcome
import com.rmjtromp.trawler.state.LeaseMutationResult
import com.rmjtromp.trawler.state.WorkTransition
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

/** Cancellation points, renewal success, pre-attempt failures, and metadata ownership in the engine runtime. */
@OptIn(ExperimentalCoroutinesApi::class)
class EngineRuntimeEdgeTest {
    private val clock = SettableClock(Instant.parse("2026-08-01T00:00:00Z"))

    @Test
    fun `cancellation while suspended in an observer, a claim, or an output propagates and cleans up`() = runTest {
        // Observer suspended on the queued-work event.
        run {
            val entered = CompletableDeferred<Unit>()
            val observer = object : Observer {
                override val id: String = "suspending-observer"
                override suspend fun onEvent(event: CrawlerEvent): ObserverResult {
                    if (event.type == CrawlerEventType.REQUEST_QUEUED) {
                        entered.complete(Unit)
                        awaitCancellation()
                    }
                    return ObserverResult.Accepted
                }
            }
            val state = HookedStateStore(clock)
            val pipeline = testPipeline("observer", bodyExtractor(), clock, discoveries = listOf(seeds("a")))
            val engine = testEngine(listOf(pipeline), clock, state = state, observers = listOf(observer))
            try {
                val run = async(start = CoroutineStart.UNDISPATCHED) { engine.runOnce() }
                entered.await()
                run.cancel()
                assertFailsWith<CancellationException> { run.await() }
                assertEquals(1, state.cancelQueuedCalls)
                assertEquals(0, state.activeWorkCount("observer"))
            } finally {
                engine.shutdown()
            }
        }
        // Claim suspended in the state adapter.
        run {
            val entered = CompletableDeferred<Unit>()
            val state = HookedStateStore(clock).apply {
                claimHook = { _, _, _, _ ->
                    entered.complete(Unit)
                    awaitCancellation()
                }
            }
            val pipeline = testPipeline("claim", bodyExtractor(), clock, discoveries = listOf(seeds("a")))
            val engine = testEngine(listOf(pipeline), clock, state = state, options = EngineRuntimeOptions(workerCount = 1))
            try {
                val run = async(start = CoroutineStart.UNDISPATCHED) { engine.runOnce() }
                entered.await()
                run.cancel()
                assertFailsWith<CancellationException> { run.await() }
                assertEquals(1, state.cancelQueuedCalls)
                assertTrue(state.finishedAttempts.isEmpty(), "No attempt starts while claiming")
            } finally {
                engine.shutdown()
            }
        }
        // Output suspended while writing a produced value.
        run {
            val entered = CompletableDeferred<Unit>()
            val output = CollectingOutput<String>("suspending-output") {
                entered.complete(Unit)
                awaitCancellation()
            }
            val state = HookedStateStore(clock)
            val pipeline = testPipeline(
                "output",
                bodyExtractor(),
                clock,
                discoveries = listOf(seeds("a")),
                outputs = listOf(output to OutputRequirement.REQUIRED),
            )
            val engine = testEngine(listOf(pipeline), clock, state = state)
            try {
                val run = async(start = CoroutineStart.UNDISPATCHED) { engine.runOnce() }
                entered.await()
                run.cancel()
                assertFailsWith<CancellationException> { run.await() }
                assertEquals(listOf(AttemptOutcome.CANCELLED), state.finishedAttempts)
                assertTrue(WorkTransition.Cancelled("work cancelled") in state.transitions)
            } finally {
                engine.shutdown()
            }
        }
    }

    @Test
    fun `accepted lease renewals keep long work alive until it completes`() = runTest {
        val state = HookedStateStore(clock)
        var renewals = 0
        state.renewHook = { work, expiresAt ->
            renewals++
            state.delegate.renew(work, expiresAt)
        }
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val transport = ScriptedTransport { request ->
            entered.complete(Unit)
            release.await()
            okResponse(request, body = "slow-body")
        }
        val output = CollectingOutput<String>("output")
        val pipeline = testPipeline(
            "renewing",
            bodyExtractor(),
            clock,
            transport = transport,
            discoveries = listOf(seeds("slow")),
            outputs = listOf(output to OutputRequirement.REQUIRED),
        )
        val engine = testEngine(
            listOf(pipeline),
            clock,
            state = state,
            options = EngineRuntimeOptions(leaseDuration = Duration.ofMinutes(3)),
        )
        try {
            val run = async(start = CoroutineStart.UNDISPATCHED) { engine.runOnce() }
            entered.await()
            advanceTimeBy(Duration.ofMinutes(2).toMillis() + 1)
            runCurrent()
            assertEquals(2, renewals, "The lease is renewed every third of its duration")
            release.complete(Unit)
            val result = run.await()
            assertEquals(RunStatus.COMPLETED, result.status)
            assertEquals(listOf("slow-body"), output.values())
            assertEquals(WorkTransition.Succeeded, state.transitions.single())
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `failures and cancellation before an attempt starts never record an attempt`() = runTest {
        val failingPacing = object : RequestPacingPolicy {
            override val id: String = "failing-pacing"
            override suspend fun acquire(request: CrawlRequest, context: RequestContext): PacingReservation =
                throw IllegalStateException("pacing backend unavailable")
            override fun observe(response: CrawlResponse, context: RequestContext) = Unit
        }
        val failingState = HookedStateStore(clock)
        val transport = ScriptedTransport()
        val failing = testPipeline(
            "pacing-failure",
            bodyExtractor(),
            clock,
            transport = transport,
            policies = BuiltInPolicies.ordinary(clock = clock).copy(pacing = failingPacing),
            discoveries = listOf(seeds("p")),
        )
        val failingEngine = testEngine(listOf(failing), clock, state = failingState)
        try {
            val run = failingEngine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.FAILED, run.status)
            assertEquals("work", run.failures.single().stage)
            assertEquals("pacing backend unavailable", run.failures.single().error.message)
            assertEquals(0, run.requestsStarted)
            assertTrue(failingState.finishedAttempts.isEmpty())
            assertEquals(WorkTransition.Failed("work"), failingState.transitions.single())
            assertTrue(transport.calls.isEmpty())
        } finally {
            failingEngine.shutdown()
        }

        val entered = CompletableDeferred<Unit>()
        val releases = mutableListOf<String>()
        val waitingPacing = object : RequestPacingPolicy {
            override val id: String = "waiting-pacing"
            override suspend fun acquire(request: CrawlRequest, context: RequestContext): PacingReservation {
                entered.complete(Unit)
                awaitCancellation()
            }
            override fun observe(response: CrawlResponse, context: RequestContext) = Unit
        }
        val waitingState = HookedStateStore(clock)
        val waiting = testPipeline(
            "pacing-wait",
            bodyExtractor(),
            clock,
            policies = BuiltInPolicies.ordinary(clock = clock).copy(
                pacing = waitingPacing,
                concurrency = object : com.rmjtromp.trawler.policy.ConcurrencyPolicy {
                    override val id: String = "recording-concurrency"
                    override suspend fun acquire(request: CrawlRequest, context: RequestContext) =
                        com.rmjtromp.trawler.policy.ConcurrencyPermit { releases += request.target }
                },
            ),
            discoveries = listOf(seeds("w")),
        )
        val waitingEngine = testEngine(listOf(waiting), clock, state = waitingState)
        try {
            val run = async(start = CoroutineStart.UNDISPATCHED) { waitingEngine.runOnce() }
            entered.await()
            run.cancel()
            assertFailsWith<CancellationException> { run.await() }
            assertTrue(waitingState.finishedAttempts.isEmpty(), "Leasing and waiting for pacing do not consume an attempt")
            assertEquals(WorkTransition.Cancelled("work cancelled"), waitingState.transitions.first())
            assertEquals(listOf("w"), releases, "The concurrency permit is released after cancellation")
        } finally {
            waitingEngine.shutdown()
        }
    }

    @Test
    fun `runtime-owned request attributes cannot be spoofed by discovery and bounded bodies pass`() = runTest {
        val transport = ScriptedTransport()
        val spoofed = CrawlRequest(
            "spoofed",
            body = RequestBody.from(byteArrayOf(1, 2, 3)),
            attributes = ContextAttributes.of(
                ContextAttribute(InMemoryResponseStore.PIPELINE_ID_ATTRIBUTE, "someone-else"),
                ContextAttribute(InMemoryResponseStore.REQUEST_ID_ATTRIBUTE, "forged-request"),
                ContextAttribute("caller", "kept"),
            ),
        )
        val pipeline = testPipeline("owner", bodyExtractor(), clock, transport = transport, discoveries = listOf(ListDiscovery("d") {
            listOf(DiscoveryEmission.Request(spoofed))
        }))
        val engine = testEngine(listOf(pipeline), clock, options = EngineRuntimeOptions(maxRequestBodyBytes = 3))
        try {
            assertEquals(RunStatus.COMPLETED, engine.runOnce().status)
            val sent = transport.calls.single()
            assertEquals("owner", sent.attributes[InMemoryResponseStore.PIPELINE_ID_ATTRIBUTE])
            val requestId = sent.attributes[InMemoryResponseStore.REQUEST_ID_ATTRIBUTE]
            assertTrue(requestId != null && requestId != "forged-request", "The request ID is engine-owned: $requestId")
            assertEquals("kept", sent.attributes["caller"])
            assertEquals(3, sent.body?.size)
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `pipeline extensions stop with failure, abandonment, or cancellation reasons and stop failures fail the run`() = runTest {
        val log = mutableListOf<String>()
        val failing = testPipeline(
            "failing",
            bodyExtractor(),
            clock,
            discoveries = listOf(ListDiscovery("broken") { error("broken discovery") }),
            extensions = listOf(LoggingExtension("failing-ext", log)),
        )
        val abandonAll = object : CrawlBudgetPolicy {
            override val id: String = "abandon"
            override fun evaluate(snapshot: BudgetSnapshot, observation: BudgetObservation): BudgetDecision =
                BudgetDecision.Stop("stop")
        }
        val abandoned = testPipeline(
            "abandoned",
            bodyExtractor(),
            clock,
            policies = BuiltInPolicies.ordinary(clock = clock).copy(budget = abandonAll),
            discoveries = listOf(seeds("x")),
            extensions = listOf(LoggingExtension("abandoned-ext", log)),
        )
        val stopFailure = testPipeline(
            "stop-failure",
            bodyExtractor(),
            clock,
            extensions = listOf(LoggingExtension("stop-failure-ext", log, stopFailure = IllegalStateException("stop failed"))),
        )
        val engine = testEngine(listOf(failing, abandoned, stopFailure), clock)
        try {
            val runs = engine.runOnce().pipelineRuns.associateBy { it.pipelineId }
            assertTrue("stop:failing-ext:PIPELINE_RUN_FAILED" in log, "$log")
            assertTrue("stop:abandoned-ext:PIPELINE_RUN_ABANDONED" in log, "$log")
            assertEquals(RunStatus.FAILED, runs.getValue("stop-failure").status)
            assertEquals("extension-stop:stop-failure-ext", runs.getValue("stop-failure").failures.single().stage)
        } finally {
            engine.shutdown()
        }

        val cancelLog = mutableListOf<String>()
        val entered = CompletableDeferred<Unit>()
        val cancelled = testPipeline(
            "cancelled",
            bodyExtractor(),
            clock,
            transport = ScriptedTransport { _ ->
                entered.complete(Unit)
                awaitCancellation()
            },
            discoveries = listOf(seeds("c")),
            extensions = listOf(LoggingExtension("cancelled-ext", cancelLog)),
        )
        val cancelEngine = testEngine(listOf(cancelled), clock)
        try {
            val run = async(start = CoroutineStart.UNDISPATCHED) { cancelEngine.runOnce() }
            entered.await()
            run.cancel()
            assertFailsWith<CancellationException> { run.await() }
            assertEquals(listOf("start:cancelled-ext", "stop:cancelled-ext:CANCELLATION"), cancelLog)
        } finally {
            cancelEngine.shutdown()
        }
    }

    @Test
    fun `a clock moving backwards reports zero elapsed time to admission and budget policies`() = runTest {
        val elapsed = mutableListOf<Duration>()
        val rewinding = object : PipelineEligibilityPolicy {
            override val id: String = "rewinding"
            override fun decide(context: PipelineEligibilityContext): PipelineEligibilityDecision {
                clock.current = clock.current.minus(Duration.ofHours(1))
                return PipelineEligibilityDecision.Eligible
            }
        }
        val budget = object : CrawlBudgetPolicy {
            override val id: String = "recording-budget"
            override fun evaluate(snapshot: BudgetSnapshot, observation: BudgetObservation): BudgetDecision {
                elapsed += snapshot.elapsed
                return BudgetDecision.Continue
            }
        }
        val admissions = mutableListOf<Duration>()
        val admission = object : com.rmjtromp.trawler.policy.DiscoveryAdmissionPolicy {
            override val id: String = "recording-admission"
            override fun decide(context: com.rmjtromp.trawler.policy.DiscoveryAdmissionContext): com.rmjtromp.trawler.policy.DiscoveryAdmissionDecision {
                admissions += context.budget.elapsed
                return com.rmjtromp.trawler.policy.DiscoveryAdmissionDecision.Admit
            }
        }
        val pipeline = testPipeline(
            "rewind",
            bodyExtractor(),
            clock,
            policies = BuiltInPolicies.ordinary(clock = clock).copy(eligibility = rewinding, budget = budget, admission = admission),
            discoveries = listOf(seeds("r")),
        )
        val engine = testEngine(listOf(pipeline), clock)
        try {
            assertEquals(RunStatus.COMPLETED, engine.runOnce().status)
            assertEquals(listOf(Duration.ZERO), admissions)
            assertTrue(elapsed.isNotEmpty() && elapsed.all { it == Duration.ZERO }, "$elapsed")
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `failure records are bounded while every failure is still counted`() = runTest {
        val output = CollectingOutput<String>("failing-output") { OutputResult.Failed(IllegalStateException("sink rejected")) }
        val budgetFailures = mutableListOf<Long>()
        val budget = object : CrawlBudgetPolicy {
            override val id: String = "failure-counter"
            override fun evaluate(snapshot: BudgetSnapshot, observation: BudgetObservation): BudgetDecision {
                budgetFailures += snapshot.failures
                return BudgetDecision.Continue
            }
        }
        val pipeline = testPipeline(
            "bounded-failures",
            LambdaExtractor { _, _ -> ExtractionResult.Success(listOf("a", "b", "c")) },
            clock,
            policies = BuiltInPolicies.ordinary(clock = clock).copy(budget = budget),
            discoveries = listOf(seeds("one", "two")),
            outputs = listOf(output to OutputRequirement.BEST_EFFORT),
        )
        val engine = testEngine(
            listOf(pipeline),
            clock,
            options = EngineRuntimeOptions(maxQueueSize = 2, maxOutputBatchSize = 1, workerCount = 1),
        )
        try {
            val run = engine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.COMPLETED, run.status, "Best-effort output failures do not fail the run")
            assertEquals(6, output.batches.size)
            assertEquals(2, run.failures.size, "The failure snapshot is bounded by the runtime queue size")
            assertTrue(run.failures.all { it.stage == "output:failing-output" })
            assertTrue(budgetFailures.last() >= 3, "Unrecorded failures are still counted: $budgetFailures")
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `lease checks rejected before response capture stop capture and the attempt`() = runTest {
        val state = HookedStateStore(clock).apply {
            checkCurrentHook = { _ -> LeaseMutationResult.Rejected("fenced") }
        }
        val store = InMemoryResponseStore()
        val pipeline = testPipeline("capture-check", bodyExtractor(), clock, discoveries = listOf(seeds("c")))
        val engine = testEngine(listOf(pipeline), clock, state = state, responseStore = store)
        try {
            val run = engine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.ABANDONED, run.status)
            assertEquals(listOf(AttemptOutcome.ABANDONED), state.finishedAttempts)
            assertTrue(
                store.replay(com.rmjtromp.trawler.response.ResponseReplayQuery.ByPipelineId("capture-check"), 10, null).isEmpty(),
                "No response is captured after a rejected lease check",
            )
        } finally {
            engine.shutdown()
        }
    }
}
