package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.policy.BudgetDecision
import com.rmjtromp.trawler.policy.BudgetObservation
import com.rmjtromp.trawler.policy.BudgetSnapshot
import com.rmjtromp.trawler.policy.BuiltInPolicies
import com.rmjtromp.trawler.policy.CrawlBudgetPolicy
import com.rmjtromp.trawler.state.CrawlStateCapability
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
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

@OptIn(ExperimentalCoroutinesApi::class)
class EngineRuntimeRunHistoryTest {
    private val clock = SettableClock(Instant.parse("2026-04-01T00:00:00Z"))
    private val bothCapabilities = setOf(CrawlStateCapability.RUN_HISTORY, CrawlStateCapability.RUN_RECOVERY)

    @Test
    fun `run history records recovery, start, and terminal summaries for completed, failed, and abandoned runs`() = runTest {
        val state = RunHistoryStateStore(clock, bothCapabilities)
        val pipeline = testPipeline("history", bodyExtractor(), clock, discoveries = listOf(seeds("a")))
        val options = EngineRuntimeOptions(runTimeout = Duration.ofMinutes(3), shutdownTimeout = Duration.ofSeconds(4))
        val engine = testEngine(listOf(pipeline), clock, state = state, options = options)
        try {
            assertEquals(RunStatus.COMPLETED, engine.runOnce(RunRequest(reason = "manual")).status)
            assertEquals(
                listOf(
                    "recover",
                    "start-engine:manual",
                    "start-pipeline:history",
                    "complete-pipeline:COMPLETED:0",
                    "complete-engine:COMPLETED:0",
                ),
                state.calls,
            )
            assertEquals(Duration.ofMinutes(3).plusSeconds(4), state.recoveredStaleAfter)

            state.calls.clear()
            engine.runOnce(RunRequest(timeout = Duration.ofSeconds(30)))
            assertEquals(Duration.ofSeconds(34), state.recoveredStaleAfter, "An explicit run timeout defines staleness")
        } finally {
            engine.shutdown()
        }

        val failedState = RunHistoryStateStore(clock, bothCapabilities)
        val failing = testPipeline(
            "failing",
            bodyExtractor(),
            clock,
            discoveries = listOf(ListDiscovery("broken") { throw IllegalStateException("discovery broke") }),
        )
        val abandonAll = object : CrawlBudgetPolicy {
            override val id: String = "abandon"
            override fun evaluate(snapshot: BudgetSnapshot, observation: BudgetObservation): BudgetDecision =
                BudgetDecision.Stop("stop immediately")
        }
        val abandoning = testPipeline(
            "abandoning",
            bodyExtractor(),
            clock,
            policies = BuiltInPolicies.ordinary(clock = clock).copy(budget = abandonAll),
            discoveries = listOf(seeds("x")),
        )
        val failedEngine = testEngine(listOf(failing, abandoning), clock, state = failedState)
        try {
            val result = failedEngine.runOnce(RunRequest(pipelineIds = setOf("failing")))
            assertEquals(RunStatus.FAILED, result.status)
            assertEquals(
                listOf("recover", "start-engine:null", "start-pipeline:failing", "complete-pipeline:FAILED:1", "complete-engine:FAILED:1"),
                failedState.calls,
            )
            failedState.calls.clear()
            val abandoned = failedEngine.runOnce(RunRequest(pipelineIds = setOf("abandoning")))
            assertEquals(RunStatus.ABANDONED, abandoned.status)
            assertEquals(
                listOf(
                    "recover",
                    "start-engine:null",
                    "start-pipeline:abandoning",
                    "complete-pipeline:ABANDONED:0",
                    "complete-engine:ABANDONED:0",
                ),
                failedState.calls,
            )
        } finally {
            failedEngine.shutdown()
        }
    }

    @Test
    fun `history and recovery capabilities are honored independently`() = runTest {
        val pipeline = testPipeline("split", bodyExtractor(), clock)
        val recoveryOnly = RunHistoryStateStore(clock, setOf(CrawlStateCapability.RUN_RECOVERY))
        val recoveryEngine = testEngine(listOf(pipeline), clock, state = recoveryOnly)
        try {
            recoveryEngine.runOnce()
            assertEquals(listOf("recover"), recoveryOnly.calls)
        } finally {
            recoveryEngine.shutdown()
        }
        val historyOnly = RunHistoryStateStore(clock, setOf(CrawlStateCapability.RUN_HISTORY))
        val historyEngine = testEngine(listOf(pipeline), clock, state = historyOnly)
        try {
            historyEngine.runOnce()
            assertEquals(
                listOf("start-engine:null", "start-pipeline:split", "complete-pipeline:COMPLETED:0", "complete-engine:COMPLETED:0"),
                historyOnly.calls,
            )
        } finally {
            historyEngine.shutdown()
        }
        val neither = RunHistoryStateStore(clock, emptySet())
        val neitherEngine = testEngine(listOf(pipeline), clock, state = neither)
        try {
            neitherEngine.runOnce()
            assertTrue(neither.calls.isEmpty(), "Implementing the interface without advertising it records nothing")
        } finally {
            neitherEngine.shutdown()
        }
    }

    @Test
    fun `timeouts record abandoned engine runs and cancelled pipeline runs`() = runTest {
        val state = RunHistoryStateStore(clock, setOf(CrawlStateCapability.RUN_HISTORY))
        val entered = CompletableDeferred<Unit>()
        val transport = ScriptedTransport { _ ->
            entered.complete(Unit)
            awaitCancellation()
        }
        val events = EventLog()
        val pipeline = testPipeline("slow", bodyExtractor(), clock, transport = transport, discoveries = listOf(seeds("slow")))
        val engine = testEngine(
            listOf(pipeline),
            clock,
            state = state,
            observers = listOf(events),
            options = EngineRuntimeOptions(runTimeout = Duration.ofMillis(10)),
        )
        try {
            val run = async(start = CoroutineStart.UNDISPATCHED) { engine.runOnce() }
            entered.await()
            advanceTimeBy(11)
            runCurrent()
            val result = run.await()
            assertEquals(RunStatus.ABANDONED, result.status)
            assertEquals(
                listOf("start-engine:null", "start-pipeline:slow", "complete-pipeline:CANCELLED:0", "complete-engine:ABANDONED:1"),
                state.calls,
            )
            assertEquals(listOf("run-timeout"), events.details(CrawlerEventType.ENGINE_RUN_ABANDONED))
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `caller cancellation records cancelled summaries and a history failure never replaces the cancellation`() = runTest {
        val state = RunHistoryStateStore(clock, setOf(CrawlStateCapability.RUN_HISTORY)).apply {
            completeEngineFailure = { IllegalStateException("history unavailable") }
        }
        val entered = CompletableDeferred<Unit>()
        val transport = ScriptedTransport { _ ->
            entered.complete(Unit)
            awaitCancellation()
        }
        val events = EventLog()
        val pipeline = testPipeline("cancel", bodyExtractor(), clock, transport = transport, discoveries = listOf(seeds("c")))
        val engine = testEngine(listOf(pipeline), clock, state = state, observers = listOf(events))
        try {
            val captured = CompletableDeferred<CancellationException>()
            val run = async(start = CoroutineStart.UNDISPATCHED) {
                try {
                    engine.runOnce()
                } catch (cancelled: CancellationException) {
                    captured.complete(cancelled)
                    throw cancelled
                }
            }
            entered.await()
            run.cancel()
            assertFailsWith<CancellationException> { run.await() }
            val failure = captured.await()
            assertEquals(
                listOf("start-engine:null", "start-pipeline:cancel", "complete-pipeline:CANCELLED:0", "complete-engine:CANCELLED:0"),
                state.calls,
            )
            assertIs<CancellationException>(failure, "The adapter failure must not mask cooperative cancellation")
            assertEquals(listOf("cooperative cancellation"), events.details(CrawlerEventType.ENGINE_RUN_CANCELLED))
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `run-history adapter failures become explicit pipeline-start failures without hiding the original cause`() = runTest {
        val startFailing = RunHistoryStateStore(clock, setOf(CrawlStateCapability.RUN_HISTORY)).apply {
            startPipelineFailure = { IllegalStateException("cannot record pipeline start") }
        }
        val transport = ScriptedTransport()
        val pipeline = testPipeline("adapter", bodyExtractor(), clock, transport = transport, discoveries = listOf(seeds("a")))
        val startEngine = testEngine(listOf(pipeline), clock, state = startFailing)
        try {
            val result = startEngine.runOnce()
            assertEquals(RunStatus.FAILED, result.status)
            val pipelineRun = result.pipelineRuns.single()
            assertEquals(RunStatus.FAILED, pipelineRun.status)
            assertEquals("pipeline-start", pipelineRun.failures.single().stage)
            assertEquals("cannot record pipeline start", pipelineRun.failures.single().error.message)
            assertTrue(transport.calls.isEmpty(), "No pipeline work starts after its durable start failed")
            assertEquals(
                listOf("start-engine:null", "start-pipeline:adapter", "complete-engine:FAILED:1"),
                startFailing.calls,
                "A pipeline whose start was never recorded is not completed",
            )
        } finally {
            startEngine.shutdown()
        }

        val completeFailing = RunHistoryStateStore(clock, setOf(CrawlStateCapability.RUN_HISTORY)).apply {
            completePipelineFailure = { IllegalStateException("cannot record pipeline completion") }
        }
        val completeEngine = testEngine(listOf(pipeline), clock, state = completeFailing)
        try {
            val result = completeEngine.runOnce()
            val pipelineRun = result.pipelineRuns.single()
            assertEquals(RunStatus.FAILED, pipelineRun.status)
            val failure = pipelineRun.failures.single()
            assertEquals("pipeline-start", failure.stage)
            assertEquals("cannot record pipeline completion", failure.error.message)
            assertEquals(listOf("cannot record pipeline completion"), failure.error.suppressed.map { it.message })
            assertEquals(
                listOf(
                    "start-engine:null",
                    "start-pipeline:adapter",
                    "complete-pipeline:COMPLETED:0",
                    "complete-pipeline:FAILED:1",
                    "complete-engine:FAILED:1",
                ),
                completeFailing.calls,
            )
        } finally {
            completeEngine.shutdown()
        }
    }

    @Test
    fun `an execution failure after typed processing is isolated to its pipeline result`() = runTest {
        val state = HookedStateStore(clock).apply {
            healthCasHook = { _, _, _ -> throw IllegalStateException("health write failed") }
        }
        val output = CollectingOutput<String>("output")
        val broken = testPipeline(
            "broken",
            bodyExtractor(),
            clock,
            discoveries = listOf(seeds("b")),
            outputs = listOf(output to OutputRequirement.REQUIRED),
        )
        val engine = testEngine(listOf(broken), clock, state = state)
        try {
            val result = engine.runOnce()
            assertEquals(RunStatus.FAILED, result.status)
            val failure = result.pipelineRuns.single().failures.single()
            assertEquals("pipeline-start", failure.stage)
            assertEquals("health write failed", failure.error.message)
            assertEquals(listOf("b"), output.values(), "Values delivered before the failure remain delivered")
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `unknown run selections are rejected before any run history is written`() = runTest {
        val state = RunHistoryStateStore(clock, bothCapabilities)
        val engine = testEngine(listOf(testPipeline("known", bodyExtractor(), clock)), clock, state = state)
        try {
            val error = assertFailsWith<IllegalArgumentException> {
                engine.runOnce(RunRequest(pipelineIds = setOf("unknown-b", "unknown-a")))
            }
            assertEquals("Unknown pipeline IDs: unknown-a, unknown-b", error.message)
            assertTrue(state.calls.isEmpty())
        } finally {
            engine.shutdown()
        }
    }
}
