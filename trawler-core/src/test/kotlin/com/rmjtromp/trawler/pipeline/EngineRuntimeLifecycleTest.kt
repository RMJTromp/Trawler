package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.ResponseReference
import com.rmjtromp.trawler.output.OutputLifecycle
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.response.ResponseLookup
import com.rmjtromp.trawler.response.ResponseStore
import com.rmjtromp.trawler.response.ResponseStoreCapability
import com.rmjtromp.trawler.response.ResponseStoreLifecycle
import com.rmjtromp.trawler.response.StoredResponse
import com.rmjtromp.trawler.schedule.ScheduleSubmission
import com.rmjtromp.trawler.schedule.ScheduleTrigger
import com.rmjtromp.trawler.schedule.Scheduler
import com.rmjtromp.trawler.schedule.SchedulerEvent
import com.rmjtromp.trawler.schedule.SchedulerHandle
import com.rmjtromp.trawler.state.CrawlStateCapability
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

@OptIn(ExperimentalCoroutinesApi::class)
class EngineRuntimeLifecycleTest {
    private val clock = SettableClock(Instant.parse("2026-03-01T00:00:00Z"))

    @Test
    fun `awaitIdle times out while a run is active, succeeds after completion, and rejects non-positive timeouts`() = runTest {
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val transport = ScriptedTransport { request ->
            entered.complete(Unit)
            release.await()
            okResponse(request)
        }
        val pipeline = testPipeline("idle", bodyExtractor(), clock, transport = transport, discoveries = listOf(seeds("a")))
        val engine = testEngine(listOf(pipeline), clock)
        try {
            assertTrue(engine.awaitIdle(Duration.ofMillis(1)), "An engine without runs is idle")
            val run = async(start = CoroutineStart.UNDISPATCHED) { engine.runOnce() }
            entered.await()
            assertFalse(engine.awaitIdle(Duration.ofMillis(50)), "An active run must keep the engine busy")
            release.complete(Unit)
            assertTrue(engine.awaitIdle(Duration.ofSeconds(1)))
            assertEquals(RunStatus.COMPLETED, run.await().status)
            assertFailsWith<IllegalArgumentException> { engine.awaitIdle(Duration.ofMillis(-1)) }
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `runs over disjoint pipelines overlap while a run on a busy pipeline waits`() = runTest {
        val aEntered = CompletableDeferred<Unit>()
        val bEntered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        fun blocking(entered: CompletableDeferred<Unit>) = ScriptedTransport { request ->
            entered.complete(Unit)
            release.await()
            okResponse(request)
        }
        val a = testPipeline("a", bodyExtractor(), clock, transport = blocking(aEntered), discoveries = listOf(seeds("a1")))
        val b = testPipeline("b", bodyExtractor(), clock, transport = blocking(bEntered), discoveries = listOf(seeds("b1")))
        val engine = testEngine(listOf(a, b), clock)
        try {
            val runA = async(start = CoroutineStart.UNDISPATCHED) { engine.runOnce(RunRequest(setOf("a"))) }
            aEntered.await()
            val runB = async(start = CoroutineStart.UNDISPATCHED) { engine.runOnce(RunRequest(setOf("b"))) }
            bEntered.await()
            assertFalse(runA.isCompleted, "b started while a was still inside its request")
            val secondA = async(start = CoroutineStart.UNDISPATCHED) { engine.runOnce(RunRequest(setOf("a"))) }
            assertFalse(engine.awaitIdle(Duration.ofMillis(50)))
            assertFalse(secondA.isCompleted, "a second run of a busy pipeline must wait for it")
            release.complete(Unit)
            assertEquals(RunStatus.COMPLETED, runA.await().status)
            assertEquals(RunStatus.COMPLETED, runB.await().status)
            assertEquals(RunStatus.COMPLETED, secondA.await().status)
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `stale-run recovery allows for the longest timeout among overlapping runs`() = runTest {
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val slow = testPipeline(
            "slow",
            bodyExtractor(),
            clock,
            transport = ScriptedTransport { request ->
                entered.complete(Unit)
                release.await()
                okResponse(request)
            },
            discoveries = listOf(seeds("s")),
        )
        val quick = testPipeline("quick", bodyExtractor(), clock, transport = ScriptedTransport { okResponse(it) }, discoveries = listOf(seeds("q")))
        val state = RunHistoryStateStore(clock, setOf(CrawlStateCapability.RUN_RECOVERY))
        val options = EngineRuntimeOptions(runTimeout = Duration.ofMinutes(1), shutdownTimeout = Duration.ofSeconds(4))
        val engine = testEngine(listOf(slow, quick), clock, state = state, options = options)
        try {
            val longRun = async(start = CoroutineStart.UNDISPATCHED) { engine.runOnce(RunRequest(setOf("slow"), timeout = Duration.ofHours(3))) }
            entered.await()
            assertEquals(RunStatus.COMPLETED, engine.runOnce(RunRequest(setOf("quick"), timeout = Duration.ofSeconds(30))).status)
            assertEquals(Duration.ofHours(3).plusSeconds(4), state.recoveredStaleAfter, "the 3 h run must not look stale to the 30 s run")
            release.complete(Unit)
            assertEquals(RunStatus.COMPLETED, longRun.await().status)
            assertEquals(RunStatus.COMPLETED, engine.runOnce(RunRequest(setOf("quick"), timeout = Duration.ofSeconds(30))).status)
            assertEquals(Duration.ofSeconds(34), state.recoveredStaleAfter, "a finished run no longer counts")
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `daemon registration rejects unknown and overlapping selections and releases reservations after failures`() = runTest {
        val alpha = testPipeline("alpha", bodyExtractor(), clock)
        val beta = testPipeline("beta", bodyExtractor(), clock)
        val engine = testEngine(listOf(alpha, beta), clock)
        val scheduler = FakeScheduler()
        try {
            val unknown = assertFailsWith<IllegalArgumentException> {
                engine.startDaemon(scheduler, DaemonConfig(pipelineIds = setOf("gamma", "alpha")))
            }
            assertEquals("Unknown pipeline IDs: gamma", unknown.message)
            assertTrue(scheduler.starts.isEmpty())

            val alphaHandle = engine.startDaemon(scheduler, DaemonConfig(pipelineIds = setOf("alpha")))
            assertEquals(setOf("alpha"), scheduler.starts.single())
            assertFailsWith<IllegalStateException> {
                engine.startDaemon(scheduler, DaemonConfig(pipelineIds = setOf("alpha", "beta")))
            }
            assertFailsWith<IllegalStateException> { engine.startDaemon(scheduler, DaemonConfig()) }
            val betaHandle = engine.startDaemon(scheduler, DaemonConfig(pipelineIds = setOf("beta")))
            assertEquals(2, scheduler.starts.size)

            assertEquals(ScheduleSubmission.Started(7), alphaHandle.trigger())
            assertSame(scheduler.handles[0].completion, alphaHandle.completion)
            alphaHandle.cancel()
            alphaHandle.cancel()
            assertEquals(1, scheduler.handles[0].cancelCalls, "Daemon cancellation is idempotent")
            engine.startDaemon(scheduler, DaemonConfig(pipelineIds = setOf("alpha")))

            scheduler.handles[1].completion.complete(Unit)
            engine.startDaemon(scheduler, DaemonConfig(pipelineIds = setOf("beta")))
            assertEquals(4, scheduler.starts.size, "A completed scheduler no longer reserves its selection")
            betaHandle.cancel()

            val failingScheduler = FakeScheduler(startFailure = IllegalStateException("scheduler refused"))
            val failingEngine = testEngine(listOf(alpha), clock)
            try {
                val refused = assertFailsWith<IllegalStateException> {
                    failingEngine.startDaemon(failingScheduler, DaemonConfig(pipelineIds = setOf("alpha")))
                }
                assertEquals("scheduler refused", refused.message)
                val recovered = failingEngine.startDaemon(scheduler, DaemonConfig(pipelineIds = setOf("alpha")))
                assertNotNull(recovered, "A failed scheduler start must release its selection reservation")
            } finally {
                failingEngine.shutdown()
            }
        } finally {
            engine.shutdown()
        }
        assertTrue(scheduler.handles.all { it.cancelCalls == 1 }, "Shutdown cancels every remaining daemon exactly once")
    }

    @Test
    fun `an all-pipeline daemon overlaps every later selection until it is cancelled`() = runTest {
        val alpha = testPipeline("alpha", bodyExtractor(), clock)
        val engine = testEngine(listOf(alpha), clock)
        val scheduler = FakeScheduler()
        try {
            val all = engine.startDaemon(scheduler, DaemonConfig())
            assertEquals(listOf<Set<String>?>(null), scheduler.starts)
            val overlap = assertFailsWith<IllegalStateException> {
                engine.startDaemon(scheduler, DaemonConfig(pipelineIds = setOf("alpha")))
            }
            assertTrue(overlap.message!!.contains("overlapping pipeline selection"))
            all.cancel()
            engine.startDaemon(scheduler, DaemonConfig(pipelineIds = setOf("alpha")))
            assertEquals(listOf(null, setOf("alpha")), scheduler.starts)
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `scheduler callbacks run engine work and map scheduler events to bounded crawler events`() = runTest {
        val events = EventLog()
        val output = CollectingOutput<String>("output")
        val pipeline = testPipeline(
            "scheduled",
            bodyExtractor(),
            clock,
            discoveries = listOf(seeds("tick")),
            outputs = listOf(output to OutputRequirement.REQUIRED),
        )
        val engine = testEngine(listOf(pipeline), clock, observers = listOf(events))
        val scheduler = FakeScheduler()
        try {
            val handle = engine.startDaemon(scheduler, DaemonConfig(pipelineIds = setOf("scheduled")))
            val callbacks = scheduler.callbacks.single()
            val result = callbacks.run(RunRequest(pipelineIds = setOf("scheduled"), reason = "scheduled tick"))
            assertEquals(RunStatus.COMPLETED, result.status)
            assertEquals(listOf("tick"), output.values())
            assertEquals(listOf("scheduled tick"), events.details(CrawlerEventType.ENGINE_RUN_STARTED))

            val at = Instant.parse("2026-03-01T01:00:00Z")
            callbacks.observe(SchedulerEvent.Triggered(1, at, setOf("scheduled")))
            callbacks.observe(SchedulerEvent.Skipped(2, "overlap"))
            callbacks.observe(SchedulerEvent.Rejected(3, "queue full"))
            callbacks.observe(SchedulerEvent.RunFailed(4, IllegalStateException("secret detail")))
            callbacks.observe(SchedulerEvent.RunCompleted(5, result))
            callbacks.observe(SchedulerEvent.Cancelled)

            assertEquals(listOf("sequence=1"), events.details(CrawlerEventType.SCHEDULER_TRIGGERED))
            assertEquals(at, events.events.single { it.type == CrawlerEventType.SCHEDULER_TRIGGERED }.context.emittedAt)
            assertEquals(listOf("sequence=2; overlap"), events.details(CrawlerEventType.SCHEDULER_TRIGGER_SKIPPED))
            assertEquals(listOf("sequence=3; queue full"), events.details(CrawlerEventType.SCHEDULER_TRIGGER_REJECTED))
            assertEquals(
                listOf("sequence=4; IllegalStateException"),
                events.details(CrawlerEventType.SCHEDULER_RUN_FAILED),
                "Scheduler failures expose only the failure type, never the message",
            )
            val schedulerEvents = events.types().filter { it.name.startsWith("SCHEDULER_") }
            assertEquals(4, schedulerEvents.size, "RunCompleted and Cancelled do not emit crawler events")

            handle.shutdown()
            assertEquals(1, scheduler.handles.single().cancelCalls)
            assertFailsWith<IllegalStateException> { engine.runOnce() }
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `daemon start fails closed when the engine stops while the scheduler is starting`() = runTest {
        val pipeline = testPipeline("racing", bodyExtractor(), clock)
        val engine = testEngine(listOf(pipeline), clock)
        val scheduler = FakeScheduler(onStart = { engine.shutdown() })
        val error = assertFailsWith<IllegalStateException> { engine.startDaemon(scheduler) }
        assertEquals("Trawler engine is no longer accepting daemon schedulers", error.message)
        assertEquals(1, scheduler.handles.single().cancelCalls, "The orphaned scheduler handle is cancelled")
        val stopped = assertFailsWith<IllegalStateException> { engine.startDaemon(FakeScheduler()) }
        assertEquals("Trawler engine is shutting down or stopped", stopped.message)
    }

    @Test
    fun `startup failure marks the engine failed for direct runs and daemons until shutdown`() = runTest {
        val log = mutableListOf<String>()
        val first = LoggingExtension("first", log)
        val broken = LoggingExtension("broken", log, startFailure = IllegalStateException("cannot start"))
        val pipeline = testPipeline("failed", bodyExtractor(), clock)
        val engine = testEngine(listOf(pipeline), clock, extensions = listOf(first, broken))
        val startError = assertFailsWith<IllegalStateException> { engine.runOnce() }
        assertEquals("cannot start", startError.message)
        assertEquals(listOf("start:first", "start:broken", "stop:first:STARTUP_FAILURE"), log)

        val daemonError = assertFailsWith<IllegalStateException> { engine.startDaemon(FakeScheduler()) }
        assertEquals("Trawler engine startup previously failed", daemonError.message)
        val runError = assertFailsWith<IllegalStateException> { engine.runOnce() }
        assertEquals("Trawler engine is not accepting runs", runError.message)
        log.clear()
        engine.shutdown()
        assertTrue(log.isEmpty(), "Rolled-back extensions are not stopped again at shutdown: $log")
    }

    @Test
    fun `shutdown closes owned lifecycles, reports the first cleanup failure, and repeated shutdown observes it`() = runTest {
        val log = mutableListOf<String>()
        val state = LifecycleStateStore(clock, log)
        val responses = LifecycleResponseStore(log)
        val failingOutput = ClosingOutput("failing-output", log, IllegalStateException("output close failed"))
        val healthyOutput = ClosingOutput("healthy-output", log)
        val failingExtension = LoggingExtension("failing-ext", log, stopFailure = IllegalStateException("extension stop failed"))
        val pipeline = testPipeline(
            "owned",
            bodyExtractor(),
            clock,
            outputs = listOf(
                failingOutput to OutputRequirement.BEST_EFFORT,
                healthyOutput to OutputRequirement.BEST_EFFORT,
            ),
        )
        val engine = testEngine(
            listOf(pipeline),
            clock,
            state = state,
            responseStore = responses,
            extensions = listOf(state, failingExtension),
        )
        assertEquals(RunStatus.COMPLETED, engine.runOnce().status)
        log.clear()

        val failure = assertFailsWith<IllegalStateException> { engine.shutdown() }
        assertEquals("extension stop failed", failure.message)
        assertEquals(
            listOf(
                "stop:failing-ext:ENGINE_SHUTDOWN",
                "stop:lifecycle-state:ENGINE_SHUTDOWN",
                "close:failing-output",
                "close:healthy-output",
                "close:responses",
            ),
            log,
            "Every owned resource is closed even after an earlier cleanup failure",
        )
        val repeated = assertFailsWith<IllegalStateException> { engine.shutdown() }
        assertEquals("extension stop failed", repeated.message)
        assertEquals(5, log.size, "Repeated shutdown waits for the first result without closing resources twice")
        assertFailsWith<IllegalStateException> { engine.runOnce() }
    }

    @Test
    fun `shutting down a never-started engine stops the state lifecycle and closes outputs and response stores`() = runTest {
        val log = mutableListOf<String>()
        val state = LifecycleStateStore(clock, log)
        val responses = LifecycleResponseStore(log)
        val output = ClosingOutput("output", log)
        val pipeline = testPipeline("unstarted", bodyExtractor(), clock, outputs = listOf(output to OutputRequirement.REQUIRED))
        val engine = testEngine(listOf(pipeline), clock, state = state, responseStore = responses, extensions = listOf(state))
        engine.shutdown()
        assertEquals(
            listOf("stop:lifecycle-state:ENGINE_SHUTDOWN", "close:output", "close:responses"),
            log,
        )
        engine.shutdown()
        assertEquals(3, log.size)
    }

    @Test
    fun `non-cooperative cleanup is bounded by the shutdown timeout and reported`() = runTest {
        val hanging = object : ExtensionLifecycle {
            override val id: String = "hanging"
            var stopEntered = false
            override suspend fun start(context: ExtensionStartContext) = Unit
            override suspend fun stop(context: ExtensionStopContext) {
                stopEntered = true
                awaitCancellation()
            }
        }
        val pipeline = testPipeline("bounded", bodyExtractor(), clock)
        val engine = testEngine(
            listOf(pipeline),
            clock,
            extensions = listOf(hanging),
            options = EngineRuntimeOptions(shutdownTimeout = Duration.ofMillis(20)),
        )
        assertEquals(RunStatus.COMPLETED, engine.runOnce().status)
        val startedAt = currentTime
        val error = assertFailsWith<IllegalStateException> { engine.shutdown() }
        assertEquals("extension hanging shutdown exceeded the configured shutdown timeout", error.message)
        assertTrue(hanging.stopEntered)
        assertTrue(currentTime - startedAt >= 20, "The shutdown wait is bounded by virtual time, not wall time")
    }

    @Test
    fun `shutdown cancels an in-flight run and a daemon whose cancellation fails is reported`() = runTest {
        val entered = CompletableDeferred<Unit>()
        val transport = ScriptedTransport { _ ->
            entered.complete(Unit)
            awaitCancellation()
        }
        val events = EventLog()
        val pipeline = testPipeline("inflight", bodyExtractor(), clock, transport = transport, discoveries = listOf(seeds("slow")))
        val engine = testEngine(listOf(pipeline), clock, observers = listOf(events))
        val scheduler = FakeScheduler(cancelFailure = IllegalStateException("scheduler stuck"))
        engine.startDaemon(scheduler, DaemonConfig(pipelineIds = setOf("inflight")))
        val run = async(start = CoroutineStart.UNDISPATCHED) { runCatching { engine.runOnce() } }
        entered.await()
        val error = assertFailsWith<IllegalStateException> { engine.shutdown() }
        assertEquals("scheduler stuck", error.message)
        runCurrent()
        assertIs<kotlinx.coroutines.CancellationException>(run.await().exceptionOrNull())
        assertTrue(events.types().contains(CrawlerEventType.ENGINE_RUN_CANCELLED))
        assertEquals(1, scheduler.handles.single().cancelCalls)
    }

    private class FakeSchedulerHandle(
        private val cancelFailure: Throwable?,
    ) : SchedulerHandle {
        override val completion: CompletableDeferred<Unit> = CompletableDeferred()
        var cancelCalls: Int = 0

        override suspend fun trigger(): ScheduleSubmission = ScheduleSubmission.Started(7)

        override suspend fun cancel() {
            cancelCalls++
            cancelFailure?.let { throw it }
            completion.complete(Unit)
        }
    }

    private class SchedulerCallbacks(
        val run: suspend (RunRequest) -> RunResult,
        val observe: suspend (SchedulerEvent) -> Unit,
    )

    private class FakeScheduler(
        private val startFailure: Throwable? = null,
        private val cancelFailure: Throwable? = null,
        private val onStart: suspend () -> Unit = {},
    ) : Scheduler {
        override val id: String = "fake-scheduler"
        val starts: MutableList<Set<String>?> = mutableListOf()
        val handles: MutableList<FakeSchedulerHandle> = mutableListOf()
        val callbacks: MutableList<SchedulerCallbacks> = mutableListOf()

        override suspend fun start(
            scope: CoroutineScope,
            trigger: ScheduleTrigger,
            clock: Clock,
            pipelineIds: Set<String>?,
            run: suspend (RunRequest) -> RunResult,
            observe: suspend (SchedulerEvent) -> Unit,
        ): SchedulerHandle {
            startFailure?.let { throw it }
            starts += pipelineIds
            callbacks += SchedulerCallbacks(run, observe)
            onStart()
            return FakeSchedulerHandle(cancelFailure).also { handles += it }
        }
    }

    private class LifecycleStateStore(
        clock: Clock,
        private val log: MutableList<String>,
    ) : HookedStateStore(clock, id = "lifecycle-state"), ExtensionLifecycle {
        override suspend fun start(context: ExtensionStartContext) {
            log += "start:$id"
        }

        override suspend fun stop(context: ExtensionStopContext) {
            log += "stop:$id:${context.reason}"
        }
    }

    private class LifecycleResponseStore(
        private val log: MutableList<String>,
    ) : ResponseStore, ResponseStoreLifecycle {
        override val id: String = "responses"
        override val capabilities: Set<ResponseStoreCapability> = emptySet()

        override suspend fun put(response: CrawlResponse): ResponseReference? = null

        override suspend fun find(query: ResponseLookup): StoredResponse? = null

        override suspend fun close() {
            log += "close:$id"
        }
    }

    private class ClosingOutput(
        id: String,
        private val log: MutableList<String>,
        private val closeFailure: Throwable? = null,
    ) : CollectingOutput<String>(id), OutputLifecycle {
        override suspend fun close() {
            log += "close:$id"
            closeFailure?.let { throw it }
        }
    }
}
