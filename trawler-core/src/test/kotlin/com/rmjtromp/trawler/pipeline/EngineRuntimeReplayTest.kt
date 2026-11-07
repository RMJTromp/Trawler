package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.ResponseReference
import com.rmjtromp.trawler.policy.BudgetDecision
import com.rmjtromp.trawler.policy.BudgetObservation
import com.rmjtromp.trawler.policy.BudgetSnapshot
import com.rmjtromp.trawler.policy.BuiltInPolicies
import com.rmjtromp.trawler.policy.CrawlBudgetPolicy
import com.rmjtromp.trawler.response.InMemoryResponseStore
import com.rmjtromp.trawler.response.ResponseLookup
import com.rmjtromp.trawler.response.ResponseReplayQuery
import com.rmjtromp.trawler.response.ResponseReplayStore
import com.rmjtromp.trawler.response.ResponseStore
import com.rmjtromp.trawler.response.ResponseStoreCapability
import com.rmjtromp.trawler.response.StoredResponse
import com.rmjtromp.trawler.schedule.ScheduleSubmission
import com.rmjtromp.trawler.schedule.ScheduleTrigger
import com.rmjtromp.trawler.schedule.Scheduler
import com.rmjtromp.trawler.schedule.SchedulerEvent
import com.rmjtromp.trawler.schedule.SchedulerHandle
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.state.InMemoryCrawlStateStore
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

@OptIn(ExperimentalCoroutinesApi::class)
class EngineRuntimeReplayTest {
    private val clock = SettableClock(Instant.parse("2026-02-01T00:00:00Z"))

    @Test
    fun `replay rejects mismatched queries, missing replay capability, and unregistered pipelines before any lifecycle starts`() = runTest {
        val log = mutableListOf<String>()
        val replayExtension = LoggingReplayExtension("replay-ext", log)
        val transport = ScriptedTransport { error("replay must not use the network") }
        val pipeline = testPipeline("replay", bodyExtractor(), clock, transport = transport)
        val store = ScriptedReplayStore(responses = { emptyList() })
        val engine = testEngine(listOf(pipeline), clock, responseStore = store, extensions = listOf(replayExtension))
        try {
            val mismatch = assertFailsWith<InvalidConfigurationException> {
                engine.replay(pipeline.replay(ResponseReplayQuery.ByPipelineId("other")))
            }
            assertEquals(listOf("replay.query.pipelineId"), mismatch.issues.map { it.path })
            assertEquals(ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION, mismatch.issues.single().code)

            val foreignInstance = testPipeline("replay", bodyExtractor(), clock, transport = transport)
            val unregistered = assertFailsWith<IllegalArgumentException> {
                engine.replay(foreignInstance.replay(ResponseReplayQuery.ByPipelineId("replay")))
            }
            assertTrue(unregistered.message!!.contains("is not registered"))
            assertTrue(log.isEmpty(), "Validation failures must not start replay lifecycles: $log")
            assertTrue(transport.calls.isEmpty())
        } finally {
            engine.shutdown()
        }

        val noStoreEngine = testEngine(listOf(pipeline), clock)
        try {
            val missing = assertFailsWith<InvalidConfigurationException> {
                noStoreEngine.replay(pipeline.replay(ResponseReplayQuery.ByRequestId("r")))
            }
            assertEquals("engine.responseStore.replay", missing.issues.single().path)
            assertEquals(ConfigurationIssueCode.UNSUPPORTED_CAPABILITY, missing.issues.single().code)
        } finally {
            noStoreEngine.shutdown()
        }

        val notAdvertised = ScriptedReplayStore(capabilities = emptySet(), responses = { error("must not replay") })
        val notAdvertisedEngine = testEngine(listOf(pipeline), clock, responseStore = notAdvertised)
        try {
            val unadvertised = assertFailsWith<InvalidConfigurationException> {
                notAdvertisedEngine.replay(pipeline.replay(ResponseReplayQuery.ByRequestId("r")))
            }
            assertEquals("engine.responseStore.capabilities.REPLAY", unadvertised.issues.single().path)
            assertEquals(0, notAdvertised.replayCalls)
        } finally {
            notAdvertisedEngine.shutdown()
        }
    }

    @Test
    fun `replay store results that exceed the limit or belong to another pipeline fail closed and stop temporary lifecycles`() = runTest {
        val log = mutableListOf<String>()
        val replayExtension = LoggingReplayExtension("replay-ext", log)
        val output = CollectingOutput<String>("replay-output")
        val pipeline = testPipeline(
            "owned",
            bodyExtractor(),
            clock,
            outputs = listOf(output to OutputRequirement.REQUIRED),
        )
        val oversized = ScriptedReplayStore(responses = { listOf(stored("owned", "a", 1), stored("owned", "b", 2)) })
        val oversizedEngine = testEngine(
            listOf(pipeline),
            clock,
            responseStore = oversized,
            extensions = listOf(replayExtension),
        )
        try {
            val error = assertFailsWith<IllegalArgumentException> {
                oversizedEngine.replay(pipeline.replay(ResponseReplayQuery.ByPipelineId("owned"), limit = 1))
            }
            assertTrue(error.message!!.contains("more responses than requested"))
            assertEquals(listOf("replay-start:replay-ext", "replay-stop:replay-ext:PIPELINE_RUN_FAILED"), log)
        } finally {
            oversizedEngine.shutdown()
        }

        log.clear()
        val foreign = ScriptedReplayStore(responses = { listOf(stored("owned", "mine", 1), stored("intruder", "theirs", 2)) })
        val foreignEngine = testEngine(
            listOf(pipeline),
            clock,
            responseStore = foreign,
            extensions = listOf(replayExtension),
        )
        try {
            val error = assertFailsWith<InvalidConfigurationException> {
                foreignEngine.replay(pipeline.replay(ResponseReplayQuery.ByRequestId("any")))
            }
            assertEquals("replay.responseStore.pipelineOwnership", error.issues.single().path)
            assertTrue(output.batches.isEmpty(), "No foreign response may reach the typed output")
            assertEquals(listOf("replay-start:replay-ext", "replay-stop:replay-ext:PIPELINE_RUN_FAILED"), log)
            assertEquals("owned", foreign.lastPipelineId)
        } finally {
            foreignEngine.shutdown()
        }
    }

    @Test
    fun `replay rejects selected lifecycle resources that are not replay-safe`() = runTest {
        val log = mutableListOf<String>()
        val lifecycleStore = LifecycleReplayStore("lifecycle-store", log)
        val lifecycleOutput = LifecycleOutput("lifecycle-output", log)
        val pipelineExtension = LoggingExtension("pipeline-ext", log)
        val pipeline = testPipeline(
            "unsafe",
            bodyExtractor(),
            clock,
            outputs = listOf(lifecycleOutput to OutputRequirement.REQUIRED),
            extensions = listOf(pipelineExtension),
        )
        val engine = testEngine(
            listOf(pipeline),
            clock,
            responseStore = lifecycleStore,
            extensions = listOf(lifecycleStore, lifecycleOutput),
        )
        try {
            val error = assertFailsWith<InvalidConfigurationException> {
                engine.replay(pipeline.replay(ResponseReplayQuery.ByPipelineId("unsafe")))
            }
            assertEquals(
                setOf(
                    "engine.responseStore.replayLifecycle",
                    "pipeline[unsafe].output[lifecycle-output].replayLifecycle",
                    "pipeline[unsafe].extension[pipeline-ext].replayLifecycle",
                ),
                error.issues.map { it.path }.toSet(),
            )
            assertTrue(error.issues.all { it.code == ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION })
            assertTrue(log.isEmpty(), "No lifecycle may start for a rejected replay: $log")
            assertEquals(0, lifecycleStore.replayCalls)
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `replay from NEW starts replay lifecycles temporarily and a later ordinary run starts the full lifecycle`() = runTest {
        val log = mutableListOf<String>()
        val replayExtension = LoggingReplayExtension("engine-replay", log)
        val ordinaryExtension = LoggingExtension("engine-ordinary", log)
        val pipelineExtension = LoggingReplayExtension("pipeline-replay", log)
        val events = EventLog()
        val output = CollectingOutput<String>("output")
        val transport = ScriptedTransport()
        val pipeline = testPipeline(
            "replay",
            bodyExtractor(),
            clock,
            transport = transport,
            outputs = listOf(output to OutputRequirement.REQUIRED),
            extensions = listOf(pipelineExtension),
        )
        val store = InMemoryResponseStore()
        store.put(okResponse(storedRequest("replay", "first-request", "stored"), body = "stored-body"))
        val engine = testEngine(
            listOf(pipeline),
            clock,
            responseStore = store,
            observers = listOf(events),
            extensions = listOf(replayExtension, ordinaryExtension),
        )
        try {
            val result = engine.replay(pipeline.replay(ResponseReplayQuery.ByPipelineId("replay")))
            assertEquals(RunStatus.COMPLETED, result.status)
            assertEquals("replay", result.pipelineId)
            assertEquals(1, result.responsesRead)
            assertEquals(listOf("stored-body"), output.values())
            val produced = output.batches.single().single().context
            assertEquals("first-request", produced.requestId)
            assertTrue(produced.engineRunId.startsWith("replay-engine-"))
            assertTrue(produced.pipelineRunId.startsWith("replay-pipeline-"))
            assertEquals(
                listOf(
                    "replay-start:engine-replay",
                    "replay-start:pipeline-replay",
                    "replay-stop:pipeline-replay:PIPELINE_RUN_COMPLETED",
                    "replay-stop:engine-replay:PIPELINE_RUN_COMPLETED",
                ),
                log,
            )
            assertEquals(listOf("replay"), events.details(CrawlerEventType.PIPELINE_RUN_STARTED))
            assertEquals(listOf("replay:COMPLETED"), events.details(CrawlerEventType.PIPELINE_RUN_COMPLETED))
            assertTrue(transport.calls.isEmpty())

            log.clear()
            assertEquals(RunStatus.COMPLETED, engine.runOnce().status)
            assertEquals(
                listOf(
                    "start:engine-replay",
                    "start:engine-ordinary",
                    "start:pipeline-replay",
                    "stop:pipeline-replay:PIPELINE_RUN_COMPLETED",
                ),
                log,
            )

            log.clear()
            val shared = engine.replay(pipeline.replay(ResponseReplayQuery.ByPipelineId("replay")))
            assertEquals(RunStatus.COMPLETED, shared.status)
            assertEquals(
                listOf("replay-start:pipeline-replay", "replay-stop:pipeline-replay:PIPELINE_RUN_COMPLETED"),
                log,
                "A replay beside a running engine must not restart shared engine extensions",
            )
        } finally {
            log.clear()
            engine.shutdown()
        }
        assertEquals(
            listOf("stop:engine-ordinary:ENGINE_SHUTDOWN", "stop:engine-replay:ENGINE_SHUTDOWN"),
            log,
        )
    }

    @Test
    fun `replay outcomes map stage failures, generic failures, budgets, and suppressed follow-ups to explicit statuses`() = runTest {
        val log = mutableListOf<String>()
        val pipelineExtension = LoggingReplayExtension("pipeline-replay", log)
        val events = EventLog()
        val output = CollectingOutput<String>("output")
        val extractor = LambdaExtractor<String>("scripted") { response, _ ->
            when (val body = response.body.utf8()) {
                "extract-failure" -> ExtractionResult.Failure(IllegalStateException("cannot parse"))
                "follow-up" -> ExtractionResult.Success(listOf("with-follow-up"), listOf(CrawlRequest("next")))
                else -> ExtractionResult.Success(listOf(body))
            }
        }
        val exploding = object : Transform<String> {
            override val id: String = "exploding"
            override suspend fun apply(value: String, context: com.rmjtromp.trawler.model.TransformContext): TransformDecision<String> =
                if (value == "explode") throw IllegalArgumentException("transform bug") else TransformDecision.Accept(value)
        }
        val pipeline = testPipeline(
            "outcomes",
            extractor,
            clock,
            transforms = listOf(exploding),
            outputs = listOf(output to OutputRequirement.REQUIRED),
            extensions = listOf(pipelineExtension),
        )
        val bodies = mutableListOf<String>()
        val store = ScriptedReplayStore(responses = { bodies.mapIndexed { index, body -> stored("outcomes", body, index.toLong()) } })
        val engine = testEngine(listOf(pipeline), clock, responseStore = store, observers = listOf(events))
        try {
            bodies += listOf("extract-failure", "explode", "ok")
            val failed = engine.replay(pipeline.replay(ResponseReplayQuery.ByPipelineId("outcomes")))
            assertEquals(RunStatus.FAILED, failed.status)
            assertEquals(listOf("replay:extraction", "replay"), failed.failures.map { it.stage })
            assertEquals("cannot parse", failed.failures[0].error.message)
            assertIs<IllegalArgumentException>(failed.failures[1].error)
            assertEquals(listOf("ok"), output.values(), "Later responses are still processed after a stage failure")
            assertEquals(3, failed.responsesRead)
            assertEquals(1, failed.producedValues)
            assertEquals(1, failed.deliveredValues)
            assertEquals("replay-stop:pipeline-replay:PIPELINE_RUN_FAILED", log.last())
            assertEquals(listOf("replay:FAILED"), events.details(CrawlerEventType.PIPELINE_RUN_FAILED))

            bodies.clear()
            bodies += "follow-up"
            output.batches.clear()
            val followUps = engine.replay(pipeline.replay(ResponseReplayQuery.ByPipelineId("outcomes")))
            assertEquals(RunStatus.COMPLETED, followUps.status)
            assertEquals(listOf("with-follow-up"), output.values())
            assertTrue(
                events.details(CrawlerEventType.CAPABILITY_WARNING).contains("replay-follow-ups-suppressed:1"),
            )
        } finally {
            engine.shutdown()
        }

        val budgetLog = mutableListOf<String>()
        val budgetExtension = LoggingReplayExtension("budget-replay", budgetLog)
        val budgetOutput = CollectingOutput<String>("budget-output")
        val stopAfterFirstValue = object : CrawlBudgetPolicy {
            override val id: String = "first-value"
            override fun evaluate(snapshot: BudgetSnapshot, observation: BudgetObservation): BudgetDecision =
                if (observation is BudgetObservation.ValuesProduced) BudgetDecision.Stop("one value") else BudgetDecision.Continue
        }
        val budgetPipeline = testPipeline(
            "budget",
            bodyExtractor(),
            clock,
            policies = BuiltInPolicies.ordinary(clock = clock).copy(budget = stopAfterFirstValue),
            outputs = listOf(budgetOutput to OutputRequirement.REQUIRED),
            extensions = listOf(budgetExtension),
        )
        val budgetStore = ScriptedReplayStore(responses = { listOf(stored("budget", "one", 1), stored("budget", "two", 2)) })
        val budgetEvents = EventLog()
        val budgetEngine = testEngine(listOf(budgetPipeline), clock, responseStore = budgetStore, observers = listOf(budgetEvents))
        try {
            val abandoned = budgetEngine.replay(budgetPipeline.replay(ResponseReplayQuery.ByPipelineId("budget")))
            assertEquals(RunStatus.ABANDONED, abandoned.status)
            assertEquals(listOf("replay-budget"), abandoned.failures.map { it.stage })
            assertEquals("one value", abandoned.failures.single().error.message)
            assertTrue(budgetOutput.batches.isEmpty(), "A budget stop before delivery must not deliver the stopped value")
            assertEquals(1, abandoned.producedValues)
            assertEquals("replay-stop:budget-replay:PIPELINE_RUN_ABANDONED", budgetLog.last())
            assertEquals(listOf("replay:ABANDONED"), budgetEvents.details(CrawlerEventType.PIPELINE_RUN_ABANDONED))
        } finally {
            budgetEngine.shutdown()
        }
    }

    @Test
    fun `replay lifecycle startup and shutdown failures roll back, propagate, and leave the engine reusable`() = runTest {
        val log = mutableListOf<String>()
        val first = LoggingReplayExtension("first", log, replayStopFailure = IllegalStateException("rollback stop failed"))
        val failing = LoggingReplayExtension("failing", log, replayStartFailure = IllegalStateException("replay start failed"))
        val pipeline = testPipeline("lifecycle", bodyExtractor(), clock)
        val store = ScriptedReplayStore(responses = { listOf(stored("lifecycle", "x", 1)) })
        val engine = testEngine(listOf(pipeline), clock, responseStore = store, extensions = listOf(first, failing))
        try {
            val error = assertFailsWith<IllegalStateException> {
                engine.replay(pipeline.replay(ResponseReplayQuery.ByPipelineId("lifecycle")))
            }
            assertEquals("replay start failed", error.message)
            assertEquals(listOf("rollback stop failed"), error.suppressedChain().map { it.message })
            assertEquals(
                listOf("replay-start:first", "replay-start:failing", "replay-stop:first:STARTUP_FAILURE"),
                log,
            )
            assertEquals(0, store.replayCalls, "No stored response is read after a lifecycle startup failure")
            log.clear()
            assertEquals(RunStatus.COMPLETED, engine.runOnce().status, "The engine returns to NEW after rollback")
            assertEquals(listOf("start:first", "start:failing"), log)
        } finally {
            engine.shutdown()
        }

        val stopLog = mutableListOf<String>()
        val failingStop = LoggingReplayExtension("failing-stop", stopLog, replayStopFailure = IllegalStateException("replay stop failed"))
        val laterStop = LoggingReplayExtension("later-stop", stopLog, replayStopFailure = IllegalStateException("later stop failed"))
        val stopEngine = testEngine(listOf(pipeline), clock, responseStore = store, extensions = listOf(failingStop, laterStop))
        try {
            val stopError = assertFailsWith<IllegalStateException> {
                stopEngine.replay(pipeline.replay(ResponseReplayQuery.ByPipelineId("lifecycle")))
            }
            assertEquals("later stop failed", stopError.message, "Reverse-order stop reports the first cleanup failure")
            assertTrue("replay-stop:failing-stop:PIPELINE_RUN_COMPLETED" in stopLog, "Later extensions are still stopped: $stopLog")
            assertEquals(1, store.replayCalls)
            val replayFailure = ScriptedReplayStore(responses = { throw IllegalStateException("store offline") })
            val suppressedEngine = testEngine(listOf(pipeline), clock, responseStore = replayFailure, extensions = listOf(failingStop))
            try {
                val primary = assertFailsWith<IllegalStateException> {
                    suppressedEngine.replay(pipeline.replay(ResponseReplayQuery.ByPipelineId("lifecycle")))
                }
                assertEquals("store offline", primary.message)
                assertEquals(listOf("replay stop failed"), primary.suppressedChain().map { it.message })
            } finally {
                runCatching { suppressedEngine.shutdown() }
            }
        } finally {
            runCatching { stopEngine.shutdown() }
        }

        val pipelineLog = mutableListOf<String>()
        val failingPipelineStop = LoggingReplayExtension(
            "pipeline-stop",
            pipelineLog,
            replayStopFailure = IllegalStateException("pipeline replay stop failed"),
        )
        val stopPipeline = testPipeline("lifecycle", bodyExtractor(), clock, extensions = listOf(failingPipelineStop))
        val pipelineStopEngine = testEngine(listOf(stopPipeline), clock, responseStore = store)
        try {
            val result = pipelineStopEngine.replay(stopPipeline.replay(ResponseReplayQuery.ByPipelineId("lifecycle")))
            assertEquals(RunStatus.FAILED, result.status)
            assertEquals(listOf("replay-extension-stop:pipeline-stop"), result.failures.map { it.stage })
            assertEquals("pipeline replay stop failed", result.failures.single().error.message)
        } finally {
            pipelineStopEngine.shutdown()
        }
    }

    @Test
    fun `cancelling a replay stops lifecycles with cancellation and engine shutdown during replay stops shared lifecycles once`() = runTest {
        val log = mutableListOf<String>()
        val engineExtension = LoggingReplayExtension("engine-replay", log)
        val pipelineExtension = LoggingReplayExtension("pipeline-replay", log)
        val entered = CompletableDeferred<Unit>()
        val blocking = LambdaExtractor<String>("blocking") { _, _ ->
            entered.complete(Unit)
            awaitCancellation()
        }
        val pipeline = testPipeline("blocked", blocking, clock, extensions = listOf(pipelineExtension))
        val store = ScriptedReplayStore(responses = { listOf(stored("blocked", "x", 1)) })
        val engine = testEngine(listOf(pipeline), clock, responseStore = store, extensions = listOf(engineExtension))
        try {
            val replay = async(start = CoroutineStart.UNDISPATCHED) {
                engine.replay(pipeline.replay(ResponseReplayQuery.ByPipelineId("blocked")))
            }
            entered.await()
            val daemonError = assertFailsWith<IllegalStateException> { engine.startDaemon(NeverStartedScheduler) }
            assertEquals("Trawler engine is replaying stored responses", daemonError.message)
            replay.cancel()
            assertFailsWith<CancellationException> { replay.await() }
            assertEquals(
                listOf(
                    "replay-start:engine-replay",
                    "replay-start:pipeline-replay",
                    "replay-stop:pipeline-replay:CANCELLATION",
                    "replay-stop:engine-replay:CANCELLATION",
                ),
                log,
            )
        } finally {
            engine.shutdown()
        }

        log.clear()
        val shutdownEntered = CompletableDeferred<Unit>()
        val shutdownBlocking = LambdaExtractor<String>("blocking") { _, _ ->
            shutdownEntered.complete(Unit)
            awaitCancellation()
        }
        val shutdownPipeline = testPipeline("blocked", shutdownBlocking, clock, extensions = listOf(pipelineExtension))
        val shutdownEngine = testEngine(
            listOf(shutdownPipeline),
            clock,
            responseStore = store,
            extensions = listOf(engineExtension),
        )
        val replay = async(start = CoroutineStart.UNDISPATCHED) {
            runCatching { shutdownEngine.replay(shutdownPipeline.replay(ResponseReplayQuery.ByPipelineId("blocked"))) }
        }
        shutdownEntered.await()
        shutdownEngine.shutdown()
        runCurrent()
        assertTrue(replay.await().exceptionOrNull() is CancellationException)
        assertEquals(
            listOf(
                "replay-start:engine-replay",
                "replay-start:pipeline-replay",
                "replay-stop:pipeline-replay:CANCELLATION",
                "replay-stop:engine-replay:ENGINE_SHUTDOWN",
            ),
            log,
            "Shutdown stops a temporarily started replay lifecycle exactly once",
        )
        val closed = assertFailsWith<IllegalStateException> {
            shutdownEngine.replay(shutdownPipeline.replay(ResponseReplayQuery.ByPipelineId("blocked")))
        }
        assertEquals("Trawler engine is not accepting runs", closed.message)
    }

    @Test
    fun `replay enforces the pending-run capacity shared with ordinary runs`() = runTest {
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val extractor = LambdaExtractor<String>("gate") { response, _ ->
            entered.complete(Unit)
            release.await()
            ExtractionResult.Success(listOf(response.body.utf8()))
        }
        val pipeline = testPipeline("pending", extractor, clock)
        val store = ScriptedReplayStore(responses = { listOf(stored("pending", "x", 1)) })
        val engine = testEngine(
            listOf(pipeline),
            clock,
            responseStore = store,
            options = EngineRuntimeOptions(maxPendingRuns = 1),
        )
        try {
            val first = async(start = CoroutineStart.UNDISPATCHED) {
                engine.replay(pipeline.replay(ResponseReplayQuery.ByPipelineId("pending")))
            }
            entered.await()
            val full = assertFailsWith<IllegalStateException> {
                engine.replay(pipeline.replay(ResponseReplayQuery.ByPipelineId("pending")))
            }
            assertEquals("Engine pending-run capacity is full", full.message)
            assertFailsWith<IllegalStateException> { engine.runOnce() }
            release.complete(Unit)
            assertEquals(RunStatus.COMPLETED, first.await().status)
            assertEquals(1, store.replayCalls)
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `stored responses exceeding runtime bounds fail replay processing without reaching the extractor`() = runTest {
        val extractor = bodyExtractor()
        val output = CollectingOutput<String>("output")
        val pipeline = testPipeline("bounded", extractor, clock, outputs = listOf(output to OutputRequirement.REQUIRED))
        val store = ScriptedReplayStore(
            responses = { listOf(stored("bounded", "x".repeat(32), 1), stored("bounded", "ok", 2)) },
        )
        val engine = testEngine(
            listOf(pipeline),
            clock,
            responseStore = store,
            options = EngineRuntimeOptions(maxResponseBodyBytes = 8),
        )
        try {
            val result = engine.replay(pipeline.replay(ResponseReplayQuery.ByPipelineId("bounded")))
            assertEquals(RunStatus.FAILED, result.status)
            assertEquals(listOf("replay:response-metadata-bound"), result.failures.map { it.stage })
            assertEquals("response body exceeds the configured bound", result.failures.single().error.message)
            assertEquals(listOf("ok"), extractor.seen.map { it.body.utf8() })
            assertEquals(listOf("ok"), output.values())
        } finally {
            engine.shutdown()
        }
    }

    private fun storedRequest(pipelineId: String, requestId: String, target: String): CrawlRequest = CrawlRequest(
        target,
        attributes = ContextAttributes.of(
            ContextAttribute(InMemoryResponseStore.PIPELINE_ID_ATTRIBUTE, pipelineId),
            ContextAttribute(InMemoryResponseStore.REQUEST_ID_ATTRIBUTE, requestId),
        ),
    )

    private fun stored(pipelineId: String, body: String, index: Long): StoredResponse = StoredResponse(
        reference = ResponseReference("ref-$index"),
        response = okResponse(storedRequest(pipelineId, "request-$index", "target-$index"), body = body),
        pipelineId = pipelineId,
    )

    private open class ScriptedReplayStore(
        override val id: String = "scripted-replay-store",
        override val capabilities: Set<ResponseStoreCapability> = setOf(ResponseStoreCapability.REPLAY),
        private val responses: suspend () -> List<StoredResponse>,
    ) : ResponseStore, ResponseReplayStore {
        var replayCalls: Int = 0
        var lastPipelineId: String? = null

        override suspend fun put(response: CrawlResponse): ResponseReference? = null

        override suspend fun find(query: ResponseLookup): StoredResponse? = null

        override suspend fun replay(query: ResponseReplayQuery, limit: Int, pipelineId: String?): List<StoredResponse> {
            replayCalls++
            lastPipelineId = pipelineId
            return responses()
        }
    }

    private class LifecycleReplayStore(
        id: String,
        private val log: MutableList<String>,
    ) : ScriptedReplayStore(id = id, responses = { emptyList() }), ExtensionLifecycle {
        override suspend fun start(context: ExtensionStartContext) {
            log += "start:$id"
        }

        override suspend fun stop(context: ExtensionStopContext) {
            log += "stop:$id"
        }
    }

    private class LifecycleOutput(
        id: String,
        private val log: MutableList<String>,
    ) : CollectingOutput<String>(id), ExtensionLifecycle {
        override suspend fun start(context: ExtensionStartContext) {
            log += "start:$id"
        }

        override suspend fun stop(context: ExtensionStopContext) {
            log += "stop:$id"
        }
    }

    private object NeverStartedScheduler : Scheduler {
        override val id: String = "never-started"

        override suspend fun start(
            scope: CoroutineScope,
            trigger: ScheduleTrigger,
            clock: Clock,
            pipelineIds: Set<String>?,
            run: suspend (RunRequest) -> RunResult,
            observe: suspend (SchedulerEvent) -> Unit,
        ): SchedulerHandle = object : SchedulerHandle {
            override val completion: Deferred<Unit> = CompletableDeferred(Unit)
            override suspend fun trigger(): ScheduleSubmission = error("unused")
            override suspend fun cancel() = Unit
        }
    }
}
