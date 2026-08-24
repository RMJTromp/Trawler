package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.Discovery
import com.rmjtromp.trawler.discovery.DiscoveryContext
import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.event.CrawlerEvent
import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.event.EventContext
import com.rmjtromp.trawler.event.Observer
import com.rmjtromp.trawler.event.ObserverResult
import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.extension.ExtensionStopReason
import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.ExtractionContext
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.model.RequestMethod
import com.rmjtromp.trawler.model.RequestBody
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.model.ResponseContext
import com.rmjtromp.trawler.model.ResponseReference
import com.rmjtromp.trawler.model.TransformContext
import com.rmjtromp.trawler.model.ValidationContext
import com.rmjtromp.trawler.output.OutputLifecycle
import com.rmjtromp.trawler.policy.AllowAllAuthorizationPolicy
import com.rmjtromp.trawler.policy.AllowAllScopePolicy
import com.rmjtromp.trawler.policy.AttemptSummary
import com.rmjtromp.trawler.policy.AuthorizationDecision
import com.rmjtromp.trawler.policy.AuthorizationPolicy
import com.rmjtromp.trawler.policy.BudgetDecision
import com.rmjtromp.trawler.policy.BudgetObservation
import com.rmjtromp.trawler.policy.BudgetSnapshot
import com.rmjtromp.trawler.policy.BuiltInPolicies
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.CrawlBudgetPolicy
import com.rmjtromp.trawler.policy.CrawlScopePolicy
import com.rmjtromp.trawler.policy.DefaultFailureClassifier
import com.rmjtromp.trawler.policy.FailureClassification
import com.rmjtromp.trawler.policy.FailureContext
import com.rmjtromp.trawler.policy.FailureClassifier
import com.rmjtromp.trawler.policy.RequestIdentity
import com.rmjtromp.trawler.policy.RequestIdentityStrategy
import com.rmjtromp.trawler.policy.RequestPacingPolicy
import com.rmjtromp.trawler.policy.ConcurrencyPolicy
import com.rmjtromp.trawler.policy.ConcurrencyPermit
import com.rmjtromp.trawler.policy.PacingReservation
import com.rmjtromp.trawler.policy.RecrawlDecision
import com.rmjtromp.trawler.policy.RecrawlPolicy
import com.rmjtromp.trawler.policy.ResourceHistory
import com.rmjtromp.trawler.policy.RetryDecision
import com.rmjtromp.trawler.policy.RetryPolicy
import com.rmjtromp.trawler.policy.ScopeDecision
import com.rmjtromp.trawler.policy.WorkDescriptor
import com.rmjtromp.trawler.policy.WorkOrderingPolicy
import com.rmjtromp.trawler.response.CaptureResponsesWhenConfigured
import com.rmjtromp.trawler.response.DoNotCaptureResponses
import com.rmjtromp.trawler.response.ResponseCacheContext
import com.rmjtromp.trawler.response.ResponseCacheDecision
import com.rmjtromp.trawler.response.ResponseCachePolicy
import com.rmjtromp.trawler.response.ResponseCaptureContext
import com.rmjtromp.trawler.response.ResponseCaptureDecision
import com.rmjtromp.trawler.response.ResponseCapturePolicy
import com.rmjtromp.trawler.response.ResponseReplayQuery
import com.rmjtromp.trawler.response.ResponseReplayStore
import com.rmjtromp.trawler.response.ResponseStore
import com.rmjtromp.trawler.response.ResponseStoreCapability
import com.rmjtromp.trawler.response.StoredResponse
import com.rmjtromp.trawler.response.InMemoryResponseStore
import com.rmjtromp.trawler.state.AttemptOutcome
import com.rmjtromp.trawler.state.AttemptStartResult
import com.rmjtromp.trawler.state.AttemptStore
import com.rmjtromp.trawler.state.CrawlStateCapability
import com.rmjtromp.trawler.state.CrawlStateStore
import com.rmjtromp.trawler.state.EnqueueResult
import com.rmjtromp.trawler.state.ExecutionAttempt
import com.rmjtromp.trawler.state.InMemoryCrawlStateStore
import com.rmjtromp.trawler.state.LeaseMutationResult
import com.rmjtromp.trawler.state.LeaseStore
import com.rmjtromp.trawler.state.LeasedWork
import com.rmjtromp.trawler.state.PipelineScopedWorkQueueStore
import com.rmjtromp.trawler.state.ResourceHistoryStore
import com.rmjtromp.trawler.state.WorkCancellationStore
import com.rmjtromp.trawler.state.WorkItemId
import com.rmjtromp.trawler.state.WorkOrderingStore
import com.rmjtromp.trawler.state.WorkQueueStatus
import com.rmjtromp.trawler.state.WorkQueueStore
import com.rmjtromp.trawler.state.WorkTransition
import com.rmjtromp.trawler.transport.RequestExecutor
import com.rmjtromp.trawler.transport.RequestMiddleware
import com.rmjtromp.trawler.transport.ResponseHandler
import com.rmjtromp.trawler.transport.ResponseMiddleware
import com.rmjtromp.trawler.transport.Transport
import com.rmjtromp.trawler.transport.TransportRouter
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.junit.jupiter.api.Test

@OptIn(ExperimentalCoroutinesApi::class)
class EngineRuntimeTest {
    @Test
    fun `heterogeneous pipelines run concurrently without cross-contaminating typed values`() = runTest {
        val clock = MutableClock(Instant.parse("2026-01-01T00:00:00Z"))
        val intStarted = CompletableDeferred<Unit>()
        val stringStarted = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val intTransport = RecordingTransport("int-transport") { request ->
            intStarted.complete(Unit)
            release.await()
            response(request, "7")
        }
        val stringTransport = RecordingTransport("string-transport") { request ->
            stringStarted.complete(Unit)
            release.await()
            response(request, "word")
        }
        val intOutput = RecordingOutput<Int>("int-output")
        val stringOutput = RecordingOutput<String>("string-output")
        val intPipeline = pipeline(
            id = "numbers",
            router = RecordingRouter("int-router", intTransport),
            extractor = FunctionExtractor("int-extractor") { result, _ ->
                ExtractionResult.Success(listOf(result.body.text().toInt()))
            },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("number")))),
            outputs = listOf(intOutput to OutputRequirement.REQUIRED),
        )
        val stringPipeline = pipeline(
            id = "words",
            router = RecordingRouter("string-router", stringTransport),
            extractor = FunctionExtractor("string-extractor") { result, _ ->
                ExtractionResult.Success(listOf(result.body.text()))
            },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("word")))),
            outputs = listOf(stringOutput to OutputRequirement.REQUIRED),
        )
        val engine = engine(listOf(intPipeline, stringPipeline), clock = clock)

        try {
            val run = async(start = CoroutineStart.UNDISPATCHED) { engine.runOnce() }
            intStarted.await()
            stringStarted.await()
            assertTrue(intOutput.calls.isEmpty())
            assertTrue(stringOutput.calls.isEmpty())

            release.complete(Unit)
            val result = run.await()

            assertEquals(RunStatus.COMPLETED, result.status)
            assertEquals(listOf("numbers", "words"), result.pipelineRuns.map { it.pipelineId })
            assertEquals(listOf(7), intOutput.values())
            assertEquals(listOf("word"), stringOutput.values())
            assertTrue(intOutput.calls.flatten().all { it.context.pipelineId == "numbers" })
            assertTrue(stringOutput.calls.flatten().all { it.context.pipelineId == "words" })
            assertEquals(1, intTransport.calls.size)
            assertEquals(1, stringTransport.calls.size)
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `request and response middleware are ordered, mutate the routed path, and preserve exceptions`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val order = mutableListOf<String>()
        val transport = RecordingTransport("transport") { request ->
            order += "transport:${request.target}"
            response(request, "raw")
        }
        val router = RecordingRouter("router", transport)
        val extractor = FunctionExtractor("extractor") { result, _ ->
            assertEquals("response-mutated", result.body.text())
            assertEquals("final-target", result.finalTarget)
            ExtractionResult.Success(listOf(result.request.target))
        }
        val requestOne = namedRequestMiddleware("request-one", order) { request, context, next ->
            next.execute(
                request.copy(target = "mutated-one", headers = request.headers.plus(Header("X-One", "1"))),
                context.copy(request = request.copy(target = "mutated-one")),
            ).also { order += "request-one-after" }
        }
        val requestTwo = namedRequestMiddleware("request-two", order) { request, context, next ->
            next.execute(
                request.copy(target = "mutated-two", headers = request.headers.plus(Header("X-Two", "2"))),
                context.copy(request = request.copy(target = "mutated-two")),
            ).also { order += "request-two-after" }
        }
        val responseOne = namedResponseMiddleware("response-one", order) { value, context, next ->
            next.handle(value, context).copy(
                body = ResponseBody.from("response-mutated".toByteArray()),
                finalTarget = "final-target",
            ).also { order += "response-one-after" }
        }
        val responseTwo = namedResponseMiddleware("response-two", order) { value, context, next ->
            next.handle(value, context).also { order += "response-two-after" }
        }
        val pipeline = pipeline(
            id = "middleware",
            router = router,
            extractor = extractor,
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("original")))),
            requestMiddlewares = listOf(requestOne, requestTwo),
            responseMiddlewares = listOf(responseOne, responseTwo),
        )
        val engine = engine(listOf(pipeline), clock = clock)
        try {
            val result = engine.runOnce()
            assertEquals(RunStatus.COMPLETED, result.status)
            assertEquals(
                listOf(
                    "request-one-before",
                    "request-two-before",
                    "transport:mutated-two",
                    "response-one-before",
                    "response-two-before",
                    "response-two-after",
                    "response-one-after",
                    "request-two-after",
                    "request-one-after",
                ),
                order,
            )
            assertEquals("mutated-two", router.selected.single().first.target)
            assertEquals(listOf("mutated-two"), extractor.seen.map { it.request.target })
        } finally {
            engine.shutdown()
        }

        val requestCause = IllegalStateException("request middleware failure")
        val responseCause = IllegalArgumentException("response middleware failure")
        val failingRequestTransport = RecordingTransport("request-failing-transport") { request ->
            response(request)
        }
        val failingResponseTransport = RecordingTransport("response-failing-transport") { request ->
            response(request)
        }
        val failingRequest = pipeline(
            id = "request-failure",
            router = RecordingRouter("request-failure-router", failingRequestTransport),
            extractor = FunctionExtractor("request-failure-extractor") { _, _ -> ExtractionResult.Ignore() },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("request-failure")))),
            requestMiddlewares = listOf(namedRequestMiddleware("throw-request", mutableListOf()) { _, _, _ ->
                throw requestCause
            }),
        )
        val failingResponse = pipeline(
            id = "response-failure",
            router = RecordingRouter("response-failure-router", failingResponseTransport),
            extractor = FunctionExtractor("response-failure-extractor") { _, _ -> ExtractionResult.Ignore() },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("response-failure")))),
            responseMiddlewares = listOf(namedResponseMiddleware("throw-response", mutableListOf()) { _, _, _ ->
                throw responseCause
            }),
        )
        val failureEngine = engine(listOf(failingRequest, failingResponse), clock = clock)
        try {
            val result = failureEngine.runOnce()
            assertEquals(RunStatus.FAILED, result.status)
            assertTrue(result.failures.any { it.error === requestCause })
            assertTrue(result.failures.any { it.error === responseCause })
            assertTrue(failingRequestTransport.calls.isEmpty())
            assertEquals(1, failingResponseTransport.calls.size)
        } finally {
            failureEngine.shutdown()
        }
    }

    @Test
    fun `scope and authorization reject before attempts and transport`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val state = RecordingStateStore(clock)
        val scopeTransport = RecordingTransport("scope-transport") { request -> response(request) }
        val authorizationTransport = RecordingTransport("authorization-transport") { request -> response(request) }
        val scopePolicies = BuiltInPolicies.ordinary(clock = clock).copy(
            scope = RejectingScopePolicy("out-of-scope"),
        )
        val authorizationPolicies = BuiltInPolicies.ordinary(clock = clock).copy(
            authorization = DenyingAuthorizationPolicy("forbidden"),
        )
        val scopePipeline = pipeline(
            id = "scope",
            router = RecordingRouter("scope-router", scopeTransport),
            extractor = FunctionExtractor("scope-extractor") { _, _ -> ExtractionResult.Ignore() },
            clock = clock,
            policies = scopePolicies,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("scope")))),
        )
        val authorizationPipeline = pipeline(
            id = "authorization",
            router = RecordingRouter("authorization-router", authorizationTransport),
            extractor = FunctionExtractor("authorization-extractor") { _, _ -> ExtractionResult.Ignore() },
            clock = clock,
            policies = authorizationPolicies,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("authorization")))),
        )
        val engine = engine(listOf(scopePipeline, authorizationPipeline), state = state, clock = clock)
        try {
            val result = engine.runOnce()
            assertEquals(RunStatus.COMPLETED, result.status)
            val scope = result.pipelineRuns.first { it.pipelineId == "scope" }
            val authorization = result.pipelineRuns.first { it.pipelineId == "authorization" }
            assertEquals(0, scope.acceptedWork)
            assertEquals(1, scope.suppressedWork)
            assertEquals(0, authorization.requestsStarted)
            assertEquals(1, authorization.acceptedWork)
            assertTrue(scopeTransport.calls.isEmpty())
            assertTrue(authorizationTransport.calls.isEmpty())
            assertTrue(state.begunAttempts.isEmpty())
            assertTrue(state.transitions.any { it.second is WorkTransition.Skipped })
            assertEquals(0, state.delegate.activeWorkCount("authorization"))
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `cache hits bypass transport and foreign-pipeline cache results fail closed`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val cacheRequest = CrawlRequest("cached")
        val cacheResponse = response(cacheRequest, "cached-value")
        val owned = StoredResponse(ResponseReference("owned-cache"), cacheResponse, pipelineId = "cached-pipeline")
        val transport = RecordingTransport("cache-transport") { request -> error("transport must not run for cache hit") }
        val cache = FixedCachePolicy(ResponseCacheDecision.Hit(owned))
        val output = RecordingOutput<String>("cache-output")
        val pipeline = pipeline(
            id = "cached-pipeline",
            router = RecordingRouter("cache-router", transport),
            extractor = FunctionExtractor("cache-extractor") { result, _ ->
                ExtractionResult.Success(listOf(result.body.text()))
            },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(cacheRequest))),
            outputs = listOf(output to OutputRequirement.REQUIRED),
        )
        val store = RecordingResponseStore()
        val engine = engine(listOf(pipeline), responseStore = store, cachePolicy = cache, clock = clock)
        try {
            val result = engine.runOnce()
            assertEquals(RunStatus.COMPLETED, result.status)
            assertEquals(listOf("cached-value"), output.values())
            assertTrue(transport.calls.isEmpty())
            assertEquals(1, cache.lookups)
            assertTrue(store.putResponses.isEmpty())
        } finally {
            engine.shutdown()
        }

        val foreign = FixedCachePolicy(
            ResponseCacheDecision.Hit(
                StoredResponse(ResponseReference("foreign-cache"), cacheResponse, pipelineId = "other-pipeline"),
            ),
        )
        val foreignTransport = RecordingTransport("foreign-transport") { request ->
            error("foreign cache result must be rejected before transport")
        }
        val foreignPipeline = pipeline(
            id = "foreign-selected",
            router = RecordingRouter("foreign-router", foreignTransport),
            extractor = FunctionExtractor("foreign-extractor") { _, _ -> ExtractionResult.Ignore() },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("foreign")))),
        )
        val foreignEngine = engine(
            listOf(foreignPipeline),
            responseStore = RecordingResponseStore(),
            cachePolicy = foreign,
            clock = clock,
        )
        try {
            val result = foreignEngine.runOnce()
            assertEquals(RunStatus.FAILED, result.status)
            assertTrue(result.failures.single().error is IllegalArgumentException)
            assertTrue(foreignTransport.calls.isEmpty())
        } finally {
            foreignEngine.shutdown()
        }
    }

    @Test
    fun `capture policy controls store, skip, decline, required failure, and thrown failure`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        suspend fun runCase(
            id: String,
            policy: ResponseCapturePolicy,
            store: RecordingResponseStore,
        ): PipelineRunResult {
            val output = RecordingOutput<String>("$id-output")
            val pipeline = pipeline(
                id = id,
                router = RecordingRouter("$id-router", RecordingTransport("$id-transport") { request -> response(request) }),
                extractor = FunctionExtractor("$id-extractor") { _, _ -> ExtractionResult.Success(listOf(id)) },
                clock = clock,
                discoveries = listOf(SeedDiscovery(listOf(CrawlRequest(id)))),
                outputs = listOf(output to OutputRequirement.REQUIRED),
            )
            val engine = engine(
                listOf(pipeline),
                responseStore = store,
                capturePolicy = policy,
                clock = clock,
            )
            return try {
                engine.runOnce().pipelineRuns.single()
            } finally {
                engine.shutdown()
            }
        }

        val storedStore = RecordingResponseStore { response -> ResponseReference("stored") }
        val stored = runCase("stored", CaptureResponsesWhenConfigured, storedStore)
        assertEquals(RunStatus.COMPLETED, stored.status)
        assertEquals(1, storedStore.putResponses.size)

        val skippedStore = RecordingResponseStore { response -> ResponseReference("must-not-store") }
        val skipped = runCase("skipped", DoNotCaptureResponses, skippedStore)
        assertEquals(RunStatus.COMPLETED, skipped.status)
        assertTrue(skippedStore.putResponses.isEmpty())

        val policyCause = IllegalStateException("capture policy failed")
        val policyFailure = runCase(
            "policy-failure",
            FixedCapturePolicy(ResponseCaptureDecision.Fail(policyCause)),
            RecordingResponseStore { ResponseReference("forbidden") },
        )
        assertEquals(RunStatus.FAILED, policyFailure.status)
        assertSame(policyCause, policyFailure.failures.single().error)

        val declineStore = RecordingResponseStore { null }
        val declined = runCase("declined", CaptureResponsesWhenConfigured, declineStore)
        assertEquals(RunStatus.COMPLETED, declined.status)
        assertEquals(1, declineStore.putResponses.size)

        val requiredDeclineStore = RecordingResponseStore { null }
        val requiredDecline = runCase("required-decline", com.rmjtromp.trawler.response.RequireResponseCapture, requiredDeclineStore)
        assertEquals(RunStatus.FAILED, requiredDecline.status)
        assertTrue(
            requiredDecline.failures.single().error.message!!.contains("declined"),
            requiredDecline.failures.joinToString { "${it.stage}:${it.error}" },
        )

        val storeCause = IllegalArgumentException("store failed")
        val throwingStore = RecordingResponseStore { throw storeCause }
        val thrown = runCase("store-throw", CaptureResponsesWhenConfigured, throwingStore)
        assertEquals(RunStatus.FAILED, thrown.status)
        assertSame(storeCause, thrown.failures.single().error)
    }

    @Test
    fun `response capture stops before storing when the lease is no longer current`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val state = RecordingStateStore(clock)
        state.checkCurrentBehavior = { work ->
            state.delegate.transition(work, WorkTransition.Abandoned("lease reclaimed"))
            LeaseMutationResult.Stale
        }
        val store = RecordingResponseStore { ResponseReference("must-not-attach") }
        val output = RecordingOutput<String>("stale-output")
        val pipeline = pipeline(
            id = "stale-capture",
            router = RecordingRouter("stale-router", RecordingTransport("stale-transport") { request -> response(request) }),
            extractor = FunctionExtractor("stale-extractor") { _, _ -> ExtractionResult.Success(listOf("value")) },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("stale-capture")))),
            outputs = listOf(output to OutputRequirement.REQUIRED),
        )
        val engine = engine(
            listOf(pipeline),
            state = state,
            responseStore = store,
            capturePolicy = CaptureResponsesWhenConfigured,
            clock = clock,
        )
        try {
            val result = engine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.ABANDONED, result.status)
            assertTrue(store.putResponses.isEmpty())
            assertTrue(output.calls.isEmpty())
            assertTrue(state.checkCurrentCalls >= 1)
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `response capture does not attach a reference after a concurrent lease loss`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val state = RecordingStateStore(clock)
        var checks = 0
        state.checkCurrentBehavior = { work ->
            checks++
            if (checks == 2) {
                state.delegate.transition(work, WorkTransition.Abandoned("lease reclaimed"))
                LeaseMutationResult.Stale
            } else {
                state.delegate.checkCurrent(work)
            }
        }
        val store = RecordingResponseStore { ResponseReference("orphaned-response") }
        val output = RecordingOutput<String>("post-capture-stale-output")
        val pipeline = pipeline(
            id = "post-capture-stale",
            router = RecordingRouter(
                "post-capture-stale-router",
                RecordingTransport("post-capture-stale-transport") { request -> response(request) },
            ),
            extractor = FunctionExtractor("post-capture-stale-extractor") { _, _ ->
                ExtractionResult.Success(listOf("value"))
            },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("post-capture-stale")))),
            outputs = listOf(output to OutputRequirement.REQUIRED),
        )
        val engine = engine(
            listOf(pipeline),
            state = state,
            responseStore = store,
            capturePolicy = CaptureResponsesWhenConfigured,
            clock = clock,
        )
        try {
            val result = engine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.ABANDONED, result.status)
            assertEquals(1, store.putResponses.size)
            assertTrue(output.calls.isEmpty())
            assertEquals(2, checks)
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `discovery uses the controlled executor, handles existing responses and follow-ups, deduplicates active work, and allows recrawl`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val transport = RecordingTransport("discovery-transport") { request -> response(request, request.target) }
        val discovered = DiscoveryWithExecutor("discovery") { context ->
            val probe = context.executor.execute(CrawlRequest("probe"), RequestContext(context.crawler, CrawlRequest("probe")))
            assertEquals("probe", probe.request.target)
            listOf(
                DiscoveryEmission.Request(CrawlRequest("root")),
                DiscoveryEmission.Request(CrawlRequest("root")),
                DiscoveryEmission.ExistingResponse(
                    request = CrawlRequest("existing"),
                    response = response(CrawlRequest("existing"), "existing"),
                ),
            )
        }
        val output = RecordingOutput<String>("discovery-output")
        val extractor = FunctionExtractor("discovery-extractor") { result, _ ->
            val target = result.request.target
            if (target == "root") {
                ExtractionResult.Success(listOf("root"), listOf(CrawlRequest("follow-up")))
            } else {
                ExtractionResult.Success(listOf(target))
            }
        }
        val pipeline = pipeline(
            id = "discovery-pipeline",
            router = RecordingRouter("discovery-router", transport),
            extractor = extractor,
            clock = clock,
            discoveries = listOf(discovered),
            outputs = listOf(output to OutputRequirement.REQUIRED),
        )
        val state = InMemoryCrawlStateStore(maxQueueSize = 8, clock = clock)
        val engine = engine(listOf(pipeline), state = state, clock = clock)
        try {
            val first = engine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.COMPLETED, first.status)
            assertEquals(3, first.discoveryEmissions)
            assertEquals(3, first.acceptedWork)
            assertEquals(1, first.suppressedWork, "accepted=${first.acceptedWork} suppressed=${first.suppressedWork}")
            assertEquals(3, first.requestsStarted)
            assertEquals(listOf("existing", "follow-up", "root"), output.values().sorted())
            assertEquals(listOf("follow-up", "probe", "root"), transport.calls.map { it.target }.sorted())
            val rootIdentity = pipeline.policies.requestIdentity.identify(
                CrawlRequest("root"),
                RequestContext(crawler("discovery-pipeline"), CrawlRequest("root")),
            )
            assertEquals(1L, state.history("discovery-pipeline", rootIdentity).visitCount)

            val second = engine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.COMPLETED, second.status)
            assertEquals(3, second.acceptedWork)
            assertEquals(1, second.suppressedWork)
            assertEquals(6, output.values().size)
            assertEquals(6, transport.calls.size)
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `extraction decisions, transforms, validators, and output requirements preserve causes and categories`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val successOutput = RecordingOutput<Int>("success-output")
        val success = pipeline(
            id = "success",
            router = RecordingRouter("success-router", RecordingTransport("success-transport") { request -> response(request, "1") }),
            extractor = FunctionExtractor("success-extractor") { _, _ -> ExtractionResult.Success(listOf(1, 2, 3)) },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("success")))),
            transforms = listOf(
                FunctionTransform("mutate") { value, _ -> TransformDecision.Mutate(value * 10) },
                FunctionTransform("drop-two") { value, _ ->
                    if (value == 20) TransformDecision.Drop("filtered") else TransformDecision.Accept(value)
                },
            ),
            validators = listOf(
                FunctionValidator("reject-thirty") { value, _ ->
                    if (value == 30) ValidationResult.Rejected("not thirty") else ValidationResult.Accepted
                },
            ),
            outputs = listOf(successOutput to OutputRequirement.REQUIRED),
        )
        val ignored = pipeline(
            id = "ignored",
            router = RecordingRouter("ignored-router", RecordingTransport("ignored-transport") { request -> response(request) }),
            extractor = FunctionExtractor("ignored-extractor") { _, _ -> ExtractionResult.Ignore("not useful") },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("ignored")))),
        )
        val extractionCause = IllegalStateException("extraction result failure")
        val extractionFailure = pipeline(
            id = "extraction-failure",
            router = RecordingRouter("extraction-failure-router", RecordingTransport("extraction-failure-transport") { request -> response(request) }),
            extractor = FunctionExtractor("extraction-failure-extractor") { _, _ -> ExtractionResult.Failure(extractionCause) },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("extraction-failure")))),
        )
        val thrownExtractionCause = IllegalArgumentException("extractor threw")
        val thrownExtraction = pipeline(
            id = "extraction-throw",
            router = RecordingRouter("extraction-throw-router", RecordingTransport("extraction-throw-transport") { request -> response(request) }),
            extractor = FunctionExtractor("extraction-throw-extractor") { _, _ -> throw thrownExtractionCause },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("extraction-throw")))),
        )
        val transformCause = IllegalStateException("transform threw")
        val transformFailure = pipeline(
            id = "transform-throw",
            router = RecordingRouter("transform-throw-router", RecordingTransport("transform-throw-transport") { request -> response(request, "1") }),
            extractor = FunctionExtractor("transform-throw-extractor") { _, _ -> ExtractionResult.Success(listOf(1)) },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("transform-throw")))),
            transforms = listOf(FunctionTransform("throw-transform") { _, _ -> throw transformCause }),
        )
        val validatorCause = IllegalStateException("validator failed")
        val validatorFailure = pipeline(
            id = "validator-failure",
            router = RecordingRouter("validator-failure-router", RecordingTransport("validator-failure-transport") { request -> response(request, "1") }),
            extractor = FunctionExtractor("validator-failure-extractor") { _, _ -> ExtractionResult.Success(listOf(1)) },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("validator-failure")))),
            validators = listOf(FunctionValidator("failed-validator") { _, _ -> ValidationResult.Failed(validatorCause) }),
        )
        val validatorThrownCause = IllegalArgumentException("validator threw")
        val validatorThrown = pipeline(
            id = "validator-throw",
            router = RecordingRouter("validator-throw-router", RecordingTransport("validator-throw-transport") { request -> response(request, "1") }),
            extractor = FunctionExtractor("validator-throw-extractor") { _, _ -> ExtractionResult.Success(listOf(1)) },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("validator-throw")))),
            validators = listOf(FunctionValidator("throw-validator") { _, _ -> throw validatorThrownCause }),
        )
        val engine = engine(listOf(success, ignored, extractionFailure, thrownExtraction, transformFailure, validatorFailure, validatorThrown), clock = clock)
        try {
            val result = engine.runOnce()
            assertEquals(RunStatus.FAILED, result.status)
            assertEquals(listOf(10), successOutput.values())
            assertEquals(RunStatus.COMPLETED, result.pipelineRuns.first { it.pipelineId == "ignored" }.status)
            assertSame(extractionCause, result.pipelineRuns.first { it.pipelineId == "extraction-failure" }.failures.single().error)
            assertSame(thrownExtractionCause, result.pipelineRuns.first { it.pipelineId == "extraction-throw" }.failures.single().error)
            assertSame(transformCause, result.pipelineRuns.first { it.pipelineId == "transform-throw" }.failures.single().error)
            assertSame(validatorCause, result.pipelineRuns.first { it.pipelineId == "validator-failure" }.failures.single().error)
            assertSame(validatorThrownCause, result.pipelineRuns.first { it.pipelineId == "validator-throw" }.failures.single().error)
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `required and best-effort output delivery, acknowledgement mismatch, no-output, and batching are explicit`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val requiredCause = IllegalStateException("required output failed")
        val required = RecordingOutput<Int>("required") { OutputResult.Failed(requiredCause) }
        val bestEffortCause = IllegalArgumentException("best effort failed")
        val bestEffort = RecordingOutput<Int>("best-effort") { OutputResult.Failed(bestEffortCause) }
        val mismatchRequired = RecordingOutput<Int>("mismatch-required") { OutputResult.Accepted(0) }
        val mismatchBestEffort = RecordingOutput<Int>("mismatch-best-effort") { OutputResult.Accepted(0) }
        val batched = RecordingOutput<Int>("batched") { items -> OutputResult.Accepted(items.size) }
        fun outputPipeline(
            id: String,
            output: Output<Int>?,
            requirement: OutputRequirement = OutputRequirement.REQUIRED,
        ): Pipeline<Int> = pipeline(
            id = id,
            router = RecordingRouter("$id-router", RecordingTransport("$id-transport") { request -> response(request, "1") }),
            extractor = FunctionExtractor("$id-extractor") { _, _ -> ExtractionResult.Success(listOf(1, 2, 3)) },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest(id)))),
            outputs = output?.let { listOf(it to requirement) } ?: emptyList(),
        )
        val noOutput = outputPipeline("no-output", null)
        val requiredPipeline = outputPipeline("required", required)
        val bestEffortPipeline = outputPipeline("best-effort", bestEffort, OutputRequirement.BEST_EFFORT)
        val mismatchRequiredPipeline = outputPipeline("mismatch-required", mismatchRequired)
        val mismatchBestEffortPipeline = outputPipeline("mismatch-best-effort", mismatchBestEffort, OutputRequirement.BEST_EFFORT)
        val batchedPipeline = outputPipeline("batched", batched)
        val options = EngineRuntimeOptions(maxOutputBatchSize = 2)
        val engine = engine(
            listOf(noOutput, requiredPipeline, bestEffortPipeline, mismatchRequiredPipeline, mismatchBestEffortPipeline, batchedPipeline),
            clock = clock,
            options = options,
        )
        try {
            val result = engine.runOnce()
            assertEquals(RunStatus.FAILED, result.status)
            assertEquals(RunStatus.COMPLETED, result.pipelineRuns.first { it.pipelineId == "no-output" }.status)
            assertSame(requiredCause, result.pipelineRuns.first { it.pipelineId == "required" }.failures.single().error)
            assertEquals(RunStatus.COMPLETED, result.pipelineRuns.first { it.pipelineId == "best-effort" }.status)
            assertTrue(result.pipelineRuns.first { it.pipelineId == "best-effort" }.failures.any { it.error === bestEffortCause })
            assertEquals(RunStatus.FAILED, result.pipelineRuns.first { it.pipelineId == "mismatch-required" }.status)
            assertTrue(result.pipelineRuns.first { it.pipelineId == "mismatch-required" }.failures.single().error.message!!.contains("unexpected"))
            assertEquals(RunStatus.COMPLETED, result.pipelineRuns.first { it.pipelineId == "mismatch-best-effort" }.status)
            assertTrue(result.pipelineRuns.first { it.pipelineId == "mismatch-best-effort" }.failures.any {
                it.error.message!!.contains("unexpected")
            })
            assertEquals(listOf(listOf(1, 2), listOf(3)), batched.calls.map { it.map(Produced<Int>::value) })
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `retry decisions remain visible, bounded, and correctly account attempts`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        suspend fun runRetryCase(
            id: String,
            decision: RetryDecision,
            maxAttempts: Int = 3,
            transportFailures: Int = Int.MAX_VALUE,
        ): Pair<PipelineRunResult, RecordingStateStore> {
            val state = RecordingStateStore(clock)
            var calls = 0
            val transport = RecordingTransport("$id-transport") { request ->
                calls++
                if (calls <= transportFailures) throw IllegalStateException("$id failure $calls")
                response(request, "ok")
            }
            val retry = RecordingRetryPolicy { _, _, _ -> decision }
            val policies = BuiltInPolicies.ordinary(clock = clock).copy(
                retry = retry,
                failureClassifier = DefaultFailureClassifier,
            )
            val pipeline = pipeline(
                id = id,
                router = RecordingRouter("$id-router", transport),
                extractor = FunctionExtractor("$id-extractor") { _, _ -> ExtractionResult.Ignore() },
                clock = clock,
                policies = policies,
                discoveries = listOf(SeedDiscovery(listOf(CrawlRequest(id)))),
            )
            val engine = engine(
                listOf(pipeline),
                state = state,
                clock = clock,
                options = EngineRuntimeOptions(maxAttemptsPerWorkItem = maxAttempts),
            )
            return try {
                engine.runOnce().pipelineRuns.single() to state
            } finally {
                engine.shutdown()
            }
        }

        val retryNow = runRetryCase("retry-now", RetryDecision.RetryNow, transportFailures = 1)
        assertEquals(RunStatus.COMPLETED, retryNow.first.status)
        assertEquals(2, retryNow.second.begunAttempts.size)
        assertEquals(listOf(1, 2), retryNow.second.begunAttempts.map { it.number })

        val retryAtState = RecordingStateStore(clock)
        var retryAtCalls = 0
        val retryAtPipeline = pipeline(
            id = "retry-at",
            router = RecordingRouter("retry-at-router", RecordingTransport("retry-at-transport") { request ->
                retryAtCalls++
                if (retryAtCalls == 1) throw IllegalStateException("retry later")
                response(request)
            }),
            extractor = FunctionExtractor("retry-at-extractor") { _, _ -> ExtractionResult.Ignore() },
            clock = clock,
            policies = BuiltInPolicies.ordinary(clock = clock).copy(
                retry = RecordingRetryPolicy { _, _, _ -> RetryDecision.RetryAt(clock.now().plusSeconds(5)) },
            ),
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("retry-at")))),
        )
        val retryAtEngine = engine(listOf(retryAtPipeline), state = retryAtState, clock = clock)
        try {
            val first = retryAtEngine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.ABANDONED, first.status)
            clock.current = clock.current.plusSeconds(5)
            val second = retryAtEngine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.COMPLETED, second.status)
            assertEquals(2, retryAtState.begunAttempts.size)
        } finally {
            retryAtEngine.shutdown()
        }

        val noRetry = runRetryCase("no-retry", RetryDecision.DoNotRetry)
        assertEquals(RunStatus.FAILED, noRetry.first.status)
        assertEquals(1, noRetry.second.begunAttempts.size)

        val abandon = runRetryCase("abandon", RetryDecision.Abandon("operator pause"))
        assertEquals(RunStatus.ABANDONED, abandon.first.status)
        assertEquals(1, abandon.second.begunAttempts.size)

        val maxAttempts = runRetryCase("max-attempts", RetryDecision.RetryNow, maxAttempts = 1)
        assertEquals(RunStatus.FAILED, maxAttempts.first.status)
        assertEquals(1, maxAttempts.second.begunAttempts.size)
        assertEquals(0, maxAttempts.second.delegate.activeWorkCount("max-attempts"))
    }

    @Test
    fun `lease loss cancels active processing and ordinary cancellation cleans queued and leased work`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val leaseState = RecordingStateStore(clock).also { state ->
            state.renewBehavior = { work, _ ->
                state.delegate.transition(work, WorkTransition.Abandoned("lease reclaimed"))
                LeaseMutationResult.Stale
            }
        }
        val started = CompletableDeferred<Unit>()
        val cancelled = CompletableDeferred<Unit>()
        val leaseTransport = RecordingTransport("lease-transport") {
            started.complete(Unit)
            try {
                awaitCancellation()
            } finally {
                cancelled.complete(Unit)
            }
        }
        val leasePipeline = pipeline(
            id = "lease-loss",
            router = RecordingRouter("lease-router", leaseTransport),
            extractor = FunctionExtractor("lease-extractor") { _, _ -> ExtractionResult.Ignore() },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("lease-loss")))),
        )
        val leaseEngine = engine(
            listOf(leasePipeline),
            state = leaseState,
            clock = clock,
            options = EngineRuntimeOptions(leaseDuration = Duration.ofMillis(6)),
        )
        try {
            val run = async(start = CoroutineStart.UNDISPATCHED) { leaseEngine.runOnce() }
            started.await()
            advanceTimeBy(2)
            runCurrent()
            val result = run.await()
            assertEquals(RunStatus.ABANDONED, result.status)
            cancelled.await()
            assertEquals(1, leaseState.renewCalls)
            assertEquals(listOf(AttemptOutcome.ABANDONED), leaseState.finishedAttempts.map { it.second })
            assertEquals(0, leaseState.delegate.activeWorkCount("lease-loss"))
        } finally {
            leaseEngine.shutdown()
        }

        val cancellationState = RecordingStateStore(clock)
        val cancellationEvents = mutableListOf<CrawlerEvent>()
        val workStarted = CompletableDeferred<Unit>()
        val workCancelled = CompletableDeferred<Unit>()
        val cancellationTransport = RecordingTransport("cancellation-transport") {
            workStarted.complete(Unit)
            try {
                awaitCancellation()
            } finally {
                workCancelled.complete(Unit)
            }
        }
        val cancellationPipeline = pipeline(
            id = "cancel",
            router = RecordingRouter("cancel-router", cancellationTransport),
            extractor = FunctionExtractor("cancel-extractor") { _, _ -> ExtractionResult.Ignore() },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("active"), CrawlRequest("queued")))),
        )
        val cancellationEngine = engine(
            listOf(cancellationPipeline),
            state = cancellationState,
            clock = clock,
            observers = listOf(RecordingObserver("cancellation-observer") { event -> cancellationEvents += event }),
            options = EngineRuntimeOptions(workerCount = 1),
        )
        try {
            val run = async(start = CoroutineStart.UNDISPATCHED) { cancellationEngine.runOnce() }
            workStarted.await()
            run.cancel()
            assertFailsWith<CancellationException> { run.await() }
            workCancelled.await()
            assertTrue(cancellationState.cancelQueuedCalls > 0)
            assertTrue(cancellationState.transitions.any { it.second is WorkTransition.Cancelled })
            assertEquals(0, cancellationState.delegate.activeWorkCount("cancel"))
            assertTrue(cancellationEvents.any { it.type == CrawlerEventType.ENGINE_RUN_CANCELLED })
            assertIs<EnqueueResult.Accepted>(
                cancellationState.delegate.enqueue(CrawlRequest("active"), RequestIdentity("active"), crawler("cancel")),
            )
        } finally {
            cancellationEngine.shutdown()
        }
    }

    @Test
    fun `run timeout and pending-run bounds are deterministic and leave resources released`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val state = RecordingStateStore(clock)
        val started = CompletableDeferred<Unit>()
        val cancelled = CompletableDeferred<Unit>()
        val transport = RecordingTransport("timeout-transport") {
            started.complete(Unit)
            try {
                awaitCancellation()
            } finally {
                cancelled.complete(Unit)
            }
        }
        val pipeline = pipeline(
            id = "timeout",
            router = RecordingRouter("timeout-router", transport),
            extractor = FunctionExtractor("timeout-extractor") { _, _ -> ExtractionResult.Ignore() },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("timeout")))),
        )
        val engine = engine(
            listOf(pipeline),
            state = state,
            clock = clock,
            options = EngineRuntimeOptions(
                maxPendingRuns = 1,
                runTimeout = Duration.ofMillis(5),
            ),
        )
        try {
            val first = async(start = CoroutineStart.UNDISPATCHED) { engine.runOnce() }
            started.await()
            assertFailsWith<IllegalStateException> { engine.runOnce() }
            advanceTimeBy(5)
            runCurrent()
            val result = first.await()
            assertEquals(RunStatus.ABANDONED, result.status)
            assertEquals("run-timeout", result.failures.single().stage)
            cancelled.await()
            assertEquals(0, state.delegate.activeWorkCount("timeout"))
            assertTrue(engine.awaitIdle(Duration.ofSeconds(1)))
            assertFailsWith<IllegalArgumentException> { engine.awaitIdle(Duration.ZERO) }
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `runtime metadata, discovery, follow-up, value, response, and work bounds are enforced`() = runTest {
        val requestCases = listOf(
            "target" to CrawlRequest("too-long"),
            "method" to CrawlRequest("method", method = RequestMethod("POST")),
            "request-body" to CrawlRequest("body", body = RequestBody.from(byteArrayOf(1, 2))),
            "header-count" to CrawlRequest("header-count", headers = Headers.of(Header("a", "1"), Header("b", "2"))),
            "header-name" to CrawlRequest("header-name", headers = Headers.of(Header("long", "1"))),
            "header-value" to CrawlRequest("header-value", headers = Headers.of(Header("a", "long"))),
            "attribute-count" to CrawlRequest("attribute-count", attributes = ContextAttributes.of(ContextAttribute("a", "1"), ContextAttribute("b", "2"))),
            "attribute-name" to CrawlRequest("attribute-name", attributes = ContextAttributes.of(ContextAttribute("long", "1"))),
            "attribute-value" to CrawlRequest("attribute-value", attributes = ContextAttributes.of(ContextAttribute("a", "long"))),
        )
        requestCases.forEach { (id, request) ->
            val clock = MutableClock(Instant.EPOCH)
            val transport = RecordingTransport("$id-transport") { value -> response(value) }
            val options = EngineRuntimeOptions(
                maxTargetBytes = if (id == "target") 1 else 64,
                maxMethodBytes = if (id == "method") 1 else 64,
                maxRequestBodyBytes = if (id == "request-body") 1 else 64,
                maxHeaderCount = if (id == "header-count") 1 else 8,
                maxHeaderNameBytes = if (id == "header-name") 1 else 64,
                maxHeaderValueBytes = if (id == "header-value") 1 else 64,
                maxAttributeCount = if (id == "attribute-count") 1 else 8,
                maxAttributeNameBytes = if (id == "attribute-name") 1 else 64,
                maxAttributeValueBytes = if (id == "attribute-value") 1 else 64,
            )
            val pipeline = pipeline(
                id = id,
                router = RecordingRouter("$id-router", transport),
                extractor = FunctionExtractor("$id-extractor") { _, _ -> ExtractionResult.Ignore() },
                clock = clock,
                discoveries = listOf(SeedDiscovery(listOf(request))),
            )
            val engine = engine(listOf(pipeline), clock = clock, options = options)
            try {
                val result = engine.runOnce().pipelineRuns.single()
                assertEquals(RunStatus.ABANDONED, result.status, id)
                assertEquals(0, result.acceptedWork, id)
                assertTrue(transport.calls.isEmpty(), id)
            } finally {
                engine.shutdown()
            }
        }

        val responseBodyClock = MutableClock(Instant.EPOCH)
        val responseBodyTransport = RecordingTransport("response-body-transport") { request ->
            response(request, "too-large")
        }
        val responseBodyPipeline = pipeline(
            id = "response-body",
            router = RecordingRouter("response-body-router", responseBodyTransport),
            extractor = FunctionExtractor("response-body-extractor") { _, _ -> ExtractionResult.Ignore() },
            clock = responseBodyClock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("response-body")))),
        )
        val responseBodyEngine = engine(
            listOf(responseBodyPipeline),
            clock = responseBodyClock,
            options = EngineRuntimeOptions(maxResponseBodyBytes = 1),
        )
        try {
            val result = responseBodyEngine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.FAILED, result.status)
            assertTrue(result.failures.single().error.message!!.contains("response body"))
        } finally {
            responseBodyEngine.shutdown()
        }

        val responseMetadataCases: List<Pair<String, (CrawlRequest) -> CrawlResponse>> = listOf(
            "response-final-target" to { request -> response(request, finalTarget = "too-long") },
            "response-header-count" to { request ->
                response(request, headers = Headers.of(Header("a", "1"), Header("b", "2")))
            },
            "response-header-name" to { request ->
                response(request, headers = Headers.of(Header("long", "1")))
            },
            "response-header-value" to { request ->
                response(request, headers = Headers.of(Header("a", "long")))
            },
            "response-attribute-count" to { request ->
                response(
                    request,
                    attributes = ContextAttributes.of(
                        ContextAttribute("a", "1"),
                        ContextAttribute("b", "2"),
                        ContextAttribute("c", "3"),
                    ),
                )
            },
            "response-attribute-name" to { request ->
                response(request, attributes = ContextAttributes.of(ContextAttribute("x".repeat(33), "1")))
            },
            "response-attribute-value" to { request ->
                response(request, attributes = ContextAttributes.of(ContextAttribute("a", "x".repeat(33))))
            },
        )
        responseMetadataCases.forEach { (id, responseFactory) ->
            val clock = MutableClock(Instant.EPOCH)
            val transport = RecordingTransport("$id-transport") { request -> responseFactory(request) }
            val options = EngineRuntimeOptions(
                maxTargetBytes = if (id == "response-final-target") 1 else 64,
                maxHeaderCount = if (id == "response-header-count") 1 else 8,
                maxHeaderNameBytes = if (id == "response-header-name") 1 else 64,
                maxHeaderValueBytes = if (id == "response-header-value") 1 else 64,
                maxAttributeCount = if (id == "response-attribute-count") 2 else 64,
                maxAttributeNameBytes = if (id == "response-attribute-name") 32 else 64,
                maxAttributeValueBytes = if (id == "response-attribute-value") 32 else 64,
            )
            val pipeline = pipeline(
                id = id,
                router = RecordingRouter("$id-router", transport),
                extractor = FunctionExtractor("$id-extractor") { _, _ -> ExtractionResult.Ignore() },
                clock = clock,
                discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("x")))),
            )
            val engine = engine(listOf(pipeline), clock = clock, options = options)
            try {
                val result = engine.runOnce().pipelineRuns.single()
                assertEquals(RunStatus.FAILED, result.status, id)
                assertEquals(1, transport.calls.size, id)
            } finally {
                engine.shutdown()
            }
        }

        val boundClock = MutableClock(Instant.EPOCH)
        val boundOutput = RecordingOutput<Int>("bound-output")
        val boundPipeline = pipeline(
            id = "stage-bounds",
            router = RecordingRouter("stage-bounds-router", RecordingTransport("stage-bounds-transport") { request -> response(request) }),
            extractor = FunctionExtractor("stage-bounds-extractor") { _, _ ->
                ExtractionResult.Success(
                    values = listOf(1, 2),
                    followUps = listOf(CrawlRequest("follow-1"), CrawlRequest("follow-2")),
                )
            },
            clock = boundClock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("stage-bounds")))),
            outputs = listOf(boundOutput to OutputRequirement.REQUIRED),
        )
        val boundEngine = engine(
            listOf(boundPipeline),
            clock = boundClock,
            options = EngineRuntimeOptions(
                maxValuesPerResponse = 1,
                maxFollowUpRequestsPerWorkItem = 1,
            ),
        )
        try {
            val result = boundEngine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.FAILED, result.status)
            assertTrue(result.failures.single().stage.contains("extraction-bound"))
            assertTrue(boundOutput.calls.isEmpty())
        } finally {
            boundEngine.shutdown()
        }

        val followUpClock = MutableClock(Instant.EPOCH)
        val followUpPipeline = pipeline(
            id = "follow-up-bound",
            router = RecordingRouter("follow-up-router", RecordingTransport("follow-up-transport") { request -> response(request) }),
            extractor = FunctionExtractor("follow-up-extractor") { _, _ ->
                ExtractionResult.Success(values = listOf(1), followUps = listOf(CrawlRequest("one"), CrawlRequest("two")))
            },
            clock = followUpClock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("follow-up-bound")))),
        )
        val followUpEngine = engine(
            listOf(followUpPipeline),
            clock = followUpClock,
            options = EngineRuntimeOptions(maxFollowUpRequestsPerWorkItem = 1),
        )
        try {
            val result = followUpEngine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.FAILED, result.status)
            assertTrue(result.failures.single().stage.contains("follow-up-bound"))
        } finally {
            followUpEngine.shutdown()
        }

        val producedClock = MutableClock(Instant.EPOCH)
        val producedOutput = RecordingOutput<Int>("produced-bound")
        val producedPipeline = pipeline(
            id = "produced-bound",
            router = RecordingRouter("produced-router", RecordingTransport("produced-transport") { request -> response(request) }),
            extractor = FunctionExtractor("produced-extractor") { _, _ -> ExtractionResult.Success(listOf(1, 2)) },
            clock = producedClock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("produced-bound")))),
            outputs = listOf(producedOutput to OutputRequirement.REQUIRED),
        )
        val producedEngine = engine(
            listOf(producedPipeline),
            clock = producedClock,
            options = EngineRuntimeOptions(maxProducedValuesPerRun = 1),
        )
        try {
            val result = producedEngine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.ABANDONED, result.status)
            assertEquals(1, result.producedValues)
            assertTrue(producedOutput.calls.isEmpty())
        } finally {
            producedEngine.shutdown()
        }

        val discoveryClock = MutableClock(Instant.EPOCH)
        val discoveryTransport = RecordingTransport("discovery-bound-transport") { request -> response(request) }
        val discoveryBoundPipeline = pipeline(
            id = "discovery-bound",
            router = RecordingRouter("discovery-bound-router", discoveryTransport),
            extractor = FunctionExtractor("discovery-bound-extractor") { _, _ -> ExtractionResult.Ignore() },
            clock = discoveryClock,
            discoveries = listOf(
                SeedDiscovery(listOf(CrawlRequest("one"), CrawlRequest("two")), maxEmissions = 2),
            ),
        )
        val discoveryBoundEngine = engine(
            listOf(discoveryBoundPipeline),
            clock = discoveryClock,
            options = EngineRuntimeOptions(maxDiscoveryEmissionsPerDiscovery = 1, maxDiscoveryEmissionsPerRun = 1),
        )
        try {
            val result = discoveryBoundEngine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.ABANDONED, result.status)
            assertEquals(1, result.discoveryEmissions)
            assertEquals(1, result.acceptedWork)
            assertEquals(1, discoveryTransport.calls.size)
        } finally {
            discoveryBoundEngine.shutdown()
        }

        listOf(
            "queue-bound" to EngineRuntimeOptions(maxQueueSize = 1),
            "work-bound" to EngineRuntimeOptions(maxWorkItemsPerRun = 1),
        ).forEach { (id, options) ->
            val clock = MutableClock(Instant.EPOCH)
            val transport = RecordingTransport("$id-transport") { request -> response(request) }
            val pipeline = pipeline(
                id = id,
                router = RecordingRouter("$id-router", transport),
                extractor = FunctionExtractor("$id-extractor") { _, _ -> ExtractionResult.Ignore() },
                clock = clock,
                discoveries = listOf(
                    SeedDiscovery(listOf(CrawlRequest("$id-one"), CrawlRequest("$id-two"))),
                ),
            )
            val engine = engine(listOf(pipeline), clock = clock, options = options)
            try {
                val result = engine.runOnce().pipelineRuns.single()
                assertEquals(RunStatus.ABANDONED, result.status, id)
                assertEquals(1, result.acceptedWork, id)
                assertEquals(1, transport.calls.size, id)
            } finally {
                engine.shutdown()
            }
        }

        val claimClock = MutableClock(Instant.EPOCH)
        val claimState = RecordingStateStore(claimClock)
        val claimTransport = RecordingTransport("claim-batch-transport") { request -> response(request) }
        val claimPipeline = pipeline(
            id = "claim-batch",
            router = RecordingRouter("claim-batch-router", claimTransport),
            extractor = FunctionExtractor("claim-batch-extractor") { _, _ -> ExtractionResult.Ignore() },
            clock = claimClock,
            discoveries = listOf(
                SeedDiscovery(
                    listOf(CrawlRequest("claim-one"), CrawlRequest("claim-two"), CrawlRequest("claim-three")),
                ),
            ),
        )
        val claimEngine = engine(
            listOf(claimPipeline),
            state = claimState,
            clock = claimClock,
            options = EngineRuntimeOptions(workerCount = 1, claimBatchSize = 2),
        )
        try {
            assertEquals(RunStatus.COMPLETED, claimEngine.runOnce().pipelineRuns.single().status)
            assertTrue(claimState.claimLimits.isNotEmpty())
            assertTrue(claimState.claimLimits.all { it == 2 })
            assertEquals(3, claimTransport.calls.size)
        } finally {
            claimEngine.shutdown()
        }
    }

    @Test
    fun `budget policy receives discovery request bytes values failures and elapsed observations`() = runTest {
        suspend fun runBudgetCase(
            id: String,
            policy: RecordingBudgetPolicy,
            extractor: Extractor<Int>,
            transport: Transport,
            retry: RetryPolicy = com.rmjtromp.trawler.policy.NoRetryPolicy,
            clock: MutableClock = MutableClock(Instant.EPOCH),
        ): PipelineRunResult {
            val pipeline = pipeline(
                id = id,
                router = RecordingRouter("$id-router", transport),
                extractor = extractor,
                clock = clock,
                policies = BuiltInPolicies.ordinary(clock = clock).copy(budget = policy, retry = retry),
                discoveries = listOf(SeedDiscovery(listOf(CrawlRequest(id)))),
            )
            val engine = engine(listOf(pipeline), clock = clock)
            return try {
                engine.runOnce().pipelineRuns.single()
            } finally {
                engine.shutdown()
            }
        }

        val discoveryPolicy = RecordingBudgetPolicy { _, observation -> observation is BudgetObservation.DiscoveryEmitted }
        val discoveryResult = runBudgetCase(
            "budget-discovery",
            discoveryPolicy,
            FunctionExtractor("budget-discovery-extractor") { _, _ -> ExtractionResult.Ignore() },
            RecordingTransport("budget-discovery-transport") { request -> response(request) },
        )
        assertEquals(RunStatus.ABANDONED, discoveryResult.status)
        assertTrue(discoveryPolicy.observations.any { it.second is BudgetObservation.DiscoveryEmitted })

        val requestPolicy = RecordingBudgetPolicy { _, observation -> observation == BudgetObservation.RequestStarted }
        val requestTransport = RecordingTransport("budget-request-transport") { request -> error("request must stop before transport") }
        val requestResult = runBudgetCase(
            "budget-request",
            requestPolicy,
            FunctionExtractor("budget-request-extractor") { _, _ -> ExtractionResult.Ignore() },
            requestTransport,
        )
        assertEquals(RunStatus.ABANDONED, requestResult.status)
        assertTrue(requestTransport.calls.isEmpty())

        val bytesPolicy = RecordingBudgetPolicy { _, observation -> observation is BudgetObservation.BytesRead }
        val bytesResult = runBudgetCase(
            "budget-bytes",
            bytesPolicy,
            FunctionExtractor("budget-bytes-extractor") { _, _ -> ExtractionResult.Ignore() },
            RecordingTransport("budget-bytes-transport") { request -> response(request, "body") },
        )
        assertEquals(RunStatus.ABANDONED, bytesResult.status)
        assertTrue(bytesPolicy.observations.any { it.second is BudgetObservation.BytesRead })

        val valuesPolicy = RecordingBudgetPolicy { _, observation -> observation is BudgetObservation.ValuesProduced }
        val valuesResult = runBudgetCase(
            "budget-values",
            valuesPolicy,
            FunctionExtractor("budget-values-extractor") { _, _ -> ExtractionResult.Success(listOf(1)) },
            RecordingTransport("budget-values-transport") { request -> response(request) },
        )
        assertEquals(RunStatus.ABANDONED, valuesResult.status)
        assertTrue(valuesPolicy.observations.any { it.second is BudgetObservation.ValuesProduced })

        val failurePolicy = RecordingBudgetPolicy { snapshot, observation ->
            snapshot.failures > 0 && observation == BudgetObservation.RequestStarted
        }
        val failureTransport = RecordingTransport("budget-failure-transport") { request ->
            if (request.target == "budget-failure") throw IllegalStateException("first failure")
            response(request)
        }
        val failureResult = runBudgetCase(
            "budget-failure",
            failurePolicy,
            FunctionExtractor("budget-failure-extractor") { _, _ -> ExtractionResult.Ignore() },
            failureTransport,
            retry = RecordingRetryPolicy { _, _, _ -> RetryDecision.RetryNow },
        )
        assertEquals(RunStatus.ABANDONED, failureResult.status)
        assertTrue(failurePolicy.observations.any { it.first.failures > 0 })

        val elapsedClock = MutableClock(Instant.EPOCH)
        val elapsedPolicy = RecordingBudgetPolicy { snapshot, _ -> snapshot.elapsed >= Duration.ofSeconds(1) }
        val elapsedResult = runBudgetCase(
            "budget-elapsed",
            elapsedPolicy,
            FunctionExtractor("budget-elapsed-extractor") { _, _ -> ExtractionResult.Ignore() },
            RecordingTransport("budget-elapsed-transport") { request ->
                elapsedClock.current = Instant.EPOCH.plusSeconds(1)
                response(request)
            },
            clock = elapsedClock,
        )
        assertEquals(RunStatus.ABANDONED, elapsedResult.status)
        assertTrue(elapsedPolicy.observations.any { it.first.elapsed >= Duration.ofSeconds(1) })
    }

    @Test
    fun `events are ordered, bounded, identified, failure-isolated, and free of bodies and values`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val events = mutableListOf<CrawlerEvent>()
        val thirdObserverEvents = mutableListOf<CrawlerEvent>()
        val throwing = object : Observer {
            override val id: String = "throwing-observer"
            override suspend fun onEvent(event: CrawlerEvent): ObserverResult =
                throw IllegalStateException("observer failure")
        }
        val observer = RecordingObserver("recording-observer") { event -> events += event }
        val third = RecordingObserver("third-observer") { event -> thirdObserverEvents += event }
        val longDiscoveryId = "d".repeat(400)
        val output = RecordingOutput<String>("event-output")
        val pipeline = pipeline(
            id = "events",
            router = RecordingRouter("event-router", RecordingTransport("event-transport") { request ->
                response(request, "body-secret")
            }),
            extractor = FunctionExtractor("event-extractor") { _, _ -> ExtractionResult.Success(listOf("produced-secret")) },
            clock = clock,
            discoveries = listOf(DiscoveryWithExecutor(longDiscoveryId) { listOf(DiscoveryEmission.Request(CrawlRequest("target-secret"))) }),
            outputs = listOf(output to OutputRequirement.REQUIRED),
        )
        val engine = engine(
            listOf(pipeline),
            clock = clock,
            observers = listOf(observer, throwing, third),
        )
        try {
            val result = engine.runOnce()
            assertEquals(RunStatus.COMPLETED, result.status)
            assertEquals(CrawlerEventType.ENGINE_RUN_STARTED, events.first().type)
            assertEquals(CrawlerEventType.ENGINE_RUN_COMPLETED, events.last().type)
            assertTrue(events.indexOfFirst { it.type == CrawlerEventType.REQUEST_STARTED } < events.indexOfFirst { it.type == CrawlerEventType.REQUEST_COMPLETED })
            assertTrue(events.indexOfFirst { it.type == CrawlerEventType.VALUE_PRODUCED } < events.indexOfFirst { it.type == CrawlerEventType.VALUE_ACCEPTED })
            assertTrue(events.all { it.context.engineRunId != null })
            assertTrue(events.filter { it.context.pipelineId == "events" }.all { it.context.pipelineRunId != null })
            val bounded = events.first { it.type == CrawlerEventType.DISCOVERY_STARTED }
            assertEquals(256, bounded.detail!!.length)
            val rendered = events.joinToString()
            assertFalse(rendered.contains("body-secret"))
            assertFalse(rendered.contains("produced-secret"))
            assertFalse(rendered.contains("target-secret"))
            assertEquals(events.map { it.type }, thirdObserverEvents.map { it.type })
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `lifecycle extensions start and stop in documented ownership order and startup failure rolls back`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val lifecycle = mutableListOf<String>()
        val engineOne = RecordingExtension("engine-one", lifecycle)
        val engineTwo = RecordingExtension("engine-two", lifecycle)
        val pipelineExtension = RecordingExtension("pipeline", lifecycle)
        val output = RecordingOutput<Int>("lifecycle-output", lifecycle)
        val pipeline = pipeline(
            id = "lifecycle",
            router = RecordingRouter("lifecycle-router", RecordingTransport("lifecycle-transport") { request -> response(request) }),
            extractor = FunctionExtractor("lifecycle-extractor") { _, _ -> ExtractionResult.Success(listOf(1)) },
            clock = clock,
            discoveries = listOf(SeedDiscovery(listOf(CrawlRequest("lifecycle")))),
            outputs = listOf(output to OutputRequirement.REQUIRED),
            extensions = listOf(pipelineExtension),
        )
        val engine = engine(
            listOf(pipeline),
            clock = clock,
            extensions = listOf(engineOne, engineTwo),
        )
        try {
            assertEquals(RunStatus.COMPLETED, engine.runOnce().status)
            assertEquals(
                listOf("start:engine-one", "start:engine-two", "start:pipeline", "stop:pipeline:PIPELINE_RUN_COMPLETED"),
                lifecycle,
            )
            engine.shutdown()
            assertEquals(
                listOf(
                    "start:engine-one",
                    "start:engine-two",
                    "start:pipeline",
                    "stop:pipeline:PIPELINE_RUN_COMPLETED",
                    "stop:engine-two:ENGINE_SHUTDOWN",
                    "stop:engine-one:ENGINE_SHUTDOWN",
                    "close:lifecycle-output",
                ),
                lifecycle,
            )
        } finally {
            engine.shutdown()
        }

        val first = RecordingExtension("first", mutableListOf())
        val secondCause = IllegalStateException("extension startup failed")
        val second = RecordingExtension("second", first.events, startFailure = secondCause)
        val failureEngine = engine(
            listOf(
                pipeline(
                    id = "never-runs",
                    router = RecordingRouter("never-router", RecordingTransport("never-transport") { request -> response(request) }),
                    extractor = FunctionExtractor("never-extractor") { _, _ -> ExtractionResult.Ignore() },
                    clock = clock,
                ),
            ),
            clock = clock,
            extensions = listOf(first, second),
        )
        try {
            val thrown = assertFailsWith<IllegalStateException> { failureEngine.runOnce() }
            assertTrue(
                thrown === secondCause || thrown.cause === secondCause,
                "startup failure was not preserved: actual=$thrown cause=${thrown.cause}",
            )
            assertEquals(
                listOf("start:first", "start:second", "stop:first:STARTUP_FAILURE"),
                first.events,
            )
            assertFailsWith<IllegalStateException> { failureEngine.runOnce() }
        } finally {
            failureEngine.shutdown()
        }
    }

    @Test
    fun `replay is typed, bounded, network-free, and does not create ordinary work`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val transport = RecordingTransport("replay-transport") { request -> error("replay must not transport") }
        val output = RecordingOutput<String>("replay-output")
        val request = CrawlRequest(
            "stored",
            attributes = ContextAttributes.of(
                ContextAttribute(InMemoryResponseStore.PIPELINE_ID_ATTRIBUTE, "replay"),
                ContextAttribute(InMemoryResponseStore.REQUEST_ID_ATTRIBUTE, "stored-request"),
            ),
        )
        val store = InMemoryResponseStore()
        store.put(response(request, "replayed"))
        val pipeline = pipeline(
            id = "replay",
            router = RecordingRouter("replay-router", transport),
            extractor = FunctionExtractor("replay-extractor") { result, _ ->
                ExtractionResult.Success(listOf(result.body.text()))
            },
            clock = clock,
            outputs = listOf(output to OutputRequirement.REQUIRED),
        )
        val state = InMemoryCrawlStateStore(clock = clock)
        val engine = engine(listOf(pipeline), state = state, responseStore = store, clock = clock)
        try {
            val replay = engine.replay(pipeline.replay(ResponseReplayQuery.ByPipelineId("replay"), limit = 1))
            assertEquals(RunStatus.COMPLETED, replay.status)
            assertEquals(1, replay.responsesRead)
            assertEquals(1, replay.producedValues)
            assertEquals(1, replay.deliveredValues)
            assertEquals(listOf("replayed"), output.values())
            assertTrue(transport.calls.isEmpty())
            assertEquals(0, state.activeWorkCount("replay"))
            val replayBoundedEngine = engine(
                listOf(pipeline),
                state = InMemoryCrawlStateStore(clock = clock),
                responseStore = store,
                clock = clock,
                options = EngineRuntimeOptions(maxReplayResponsesPerRun = 1),
            )
            try {
                assertFailsWith<IllegalArgumentException> {
                    replayBoundedEngine.replay(
                        pipeline.replay(ResponseReplayQuery.ByPipelineId("replay"), limit = 2),
                    )
                }
                assertTrue(transport.calls.isEmpty())
            } finally {
                replayBoundedEngine.shutdown()
            }
        } finally {
            engine.shutdown()
        }
    }

    private fun <T : Any> pipeline(
        id: String,
        router: TransportRouter,
        extractor: Extractor<T>,
        clock: Clock,
        policies: PipelinePolicies = BuiltInPolicies.ordinary(clock = clock),
        discoveries: List<Discovery> = emptyList(),
        requestMiddlewares: List<RequestMiddleware> = emptyList(),
        responseMiddlewares: List<ResponseMiddleware> = emptyList(),
        transforms: List<Transform<T>> = emptyList(),
        validators: List<Validator<T>> = emptyList(),
        outputs: List<Pair<Output<T>, OutputRequirement>> = emptyList(),
        extensions: List<ExtensionLifecycle> = emptyList(),
    ): Pipeline<T> = PipelineBuilder<T>(id).apply {
        transport(router)
        extract(extractor)
        this.policies(policies)
        discoveries.forEach(::discover)
        requestMiddlewares.forEach(::addRequestMiddleware)
        responseMiddlewares.forEach(::addResponseMiddleware)
        transforms.forEach(::transform)
        validators.forEach(::validator)
        outputs.forEach { (output, requirement) -> output(output, requirement) }
        extensions.forEach(::extension)
    }.build()

    private fun engine(
        pipelines: List<Pipeline<*>>,
        state: CrawlStateStore? = null,
        responseStore: ResponseStore? = null,
        capturePolicy: ResponseCapturePolicy = CaptureResponsesWhenConfigured,
        cachePolicy: ResponseCachePolicy = com.rmjtromp.trawler.response.NoResponseCachePolicy,
        observers: List<Observer> = emptyList(),
        extensions: List<ExtensionLifecycle> = emptyList(),
        clock: Clock,
        options: EngineRuntimeOptions = EngineRuntimeOptions(),
    ): TrawlerEngine {
        val selectedState = state ?: InMemoryCrawlStateStore(
            maxQueueSize = options.maxQueueSize,
            maxRetainedJobs = maxOf(options.maxRetainedJobs, options.maxQueueSize),
            maxRetainedResources = maxOf(options.maxRetainedResources, options.maxQueueSize),
            maxRetainedDiscoveryState = options.maxRetainedDiscoveryState,
            maxCheckpointPayloadBytes = options.maxCheckpointPayloadBytes,
            leaseDuration = options.leaseDuration,
            clock = clock,
        )
        return TrawlerEngine(
            EngineConfiguration(
                pipelines = pipelines,
                stateStore = selectedState,
                responseStore = responseStore,
                responseCapturePolicy = capturePolicy,
                responseCachePolicy = cachePolicy,
                observers = observers,
                extensions = extensions,
                clock = clock,
                runtimeOptions = options,
            ),
        )
    }

    private fun response(
        request: CrawlRequest,
        body: String = request.target,
        finalTarget: String? = null,
        headers: Headers = Headers.empty(),
        attributes: ContextAttributes = ContextAttributes.empty(),
    ): CrawlResponse = CrawlResponse(
        request = request,
        statusCode = 200,
        headers = headers,
        body = ResponseBody.from(body.toByteArray()),
        receivedAt = Instant.EPOCH,
        finalTarget = finalTarget,
        attributes = attributes,
    )

    private fun SeedDiscovery(
        requests: List<CrawlRequest>,
        maxEmissions: Int = requests.size,
    ): Discovery = com.rmjtromp.trawler.discovery.SeedDiscovery(
        requests = requests,
        maxEmissions = maxEmissions,
    )

    private fun ResponseBody.text(): String = String(toByteArray(), Charsets.UTF_8)

    private fun crawler(pipelineId: String): CrawlerContext =
        CrawlerContext("engine", "$pipelineId-run", pipelineId)

    private class MutableClock(var current: Instant) : Clock {
        override fun now(): Instant = current
    }

    private class RecordingTransport(
        override val id: String,
        private val handler: suspend (CrawlRequest) -> CrawlResponse,
    ) : Transport {
        val calls: MutableList<CrawlRequest> = mutableListOf()
        val cancellations: MutableList<CrawlRequest> = mutableListOf()

        override suspend fun execute(request: CrawlRequest): CrawlResponse {
            calls += request
            return try {
                handler(request)
            } catch (cancellation: CancellationException) {
                cancellations += request
                throw cancellation
            }
        }
    }

    private class RecordingRouter(
        override val id: String,
        private val transport: Transport,
    ) : TransportRouter {
        val selected: MutableList<Pair<CrawlRequest, RequestContext>> = mutableListOf()

        override fun select(request: CrawlRequest, context: RequestContext): Transport {
            selected += request to context
            return transport
        }
    }

    private class FunctionExtractor<T : Any>(
        override val id: String,
        private val handler: suspend (CrawlResponse, ExtractionContext) -> ExtractionResult<T>,
    ) : Extractor<T> {
        val seen: MutableList<CrawlResponse> = mutableListOf()

        override suspend fun extract(response: CrawlResponse, context: ExtractionContext): ExtractionResult<T> {
            seen += response
            return handler(response, context)
        }
    }

    private class FunctionTransform<T : Any>(
        override val id: String,
        private val handler: suspend (T, TransformContext) -> TransformDecision<T>,
    ) : Transform<T> {
        override suspend fun apply(value: T, context: TransformContext): TransformDecision<T> = handler(value, context)
    }

    private class FunctionValidator<T : Any>(
        override val id: String,
        private val handler: suspend (T, ValidationContext) -> ValidationResult,
    ) : Validator<T> {
        override suspend fun validate(value: T, context: ValidationContext): ValidationResult = handler(value, context)
    }

    private class RecordingOutput<T : Any>(
        override val id: String,
        private val lifecycle: MutableList<String>? = null,
        private val behavior: suspend (List<Produced<T>>) -> OutputResult = { OutputResult.Accepted(it.size) },
    ) : Output<T>, OutputLifecycle {
        val calls: MutableList<List<Produced<T>>> = mutableListOf()

        override suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult {
            calls += items.toList()
            return behavior(items)
        }

        override suspend fun close() {
            lifecycle?.add("close:$id")
        }

        fun values(): List<T> = calls.flatten().map { it.value }
    }

    private class DiscoveryWithExecutor(
        override val id: String,
        private val handler: suspend (DiscoveryContext) -> List<DiscoveryEmission>,
    ) : Discovery {
        override fun discover(context: DiscoveryContext): Flow<DiscoveryEmission> = flow {
            handler(context).forEach { emit(it) }
        }
    }

    private class RecordingResponseStore(
        override val capabilities: Set<ResponseStoreCapability> = emptySet(),
        private val putHandler: (suspend (CrawlResponse) -> ResponseReference?)? = null,
    ) : ResponseStore {
        override val id: String = "recording-response-store"
        val putResponses: MutableList<CrawlResponse> = mutableListOf()

        override suspend fun put(response: CrawlResponse): ResponseReference? {
            putResponses += response
            return putHandler?.invoke(response)
        }

        override suspend fun find(query: com.rmjtromp.trawler.response.ResponseLookup): StoredResponse? = null
    }

    private class FixedCachePolicy(
        private val decision: ResponseCacheDecision,
    ) : ResponseCachePolicy {
        override val id: String = "fixed-cache"
        override val requiredCapabilities: Set<ResponseStoreCapability> = emptySet()
        var lookups: Int = 0

        override suspend fun lookup(context: ResponseCacheContext, store: ResponseStore): ResponseCacheDecision {
            lookups++
            return decision
        }
    }

    private class FixedCapturePolicy(
        private val decision: ResponseCaptureDecision,
        override val requiresStore: Boolean = false,
    ) : ResponseCapturePolicy {
        override val id: String = "fixed-capture"

        override fun decide(context: ResponseCaptureContext): ResponseCaptureDecision = decision
    }

    private fun namedRequestMiddleware(
        id: String,
        order: MutableList<String>,
        handler: suspend (CrawlRequest, RequestContext, RequestExecutor) -> CrawlResponse,
    ): RequestMiddleware = object : RequestMiddleware {
        override val id: String = id

        override suspend fun intercept(
            request: CrawlRequest,
            context: RequestContext,
            next: RequestExecutor,
        ): CrawlResponse {
            order += "$id-before"
            return handler(request, context, next)
        }
    }

    private fun namedResponseMiddleware(
        id: String,
        order: MutableList<String>,
        handler: suspend (CrawlResponse, ResponseContext, ResponseHandler) -> CrawlResponse,
    ): ResponseMiddleware = object : ResponseMiddleware {
        override val id: String = id

        override suspend fun intercept(
            response: CrawlResponse,
            context: ResponseContext,
            next: ResponseHandler,
        ): CrawlResponse {
            order += "$id-before"
            return handler(response, context, next)
        }
    }

    private class RejectingScopePolicy(
        private val reason: String,
    ) : CrawlScopePolicy {
        override val id: String = "rejecting-scope"
        override fun evaluate(request: CrawlRequest, context: RequestContext): ScopeDecision = ScopeDecision.Rejected(reason)
    }

    private class DenyingAuthorizationPolicy(
        private val reason: String,
    ) : AuthorizationPolicy {
        override val id: String = "denying-authorization"
        override suspend fun authorize(request: CrawlRequest, context: RequestContext): AuthorizationDecision =
            AuthorizationDecision.Denied(reason)
    }

    private class RecordingRetryPolicy(
        private val decision: (FailureClassification, AttemptSummary, FailureContext) -> RetryDecision,
    ) : RetryPolicy {
        override val id: String = "recording-retry"
        val calls: MutableList<AttemptSummary> = mutableListOf()

        override fun decide(
            failure: FailureClassification,
            attempt: AttemptSummary,
            context: FailureContext,
        ): RetryDecision {
            calls += attempt
            return decision(failure, attempt, context)
        }
    }

    private class RecordingBudgetPolicy(
        private val stop: (BudgetSnapshot, BudgetObservation) -> Boolean,
    ) : CrawlBudgetPolicy {
        override val id: String = "recording-budget"
        val observations: MutableList<Pair<BudgetSnapshot, BudgetObservation>> = mutableListOf()

        override fun evaluate(snapshot: BudgetSnapshot, observation: BudgetObservation): BudgetDecision {
            observations += snapshot to observation
            return if (stop(snapshot, observation)) BudgetDecision.Stop("test budget") else BudgetDecision.Continue
        }
    }

    private class RecordingObserver(
        override val id: String,
        private val accept: (CrawlerEvent) -> Unit,
    ) : Observer {
        override suspend fun onEvent(event: CrawlerEvent): ObserverResult {
            accept(event)
            return ObserverResult.Accepted
        }
    }

    private class RecordingExtension(
        override val id: String,
        val events: MutableList<String>,
        private val startFailure: Throwable? = null,
    ) : ExtensionLifecycle {
        override suspend fun start(context: ExtensionStartContext) {
            events += "start:$id"
            startFailure?.let { throw it }
        }

        override suspend fun stop(context: ExtensionStopContext) {
            events += "stop:$id:${context.reason}"
        }
    }

    private class RecordingStateStore(
        clock: Clock,
    ) : CrawlStateStore,
        PipelineScopedWorkQueueStore,
        LeaseStore,
        AttemptStore,
        ResourceHistoryStore,
        WorkCancellationStore,
        WorkOrderingStore,
        WorkQueueStatus {
        val delegate = InMemoryCrawlStateStore(clock = clock)
        override val id: String = "recording-state"
        override val capabilities: Set<CrawlStateCapability> = setOf(
            CrawlStateCapability.WORK_QUEUE,
            CrawlStateCapability.LEASE_FENCING,
            CrawlStateCapability.RESOURCE_HISTORY,
            CrawlStateCapability.ATTEMPT_HISTORY,
            CrawlStateCapability.PIPELINE_SCOPED_WORK,
            CrawlStateCapability.CANCELLATION,
            CrawlStateCapability.WORK_ORDERING,
            CrawlStateCapability.WORK_STATUS,
        )
        val begunAttempts: MutableList<ExecutionAttempt> = mutableListOf()
        val finishedAttempts: MutableList<Pair<ExecutionAttempt, AttemptOutcome>> = mutableListOf()
        val transitions: MutableList<Pair<LeasedWork, WorkTransition>> = mutableListOf()
        val claimLimits: MutableList<Int> = mutableListOf()
        var renewCalls: Int = 0
        var checkCurrentCalls: Int = 0
        var cancelQueuedCalls: Int = 0
        var checkCurrentBehavior: (suspend (LeasedWork) -> LeaseMutationResult)? = null
        var renewBehavior: (suspend (LeasedWork, Instant) -> LeaseMutationResult)? = null

        override suspend fun enqueue(request: CrawlRequest, identity: RequestIdentity, context: CrawlerContext): EnqueueResult =
            delegate.enqueue(request, identity, context)

        override suspend fun claim(limit: Int, ownerId: String, now: Instant): List<LeasedWork> {
            claimLimits += limit
            return delegate.claim(limit, ownerId, now)
        }

        override suspend fun claim(pipelineId: String, limit: Int, ownerId: String, now: Instant): List<LeasedWork> {
            claimLimits += limit
            return delegate.claim(pipelineId, limit, ownerId, now)
        }

        override suspend fun renew(work: LeasedWork, expiresAt: Instant): LeaseMutationResult {
            renewCalls++
            return renewBehavior?.invoke(work, expiresAt) ?: delegate.renew(work, expiresAt)
        }

        override suspend fun checkCurrent(work: LeasedWork): LeaseMutationResult {
            checkCurrentCalls++
            return checkCurrentBehavior?.invoke(work) ?: delegate.checkCurrent(work)
        }

        override suspend fun transition(work: LeasedWork, transition: WorkTransition): LeaseMutationResult {
            transitions += work to transition
            return delegate.transition(work, transition)
        }

        override suspend fun beginAttempt(work: LeasedWork, startedAt: Instant): AttemptStartResult {
            val result = delegate.beginAttempt(work, startedAt)
            if (result is AttemptStartResult.Started) begunAttempts += result.attempt
            return result
        }

        override suspend fun finishAttempt(
            work: LeasedWork,
            attempt: ExecutionAttempt,
            outcome: AttemptOutcome,
        ): LeaseMutationResult {
            finishedAttempts += attempt to outcome
            return delegate.finishAttempt(work, attempt, outcome)
        }

        override suspend fun history(pipelineId: String, identity: RequestIdentity): ResourceHistory =
            delegate.history(pipelineId, identity)

        override suspend fun cancelQueued(pipelineId: String, transition: WorkTransition): Int {
            cancelQueuedCalls++
            return delegate.cancelQueued(pipelineId, transition)
        }

        override suspend fun setOrdering(pipelineId: String, policy: WorkOrderingPolicy) =
            delegate.setOrdering(pipelineId, policy)

        override suspend fun activeWorkCount(pipelineId: String): Int = delegate.activeWorkCount(pipelineId)
    }
}
