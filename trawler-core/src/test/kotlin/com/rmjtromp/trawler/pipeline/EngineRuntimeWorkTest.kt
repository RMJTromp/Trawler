package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.event.CrawlerEvent
import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.event.Observer
import com.rmjtromp.trawler.event.ObserverResult
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.model.ResponseReference
import com.rmjtromp.trawler.output.NoOutput
import com.rmjtromp.trawler.policy.AttemptSummary
import com.rmjtromp.trawler.policy.AuthorizationDecision
import com.rmjtromp.trawler.policy.AuthorizationPolicy
import com.rmjtromp.trawler.policy.BudgetDecision
import com.rmjtromp.trawler.policy.BudgetObservation
import com.rmjtromp.trawler.policy.BudgetSnapshot
import com.rmjtromp.trawler.policy.BuiltInPolicies
import com.rmjtromp.trawler.policy.CrawlBudgetPolicy
import com.rmjtromp.trawler.policy.CrawlScopePolicy
import com.rmjtromp.trawler.policy.DeduplicationDecision
import com.rmjtromp.trawler.policy.DeduplicationPolicy
import com.rmjtromp.trawler.policy.FailureClassification
import com.rmjtromp.trawler.policy.FailureContext
import com.rmjtromp.trawler.policy.RecrawlDecision
import com.rmjtromp.trawler.policy.RecrawlPolicy
import com.rmjtromp.trawler.policy.RequestCanonicalizer
import com.rmjtromp.trawler.policy.RequestIdentity
import com.rmjtromp.trawler.policy.ResourceHistory
import com.rmjtromp.trawler.policy.RetryDecision
import com.rmjtromp.trawler.policy.RetryPolicy
import com.rmjtromp.trawler.policy.ScopeDecision
import com.rmjtromp.trawler.response.ResponseLookup
import com.rmjtromp.trawler.response.ResponseStore
import com.rmjtromp.trawler.response.ResponseStoreCapability
import com.rmjtromp.trawler.response.StoredResponse
import com.rmjtromp.trawler.state.AttemptOutcome
import com.rmjtromp.trawler.state.AttemptStartResult
import com.rmjtromp.trawler.state.LeaseMutationResult
import com.rmjtromp.trawler.state.WorkTransition
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

/** Work admission, worker, lease, retry, and delivery edge paths of the engine runtime. */
@OptIn(ExperimentalCoroutinesApi::class)
class EngineRuntimeWorkTest {
    private val clock = SettableClock(Instant.parse("2026-07-01T00:00:00Z"))
    private fun policies(): PipelinePolicies = BuiltInPolicies.ordinary(clock = clock)

    @Test
    fun `canonicalization, recrawl, and deduplication policies reject or defer admission explicitly`() = runTest {
        val events = EventLog()
        val transport = ScriptedTransport()
        val oversizing = object : RequestCanonicalizer {
            override val id: String = "oversizing"
            override fun canonicalize(request: CrawlRequest, context: RequestContext): CrawlRequest =
                request.copy(target = request.target + "-".repeat(64))
        }
        val recrawl = object : RecrawlPolicy {
            override val id: String = "scripted-recrawl"
            override fun decide(request: CrawlRequest, history: ResourceHistory, context: RequestContext): RecrawlDecision =
                when (request.target) {
                    "deferred-silent" -> RecrawlDecision.Deferred(clock.current.plusSeconds(60))
                    "deferred" -> RecrawlDecision.Deferred(clock.current.plusSeconds(60), "cooling down")
                    "rejected" -> RecrawlDecision.Rejected("seen before")
                    else -> RecrawlDecision.Eligible
                }
        }
        val dedup = object : DeduplicationPolicy {
            override val id: String = "scripted-dedup"
            override suspend fun decide(request: CrawlRequest, identity: RequestIdentity, context: RequestContext): DeduplicationDecision =
                if (request.target == "duplicate") DeduplicationDecision.Suppress("bloom hit") else DeduplicationDecision.Admit
        }
        val canonical = testPipeline(
            "canonical",
            bodyExtractor(),
            clock,
            transport = transport,
            policies = policies().copy(requestCanonicalizer = oversizing),
            discoveries = listOf(seeds("short")),
        )
        val admission = testPipeline(
            "admission",
            bodyExtractor(),
            clock,
            transport = transport,
            policies = policies().copy(recrawl = recrawl, deduplication = dedup),
            discoveries = listOf(seeds("deferred-silent", "deferred", "rejected", "duplicate", "fresh")),
        )
        val engine = testEngine(
            listOf(canonical, admission),
            clock,
            observers = listOf(events),
            options = EngineRuntimeOptions(maxTargetBytes = 32),
        )
        try {
            val runs = engine.runOnce().pipelineRuns.associateBy { it.pipelineId }
            val canonicalRun = runs.getValue("canonical")
            assertEquals(RunStatus.ABANDONED, canonicalRun.status)
            assertEquals(0, canonicalRun.acceptedWork, "A rejected canonical request releases its work reservation")
            assertTrue("canonical-request-metadata-bound" in events.details(CrawlerEventType.CAPABILITY_WARNING))

            val admissionRun = runs.getValue("admission")
            assertEquals(RunStatus.ABANDONED, admissionRun.status, "A deferred recrawl leaves the run resumable")
            assertEquals(1, admissionRun.acceptedWork)
            assertEquals(2, admissionRun.suppressedWork, "Rejected recrawl and deduplicated work are suppressed")
            assertEquals(listOf("fresh"), transport.calls.map { it.target })
            val failures = events.details(CrawlerEventType.REQUEST_FAILED)
            assertTrue("recrawl-deferred:policy" in failures, "$failures")
            assertTrue("recrawl-deferred:cooling down" in failures, "$failures")
            assertTrue("recrawl-rejected:seen before" in failures, "$failures")
            assertTrue("deduplicated:bloom hit" in failures, "$failures")
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `the discovery executor enforces bounds, scope, and authorization before transport`() = runTest {
        val transport = ScriptedTransport()
        val scope = object : CrawlScopePolicy {
            override val id: String = "scope"
            override fun evaluate(request: CrawlRequest, context: RequestContext): ScopeDecision =
                if (request.target.startsWith("outside")) ScopeDecision.Rejected("outside crawl scope") else ScopeDecision.Allowed
        }
        val authorization = object : AuthorizationPolicy {
            override val id: String = "authorization"
            override suspend fun authorize(request: CrawlRequest, context: RequestContext): AuthorizationDecision =
                if (request.target.startsWith("forbidden")) AuthorizationDecision.Denied("no consent") else AuthorizationDecision.Authorized
        }
        fun probing(id: String, target: String) = testPipeline(
            id,
            bodyExtractor(),
            clock,
            transport = transport,
            policies = policies().copy(scope = scope, authorization = authorization),
            discoveries = listOf(
                ListDiscovery("probe") { context ->
                    val probe = CrawlRequest(target)
                    val response = context.executor.execute(probe, RequestContext(context.crawler, probe))
                    listOf(DiscoveryEmission.ExistingResponse(probe, response))
                },
            ),
        )
        val engine = testEngine(
            listOf(
                probing("oversized", "x".repeat(40)),
                probing("outside", "outside-page"),
                probing("forbidden", "forbidden-page"),
                probing("allowed", "allowed-page"),
            ),
            clock,
            options = EngineRuntimeOptions(maxTargetBytes = 32),
        )
        try {
            val runs = engine.runOnce().pipelineRuns.associateBy { it.pipelineId }
            mapOf(
                "oversized" to "request exceeds the configured runtime bounds",
                "outside" to "outside crawl scope",
                "forbidden" to "no consent",
            ).forEach { (pipelineId, message) ->
                val run = runs.getValue(pipelineId)
                assertEquals(RunStatus.FAILED, run.status, pipelineId)
                assertEquals("discovery:probe", run.failures.single().stage)
                assertEquals(message, run.failures.single().error.message)
            }
            assertEquals(RunStatus.COMPLETED, runs.getValue("allowed").status)
            assertEquals(1, runs.getValue("allowed").producedValues)
            assertEquals(listOf("allowed-page"), transport.calls.map { it.target }, "Only the allowed probe reached transport")
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `the per-run discovery bound stops later discoveries and discovery cancellation cleans queued work`() = runTest {
        val events = EventLog()
        val second = ListDiscovery("second") { listOf(DiscoveryEmission.Request(CrawlRequest("c"))) }
        val third = ListDiscovery("third") { error("must not run after the run bound is reached") }
        val pipeline = testPipeline(
            "bounded",
            bodyExtractor(),
            clock,
            discoveries = listOf(seeds("a", "b"), second, third),
        )
        val engine = testEngine(
            listOf(pipeline),
            clock,
            observers = listOf(events),
            options = EngineRuntimeOptions(maxDiscoveryEmissionsPerRun = 2),
        )
        try {
            val run = engine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.ABANDONED, run.status)
            assertEquals(2, run.discoveryEmissions)
            assertEquals(2, run.acceptedWork, "Work admitted before the bound is still processed")
            assertTrue("budget:discovery-per-run-bound" in events.details(CrawlerEventType.CAPABILITY_WARNING))
            assertTrue(third.contexts.isEmpty())
        } finally {
            engine.shutdown()
        }

        val state = HookedStateStore(clock)
        val cancelEvents = EventLog()
        val entered = CompletableDeferred<Unit>()
        val hanging = ListDiscovery("hanging") {
            entered.complete(Unit)
            awaitCancellation()
        }
        val cancelling = testPipeline("cancelling", bodyExtractor(), clock, discoveries = listOf(seeds("queued"), hanging))
        val cancelEngine = testEngine(listOf(cancelling), clock, state = state, observers = listOf(cancelEvents))
        try {
            val run = async(start = CoroutineStart.UNDISPATCHED) { cancelEngine.runOnce() }
            entered.await()
            run.cancel()
            assertFailsWith<CancellationException> { run.await() }
            assertEquals(1, state.cancelQueuedCalls)
            assertEquals(0, state.activeWorkCount("cancelling"), "Queued work is cancelled with the discovery")
            assertEquals(listOf("cooperative cancellation"), cancelEvents.details(CrawlerEventType.PIPELINE_RUN_CANCELLED))
            assertTrue(CrawlerEventType.DISCOVERY_FAILED !in cancelEvents.types(), "Cancellation is not a discovery failure")
        } finally {
            cancelEngine.shutdown()
        }
    }

    @Test
    fun `claim failures and failures escaping work handling are terminal and transition the work`() = runTest {
        val claimState = HookedStateStore(clock).apply {
            claimHook = { _, _, _, _ -> throw IllegalStateException("claim query failed") }
        }
        val pipeline = testPipeline("claims", bodyExtractor(), clock, discoveries = listOf(seeds("a")))
        val claimEngine = testEngine(listOf(pipeline), clock, state = claimState, options = EngineRuntimeOptions(workerCount = 1))
        try {
            val run = claimEngine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.FAILED, run.status)
            assertEquals("claim query failed", run.failures.first { it.stage == "claim" }.error.message)
            assertEquals(0, run.requestsStarted)
        } finally {
            claimEngine.shutdown()
        }

        val workerState = HookedStateStore(clock)
        val explodingRetry = object : RetryPolicy {
            override val id: String = "exploding-retry"
            override fun decide(failure: FailureClassification, attempt: AttemptSummary, context: FailureContext): RetryDecision =
                throw IllegalStateException("retry policy bug")
        }
        val failing = testPipeline(
            "worker",
            bodyExtractor(),
            clock,
            transport = ScriptedTransport { throw IllegalStateException("transport down") },
            policies = policies().copy(retry = explodingRetry),
            discoveries = listOf(seeds("w")),
        )
        val workerEngine = testEngine(listOf(failing), clock, state = workerState)
        try {
            val run = workerEngine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.FAILED, run.status)
            assertEquals(listOf("work", "worker"), run.failures.map { it.stage })
            assertEquals("retry policy bug", run.failures.last().error.message)
            assertEquals(WorkTransition.Failed("worker"), workerState.transitions.last())
            assertEquals(0, workerState.activeWorkCount("worker"))
        } finally {
            workerEngine.shutdown()
        }
    }

    @Test
    fun `rejected or failing lease renewals and rejected lease checks stop processing as lease loss`() = runTest {
        val scenarios = listOf<Triple<String, (HookedStateStore) -> Unit, Boolean>>(
            Triple("renew-rejected", { state -> state.renewHook = { _, _ -> LeaseMutationResult.Rejected("fencing token mismatch") } }, true),
            Triple("renew-throws", { state -> state.renewHook = { _, _ -> throw IllegalStateException("database unavailable") } }, true),
            Triple("check-rejected", { state -> state.checkCurrentHook = { _ -> LeaseMutationResult.Rejected("lease row locked") } }, false),
        )
        scenarios.forEach { (name, configure, blockTransport) ->
            val state = HookedStateStore(clock).also(configure)
            val events = EventLog()
            val entered = CompletableDeferred<Unit>()
            val transport = ScriptedTransport { request ->
                entered.complete(Unit)
                if (blockTransport) awaitCancellation()
                okResponse(request)
            }
            val output = CollectingOutput<String>("output")
            val pipeline = testPipeline(
                name,
                bodyExtractor(),
                clock,
                transport = transport,
                discoveries = listOf(seeds("leased")),
                outputs = listOf(output to OutputRequirement.REQUIRED),
            )
            val engine = testEngine(
                listOf(pipeline),
                clock,
                state = state,
                observers = listOf(events),
                options = EngineRuntimeOptions(leaseDuration = Duration.ofMillis(6)),
            )
            try {
                val run = async(start = CoroutineStart.UNDISPATCHED) { engine.runOnce() }
                entered.await()
                advanceTimeBy(3)
                runCurrent()
                val result = run.await().pipelineRuns.single()
                assertEquals(RunStatus.ABANDONED, result.status, name)
                assertTrue(output.batches.isEmpty(), "A lost lease never delivers values: $name")
                assertEquals(listOf(AttemptOutcome.ABANDONED), state.finishedAttempts, name)
                assertTrue("lease-lost" in events.details(CrawlerEventType.REQUEST_FAILED), name)
                assertTrue(state.transitions.none { it == WorkTransition.Succeeded }, name)
            } finally {
                engine.shutdown()
            }
        }
    }

    @Test
    fun `work-time scope and authorization rejections skip work and stale or rejected transitions are not reported as skips`() = runTest {
        val outcomes = listOf<LeaseMutationResult?>(null, LeaseMutationResult.Stale, LeaseMutationResult.Rejected("row missing"))
        outcomes.forEach { outcome ->
            listOf("scope", "authorization").forEach { kind ->
                val state = HookedStateStore(clock).apply {
                    transitionHook = { _, transition -> if (transition is WorkTransition.Skipped) outcome else null }
                }
                val events = EventLog()
                var scopeCalls = 0
                val scope = object : CrawlScopePolicy {
                    override val id: String = "late-scope"
                    override fun evaluate(request: CrawlRequest, context: RequestContext): ScopeDecision {
                        scopeCalls++
                        return if (kind == "scope" && scopeCalls > 1) ScopeDecision.Rejected("scope changed") else ScopeDecision.Allowed
                    }
                }
                val authorization = object : AuthorizationPolicy {
                    override val id: String = "late-authorization"
                    override suspend fun authorize(request: CrawlRequest, context: RequestContext): AuthorizationDecision =
                        if (kind == "authorization") AuthorizationDecision.Denied("revoked") else AuthorizationDecision.Authorized
                }
                val transport = ScriptedTransport()
                val pipeline = testPipeline(
                    "late",
                    bodyExtractor(),
                    clock,
                    transport = transport,
                    policies = policies().copy(scope = scope, authorization = authorization),
                    discoveries = listOf(seeds("target")),
                )
                val engine = testEngine(listOf(pipeline), clock, state = state, observers = listOf(events))
                try {
                    val run = engine.runOnce().pipelineRuns.single()
                    val label = "$kind/$outcome"
                    assertTrue(transport.calls.isEmpty(), label)
                    assertEquals(0, run.requestsStarted, "Rejected work never starts an attempt: $label")
                    val operation = if (kind == "scope") "scope-rejection" else "authorization-denial"
                    val details = events.details(CrawlerEventType.REQUEST_FAILED)
                    when (outcome) {
                        null -> {
                            assertEquals(RunStatus.COMPLETED, run.status, label)
                            val expected = if (kind == "scope") "scope-rejected:scope changed" else "authorization-denied"
                            assertTrue(expected in details, "$label $details")
                        }
                        LeaseMutationResult.Stale -> {
                            assertEquals(RunStatus.ABANDONED, run.status, label)
                            assertTrue("$operation:lease-lost" in details, "$label $details")
                        }
                        else -> {
                            assertEquals(RunStatus.FAILED, run.status, label)
                            assertEquals("lease:$operation", run.failures.single().stage, label)
                            assertTrue("$operation:rejected" in details, "$label $details")
                        }
                    }
                } finally {
                    engine.shutdown()
                }
            }
        }
    }

    @Test
    fun `attempt start results and stale finishing mutations prevent false success reports`() = runTest {
        val staleStart = HookedStateStore(clock).apply { beginAttemptHook = { _, _ -> AttemptStartResult.Stale } }
        runScenario("stale-start", staleStart).also { (run, events, transport) ->
            assertEquals(RunStatus.ABANDONED, run.status, "A stale attempt start leaves the work to its current owner")
            assertTrue("resumable-work-remains" in events.details(CrawlerEventType.CAPABILITY_WARNING))
            assertEquals(0, run.requestsStarted)
            assertTrue(transport.calls.isEmpty())
            assertTrue(CrawlerEventType.REQUEST_COMPLETED !in events.types())
        }
        val rejectedStart = HookedStateStore(clock).apply { beginAttemptHook = { _, _ -> AttemptStartResult.Rejected("attempt limit") } }
        runScenario("rejected-start", rejectedStart).also { (run, _, transport) ->
            assertEquals(RunStatus.FAILED, run.status)
            assertEquals("attempt-start", run.failures.single().stage)
            assertEquals("attempt limit", run.failures.single().error.message)
            assertTrue(transport.calls.isEmpty())
            assertEquals(WorkTransition.Failed("attempt-start"), rejectedStart.transitions.single())
        }
        val staleFinish = HookedStateStore(clock).apply {
            finishAttemptHook = { _, _, outcome -> if (outcome == AttemptOutcome.SUCCEEDED) LeaseMutationResult.Stale else null }
        }
        runScenario("stale-finish", staleFinish).also { (run, events, _) ->
            assertEquals(RunStatus.ABANDONED, run.status)
            assertTrue("attempt-finish:lease-lost" in events.details(CrawlerEventType.REQUEST_FAILED))
            assertTrue(staleFinish.transitions.isEmpty(), "No completion transition follows a stale attempt finish")
        }
        val staleCompletion = HookedStateStore(clock).apply {
            transitionHook = { _, transition -> if (transition == WorkTransition.Succeeded) LeaseMutationResult.Stale else null }
        }
        runScenario("stale-completion", staleCompletion).also { (run, events, _) ->
            assertEquals(RunStatus.ABANDONED, run.status)
            assertTrue("completion:lease-lost" in events.details(CrawlerEventType.REQUEST_FAILED))
            assertTrue(CrawlerEventType.REQUEST_COMPLETED !in events.types())
        }
    }

    @Test
    fun `budget stops and failures during work honor stale attempt and abandonment mutations`() = runTest {
        val stopOnRequest = object : CrawlBudgetPolicy {
            override val id: String = "stop-on-request"
            override fun evaluate(snapshot: BudgetSnapshot, observation: BudgetObservation): BudgetDecision =
                if (observation == BudgetObservation.RequestStarted) BudgetDecision.Stop("request budget") else BudgetDecision.Continue
        }
        val budgetPolicies = policies().copy(budget = stopOnRequest)

        val budgetState = HookedStateStore(clock)
        runScenario("budget", budgetState, budgetPolicies).also { (run, events, transport) ->
            assertEquals(RunStatus.ABANDONED, run.status)
            assertEquals(listOf(AttemptOutcome.ABANDONED), budgetState.finishedAttempts)
            assertEquals(WorkTransition.Abandoned("request budget"), budgetState.transitions.single())
            assertTrue("budget:request budget" in events.details(CrawlerEventType.CAPABILITY_WARNING))
            assertTrue(transport.calls.isEmpty())
        }
        val staleBudgetFinish = HookedStateStore(clock).apply { finishAttemptHook = { _, _, _ -> LeaseMutationResult.Stale } }
        runScenario("budget-finish", staleBudgetFinish, budgetPolicies).also { (run, events, _) ->
            assertEquals(RunStatus.ABANDONED, run.status)
            assertTrue("budget-attempt-finish:lease-lost" in events.details(CrawlerEventType.REQUEST_FAILED))
            assertTrue(staleBudgetFinish.transitions.isEmpty())
        }
        val staleAbandon = HookedStateStore(clock).apply {
            transitionHook = { _, transition -> if (transition is WorkTransition.Abandoned) LeaseMutationResult.Stale else null }
        }
        runScenario("budget-abandon", staleAbandon, budgetPolicies).also { (_, events, _) ->
            assertTrue("budget-abandonment:lease-lost" in events.details(CrawlerEventType.REQUEST_FAILED))
            assertTrue("budget:request budget" !in events.details(CrawlerEventType.CAPABILITY_WARNING))
        }

        val staleFailureFinish = HookedStateStore(clock).apply { finishAttemptHook = { _, _, _ -> LeaseMutationResult.Stale } }
        runScenario(
            "stage-failure",
            staleFailureFinish,
            extractor = LambdaExtractor { _, _ -> ExtractionResult.Failure(IllegalStateException("bad markup")) },
        ).also { (run, events, _) ->
            assertEquals(RunStatus.ABANDONED, run.status)
            assertTrue("failure-attempt-finish:lease-lost" in events.details(CrawlerEventType.REQUEST_FAILED))
            assertTrue(staleFailureFinish.transitions.isEmpty(), "A stale worker must not transition failed work")
        }
        val staleErrorFinish = HookedStateStore(clock).apply { finishAttemptHook = { _, _, _ -> LeaseMutationResult.Stale } }
        runScenario(
            "error",
            staleErrorFinish,
            transport = ScriptedTransport { throw IllegalStateException("socket reset") },
        ).also { (run, events, _) ->
            assertEquals(RunStatus.ABANDONED, run.status)
            assertTrue("failure-attempt-finish:lease-lost" in events.details(CrawlerEventType.REQUEST_FAILED))
            assertTrue(staleErrorFinish.transitions.isEmpty())
        }
    }

    @Test
    fun `retry decisions honor stale and rejected lease transitions`() = runTest {
        val decisions = listOf(
            RetryDecision.RetryNow to "retry-now",
            RetryDecision.RetryAt(clock.current.plusSeconds(30)) to "retry-at",
            RetryDecision.DoNotRetry to "terminal-failure",
            RetryDecision.Abandon("operator abandon") to "retry-abandonment",
        )
        decisions.forEach { (decision, operation) ->
            listOf<LeaseMutationResult>(LeaseMutationResult.Stale, LeaseMutationResult.Rejected("conflict")).forEach { mutation ->
                val retry = object : RetryPolicy {
                    override val id: String = "fixed-retry"
                    override fun decide(failure: FailureClassification, attempt: AttemptSummary, context: FailureContext): RetryDecision =
                        decision
                }
                val state = HookedStateStore(clock).apply { transitionHook = { _, _ -> mutation } }
                val (run, events, _) = runScenario(
                    "retry",
                    state,
                    policies().copy(retry = retry),
                    transport = ScriptedTransport { throw IllegalStateException("flaky") },
                )
                val label = "$operation/$mutation"
                val details = events.details(CrawlerEventType.REQUEST_FAILED)
                if (mutation == LeaseMutationResult.Stale) {
                    assertTrue("$operation:lease-lost" in details, "$label $details")
                    if (decision != RetryDecision.DoNotRetry) assertEquals(RunStatus.ABANDONED, run.status, label)
                } else {
                    assertEquals(RunStatus.FAILED, run.status, label)
                    assertTrue(run.failures.any { it.stage == "lease:$operation" }, "$label ${run.failures.map { it.stage }}")
                    assertTrue("$operation:rejected" in details, "$label $details")
                }
                assertTrue(CrawlerEventType.RETRY_SCHEDULED !in events.types(), "A failed retry transition is not reported as scheduled: $label")
            }
        }
    }

    @Test
    fun `output failures, skips, and empty extractions follow their delivery requirement`() = runTest {
        val events = EventLog()
        val throwing = CollectingOutput<String>("throwing") { throw IllegalStateException("sink offline") }
        val skippingRequired = CollectingOutput<String>("skipping-required") { OutputResult.Skipped() }
        val skippingBestEffort = CollectingOutput<String>("skipping-best-effort") { OutputResult.Skipped("not today") }
        fun outputPipeline(id: String, output: Output<String>, requirement: OutputRequirement, extractor: Extractor<String> = bodyExtractor()) =
            testPipeline(id, extractor, clock, discoveries = listOf(seeds(id)), outputs = listOf(output to requirement))
        val engine = testEngine(
            listOf(
                outputPipeline("throwing-required", throwing, OutputRequirement.REQUIRED),
                outputPipeline("throwing-best-effort", CollectingOutput("throwing-2") { throw IllegalStateException("sink offline") }, OutputRequirement.BEST_EFFORT),
                outputPipeline("skip-required", skippingRequired, OutputRequirement.REQUIRED),
                outputPipeline("skip-best-effort", skippingBestEffort, OutputRequirement.BEST_EFFORT),
                outputPipeline(
                    "skip-required-reason",
                    CollectingOutput("skipping-with-reason") { OutputResult.Skipped("quota exhausted") },
                    OutputRequirement.REQUIRED,
                ),
                outputPipeline("no-output", NoOutput("explicit-no-output"), OutputRequirement.REQUIRED),
                outputPipeline(
                    "empty",
                    CollectingOutput("never-called"),
                    OutputRequirement.REQUIRED,
                    LambdaExtractor { _, _ -> ExtractionResult.Success(emptyList()) },
                ),
            ),
            clock,
            observers = listOf(events),
        )
        try {
            val runs = engine.runOnce().pipelineRuns.associateBy { it.pipelineId }
            val throwingRun = runs.getValue("throwing-required")
            assertEquals(RunStatus.FAILED, throwingRun.status)
            assertEquals("output:throwing", throwingRun.failures.single().stage)
            assertEquals("sink offline", throwingRun.failures.single().error.message)
            val bestEffortThrow = runs.getValue("throwing-best-effort")
            assertEquals(RunStatus.COMPLETED, bestEffortThrow.status)
            assertEquals(listOf("output:throwing-2"), bestEffortThrow.failures.map { it.stage })
            assertEquals(0, bestEffortThrow.deliveredValues)

            val skipRequired = runs.getValue("skip-required")
            assertEquals(RunStatus.FAILED, skipRequired.status)
            assertEquals("Required output skipped delivery", skipRequired.failures.single().error.message)
            assertEquals("output:skipping-with-reason", runs.getValue("skip-required-reason").failures.single().stage)
            assertEquals("quota exhausted", runs.getValue("skip-required-reason").failures.single().error.message)
            assertEquals(RunStatus.COMPLETED, runs.getValue("skip-best-effort").status)
            assertEquals(0, runs.getValue("skip-best-effort").deliveredValues)

            val noOutput = runs.getValue("no-output")
            assertEquals(RunStatus.COMPLETED, noOutput.status, "NoOutput is an explicit drop, not a required-output failure")
            val noOutputDrops = events.events.filter {
                it.type == CrawlerEventType.VALUE_DROPPED && it.context.pipelineId == "no-output"
            }
            assertEquals(1, noOutputDrops.size, "One skipped batch emits exactly one drop event")
            assertEquals("No output destination selected", noOutputDrops.single().detail)
            assertEquals(
                listOf("not today"),
                events.events.filter { it.type == CrawlerEventType.VALUE_DROPPED && it.context.pipelineId == "skip-best-effort" }.map { it.detail },
            )

            val empty = runs.getValue("empty")
            assertEquals(RunStatus.COMPLETED, empty.status)
            assertEquals(0, empty.producedValues)
            assertTrue(events.events.none { it.type == CrawlerEventType.DELIVERY_FAILED && it.context.pipelineId == "empty" })
            assertTrue(events.details(CrawlerEventType.DELIVERY_FAILED).containsAll(listOf("throwing", "throwing-2")))
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `response capture failures with anonymous causes are reported without leaking messages`() = runTest {
        val anonymous = object : RuntimeException("secret path /var/data") {}
        val store = object : ResponseStore {
            override val id: String = "anonymous-failure-store"
            override val capabilities: Set<ResponseStoreCapability> = emptySet()
            override suspend fun put(response: CrawlResponse): ResponseReference? = throw anonymous
            override suspend fun find(query: ResponseLookup): StoredResponse? = null
        }
        val events = EventLog()
        val pipeline = testPipeline("capture", bodyExtractor(), clock, discoveries = listOf(seeds("c")))
        val engine = testEngine(listOf(pipeline), clock, responseStore = store, observers = listOf(events))
        try {
            val run = engine.runOnce().pipelineRuns.single()
            assertEquals(RunStatus.FAILED, run.status)
            assertEquals("response-store", run.failures.single().stage)
            assertTrue(run.failures.single().error === anonymous, "The original cause is preserved internally")
            assertEquals(listOf("failure"), events.details(CrawlerEventType.RESPONSE_STORE_FAILED))
        } finally {
            engine.shutdown()
        }

        val entered = CompletableDeferred<Unit>()
        val hangingStore = object : ResponseStore {
            override val id: String = "hanging-store"
            override val capabilities: Set<ResponseStoreCapability> = emptySet()
            override suspend fun put(response: CrawlResponse): ResponseReference? {
                entered.complete(Unit)
                awaitCancellation()
            }
            override suspend fun find(query: ResponseLookup): StoredResponse? = null
        }
        val state = HookedStateStore(clock)
        val hangingEngine = testEngine(listOf(pipeline), clock, state = state, responseStore = hangingStore)
        try {
            val run = async(start = CoroutineStart.UNDISPATCHED) { hangingEngine.runOnce() }
            entered.await()
            run.cancel()
            assertFailsWith<CancellationException> { run.await() }
            assertEquals(listOf(AttemptOutcome.CANCELLED), state.finishedAttempts, "Cancelled capture is not a failed attempt")
            assertEquals(WorkTransition.Cancelled("work cancelled"), state.transitions.first())
        } finally {
            hangingEngine.shutdown()
        }
    }

    @Test
    fun `throwing observers are isolated from the run and from later observers`() = runTest {
        val throwing = object : Observer {
            override val id: String = "throwing-observer"
            var calls = 0
            override suspend fun onEvent(event: CrawlerEvent): ObserverResult {
                calls++
                throw IllegalStateException("observer bug")
            }
        }
        val healthy = EventLog("healthy")
        val output = CollectingOutput<String>("output")
        val pipeline = testPipeline(
            "observed",
            bodyExtractor(),
            clock,
            discoveries = listOf(seeds("o")),
            outputs = listOf(output to OutputRequirement.REQUIRED),
        )
        val engine = testEngine(listOf(pipeline), clock, observers = listOf(throwing, healthy))
        try {
            assertEquals(RunStatus.COMPLETED, engine.runOnce().status)
            assertEquals(listOf("o"), output.values())
            assertEquals(healthy.events.size, throwing.calls, "Every event still reaches the next observer")
            assertTrue(CrawlerEventType.ENGINE_RUN_COMPLETED in healthy.types())
        } finally {
            engine.shutdown()
        }
    }

    private data class Scenario(val run: PipelineRunResult, val events: EventLog, val transport: ScriptedTransport)

    private suspend fun runScenario(
        id: String,
        state: HookedStateStore,
        policies: PipelinePolicies = policies(),
        transport: ScriptedTransport = ScriptedTransport(),
        extractor: Extractor<String> = bodyExtractor(),
    ): Scenario {
        val events = EventLog()
        val pipeline = testPipeline(id, extractor, clock, transport = transport, policies = policies, discoveries = listOf(seeds("$id-target")))
        val engine = testEngine(listOf(pipeline), clock, state = state, observers = listOf(events))
        try {
            return Scenario(engine.runOnce().pipelineRuns.single(), events, transport)
        } finally {
            engine.shutdown()
        }
    }
}
