package com.rmjtromp.trawler.policy

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.model.RequestMethod
import com.rmjtromp.trawler.state.CheckpointPayload
import com.rmjtromp.trawler.state.DiscoveryCheckpoint
import com.rmjtromp.trawler.state.PipelineEligibilityOverride
import com.rmjtromp.trawler.state.PipelineHealthSnapshot
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class BuiltInPoliciesTest {
    @Test
    fun `digest identity distinguishes method target and body variants`() {
        val context = requestContext()
        val same = CrawlRequest("resource", body = com.rmjtromp.trawler.model.RequestBody.from(byteArrayOf(1)))
        val sameAgain = CrawlRequest("resource", body = com.rmjtromp.trawler.model.RequestBody.from(byteArrayOf(1)))
        val differentTarget = same.copy(target = "other")
        val differentMethod = same.copy(method = RequestMethod("POST"))
        val differentBody = same.copy(
            body = com.rmjtromp.trawler.model.RequestBody.from(byteArrayOf(2)),
        )

        assertEquals(
            DigestRequestIdentityStrategy.identify(same, context),
            DigestRequestIdentityStrategy.identify(sameAgain, context),
        )
        assertNotEquals(
            DigestRequestIdentityStrategy.identify(same, context),
            DigestRequestIdentityStrategy.identify(differentTarget, context),
        )
        assertNotEquals(
            DigestRequestIdentityStrategy.identify(same, context),
            DigestRequestIdentityStrategy.identify(differentMethod, context),
        )
        assertNotEquals(
            DigestRequestIdentityStrategy.identify(same, context),
            DigestRequestIdentityStrategy.identify(differentBody, context),
        )
        assertSame(same, IdentityRequestCanonicalizer.canonicalize(same, context))
    }

    @Test
    fun `recrawl policies preserve active deduplication and exact eligibility boundaries`() = runTest {
        val context = requestContext()
        val request = context.request
        val now = Instant.parse("2026-01-01T00:00:10Z")
        val clock = MutableClock(now)

        assertIs<RecrawlDecision.Eligible>(AlwaysRecrawlPolicy.decide(request, history(visitCount = 7), context))
        assertIs<RecrawlDecision.Eligible>(NeverRecrawlPolicy.decide(request, history(visitCount = 0), context))
        assertIs<RecrawlDecision.Rejected>(NeverRecrawlPolicy.decide(request, history(visitCount = 1), context))

        val after = RecrawlAfter(Duration.ofSeconds(10), clock)
        assertIs<RecrawlDecision.Deferred>(
            after.decide(
                request,
                history(lastCompletedAt = now.minusSeconds(1)),
                context,
            ),
        )
        clock.current = now.plusSeconds(10)
        assertIs<RecrawlDecision.Eligible>(
            after.decide(
                request,
                history(lastCompletedAt = now),
                context,
            ),
        )
        clock.current = now
        assertIs<RecrawlDecision.Deferred>(
            after.decide(
                request,
                history(lastCompletedAt = now, nextEligibleAt = now.plusSeconds(20)),
                context,
            ),
        )
        clock.current = now.plusSeconds(20)
        assertIs<RecrawlDecision.Eligible>(
            after.decide(request, history(lastCompletedAt = now, nextEligibleAt = now.plusSeconds(20)), context),
        )

        assertIs<DeduplicationDecision.Admit>(
            ActiveWorkOnlyDeduplicationPolicy.decide(request, RequestIdentity("identity"), context),
        )
        assertFailsWith<IllegalArgumentException> { RecrawlAfter(Duration.ofNanos(-1)) }
    }

    @Test
    fun `default failure and retry policies preserve cancellation classification`() {
        val context = FailureContext(requestContext().crawler, requestContext().request)
        val cancellation = CancellationException("cancelled")
        val cancellationClassification = DefaultFailureClassifier.classify(cancellation, context)
        val ordinaryClassification = DefaultFailureClassifier.classify(IllegalStateException("broken"), context)

        assertEquals("cancelled", cancellationClassification.category)
        assertFalse(cancellationClassification.retryableByDefault)
        assertEquals("IllegalStateException", ordinaryClassification.category)
        assertFalse(ordinaryClassification.retryableByDefault)
        assertEquals(
            RetryDecision.DoNotRetry,
            NoRetryPolicy.decide(ordinaryClassification, AttemptSummary(1, Instant.EPOCH, 0), context),
        )
    }

    @Test
    fun `fixed concurrency cancellation does not consume a permit and release is idempotent`() = runTest {
        val policy = FixedConcurrencyPolicy(1)
        val context = requestContext()
        val first = policy.acquire(context.request, context)
        val waiting = async(start = CoroutineStart.UNDISPATCHED) {
            policy.acquire(context.request, context)
        }

        assertFalse(waiting.isCompleted)
        waiting.cancelAndJoin()
        first.release()
        first.release()

        val second = policy.acquire(context.request, context)
        second.release()
        assertFailsWith<IllegalArgumentException> { FixedConcurrencyPolicy(0) }
    }

    @Test
    fun `built-in simple policies expose their documented decisions`() = runTest {
        val context = requestContext()
        val request = context.request
        val response = com.rmjtromp.trawler.model.CrawlResponse(
            request = request,
            statusCode = 200,
            headers = com.rmjtromp.trawler.model.Headers.empty(),
            body = com.rmjtromp.trawler.model.ResponseBody.empty(),
            receivedAt = Instant.EPOCH,
        )

        assertEquals(ScopeDecision.Allowed, AllowAllScopePolicy.evaluate(request, context))
        assertEquals(
            AuthorizationDecision.Authorized,
            AllowAllAuthorizationPolicy.authorize(request, context),
        )
        assertEquals(BudgetDecision.Continue, UnlimitedBudgetPolicy.evaluate(emptyBudget(), BudgetObservation.RequestStarted))
        assertEquals(PipelineEligibilityDecision.Eligible, AlwaysEligiblePipelinePolicy.decide(eligibilityContext()))
        assertEquals(CircuitBreakerDecision.Closed, NeverOpenCircuitBreakerPolicy.decide(PipelineHealthSnapshot(), Instant.EPOCH))
        assertEquals(DiscoveryAdmissionDecision.Admit, Continuous.decide(admissionContext(0)))
        assertEquals(0, FifoWorkOrderingPolicy.compare(workDescriptor(0), workDescriptor(0)))
        assertTrue(FifoWorkOrderingPolicy.compare(workDescriptor(0), workDescriptor(1)) < 0)
        ImmediatePacingPolicy.acquire(request, context).release()
        ImmediatePacingPolicy.acquire(request, context).release()
        ImmediatePacingPolicy.observe(response, context)

        val policies = BuiltInPolicies.ordinary(maxConcurrency = 2, clock = MutableClock(Instant.EPOCH))
        assertEquals("sha256-request-identity", policies.requestIdentity.id)
        assertEquals("active-work-only", policies.deduplication.id)
        assertEquals("fifo", policies.ordering.id)
        assertFailsWith<IllegalArgumentException> { BuiltInPolicies.ordinary(maxConcurrency = 0) }
    }

    @Test
    fun `eligibility override expires at the exact clock boundary`() {
        val now = Instant.parse("2026-01-01T00:00:00Z")
        val disabledUntilLater = PipelineEligibilityOverride(false, "maintenance", now.plusSeconds(1))
        val disabledUntilNow = PipelineEligibilityOverride(false, "maintenance", now)
        val context = eligibilityContext(now)

        val deferred = AlwaysEligiblePipelinePolicy.decide(context.copy(override = disabledUntilLater))
        assertIs<PipelineEligibilityDecision.Disabled>(deferred)
        assertEquals("maintenance", deferred.reason)
        assertEquals(
            PipelineEligibilityDecision.Eligible,
            AlwaysEligiblePipelinePolicy.decide(context.copy(override = disabledUntilNow)),
        )
        assertEquals(
            PipelineEligibilityDecision.Eligible,
            AlwaysEligiblePipelinePolicy.decide(context.copy(override = PipelineEligibilityOverride(true))),
        )
    }

    @Test
    fun `health counters reset consecutive failures and retain totals`() {
        val start = PipelineHealthSnapshot()
        val failed = NoOpPipelineHealthPolicy.observe(
            start,
            PipelineHealthObservation(PipelineHealthOutcome.FAILURE, Instant.EPOCH),
        )
        val failedAgain = NoOpPipelineHealthPolicy.observe(
            failed,
            PipelineHealthObservation(PipelineHealthOutcome.FAILURE, Instant.EPOCH.plusSeconds(1)),
        )
        val recovered = NoOpPipelineHealthPolicy.observe(
            failedAgain,
            PipelineHealthObservation(PipelineHealthOutcome.SUCCESS, Instant.EPOCH.plusSeconds(2)),
        )
        val cancelled = NoOpPipelineHealthPolicy.observe(
            recovered,
            PipelineHealthObservation(PipelineHealthOutcome.CANCELLED, Instant.EPOCH.plusSeconds(3)),
        )

        assertEquals(1, failed.revision)
        assertEquals(1, failed.consecutiveFailures)
        assertEquals(1L, failed.totalFailures)
        assertEquals(2, failedAgain.consecutiveFailures)
        assertEquals(2L, failedAgain.totalFailures)
        assertEquals(0, recovered.consecutiveFailures)
        assertEquals(1L, recovered.totalSuccesses)
        assertEquals(4L, cancelled.revision)
        assertEquals(0, cancelled.consecutiveFailures)
        assertEquals(2L, cancelled.totalFailures)
    }

    @Test
    fun `circuit breaker opens at threshold and honors persisted cooldown`() {
        val now = Instant.parse("2026-01-01T00:00:00Z")
        val policy = ConsecutiveFailureCircuitBreaker(2, Duration.ofSeconds(5))

        assertEquals(
            CircuitBreakerDecision.Closed,
            policy.decide(PipelineHealthSnapshot(consecutiveFailures = 1), now),
        )
        val opened = policy.decide(PipelineHealthSnapshot(consecutiveFailures = 2), now)
        assertIs<CircuitBreakerDecision.Open>(opened)
        assertEquals(now.plusSeconds(5), opened.until)

        val persisted = policy.decide(
            PipelineHealthSnapshot(consecutiveFailures = 0, circuitOpenUntil = now.plusSeconds(3)),
            now,
        )
        assertIs<CircuitBreakerDecision.Open>(persisted)
        assertEquals(now.plusSeconds(3), persisted.until)
        assertEquals(
            CircuitBreakerDecision.Closed,
            policy.decide(PipelineHealthSnapshot(consecutiveFailures = 0, circuitOpenUntil = now), now),
        )
        assertFailsWith<IllegalArgumentException> {
            ConsecutiveFailureCircuitBreaker(0, Duration.ofSeconds(1))
        }
        assertFailsWith<IllegalArgumentException> {
            ConsecutiveFailureCircuitBreaker(1, Duration.ZERO)
        }
    }

    @Test
    fun `backlog-first admits unfinished generations but defers fresh work`() {
        assertEquals(DiscoveryAdmissionDecision.Admit, BacklogFirst.decide(admissionContext(0)))
        assertIs<DiscoveryAdmissionDecision.Defer>(BacklogFirst.decide(admissionContext(1)))
        assertEquals(
            DiscoveryAdmissionDecision.Admit,
            BacklogFirst.decide(admissionContext(1, checkpointCompleted = false)),
        )
    }

    @Test
    fun `queue watermark hysteresis closes at high and reopens at low`() {
        val policy = QueueWatermark(highWatermark = 3, lowWatermark = 1)

        assertEquals(DiscoveryAdmissionDecision.Admit, policy.decide(admissionContext(0)))
        assertIs<DiscoveryAdmissionDecision.Defer>(policy.decide(admissionContext(3)))
        assertIs<DiscoveryAdmissionDecision.Defer>(policy.decide(admissionContext(2)))
        assertEquals(DiscoveryAdmissionDecision.Admit, policy.decide(admissionContext(1)))
        assertEquals(DiscoveryAdmissionDecision.Admit, policy.decide(admissionContext(2)))
        assertIs<DiscoveryAdmissionDecision.Defer>(policy.decide(admissionContext(3)))
        assertEquals(
            DiscoveryAdmissionDecision.Admit,
            policy.decide(admissionContext(3, checkpointCompleted = false)),
        )
        assertFailsWith<IllegalArgumentException> { QueueWatermark(1, 1) }
        assertFailsWith<IllegalArgumentException> { QueueWatermark(1, -1) }
    }

    @Test
    fun `built-in policy constructors report their own validation messages`() {
        listOf(
            "Concurrency permit count must be positive" to { FixedConcurrencyPolicy(0) },
            "Concurrency permit count must be positive" to { FixedConcurrencyPolicy(-1) },
            "Concurrency policy ID must not be blank" to { FixedConcurrencyPolicy(1, id = " ") },
            "Recrawl duration must not be negative" to { RecrawlAfter(Duration.ofNanos(-1)) },
            "Recrawl policy ID must not be blank" to { RecrawlAfter(Duration.ZERO, id = "") },
            "Circuit failure threshold must be positive" to { ConsecutiveFailureCircuitBreaker(0, Duration.ofSeconds(1)) },
            "Circuit cooldown must be positive" to { ConsecutiveFailureCircuitBreaker(1, Duration.ZERO) },
            "Circuit cooldown must be positive" to { ConsecutiveFailureCircuitBreaker(1, Duration.ofNanos(-1)) },
            "Circuit policy ID must not be blank" to {
                ConsecutiveFailureCircuitBreaker(1, Duration.ofSeconds(1), id = " ")
            },
            "Queue low watermark must not be negative" to { QueueWatermark(1, -1) },
            "Queue high watermark must exceed low watermark" to { QueueWatermark(1, 1) },
            "Queue watermark policy ID must not be blank" to { QueueWatermark(2, 1, id = "") },
        ).forEach { (message, construct) ->
            assertEquals(message, assertFailsWith<IllegalArgumentException> { construct() }.message)
        }
        assertEquals("concurrency-3", FixedConcurrencyPolicy(3).id)
        assertEquals("recrawl-after-1500ms", RecrawlAfter(Duration.ofMillis(1500)).id)
        assertEquals("consecutive-failure-circuit-2", ConsecutiveFailureCircuitBreaker(2, Duration.ofSeconds(1)).id)
        assertEquals("queue-watermark-1-3", QueueWatermark(3, 1).id)
    }

    @Test
    fun `recrawl-after uses whichever eligibility bound is present or later`() {
        val context = requestContext()
        val now = Instant.parse("2026-01-01T00:00:10Z")
        val policy = RecrawlAfter(Duration.ofSeconds(10), MutableClock(now))

        assertEquals(RecrawlDecision.Eligible, policy.decide(context.request, history(), context))
        val nextOnly = policy.decide(context.request, history(nextEligibleAt = now.plusSeconds(1)), context)
        assertEquals(RecrawlDecision.Deferred(now.plusSeconds(1), "Recrawl cooldown is active"), nextOnly)
        assertEquals(
            RecrawlDecision.Eligible,
            policy.decide(context.request, history(nextEligibleAt = now), context),
        )
        val policyLater = policy.decide(
            context.request,
            history(lastCompletedAt = now.minusSeconds(5), nextEligibleAt = now.plusSeconds(1)),
            context,
        )
        assertEquals(RecrawlDecision.Deferred(now.plusSeconds(5), "Recrawl cooldown is active"), policyLater)
    }

    @Test
    fun `failure classifier falls back to a generic category for anonymous failures`() {
        val context = FailureContext(requestContext().crawler, null)
        val anonymous = object : RuntimeException("anonymous") {}
        val classification = DefaultFailureClassifier.classify(anonymous, context)
        assertEquals("failure", classification.category)
        assertFalse(classification.retryableByDefault)
    }

    @Test
    fun `disabled override without a reason uses the documented default reason`() {
        val decision = AlwaysEligiblePipelinePolicy.decide(
            eligibilityContext().copy(override = PipelineEligibilityOverride(enabled = false)),
        )
        assertEquals(PipelineEligibilityDecision.Disabled("Pipeline disabled by operator override"), decision)
    }

    @Test
    fun `admission policies treat a missing checkpoint as fresh discovery`() {
        assertIs<DiscoveryAdmissionDecision.Defer>(BacklogFirst.decide(admissionContext(1).copy(checkpoint = null)))
        assertEquals(DiscoveryAdmissionDecision.Admit, BacklogFirst.decide(admissionContext(0).copy(checkpoint = null)))
        val watermark = QueueWatermark(highWatermark = 2, lowWatermark = 0)
        assertIs<DiscoveryAdmissionDecision.Defer>(watermark.decide(admissionContext(2).copy(checkpoint = null)))
        assertIs<DiscoveryAdmissionDecision.Defer>(watermark.decide(admissionContext(1).copy(checkpoint = null)))
        assertEquals(DiscoveryAdmissionDecision.Admit, watermark.decide(admissionContext(0).copy(checkpoint = null)))
    }

    private fun requestContext(): RequestContext {
        val crawler = CrawlerContext("engine", "pipeline-run", "pipeline")
        return RequestContext(crawler, CrawlRequest("resource"))
    }

    private fun history(
        lastCompletedAt: Instant? = null,
        activeWorkCount: Int = 0,
        visitCount: Long = 0,
        nextEligibleAt: Instant? = null,
    ): ResourceHistory = ResourceHistory(lastCompletedAt, activeWorkCount, visitCount, nextEligibleAt)

    private fun emptyBudget(): BudgetSnapshot = BudgetSnapshot(0, 0, 0, 0, 0, Duration.ZERO)

    private fun eligibilityContext(now: Instant = Instant.EPOCH): PipelineEligibilityContext =
        PipelineEligibilityContext("pipeline", now, PipelineHealthSnapshot(), null)

    private fun admissionContext(openWorkCount: Int, checkpointCompleted: Boolean = true): DiscoveryAdmissionContext =
        DiscoveryAdmissionContext(
            pipelineId = "pipeline",
            discoveryId = "discovery",
            openWorkCount = openWorkCount,
            oldestOpenWorkAt = null,
            checkpoint = DiscoveryCheckpoint(
                revision = 0,
                codecId = "codec",
                codecVersion = 1,
                payload = CheckpointPayload.from(byteArrayOf()),
                completed = checkpointCompleted,
            ),
            harvestState = null,
            budget = emptyBudget(),
            health = PipelineHealthSnapshot(),
            now = Instant.EPOCH,
        )

    private fun workDescriptor(sequence: Long): WorkDescriptor = WorkDescriptor(
        request = CrawlRequest("resource-$sequence"),
        enqueuedAt = Instant.EPOCH,
        sequence = sequence,
    )

    private class MutableClock(var current: Instant) : Clock {
        override fun now(): Instant = current
    }
}
