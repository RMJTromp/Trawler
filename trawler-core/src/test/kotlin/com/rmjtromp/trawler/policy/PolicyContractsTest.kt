package com.rmjtromp.trawler.policy

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.state.PipelineHealthSnapshot
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import org.junit.jupiter.api.Test

class PolicyContractsTest {
    @Test
    fun `retry hints accept absent or non-negative delays and reject negative delays or blank reasons`() {
        val defaults = RetryHint(retryable = true)
        assertEquals(true, defaults.retryable)
        assertNull(defaults.delay)
        assertNull(defaults.reason)
        assertEquals(Duration.ZERO, RetryHint(false, Duration.ZERO, "rate limited").delay)
        assertEquals("rate limited", RetryHint(false, Duration.ofSeconds(1), "rate limited").reason)

        assertMessage("Retry hint delay must not be negative") { RetryHint(true, Duration.ofNanos(-1)) }
        assertMessage("Retry hint reason must not be blank when present") { RetryHint(true, reason = "") }
        assertMessage("Retry hint reason must not be blank when present") { RetryHint(true, reason = " \t") }
    }

    @Test
    fun `request identity and expected decisions reject blank reasons`() {
        assertEquals("id", RequestIdentity("id").value)
        assertMessage("Request identity must not be blank") { RequestIdentity(" ") }

        assertEquals("duplicate", DeduplicationDecision.Suppress("duplicate").reason)
        assertMessage("Deduplication suppression reason must not be blank") { DeduplicationDecision.Suppress("") }
        assertMessage("Recrawl rejection reason must not be blank") { RecrawlDecision.Rejected(" ") }
        assertMessage("Abandonment reason must not be blank") { RetryDecision.Abandon(" ") }
        assertMessage("Scope rejection reason must not be blank") { ScopeDecision.Rejected("") }
        assertMessage("Authorization denial reason must not be blank") { AuthorizationDecision.Denied(" ") }
        assertMessage("Budget stop reason must not be blank") { BudgetDecision.Stop("") }
        assertMessage("Discovery deferral reason must not be blank") { DiscoveryAdmissionDecision.Defer(" ") }
        assertMessage("Circuit-open reason must not be blank") { CircuitBreakerDecision.Open(reason = " ") }
        assertMessage("Eligibility deferral reason must not be blank") {
            PipelineEligibilityDecision.Deferred(reason = "")
        }
        assertMessage("Eligibility disable reason must not be blank") { PipelineEligibilityDecision.Disabled(" ") }

        assertEquals("abandoned", RetryDecision.Abandon("abandoned").reason)
        assertEquals("denied", AuthorizationDecision.Denied("denied").reason)
        assertEquals("rejected", ScopeDecision.Rejected("rejected").reason)
        assertEquals("rejected", RecrawlDecision.Rejected("rejected").reason)
    }

    @Test
    fun `optional decision fields default to absent`() {
        val at = Instant.parse("2026-01-01T00:00:00Z")
        val deferredRecrawl = RecrawlDecision.Deferred(at)
        assertEquals(at, deferredRecrawl.eligibleAt)
        assertNull(deferredRecrawl.reason)

        val deferredEligibility = PipelineEligibilityDecision.Deferred(reason = "cooling down")
        assertNull(deferredEligibility.until)
        assertEquals("cooling down", deferredEligibility.reason)
        assertEquals(at, PipelineEligibilityDecision.Deferred(at, "cooling down").until)

        val open = CircuitBreakerDecision.Open(reason = "operator hold")
        assertNull(open.until)
        assertEquals(at, CircuitBreakerDecision.Open(at, "cooldown").until)
        assertEquals(at, RetryDecision.RetryAt(at).at)
    }

    @Test
    fun `resource history and attempt summaries reject negative counters at zero boundaries`() {
        val history = ResourceHistory(lastCompletedAt = null, activeWorkCount = 0, visitCount = 0)
        assertNull(history.nextEligibleAt)
        assertMessage("Active work count must not be negative") { ResourceHistory(null, -1, 0) }
        assertMessage("Visit count must not be negative") { ResourceHistory(null, 0, -1) }

        val attempt = AttemptSummary(attemptNumber = 0, startedAt = null, previousFailures = 0)
        assertEquals(0, attempt.attemptNumber)
        assertMessage("Attempt number must not be negative") { AttemptSummary(-1, null, 0) }
        assertMessage("Previous failure count must not be negative") { AttemptSummary(0, null, -1) }
    }

    @Test
    fun `failure classifications require a category and a non-blank detail when present`() {
        val classification = FailureClassification("timeout", retryableByDefault = true)
        assertNull(classification.detail)
        assertEquals("slow origin", FailureClassification("timeout", true, "slow origin").detail)
        assertMessage("Failure category must not be blank") { FailureClassification(" ", false) }
        assertMessage("Failure detail must not be blank when present") { FailureClassification("timeout", false, "") }
        assertMessage("Failure detail must not be blank when present") { FailureClassification("timeout", false, " ") }

        val context = FailureContext(CrawlerContext("engine", "run", "pipeline"), request = null)
        assertNull(context.request)
    }

    @Test
    fun `work descriptors and budget snapshots enforce non-negative boundaries`() {
        val descriptor = WorkDescriptor(CrawlRequest("resource"), Instant.EPOCH, sequence = 0)
        assertEquals(0, descriptor.priority)
        assertMessage("Work sequence must not be negative") { WorkDescriptor(CrawlRequest("r"), Instant.EPOCH, -1) }

        val zero = BudgetSnapshot(0, 0, 0, 0, 0, Duration.ZERO)
        assertEquals(Duration.ZERO, zero.elapsed)
        assertMessage("Request count must not be negative") { BudgetSnapshot(-1, 0, 0, 0, 0, Duration.ZERO) }
        assertMessage("Produced value count must not be negative") { BudgetSnapshot(0, -1, 0, 0, 0, Duration.ZERO) }
        assertMessage("Read byte count must not be negative") { BudgetSnapshot(0, 0, -1, 0, 0, Duration.ZERO) }
        assertMessage("Failure count must not be negative") { BudgetSnapshot(0, 0, 0, -1, 0, Duration.ZERO) }
        assertMessage("Discovery emission count must not be negative") {
            BudgetSnapshot(0, 0, 0, 0, -1, Duration.ZERO)
        }
        assertMessage("Elapsed duration must not be negative") {
            BudgetSnapshot(0, 0, 0, 0, 0, Duration.ofNanos(-1))
        }

        assertEquals(0L, BudgetObservation.BytesRead(0).bytes)
        assertEquals(0L, BudgetObservation.ValuesProduced(0).count)
        assertEquals(0L, BudgetObservation.DiscoveryEmitted(0).count)
        assertMessage("Observed bytes must not be negative") { BudgetObservation.BytesRead(-1) }
        assertMessage("Produced value count must not be negative") { BudgetObservation.ValuesProduced(-1) }
        assertMessage("Discovery emission count must not be negative") { BudgetObservation.DiscoveryEmitted(-1) }
    }

    @Test
    fun `eligibility and admission contexts reject blank identifiers and negative open work`() {
        val health = PipelineHealthSnapshot()
        val budget = BudgetSnapshot(0, 0, 0, 0, 0, Duration.ZERO)
        assertEquals("pipeline", PipelineEligibilityContext("pipeline", Instant.EPOCH, health, null).pipelineId)
        assertMessage("Pipeline ID must not be blank") { PipelineEligibilityContext(" ", Instant.EPOCH, health, null) }

        fun admission(pipelineId: String = "pipeline", discoveryId: String = "discovery", open: Int = 0) =
            DiscoveryAdmissionContext(pipelineId, discoveryId, open, null, null, null, budget, health, Instant.EPOCH)

        assertEquals(0, admission().openWorkCount)
        assertMessage("Pipeline ID must not be blank") { admission(pipelineId = "") }
        assertMessage("Discovery ID must not be blank") { admission(discoveryId = " ") }
        assertMessage("Open work count must not be negative") { admission(open = -1) }
    }

    @Test
    fun `incremental windows are half-open and reject a start after the end`() {
        val end = Instant.parse("2026-01-01T00:00:00Z")
        assertNull(IncrementalBoundaryPolicy.Window(null, end).startInclusive)
        assertEquals(end, IncrementalBoundaryPolicy.Window(end, end).startInclusive)
        assertEquals(end.minusMillis(1), IncrementalBoundaryPolicy.Window(end.minusMillis(1), end).startInclusive)
        assertMessage("Incremental window start must not follow its end") {
            IncrementalBoundaryPolicy.Window(end.plusMillis(1), end)
        }
    }

    private fun assertMessage(expected: String, block: () -> Unit) {
        val failure = assertFailsWith<IllegalArgumentException> { block() }
        assertEquals(expected, failure.message)
    }
}
