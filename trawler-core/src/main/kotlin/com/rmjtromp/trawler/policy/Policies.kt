package com.rmjtromp.trawler.policy

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.RequestContext
import java.time.Duration
import java.time.Instant

/**
 * A non-binding hint returned by extraction or output code.
 *
 * Retry ownership remains with the engine's [RetryPolicy]. The hint is not a
 * command, does not mutate crawl state, and must not cause an extension to
 * retry behind the engine's back.
 */
public data class RetryHint(
    public val retryable: Boolean,
    public val delay: Duration? = null,
    public val reason: String? = null,
) {
    init {
        require(delay == null || !delay.isNegative) { "Retry hint delay must not be negative" }
        require(reason == null || reason.isNotBlank()) { "Retry hint reason must not be blank when present" }
    }
}

/** Opaque stable identity selected by the host's request identity policy. */
@JvmInline
public value class RequestIdentity(public val value: String) {
    init {
        require(value.isNotBlank()) { "Request identity must not be blank" }
    }
}

/**
 * Canonicalizes a request before identity and deduplication.
 *
 * Canonicalization is policy, not an engine invariant. It must be pure with
 * respect to the supplied request and must preserve caller-significant data.
 * The returned request remains immutable and no network or persistence work
 * is allowed in this contract.
 */
public interface RequestCanonicalizer {
    /** Stable identifier used for deterministic configuration diagnostics. */
    public val id: String

    /** Returns the canonical request to use for subsequent policy decisions. */
    public fun canonicalize(request: CrawlRequest, context: RequestContext): CrawlRequest
}

/**
 * Selects stable logical identity for a request.
 *
 * Identity belongs to the host/pipeline because method, body, credentials,
 * locale, and other variants may be significant. The strategy must not
 * permanently suppress recrawls; active deduplication and recrawl are
 * separate policies.
 */
public interface RequestIdentityStrategy {
    /** Stable identifier used for deterministic configuration diagnostics. */
    public val id: String

    /** Computes identity without serializing user-produced values. */
    public fun identify(request: CrawlRequest, context: RequestContext): RequestIdentity
}

/** Decides whether equivalent active work may coexist. */
public interface DeduplicationPolicy {
    /** Stable identifier used for deterministic configuration diagnostics. */
    public val id: String

    /** Returns an expected admission decision for the active-work candidate. */
    public suspend fun decide(
        request: CrawlRequest,
        identity: RequestIdentity,
        context: RequestContext,
    ): DeduplicationDecision
}

/** Expected active-work deduplication decisions. */
public sealed interface DeduplicationDecision {
    /** The candidate may be admitted subject to other policies. */
    public data object Admit : DeduplicationDecision

    /** The candidate is suppressed as an active duplicate. */
    public data class Suppress(
        public val reason: String,
    ) : DeduplicationDecision {
        init {
            require(reason.isNotBlank()) { "Deduplication suppression reason must not be blank" }
        }
    }
}

/** Snapshot of stable-resource history supplied to a recrawl policy. */
public data class ResourceHistory(
    public val lastCompletedAt: Instant?,
    public val activeWorkCount: Int,
    public val visitCount: Long,
) {
    init {
        require(activeWorkCount >= 0) { "Active work count must not be negative" }
        require(visitCount >= 0) { "Visit count must not be negative" }
    }
}

/** Decides whether a completed logical resource may receive another visit. */
public interface RecrawlPolicy {
    /** Stable identifier used for deterministic configuration diagnostics. */
    public val id: String

    /** Evaluates recrawl eligibility without changing state. */
    public fun decide(
        request: CrawlRequest,
        history: ResourceHistory,
        context: RequestContext,
    ): RecrawlDecision
}

/** Expected recrawl eligibility decisions. */
public sealed interface RecrawlDecision {
    /** A new visit may be scheduled. */
    public data object Eligible : RecrawlDecision

    /** A visit is deferred until [eligibleAt] and may remain resumable. */
    public data class Deferred(
        public val eligibleAt: Instant,
        public val reason: String? = null,
    ) : RecrawlDecision

    /** The policy permanently rejects this candidate for the current scope. */
    public data class Rejected(
        public val reason: String,
    ) : RecrawlDecision {
        init {
            require(reason.isNotBlank()) { "Recrawl rejection reason must not be blank" }
        }
    }
}

/** Context supplied when classifying an error before retry policy runs. */
public data class FailureContext(
    public val crawler: CrawlerContext,
    public val request: CrawlRequest?,
)

/** Extensible classification metadata for an observed failure. */
public data class FailureClassification(
    public val category: String,
    public val retryableByDefault: Boolean,
    public val detail: String? = null,
) {
    init {
        require(category.isNotBlank()) { "Failure category must not be blank" }
        require(detail == null || detail.isNotBlank()) { "Failure detail must not be blank when present" }
    }
}

/** Classifies failures without deciding whether to retry them. */
public interface FailureClassifier {
    /** Stable identifier used for deterministic configuration diagnostics. */
    public val id: String

    /** Returns extensible classification metadata while preserving the cause separately. */
    public fun classify(error: Throwable, context: FailureContext): FailureClassification
}

/** Meaningful execution-attempt metadata supplied to retry policy. */
public data class AttemptSummary(
    public val attemptNumber: Int,
    public val startedAt: Instant?,
    public val previousFailures: Int,
) {
    init {
        require(attemptNumber >= 0) { "Attempt number must not be negative" }
        require(previousFailures >= 0) { "Previous failure count must not be negative" }
    }
}

/** Decides retry, defer, reroute, or abandonment after classification. */
public interface RetryPolicy {
    /** Stable identifier used for deterministic configuration diagnostics. */
    public val id: String

    /** Returns a policy decision; the engine owns scheduling and attempt accounting. */
    public fun decide(
        failure: FailureClassification,
        attempt: AttemptSummary,
        context: FailureContext,
    ): RetryDecision
}

/** Expected retry outcomes. */
public sealed interface RetryDecision {
    /** Retry through the visible engine attempt path. */
    public data object RetryNow : RetryDecision

    /** Retry no earlier than [at]. */
    public data class RetryAt(
        public val at: Instant,
    ) : RetryDecision

    /** Do not retry this work item. */
    public data object DoNotRetry : RetryDecision

    /** Retain resumable work while abandoning the current pipeline run. */
    public data class Abandon(
        public val reason: String,
    ) : RetryDecision {
        init {
            require(reason.isNotBlank()) { "Abandonment reason must not be blank" }
        }
    }
}

/**
 * Acquires a pacing reservation before a request.
 *
 * The engine owns the suspension and must release the returned reservation in
 * a `finally` path. Implementations must be cancellation-cooperative and
 * bounded; they must not hold unrelated origin or database resources while
 * waiting.
 */
public interface RequestPacingPolicy {
    /** Stable identifier used for deterministic configuration diagnostics. */
    public val id: String

    /** Waits for request pacing and returns a releaseable reservation. */
    public suspend fun acquire(request: CrawlRequest, context: RequestContext): PacingReservation

    /** Observes a completed response without changing request identity. */
    public fun observe(response: CrawlResponse, context: RequestContext)
}

/** Releaseable pacing reservation owned by the engine's structured scope. */
public fun interface PacingReservation {
    /** Releases the reservation; implementations must make this idempotent. */
    public fun release()
}

/**
 * Limits concurrent work using an adapter/policy-owned permit.
 *
 * Permit acquisition is suspending and cancellation-cooperative. The engine
 * must release the permit in `finally` and must not retain it across unrelated
 * network waits, cooldowns, or lifecycle shutdown.
 */
public interface ConcurrencyPolicy {
    /** Stable identifier used for deterministic configuration diagnostics. */
    public val id: String

    /** Acquires one bounded concurrency permit. */
    public suspend fun acquire(request: CrawlRequest, context: RequestContext): ConcurrencyPermit
}

/** Releaseable concurrency permit owned by the engine's structured scope. */
public fun interface ConcurrencyPermit {
    /** Releases the permit; implementations must make this idempotent. */
    public fun release()
}

/** Evaluates whether a request is in the configured crawl scope. */
public interface CrawlScopePolicy {
    /** Stable identifier used for deterministic configuration diagnostics. */
    public val id: String

    /** Evaluates scope without performing network or persistence work. */
    public fun evaluate(request: CrawlRequest, context: RequestContext): ScopeDecision
}

/** Expected scope decisions. */
public sealed interface ScopeDecision {
    /** The request is in scope for this pipeline. */
    public data object Allowed : ScopeDecision

    /** The request is outside scope and should be recorded as a policy outcome. */
    public data class Rejected(
        public val reason: String,
    ) : ScopeDecision {
        init {
            require(reason.isNotBlank()) { "Scope rejection reason must not be blank" }
        }
    }
}

/** Performs host-selected authorization before transport execution. */
public interface AuthorizationPolicy {
    /** Stable identifier used for deterministic configuration diagnostics. */
    public val id: String

    /** Authorizes [request] through a cancellation-cooperative policy call. */
    public suspend fun authorize(request: CrawlRequest, context: RequestContext): AuthorizationDecision
}

/** Expected authorization decisions. */
public sealed interface AuthorizationDecision {
    /** The request is authorized by this policy. */
    public data object Authorized : AuthorizationDecision

    /** The request is denied without invoking the transport. */
    public data class Denied(
        public val reason: String,
    ) : AuthorizationDecision {
        init {
            require(reason.isNotBlank()) { "Authorization denial reason must not be blank" }
        }
    }
}

/** Immutable work metadata presented to ordering policy. */
public data class WorkDescriptor(
    public val request: CrawlRequest,
    public val enqueuedAt: Instant,
    public val sequence: Long,
    public val priority: Int = 0,
) {
    init {
        require(sequence >= 0) { "Work sequence must not be negative" }
    }
}

/** Orders accepted work; persistence adapters may reject unsupported custom orderings. */
public interface WorkOrderingPolicy {
    /** Stable identifier used for deterministic configuration diagnostics. */
    public val id: String

    /** Compares [left] and [right] using a deterministic ordering contract. */
    public fun compare(left: WorkDescriptor, right: WorkDescriptor): Int
}

/** Immutable counters supplied to budget policy. */
public data class BudgetSnapshot(
    public val requestsStarted: Long,
    public val producedValues: Long,
    public val bytesRead: Long,
    public val failures: Long,
    public val discoveryEmissions: Long,
    public val elapsed: Duration,
) {
    init {
        require(requestsStarted >= 0) { "Request count must not be negative" }
        require(producedValues >= 0) { "Produced value count must not be negative" }
        require(bytesRead >= 0) { "Read byte count must not be negative" }
        require(failures >= 0) { "Failure count must not be negative" }
        require(discoveryEmissions >= 0) { "Discovery emission count must not be negative" }
        require(!elapsed.isNegative) { "Elapsed duration must not be negative" }
    }
}

/** A value-free budget observation that cannot erase or serialize `T`. */
public sealed interface BudgetObservation {
    /** One request became a meaningful execution attempt. */
    public data object RequestStarted : BudgetObservation

    /** A response body contributed [bytes] bytes to the budget. */
    public data class BytesRead(public val bytes: Long) : BudgetObservation {
        init {
            require(bytes >= 0) { "Observed bytes must not be negative" }
        }
    }

    /** Extraction produced [count] values; values themselves remain outside policy state. */
    public data class ValuesProduced(public val count: Long) : BudgetObservation {
        init {
            require(count >= 0) { "Produced value count must not be negative" }
        }
    }

    /** Discovery emitted [count] work candidates. */
    public data class DiscoveryEmitted(public val count: Long) : BudgetObservation {
        init {
            require(count >= 0) { "Discovery emission count must not be negative" }
        }
    }
}

/** Decides whether work may continue under configured budgets. */
public interface CrawlBudgetPolicy {
    /** Stable identifier used for deterministic configuration diagnostics. */
    public val id: String

    /** Evaluates an observation against an immutable budget snapshot. */
    public fun evaluate(snapshot: BudgetSnapshot, observation: BudgetObservation): BudgetDecision
}

/** Expected budget decisions. */
public sealed interface BudgetDecision {
    /** Continue admitting work. */
    public data object Continue : BudgetDecision

    /** Stop the current run while retaining resumable work where supported. */
    public data class Stop(
        public val reason: String,
    ) : BudgetDecision {
        init {
            require(reason.isNotBlank()) { "Budget stop reason must not be blank" }
        }
    }
}
