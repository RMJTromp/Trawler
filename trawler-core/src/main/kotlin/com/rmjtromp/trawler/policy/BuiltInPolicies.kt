package com.rmjtromp.trawler.policy

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.pipeline.PipelinePolicies
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Semaphore
import java.security.MessageDigest
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

/** Lightweight dependency-free policy implementations for ordinary runs. */
public object BuiltInPolicies {
    /**
     * Returns a complete ordinary policy bundle. The policies are generic
     * conveniences and each can be replaced on a pipeline builder.
     */
    public fun ordinary(maxConcurrency: Int = 4, clock: Clock = SystemClock): PipelinePolicies =
        PipelinePolicies(
            requestCanonicalizer = IdentityRequestCanonicalizer,
            requestIdentity = DigestRequestIdentityStrategy,
            deduplication = ActiveWorkOnlyDeduplicationPolicy,
            recrawl = AlwaysRecrawlPolicy,
            failureClassifier = DefaultFailureClassifier,
            retry = NoRetryPolicy,
            pacing = ImmediatePacingPolicy,
            concurrency = FixedConcurrencyPolicy(maxConcurrency),
            scope = AllowAllScopePolicy,
            authorization = AllowAllAuthorizationPolicy,
            ordering = FifoWorkOrderingPolicy,
            budget = UnlimitedBudgetPolicy,
        )
}

/** Canonicalization convenience that preserves the caller's immutable request. */
public object IdentityRequestCanonicalizer : RequestCanonicalizer {
    override val id: String = "identity-canonicalizer"

    override fun canonicalize(request: CrawlRequest, context: RequestContext): CrawlRequest = request
}

/**
 * JDK-only request identity using method, target, and body bytes. The digest
 * bounds the stored identity length without choosing a URL or serialization
 * convention; callers may replace it when headers or other variants matter.
 */
public object DigestRequestIdentityStrategy : RequestIdentityStrategy {
    override val id: String = "sha256-request-identity"

    override fun identify(request: CrawlRequest, context: RequestContext): RequestIdentity {
        val digest = MessageDigest.getInstance("SHA-256")
        digest.update(request.method.value.toByteArray(Charsets.UTF_8))
        digest.update(0)
        digest.update(request.target.toByteArray(Charsets.UTF_8))
        digest.update(0)
        request.body?.toByteArray()?.let(digest::update)
        return RequestIdentity(digest.digest().toHex())
    }
}

/** The state store suppresses equivalent work while it is active. */
public object ActiveWorkOnlyDeduplicationPolicy : DeduplicationPolicy {
    override val id: String = "active-work-only"

    override suspend fun decide(
        request: CrawlRequest,
        identity: RequestIdentity,
        context: RequestContext,
    ): DeduplicationDecision = DeduplicationDecision.Admit
}

/** Allows a completed resource to be visited again whenever discovery emits it. */
public object AlwaysRecrawlPolicy : RecrawlPolicy {
    override val id: String = "always-recrawl"

    override fun decide(
        request: CrawlRequest,
        history: ResourceHistory,
        context: RequestContext,
    ): RecrawlDecision = RecrawlDecision.Eligible
}

/** Allows only the first completed visit of a stable resource. */
public object NeverRecrawlPolicy : RecrawlPolicy {
    override val id: String = "never-recrawl"

    override fun decide(
        request: CrawlRequest,
        history: ResourceHistory,
        context: RequestContext,
    ): RecrawlDecision = if (history.visitCount == 0L) {
        RecrawlDecision.Eligible
    } else {
        RecrawlDecision.Rejected("Resource has already completed a visit")
    }
}

/**
 * Allows a later visit after the selected duration since the last completion.
 * The decision is pure with respect to crawl state; the engine decides what
 * to do with a deferred request on this run. No coroutine or resource is
 * owned by this policy.
 */
public class RecrawlAfter(
    private val duration: Duration,
    private val clock: Clock = SystemClock,
    override val id: String = "recrawl-after-${duration.toMillis()}ms",
) : RecrawlPolicy {
    init {
        require(!duration.isNegative) { "Recrawl duration must not be negative" }
        require(id.isNotBlank()) { "Recrawl policy ID must not be blank" }
    }

    override fun decide(
        request: CrawlRequest,
        history: ResourceHistory,
        context: RequestContext,
    ): RecrawlDecision {
        val completedAt = history.lastCompletedAt ?: return RecrawlDecision.Eligible
        val eligibleAt = completedAt.plus(duration)
        return if (!eligibleAt.isAfter(clock.now())) {
            RecrawlDecision.Eligible
        } else {
            RecrawlDecision.Deferred(eligibleAt, "Recrawl cooldown is active")
        }
    }
}

/** Classifies failures without making transport-specific retry assumptions. */
public object DefaultFailureClassifier : FailureClassifier {
    override val id: String = "generic-failure-classifier"

    override fun classify(error: Throwable, context: FailureContext): FailureClassification =
        if (error is CancellationException) {
            FailureClassification("cancelled", retryableByDefault = false)
        } else {
            FailureClassification(
                category = error::class.simpleName ?: "failure",
                retryableByDefault = false,
            )
        }
}

/** Ordinary runs do not retry unless a caller explicitly installs a policy. */
public object NoRetryPolicy : RetryPolicy {
    override val id: String = "no-retry"

    override fun decide(
        failure: FailureClassification,
        attempt: AttemptSummary,
        context: FailureContext,
    ): RetryDecision = RetryDecision.DoNotRetry
}

/** No-delay pacing policy with an idempotent no-op reservation. */
public object ImmediatePacingPolicy : RequestPacingPolicy {
    override val id: String = "immediate-pacing"

    override suspend fun acquire(request: CrawlRequest, context: RequestContext): PacingReservation =
        PacingReservation { }

    override fun observe(response: CrawlResponse, context: RequestContext) = Unit
}

/**
 * Fixed bounded concurrency policy backed by a coroutine semaphore. The
 * returned permit is idempotently releasable and cancellation of acquisition
 * never consumes capacity.
 */
public class FixedConcurrencyPolicy(
    private val maxPermits: Int,
    override val id: String = "concurrency-$maxPermits",
) : ConcurrencyPolicy {
    private val semaphore = Semaphore(maxPermits)

    init {
        require(maxPermits > 0) { "Concurrency permit count must be positive" }
        require(id.isNotBlank()) { "Concurrency policy ID must not be blank" }
    }

    override suspend fun acquire(request: CrawlRequest, context: RequestContext): ConcurrencyPermit {
        semaphore.acquire()
        val released = AtomicBoolean(false)
        return ConcurrencyPermit {
            if (released.compareAndSet(false, true)) semaphore.release()
        }
    }
}

/** Allows every request without network or persistence work. */
public object AllowAllScopePolicy : CrawlScopePolicy {
    override val id: String = "allow-all-scope"

    override fun evaluate(request: CrawlRequest, context: RequestContext): ScopeDecision = ScopeDecision.Allowed
}

/** Allows every request; hosts should replace this for authorization-sensitive runs. */
public object AllowAllAuthorizationPolicy : AuthorizationPolicy {
    override val id: String = "allow-all-authorization"

    override suspend fun authorize(request: CrawlRequest, context: RequestContext): AuthorizationDecision =
        AuthorizationDecision.Authorized
}

/** FIFO ordering based on the state store's monotonic admission sequence. */
public object FifoWorkOrderingPolicy : WorkOrderingPolicy {
    override val id: String = "fifo"

    override fun compare(left: WorkDescriptor, right: WorkDescriptor): Int =
        left.sequence.compareTo(right.sequence)
}

/** A budget that leaves finite queue/discovery/runtime bounds to the engine. */
public object UnlimitedBudgetPolicy : CrawlBudgetPolicy {
    override val id: String = "engine-bounded"

    override fun evaluate(snapshot: BudgetSnapshot, observation: BudgetObservation): BudgetDecision =
        BudgetDecision.Continue
}

private fun ByteArray.toHex(): String = buildString(size * 2) {
    for (byte in this@toHex) append("%02x".format(byte))
}
