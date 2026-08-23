package com.rmjtromp.trawler.state

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.policy.RequestIdentity
import com.rmjtromp.trawler.policy.WorkOrderingPolicy
import java.time.Duration
import java.time.Instant

/**
 * Capability advertised by a crawl-state adapter.
 *
 * Capabilities describe operational state only: work, leases, checkpoints,
 * and run history. They do not authorize the adapter to store produced `T`,
 * response bodies, schemas, or serialized records.
 */
public enum class CrawlStateCapability {
    /** The adapter can accept and expose queued crawl work. */
    WORK_QUEUE,

    /** State survives process restart according to the adapter's guarantee. */
    DURABLE,

    /** State mutations expose the adapter's transaction boundary. */
    TRANSACTIONAL,

    /** Multiple workers can coordinate through this adapter. */
    MULTI_PROCESS,

    /** Lease-owned mutations reject stale fencing tokens. */
    LEASE_FENCING,

    /** Discovery checkpoints can be loaded and advanced. */
    DISCOVERY_CHECKPOINTS,

    /** Work acceptance and checkpoint advancement share one atomic boundary. */
    ATOMIC_ENQUEUE_AND_CHECKPOINT,

    /** Incompatible checkpoints can be retained for explicit operator action. */
    CHECKPOINT_QUARANTINE,

    /** Opaque harvest state can be loaded and advanced. */
    HARVEST_STATE,

    /** Incompatible harvest state can be retained for explicit operator action. */
    HARVEST_STATE_QUARANTINE,

    /** Queue metrics needed by admission watermarks are available. */
    QUEUE_WATERMARKS,

    /** Pipeline health snapshots can be loaded and compare-and-set. */
    PIPELINE_HEALTH,

    /** Operator pipeline eligibility overrides can be loaded and changed. */
    PIPELINE_ELIGIBILITY_OVERRIDES,

    /** Historical operational state can be queried by the selected adapter. */
    QUERYABLE_HISTORY,

    /** Durable engine and pipeline run summaries can be recorded. */
    RUN_HISTORY,

    /** Stale running engine and pipeline summaries can be recovered. */
    RUN_RECOVERY,

    /** Stable resource visit history is available to recrawl policies. */
    RESOURCE_HISTORY,

    /** Meaningful execution attempts are recorded separately from leases. */
    ATTEMPT_HISTORY,

    /** Work claims can be restricted to one pipeline without cross-pipeline leakage. */
    PIPELINE_SCOPED_WORK,

    /** Queued work can be transitioned during cooperative cancellation. */
    CANCELLATION,

    /** The state adapter accepts the effective per-pipeline work ordering policy. */
    WORK_ORDERING,

    /** The runtime can inspect bounded active-work counts for finalization. */
    WORK_STATUS,
}

/**
 * Root descriptor for one operational crawl-state adapter.
 *
 * This deliberately contains no universal storage methods. An adapter
 * implements focused capability contracts such as [WorkQueueStore] or
 * [DiscoveryCheckpointStore] in addition to this descriptor. The engine
 * validates required capabilities before runtime work; lifecycle ownership,
 * cancellation, transactions, and cleanup remain with the adapter.
 */
public interface CrawlStateStore {
    /** Stable identifier used for configuration diagnostics. */
    public val id: String

    /** Immutable capability set advertised by this adapter. */
    public val capabilities: Set<CrawlStateCapability>
}

/** Stable identity for one admitted operational work item. */
@JvmInline
public value class WorkItemId(public val value: String) {
    init {
        require(value.isNotBlank()) { "Work item ID must not be blank" }
    }
}

/** Fencing token that must accompany every lease-owned state mutation. */
@JvmInline
public value class LeaseToken(public val value: String) {
    init {
        require(value.isNotBlank()) { "Lease token must not be blank" }
    }
}

/** Result of an enqueue attempt, without exposing any produced-data type. */
public sealed interface EnqueueResult {
    /** The adapter durably or locally accepted a new work item. */
    public data class Accepted(public val workItemId: WorkItemId) : EnqueueResult

    /** Active duplicate work was suppressed according to the selected policy. */
    public data class AlreadyActive(public val workItemId: WorkItemId? = null) : EnqueueResult

    /** The adapter rejected the work for an operational reason. */
    public data class Rejected(public val reason: String) : EnqueueResult {
        init {
            require(reason.isNotBlank()) { "Enqueue rejection reason must not be blank" }
        }
    }
}

/** A bounded leased work item owned by one worker until expiry or release. */
public data class LeasedWork(
    public val workItemId: WorkItemId,
    public val request: CrawlRequest,
    public val identity: RequestIdentity,
    public val leaseToken: LeaseToken,
    public val ownerId: String,
    public val expiresAt: Instant,
    public val pipelineId: String = "",
)

/**
 * Focused work-queue capability for a crawl-state adapter.
 *
 * Implementations must keep queue and batch bounds explicit. Calls are
 * suspending and cancellation-cooperative; a cancellation after acceptance
 * must be reported according to the adapter's documented commit boundary.
 */
public interface WorkQueueStore {
    /** Enqueues one request using the policy-selected logical identity. */
    public suspend fun enqueue(
        request: CrawlRequest,
        identity: RequestIdentity,
        context: CrawlerContext,
    ): EnqueueResult

    /** Claims at most [limit] items for [ownerId] with bounded lease ownership. */
    public suspend fun claim(
        limit: Int,
        ownerId: String,
        now: Instant,
    ): List<LeasedWork>
}

/**
 * Optional scoped work-queue capability used when one state store serves
 * multiple pipelines. Claims must not return work belonging to another
 * pipeline. The engine requires this capability for the built-in
 * multi-pipeline runtime; an adapter without it may still implement the
 * lower-level [WorkQueueStore] contract for its own caller-controlled use.
 */
public interface PipelineScopedWorkQueueStore : WorkQueueStore {
    /** Claims at most [limit] queued items belonging to [pipelineId]. */
    public suspend fun claim(
        pipelineId: String,
        limit: Int,
        ownerId: String,
        now: Instant,
    ): List<LeasedWork>
}

/**
 * Optional in-process ordering surface. The policy remains extension-owned;
 * the adapter only applies it to its bounded operational queue and never
 * inspects produced values.
 */
public interface WorkOrderingStore {
    /**
     * Returns whether this adapter can apply [policy] without silently
     * substituting another ordering. This check is non-suspending so core can
     * reject unsupported configuration before runtime work; adapters must
     * keep it independent of database or network I/O.
     */
    public fun supports(policy: WorkOrderingPolicy): Boolean = true

    /** Installs the effective ordering for one pipeline before work is claimed. */
    public suspend fun setOrdering(pipelineId: String, policy: WorkOrderingPolicy)
}

/** Bounded operational work status used to distinguish drained from resumable runs. */
public interface WorkQueueStatus {
    /** Returns active queued, leased, or retry-wait work for [pipelineId]. */
    public suspend fun activeWorkCount(pipelineId: String): Int
}

/** Durable lifecycle status stored by a run-history adapter. */
public enum class DurableRunStatus {
    RUNNING,
    COMPLETED,
    ABANDONED,
    FAILED,
    CANCELLED,
}

/**
 * Focused durable engine/pipeline run-history capability.
 *
 * The capability stores only operational lifecycle metadata and bounded
 * failure counts. It never receives produced values, response bodies, or
 * application records. Core calls the methods from its structured run scope;
 * implementations must honor cancellation, define their transaction
 * boundaries, and preserve the original failure by propagating adapter
 * failures. [recoverStaleRuns] may mark only stale RUNNING summaries and must
 * not rewrite or release job leases as a shortcut for recovery.
 */
public interface RunHistoryStore {
    /** Recovers stale RUNNING summaries before a new engine run begins. */
    public suspend fun recoverStaleRuns(
        now: Instant,
        staleAfter: Duration,
    ): Int

    /** Persists one engine run's RUNNING summary before pipeline work starts. */
    public suspend fun startEngineRun(
        engineRunId: String,
        startedAt: Instant,
        reason: String?,
    )

    /** Persists one pipeline run's RUNNING summary before its typed work starts. */
    public suspend fun startPipelineRun(
        engineRunId: String,
        pipelineRunId: String,
        pipelineId: String,
        startedAt: Instant,
    )

    /** Completes one pipeline summary without changing any job lease. */
    public suspend fun completePipelineRun(
        pipelineRunId: String,
        status: DurableRunStatus,
        completedAt: Instant,
        failureCount: Long,
    )

    /** Completes one engine summary without changing any job lease. */
    public suspend fun completeEngineRun(
        engineRunId: String,
        status: DurableRunStatus,
        completedAt: Instant,
        failureCount: Long,
    )
}

/** Result of a lease-owned mutation. */
public sealed interface LeaseMutationResult {
    /** The mutation was accepted for the current fencing token. */
    public data object Accepted : LeaseMutationResult

    /** The lease was stale, reclaimed, or unknown; the caller must stop mutating. */
    public data object Stale : LeaseMutationResult

    /** The adapter rejected the mutation for a non-fencing reason. */
    public data class Rejected(public val reason: String) : LeaseMutationResult {
        init {
            require(reason.isNotBlank()) { "Lease mutation rejection reason must not be blank" }
        }
    }
}

/**
 * Focused lease capability for fenced state adapters.
 *
 * Every mutation receives the leased work and therefore its fencing token.
 * Implementations must reject stale workers rather than silently applying a
 * late completion. Cancellation must release or leave reclaimable ownership
 * according to the adapter's documented lease semantics.
 */
public interface LeaseStore {
    /** Renews a lease only when the supplied token still owns the work item. */
    public suspend fun renew(work: LeasedWork, expiresAt: Instant): LeaseMutationResult

    /** Completes or transitions work only when the supplied token is current. */
    public suspend fun transition(work: LeasedWork, transition: WorkTransition): LeaseMutationResult
}

/**
 * Records meaningful execution attempts independently of lease acquisition.
 * Implementations must reject a stale lease when beginning or finishing an
 * attempt. The engine calls [beginAttempt] immediately before substantive
 * transport or response processing, so claiming a lease never consumes retry
 * budget. Calls are suspending, cancellation-cooperative, and owned by the
 * state adapter's transaction/commit boundary.
 */
public interface AttemptStore {
    /** Starts one attempt for the current lease, or returns a stale result. */
    public suspend fun beginAttempt(
        work: LeasedWork,
        startedAt: Instant,
    ): AttemptStartResult

    /** Records the outcome for an attempt owned by the current lease. */
    public suspend fun finishAttempt(
        work: LeasedWork,
        attempt: ExecutionAttempt,
        outcome: AttemptOutcome,
    ): LeaseMutationResult
}

/** A meaningful attempt identity and number; lease acquisition is not an attempt. */
public data class ExecutionAttempt(
    public val id: AttemptId,
    public val number: Int,
    public val startedAt: Instant,
) {
    init {
        require(number > 0) { "Attempt number must be positive" }
    }
}

/** Stable identifier for one meaningful execution attempt. */
@JvmInline
public value class AttemptId(public val value: String) {
    init {
        require(value.isNotBlank()) { "Attempt ID must not be blank" }
    }
}

/** Expected outcomes recorded for a meaningful execution attempt. */
public enum class AttemptOutcome {
    SUCCEEDED,
    FAILED,
    CANCELLED,
    ABANDONED,
}

/** Results of starting an attempt under a fenced lease. */
public sealed interface AttemptStartResult {
    /** The attempt was created after substantive work was about to begin. */
    public data class Started(public val attempt: ExecutionAttempt) : AttemptStartResult

    /** The lease was stale or already reclaimed. */
    public data object Stale : AttemptStartResult

    /** The adapter rejected attempt creation for an operational reason. */
    public data class Rejected(public val reason: String) : AttemptStartResult {
        init {
            require(reason.isNotBlank()) { "Attempt rejection reason must not be blank" }
        }
    }
}

/**
 * Provides stable-resource history to recrawl policy evaluation. History is
 * operational metadata only; it must not contain produced values, response
 * bodies, schemas, or serialized records.
 */
public interface ResourceHistoryStore {
    /** Returns bounded history for one pipeline/resource identity. */
    public suspend fun history(
        pipelineId: String,
        identity: RequestIdentity,
    ): com.rmjtromp.trawler.policy.ResourceHistory
}

/**
 * Cancels queued work during cooperative shutdown or pipeline cancellation.
 * Active lease owners still perform fenced transitions in their own
 * cancellation handlers. The returned count is operational metadata only.
 */
public interface WorkCancellationStore {
    /** Transitions all still-queued work for [pipelineId] and returns its count. */
    public suspend fun cancelQueued(
        pipelineId: String,
        transition: WorkTransition,
    ): Int
}

/** Operational transition requested by a lease owner. */
public sealed interface WorkTransition {
    /** The work completed successfully. */
    public data object Succeeded : WorkTransition

    /** The work should be attempted again after [notBefore]. */
    public data class Retry(public val notBefore: Instant) : WorkTransition

    /** The work failed permanently for the current policy scope. */
    public data class Failed(public val reason: String) : WorkTransition {
        init {
            require(reason.isNotBlank()) { "Work failure reason must not be blank" }
        }
    }

    /** The work was intentionally skipped by policy. */
    public data class Skipped(public val reason: String) : WorkTransition {
        init {
            require(reason.isNotBlank()) { "Work skip reason must not be blank" }
        }
    }

    /** The caller cooperatively cancelled this visit. */
    public data class Cancelled(public val reason: String) : WorkTransition {
        init {
            require(reason.isNotBlank()) { "Work cancellation reason must not be blank" }
        }
    }

    /** The visit stopped without being a terminal infrastructure failure. */
    public data class Abandoned(public val reason: String) : WorkTransition {
        init {
            require(reason.isNotBlank()) { "Work abandonment reason must not be blank" }
        }
    }
}

/** Immutable codec-owned discovery checkpoint payload. */
public class CheckpointPayload private constructor(
    private val bytes: ByteArray,
) {
    /** Number of bytes in the opaque payload. */
    public val size: Int
        get() = bytes.size

    /** Returns a defensive copy for the owning discovery codec. */
    public fun toByteArray(): ByteArray = bytes.copyOf()

    override fun equals(other: Any?): Boolean =
        other is CheckpointPayload && bytes.contentEquals(other.bytes)

    override fun hashCode(): Int = bytes.contentHashCode()

    override fun toString(): String = "CheckpointPayload(size=$size)"

    public companion object {
        /** Creates an immutable checkpoint payload by copying [bytes]. */
        public fun from(bytes: ByteArray): CheckpointPayload = CheckpointPayload(bytes.copyOf())
    }
}

/** Opaque discovery state with extension-owned codec identity and revision. */
public data class DiscoveryCheckpoint(
    public val revision: Long,
    public val codecId: String,
    public val codecVersion: Int,
    public val payload: CheckpointPayload,
    public val generationId: String? = null,
    public val strategyId: String? = null,
    public val completed: Boolean = false,
) {
    init {
        require(revision >= 0) { "Checkpoint revision must not be negative" }
        require(codecId.isNotBlank()) { "Checkpoint codec ID must not be blank" }
        require(codecVersion >= 0) { "Checkpoint codec version must not be negative" }
        require(generationId == null || generationId.isNotBlank()) {
            "Checkpoint generation ID must not be blank when present"
        }
        require(strategyId == null || strategyId.isNotBlank()) {
            "Checkpoint strategy ID must not be blank when present"
        }
    }
}

/**
 * Focused compare-and-set checkpoint capability.
 *
 * Checkpoints advance only after the engine confirms emitted work was
 * accepted. Implementations must reject stale revisions and document whether
 * enqueue plus checkpoint advancement share an atomic transaction.
 */
public interface DiscoveryCheckpointStore {
    /** Loads the latest checkpoint for one pipeline/discovery pair. */
    public suspend fun load(pipelineId: String, discoveryId: String): DiscoveryCheckpoint?

    /** Advances the checkpoint only when [expectedRevision] still matches. */
    public suspend fun compareAndSet(
        pipelineId: String,
        discoveryId: String,
        expectedRevision: Long?,
        next: DiscoveryCheckpoint,
    ): Boolean
}

/** Codec descriptor persisted beside an opaque checkpoint payload. */
public data class DiscoveryCheckpointCodecDescriptor(
    public val id: String,
    public val version: Int,
) {
    init {
        require(id.isNotBlank()) { "Discovery checkpoint codec ID must not be blank" }
        require(version >= 0) { "Discovery checkpoint codec version must not be negative" }
    }
}

/** Explicit result of resolving a stored checkpoint against a current codec. */
public sealed interface CheckpointResolution {
    /** The loaded checkpoint is compatible and may be supplied to discovery. */
    public data class Use(public val checkpoint: DiscoveryCheckpoint) : CheckpointResolution

    /** An older payload was converted by the extension-owned codec. */
    public data class Migrated(public val checkpoint: DiscoveryCheckpoint) : CheckpointResolution

    /** The state is intentionally reset to an empty current-codec payload. */
    public data class Reset(public val reason: String) : CheckpointResolution {
        init {
            require(reason.isNotBlank()) { "Checkpoint reset reason must not be blank" }
        }
    }

    /** The incompatible state must be retained for explicit operator action. */
    public data class Quarantine(public val reason: String) : CheckpointResolution {
        init {
            require(reason.isNotBlank()) { "Checkpoint quarantine reason must not be blank" }
        }
    }
}

/** Work item supplied to an atomic discovery admission operation. */
public data class DiscoveryWorkItem(
    public val request: CrawlRequest,
    public val identity: RequestIdentity,
    public val context: CrawlerContext,
)

/** Result of an adapter-owned enqueue plus checkpoint transaction. */
public sealed interface AtomicEnqueueCheckpointResult {
    /** All items were accepted/suppressed and the checkpoint committed. */
    public data class Advanced(
        public val enqueueResults: List<EnqueueResult>,
        public val checkpoint: DiscoveryCheckpoint,
    ) : AtomicEnqueueCheckpointResult

    /** A different writer advanced the checkpoint first; no work was committed. */
    public data object RevisionConflict : AtomicEnqueueCheckpointResult

    /** The bounded queue could not accept the batch; no checkpoint advanced. */
    public data class Rejected(public val reason: String) : AtomicEnqueueCheckpointResult {
        init {
            require(reason.isNotBlank()) { "Atomic enqueue rejection reason must not be blank" }
        }
    }
}

/**
 * Optional transaction boundary for durable discovery admission.
 *
 * Implementations must keep [items] bounded, perform active deduplication in
 * the same transaction as checkpoint advancement, and roll back both when a
 * revision conflict or queue rejection occurs. The engine still owns the
 * response supplied by [com.rmjtromp.trawler.discovery.DiscoveryEmission.ExistingResponse].
 */
public interface AtomicEnqueueAndCheckpointStore : DiscoveryCheckpointStore {
    /** Enqueues [items] and advances [next] under one adapter-owned boundary. */
    public suspend fun enqueueAndCheckpoint(
        pipelineId: String,
        discoveryId: String,
        expectedRevision: Long?,
        next: DiscoveryCheckpoint,
        items: List<DiscoveryWorkItem>,
    ): AtomicEnqueueCheckpointResult
}

/** Retains incompatible checkpoint bytes and the reason they were quarantined. */
public interface CheckpointQuarantineStore {
    /** Stores one bounded operational quarantine record without executing it. */
    public suspend fun quarantine(
        pipelineId: String,
        discoveryId: String,
        checkpoint: DiscoveryCheckpoint,
        reason: String,
    )
}

/** Immutable queue metrics used by admission policies. */
public data class WorkQueueMetrics(
    public val openWorkCount: Int,
    public val oldestOpenWorkAt: Instant?,
) {
    init {
        require(openWorkCount >= 0) { "Open work count must not be negative" }
    }
}

/** Optional bounded queue metrics capability for watermark admission. */
public interface WorkQueueMetricsStore {
    /** Returns scalar metrics without loading work rows into memory. */
    public suspend fun metrics(pipelineId: String): WorkQueueMetrics
}
