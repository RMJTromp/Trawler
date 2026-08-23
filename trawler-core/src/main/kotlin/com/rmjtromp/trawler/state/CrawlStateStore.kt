package com.rmjtromp.trawler.state

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.policy.RequestIdentity
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

    /** Historical operational state can be queried by the selected adapter. */
    QUERYABLE_HISTORY,
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
) {
    init {
        require(revision >= 0) { "Checkpoint revision must not be negative" }
        require(codecId.isNotBlank()) { "Checkpoint codec ID must not be blank" }
        require(codecVersion >= 0) { "Checkpoint codec version must not be negative" }
        require(generationId == null || generationId.isNotBlank()) {
            "Checkpoint generation ID must not be blank when present"
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
