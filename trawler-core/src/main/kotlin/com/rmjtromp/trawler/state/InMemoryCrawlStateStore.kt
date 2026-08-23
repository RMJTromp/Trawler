package com.rmjtromp.trawler.state

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.RequestIdentity
import com.rmjtromp.trawler.policy.ResourceHistory
import com.rmjtromp.trawler.policy.SystemClock
import com.rmjtromp.trawler.policy.WorkDescriptor
import com.rmjtromp.trawler.policy.WorkOrderingPolicy
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Duration
import java.time.Instant

/**
 * Bounded, process-local crawl state for the database-free runtime profile.
 *
 * The store keeps stable resource history, visit jobs, lease ownership, and
 * meaningful attempts as separate internal records. It never stores
 * produced values or response bodies. All methods are protected by one
 * coroutine mutex; callers remain responsible for structured scope ownership
 * and cancellation. Expired leases become reclaimable when the next store
 * operation runs; lease expiry alone is not recorded as a failed attempt.
 *
 * This implementation provides no restart, multi-process, or durable
 * guarantee. Queue capacity and retained operational history are finite, and
 * terminal history may be evicted when those bounds are reached.
 */
public class InMemoryCrawlStateStore(
    override val id: String = "memory-state",
    public val maxQueueSize: Int = 1_024,
    public val maxRetainedJobs: Int = 8_192,
    public val maxRetainedResources: Int = 8_192,
    public val leaseDuration: Duration = Duration.ofMinutes(5),
    private val clock: Clock = SystemClock,
) : CrawlStateStore,
    PipelineScopedWorkQueueStore,
    LeaseStore,
    AttemptStore,
    ResourceHistoryStore,
    WorkCancellationStore,
    WorkOrderingStore,
    WorkQueueStatus {

    private val mutex = Mutex()
    private var nextSequence: Long = 0
    private var nextLease: Long = 0
    private var nextAttempt: Long = 0
    private val jobs = LinkedHashMap<WorkItemId, MutableJob>()
    private val resources = LinkedHashMap<ResourceKey, MutableResource>()
    private val attempts = LinkedHashMap<AttemptId, MutableAttempt>()
    private val orderings = mutableMapOf<String, WorkOrderingPolicy>()

    override val capabilities: Set<CrawlStateCapability> = setOf(
        CrawlStateCapability.WORK_QUEUE,
        CrawlStateCapability.LEASE_FENCING,
        CrawlStateCapability.RESOURCE_HISTORY,
        CrawlStateCapability.ATTEMPT_HISTORY,
        CrawlStateCapability.PIPELINE_SCOPED_WORK,
        CrawlStateCapability.CANCELLATION,
        CrawlStateCapability.WORK_ORDERING,
        CrawlStateCapability.WORK_STATUS,
        CrawlStateCapability.QUERYABLE_HISTORY,
    )

    init {
        require(id.isNotBlank()) { "In-memory state store ID must not be blank" }
        require(maxQueueSize > 0) { "In-memory queue size must be positive" }
        require(maxRetainedJobs >= maxQueueSize) {
            "Retained job history must be at least the queue size"
        }
        require(maxRetainedResources >= maxQueueSize) {
            "Retained resource history must be at least the queue size"
        }
        require(!leaseDuration.isNegative && !leaseDuration.isZero) {
            "In-memory lease duration must be positive"
        }
    }

    override suspend fun enqueue(
        request: CrawlRequest,
        identity: RequestIdentity,
        context: CrawlerContext,
    ): EnqueueResult = mutex.withLock {
        reclaimExpiredLeases(context.pipelineId, clock.now())
        val key = ResourceKey(context.pipelineId, identity)
        val existingResource = resources[key]
        val active = existingResource?.activeJobs?.firstNotNullOfOrNull { jobs[it] }
        if (active != null) {
            return@withLock EnqueueResult.AlreadyActive(active.id)
        }
        if (jobs.values.count { it.isActive() } >= maxQueueSize) {
            return@withLock EnqueueResult.Rejected("In-memory active work capacity is full")
        }

        val resource = resources.getOrPut(key) { MutableResource(key) }
        val workItemId = WorkItemId("memory-work-${++nextSequence}")
        val job = MutableJob(
            id = workItemId,
            pipelineId = context.pipelineId,
            request = request,
            identity = identity,
            sequence = nextSequence,
            enqueuedAt = clock.now(),
        )
        jobs[workItemId] = job
        resource.activeJobs += workItemId
        trimHistory()
        EnqueueResult.Accepted(workItemId)
    }

    override suspend fun claim(
        limit: Int,
        ownerId: String,
        now: Instant,
    ): List<LeasedWork> = mutex.withLock {
        claimLocked(pipelineId = null, limit = limit, ownerId = ownerId, now = now)
    }

    override suspend fun claim(
        pipelineId: String,
        limit: Int,
        ownerId: String,
        now: Instant,
    ): List<LeasedWork> = mutex.withLock {
        require(pipelineId.isNotBlank()) { "Pipeline ID must not be blank" }
        claimLocked(pipelineId, limit, ownerId, now)
    }

    override suspend fun renew(work: LeasedWork, expiresAt: Instant): LeaseMutationResult = mutex.withLock {
        val job = currentLease(work)
            ?: return@withLock LeaseMutationResult.Stale
        if (!expiresAt.isAfter(clock.now())) {
            return@withLock LeaseMutationResult.Rejected("Lease expiry must be in the future")
        }
        job.expiresAt = expiresAt
        LeaseMutationResult.Accepted
    }

    override suspend fun transition(
        work: LeasedWork,
        transition: WorkTransition,
    ): LeaseMutationResult = mutex.withLock {
        val job = currentLease(work)
            ?: return@withLock LeaseMutationResult.Stale
        applyTransition(job, transition)
        trimHistory()
        LeaseMutationResult.Accepted
    }

    override suspend fun beginAttempt(
        work: LeasedWork,
        startedAt: Instant,
    ): AttemptStartResult = mutex.withLock {
        val job = currentLease(work)
            ?: return@withLock AttemptStartResult.Stale
        if (job.activeAttempt != null) {
            return@withLock AttemptStartResult.Rejected("A meaningful attempt is already active")
        }
        val attemptId = AttemptId("memory-attempt-${++nextAttempt}")
        val attempt = ExecutionAttempt(attemptId, job.attemptCount + 1, startedAt)
        job.attemptCount = attempt.number
        job.activeAttempt = attemptId
        attempts[attemptId] = MutableAttempt(attempt, job.id)
        AttemptStartResult.Started(attempt)
    }

    override suspend fun finishAttempt(
        work: LeasedWork,
        attempt: ExecutionAttempt,
        outcome: AttemptOutcome,
    ): LeaseMutationResult = mutex.withLock {
        val job = currentLease(work)
            ?: return@withLock LeaseMutationResult.Stale
        if (job.activeAttempt != attempt.id) {
            return@withLock LeaseMutationResult.Stale
        }
        val stored = attempts[attempt.id]
            ?: return@withLock LeaseMutationResult.Stale
        stored.outcome = outcome
        job.activeAttempt = null
        trimHistory()
        LeaseMutationResult.Accepted
    }

    override suspend fun history(
        pipelineId: String,
        identity: RequestIdentity,
    ): ResourceHistory = mutex.withLock {
        val resource = resources[ResourceKey(pipelineId, identity)]
        ResourceHistory(
            lastCompletedAt = resource?.lastCompletedAt,
            nextEligibleAt = resource?.nextEligibleAt,
            activeWorkCount = resource?.activeJobs?.count { jobs[it]?.isActive() == true } ?: 0,
            visitCount = resource?.visitCount ?: 0,
        )
    }

    override suspend fun cancelQueued(
        pipelineId: String,
        transition: WorkTransition,
    ): Int = mutex.withLock {
        require(pipelineId.isNotBlank()) { "Pipeline ID must not be blank" }
        val queued = jobs.values.filter {
            it.pipelineId == pipelineId && (it.status == JobStatus.QUEUED || it.status == JobStatus.RETRY_WAIT)
        }
        queued.forEach { job ->
            applyQueuedTransition(job, transition)
        }
        trimHistory()
        queued.size
    }

    override suspend fun setOrdering(pipelineId: String, policy: WorkOrderingPolicy) = mutex.withLock {
        require(pipelineId.isNotBlank()) { "Pipeline ID must not be blank" }
        orderings[pipelineId] = policy
    }

    /** Returns whether no active job remains for [pipelineId] or the store. */
    public suspend fun isIdle(pipelineId: String? = null): Boolean = mutex.withLock {
        jobs.values.none { job ->
            (pipelineId == null || job.pipelineId == pipelineId) && job.isActive()
        }
    }

    override suspend fun activeWorkCount(pipelineId: String): Int = mutex.withLock {
        require(pipelineId.isNotBlank()) { "Pipeline ID must not be blank" }
        jobs.values.count { it.pipelineId == pipelineId && it.isActive() }
    }

    private fun claimLocked(
        pipelineId: String?,
        limit: Int,
        ownerId: String,
        now: Instant,
    ): List<LeasedWork> {
        require(limit > 0) { "Claim limit must be positive" }
        require(ownerId.isNotBlank()) { "Lease owner ID must not be blank" }
        reclaimExpiredLeases(pipelineId, now)
        val ordering = pipelineId?.let { orderings[it] }
        val candidates = jobs.values
            .asSequence()
            .filter { job ->
                (pipelineId == null || job.pipelineId == pipelineId) &&
                    (job.status == JobStatus.QUEUED ||
                        (job.status == JobStatus.RETRY_WAIT &&
                            (job.nextEligibleAt == null || !job.nextEligibleAt!!.isAfter(now))))
            }
            .sortedWith { left, right ->
                ordering?.compare(
                    WorkDescriptor(left.request, left.enqueuedAt, left.sequence),
                    WorkDescriptor(right.request, right.enqueuedAt, right.sequence),
                ) ?: left.sequence.compareTo(right.sequence)
            }
            .take(limit)
            .toList()

        return candidates.map { job ->
            val leaseToken = LeaseToken("memory-lease-${++nextLease}")
            job.status = JobStatus.LEASED
            job.leaseToken = leaseToken
            job.ownerId = ownerId
            job.expiresAt = now.plus(leaseDuration)
            LeasedWork(
                workItemId = job.id,
                request = job.request,
                identity = job.identity,
                leaseToken = leaseToken,
                ownerId = ownerId,
                expiresAt = requireNotNull(job.expiresAt),
                pipelineId = job.pipelineId,
            )
        }
    }

    private fun currentLease(work: LeasedWork): MutableJob? {
        val job = jobs[work.workItemId]
        if (job == null || job.status != JobStatus.LEASED) return null
        if (job.pipelineId != work.pipelineId && work.pipelineId.isNotBlank()) return null
        if (job.leaseToken != work.leaseToken || job.ownerId != work.ownerId) return null
        if (job.expiresAt?.isAfter(clock.now()) != true) {
            reclaimExpiredLease(job)
            return null
        }
        return job
    }

    private fun reclaimExpiredLeases(pipelineId: String?, now: Instant) {
        jobs.values
            .filter { it.status == JobStatus.LEASED && (pipelineId == null || it.pipelineId == pipelineId) }
            .filter { it.expiresAt?.isAfter(now) != true }
            .forEach(::reclaimExpiredLease)
    }

    private fun reclaimExpiredLease(job: MutableJob) {
        job.activeAttempt?.let { attemptId ->
            attempts[attemptId]?.outcome = AttemptOutcome.ABANDONED
        }
        job.status = JobStatus.QUEUED
        job.leaseToken = null
        job.ownerId = null
        job.expiresAt = null
        job.activeAttempt = null
    }

    private fun applyTransition(job: MutableJob, transition: WorkTransition) {
        when (transition) {
            WorkTransition.Succeeded -> {
                job.status = JobStatus.SUCCEEDED
                job.nextEligibleAt = null
                completeResourceVisit(job)
                releaseResource(job)
            }

            is WorkTransition.Retry -> {
                job.status = JobStatus.RETRY_WAIT
                job.nextEligibleAt = transition.notBefore
                val resource = resources[ResourceKey(job.pipelineId, job.identity)]
                resource?.nextEligibleAt = resource.nextEligibleAt?.let {
                    maxOf(it, transition.notBefore)
                } ?: transition.notBefore
                clearLease(job)
            }

            is WorkTransition.Failed -> {
                job.status = JobStatus.FAILED
                releaseResource(job)
            }

            is WorkTransition.Skipped -> {
                job.status = JobStatus.SKIPPED
                releaseResource(job)
            }

            is WorkTransition.Cancelled -> {
                job.status = JobStatus.CANCELLED
                releaseResource(job)
            }

            is WorkTransition.Abandoned -> {
                job.status = JobStatus.ABANDONED
                releaseResource(job)
            }
        }
        clearLease(job)
    }

    private fun applyQueuedTransition(job: MutableJob, transition: WorkTransition) {
        when (transition) {
            is WorkTransition.Cancelled,
            is WorkTransition.Abandoned,
            is WorkTransition.Failed,
            is WorkTransition.Skipped,
            -> {
                applyTransition(job, transition)
            }

            WorkTransition.Succeeded,
            is WorkTransition.Retry,
            -> error("Queued cancellation cannot use transition $transition")
        }
    }

    private fun completeResourceVisit(job: MutableJob) {
        val resource = resources[ResourceKey(job.pipelineId, job.identity)] ?: return
        resource.lastCompletedAt = clock.now()
        resource.nextEligibleAt = null
        resource.visitCount++
    }

    private fun releaseResource(job: MutableJob) {
        resources[ResourceKey(job.pipelineId, job.identity)]?.activeJobs?.remove(job.id)
    }

    private fun clearLease(job: MutableJob) {
        job.leaseToken = null
        job.ownerId = null
        job.expiresAt = null
        job.activeAttempt = null
    }

    private fun trimHistory() {
        while (jobs.size > maxRetainedJobs) {
            val candidate = jobs.values.firstOrNull { !it.isActive() } ?: break
            jobs.remove(candidate.id)
            resources[ResourceKey(candidate.pipelineId, candidate.identity)]?.activeJobs?.remove(candidate.id)
        }

        while (resources.size > maxRetainedResources) {
            val candidate = resources.values.firstOrNull { it.activeJobs.none { jobId -> jobs[jobId]?.isActive() == true } }
                ?: break
            resources.remove(candidate.key)
        }

        while (attempts.size > maxRetainedJobs) {
            val candidate = attempts.values.firstOrNull { it.outcome != null } ?: break
            attempts.remove(candidate.attempt.id)
        }
    }

    private data class ResourceKey(
        val pipelineId: String,
        val identity: RequestIdentity,
    )

    private class MutableResource(
        val key: ResourceKey,
        val activeJobs: MutableSet<WorkItemId> = linkedSetOf(),
        var lastCompletedAt: Instant? = null,
        var nextEligibleAt: Instant? = null,
        var visitCount: Long = 0,
    )

    private class MutableJob(
        val id: WorkItemId,
        val pipelineId: String,
        val request: CrawlRequest,
        val identity: RequestIdentity,
        val sequence: Long,
        val enqueuedAt: Instant,
        var status: JobStatus = JobStatus.QUEUED,
        var leaseToken: LeaseToken? = null,
        var ownerId: String? = null,
        var expiresAt: Instant? = null,
        var nextEligibleAt: Instant? = null,
        var attemptCount: Int = 0,
        var activeAttempt: AttemptId? = null,
    ) {
        fun isActive(): Boolean = when (status) {
            JobStatus.QUEUED,
            JobStatus.LEASED,
            JobStatus.RETRY_WAIT,
            -> true

            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.SKIPPED,
            JobStatus.CANCELLED,
            JobStatus.ABANDONED,
            -> false
        }
    }

    private class MutableAttempt(
        val attempt: ExecutionAttempt,
        val workItemId: WorkItemId,
        var outcome: AttemptOutcome? = null,
    )

    private enum class JobStatus {
        QUEUED,
        LEASED,
        RETRY_WAIT,
        SUCCEEDED,
        FAILED,
        SKIPPED,
        CANCELLED,
        ABANDONED,
    }
}
