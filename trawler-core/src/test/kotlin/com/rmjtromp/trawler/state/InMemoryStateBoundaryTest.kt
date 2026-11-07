package com.rmjtromp.trawler.state

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.RequestIdentity
import com.rmjtromp.trawler.policy.ResourceHistory
import com.rmjtromp.trawler.policy.WorkDescriptor
import com.rmjtromp.trawler.policy.WorkOrderingPolicy
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class InMemoryStateBoundaryTest {
    @Test
    fun `negative lease duration is rejected like zero`() {
        assertFailsWith<IllegalArgumentException> {
            InMemoryCrawlStateStore(leaseDuration = Duration.ofSeconds(-1))
        }
    }

    @Test
    fun `atomic admission validates every argument before changing queue or checkpoint`() = runTest {
        val store = InMemoryCrawlStateStore(maxQueueSize = 2, maxCheckpointPayloadBytes = 1)
        val valid = checkpoint(0, byteArrayOf(1))
        val item = discoveryItem("pipeline", "one")

        assertFailsWith<IllegalArgumentException> {
            store.enqueueAndCheckpoint(" ", "discovery", null, valid, listOf(item))
        }
        assertFailsWith<IllegalArgumentException> {
            store.enqueueAndCheckpoint("pipeline", " ", null, valid, listOf(item))
        }
        assertFailsWith<IllegalArgumentException> {
            store.enqueueAndCheckpoint("pipeline", "discovery", null, valid, emptyList())
        }
        assertFailsWith<IllegalArgumentException> {
            store.enqueueAndCheckpoint(
                "pipeline",
                "discovery",
                null,
                valid,
                listOf(item, discoveryItem("pipeline", "two"), discoveryItem("pipeline", "three")),
            )
        }
        assertFailsWith<IllegalArgumentException> {
            store.enqueueAndCheckpoint("pipeline", "discovery", null, checkpoint(1, byteArrayOf(1)), listOf(item))
        }
        assertFailsWith<IllegalArgumentException> {
            store.enqueueAndCheckpoint("pipeline", "discovery", null, checkpoint(0, byteArrayOf(1, 2)), listOf(item))
        }

        assertNull(store.load("pipeline", "discovery"))
        assertEquals(0, store.activeWorkCount("pipeline"))
        assertEquals(WorkQueueMetrics(0, null), store.metrics("pipeline"))
    }

    @Test
    fun `atomic admission capacity counts only new distinct resources`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val store = InMemoryCrawlStateStore(maxQueueSize = 3, clock = clock)
        val context = crawler("pipeline")
        completeWithoutAttempt(store, "x", clock.current)
        val activeA = assertIs<EnqueueResult.Accepted>(
            store.enqueue(CrawlRequest("a"), RequestIdentity("a"), context),
        )
        assertIs<EnqueueResult.Accepted>(store.enqueue(CrawlRequest("c"), RequestIdentity("c"), context))
        assertEquals(2, store.activeWorkCount("pipeline"))

        val advanced = assertIs<AtomicEnqueueCheckpointResult.Advanced>(
            store.enqueueAndCheckpoint(
                "pipeline",
                "discovery",
                null,
                checkpoint(0, byteArrayOf(1)),
                listOf(
                    discoveryItem("pipeline", "a"),
                    discoveryItem("pipeline", "b"),
                    discoveryItem("pipeline", "b"),
                ),
            ),
        )
        assertEquals(EnqueueResult.AlreadyActive(activeA.workItemId), advanced.enqueueResults[0])
        val acceptedB = assertIs<EnqueueResult.Accepted>(advanced.enqueueResults[1])
        assertEquals(EnqueueResult.AlreadyActive(acceptedB.workItemId), advanced.enqueueResults[2])
        assertEquals(3, store.activeWorkCount("pipeline"))
        assertEquals(0L, store.load("pipeline", "discovery")?.revision)

        assertEquals(
            AtomicEnqueueCheckpointResult.Rejected("In-memory active work capacity is full"),
            store.enqueueAndCheckpoint(
                "pipeline",
                "discovery",
                0,
                checkpoint(1, byteArrayOf(2)),
                listOf(discoveryItem("pipeline", "x")),
            ),
        )
        assertEquals(0L, store.load("pipeline", "discovery")?.revision)
        assertEquals(1L, store.history("pipeline", RequestIdentity("x")).visitCount)
        assertEquals(0, store.history("pipeline", RequestIdentity("x")).activeWorkCount)
    }

    @Test
    fun `lease fencing rejects unknown items wrong owners wrong pipelines and released work`() = runTest {
        val store = InMemoryCrawlStateStore(clock = MutableClock(Instant.EPOCH))
        store.enqueue(CrawlRequest("resource"), RequestIdentity("resource"), crawler("pipeline"))
        val lease = store.claim("pipeline", 1, "owner", Instant.EPOCH).single()

        assertEquals(LeaseMutationResult.Stale, store.checkCurrent(lease.copy(workItemId = WorkItemId("unknown"))))
        assertEquals(LeaseMutationResult.Stale, store.checkCurrent(lease.copy(ownerId = "other-owner")))
        assertEquals(LeaseMutationResult.Stale, store.checkCurrent(lease.copy(pipelineId = "other-pipeline")))
        assertEquals(LeaseMutationResult.Stale, store.transition(lease.copy(pipelineId = "other"), WorkTransition.Succeeded))
        assertEquals(LeaseMutationResult.Accepted, store.checkCurrent(lease.copy(pipelineId = "")))
        assertEquals(LeaseMutationResult.Accepted, store.checkCurrent(lease))

        assertEquals(LeaseMutationResult.Accepted, store.transition(lease, WorkTransition.Succeeded))
        assertEquals(LeaseMutationResult.Stale, store.checkCurrent(lease))
        assertEquals(LeaseMutationResult.Stale, store.renew(lease, Instant.EPOCH.plusSeconds(60)))
        assertEquals(LeaseMutationResult.Stale, store.transition(lease, WorkTransition.Failed("late")))
        assertEquals(AttemptStartResult.Stale, store.beginAttempt(lease, Instant.EPOCH))
        assertEquals(1L, store.history("pipeline", RequestIdentity("resource")).visitCount)
    }

    @Test
    fun `attempt finishing is fenced to the active attempt and cannot be repeated`() = runTest {
        val store = InMemoryCrawlStateStore(clock = MutableClock(Instant.EPOCH))
        store.enqueue(CrawlRequest("resource"), RequestIdentity("resource"), crawler("pipeline"))
        val lease = store.claim("pipeline", 1, "owner", Instant.EPOCH).single()
        val attempt = assertIs<AttemptStartResult.Started>(store.beginAttempt(lease, Instant.EPOCH)).attempt
        val forged = attempt.copy(id = AttemptId("forged-attempt"))

        assertEquals(LeaseMutationResult.Stale, store.finishAttempt(lease, forged, AttemptOutcome.SUCCEEDED))
        assertEquals(LeaseMutationResult.Accepted, store.finishAttempt(lease, attempt, AttemptOutcome.SUCCEEDED))
        assertEquals(LeaseMutationResult.Stale, store.finishAttempt(lease, attempt, AttemptOutcome.FAILED))

        val next = assertIs<AttemptStartResult.Started>(store.beginAttempt(lease, Instant.EPOCH)).attempt
        assertEquals(2, next.number)
    }

    @Test
    fun `an expired lease observed by checkCurrent is reclaimed for the next claim`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val store = InMemoryCrawlStateStore(leaseDuration = Duration.ofSeconds(5), clock = clock)
        store.enqueue(CrawlRequest("resource"), RequestIdentity("resource"), crawler("pipeline"))
        val lease = store.claim("pipeline", 1, "owner", clock.current).single()
        assertIs<AttemptStartResult.Started>(store.beginAttempt(lease, clock.current))

        clock.current = clock.current.plusSeconds(5)
        assertEquals(LeaseMutationResult.Stale, store.checkCurrent(lease))
        assertEquals(LeaseMutationResult.Stale, store.renew(lease, clock.current.plusSeconds(60)))
        val reclaimed = store.claim("pipeline", 1, "next-owner", clock.current).single()
        assertEquals(lease.workItemId, reclaimed.workItemId)
        assertEquals(LeaseMutationResult.Accepted, store.checkCurrent(reclaimed))
        val attempt = assertIs<AttemptStartResult.Started>(store.beginAttempt(reclaimed, clock.current)).attempt
        assertEquals(2, attempt.number)
    }

    @Test
    fun `unscoped claims reclaim expired leases across pipelines while scoped claims stay isolated`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val store = InMemoryCrawlStateStore(leaseDuration = Duration.ofSeconds(5), clock = clock)
        store.enqueue(CrawlRequest("p1"), RequestIdentity("p1"), crawler("p1"))
        store.enqueue(CrawlRequest("p2"), RequestIdentity("p2"), crawler("p2"))
        val p1Lease = store.claim("p1", 1, "owner", clock.current).single()

        val expiredAt = clock.current.plusSeconds(5)
        val p2Claims = store.claim("p2", 10, "owner", expiredAt)
        assertEquals(listOf("p2"), p2Claims.map { it.request.target })
        assertTrue(store.claim("p2", 10, "owner", expiredAt).isEmpty())

        val unscoped = store.claim(10, "other-owner", expiredAt).single()
        assertEquals(p1Lease.workItemId, unscoped.workItemId)
        clock.current = expiredAt
        assertEquals(LeaseMutationResult.Stale, store.checkCurrent(p1Lease))
        assertEquals(LeaseMutationResult.Accepted, store.checkCurrent(unscoped))
        assertEquals("p1", unscoped.pipelineId)
        assertEquals(LeaseMutationResult.Accepted, store.checkCurrent(p2Claims.single()))
    }

    @Test
    fun `unscoped claims ignore per-pipeline ordering while scoped claims apply it`() = runTest {
        val store = InMemoryCrawlStateStore(clock = MutableClock(Instant.EPOCH))
        store.setOrdering("pipeline", ReverseSequenceOrdering)
        listOf("first", "second", "third").forEach { target ->
            store.enqueue(CrawlRequest(target), RequestIdentity(target), crawler("pipeline"))
        }

        assertEquals(listOf("first"), store.claim(1, "owner", Instant.EPOCH).map { it.request.target })
        assertEquals(listOf("third", "second"), store.claim("pipeline", 2, "owner", Instant.EPOCH).map { it.request.target })
    }

    @Test
    fun `scoped operations reject blank pipeline identifiers`() = runTest {
        val store = InMemoryCrawlStateStore()
        assertFailsWith<IllegalArgumentException> { store.claim(" ", 1, "owner", Instant.EPOCH) }
        assertFailsWith<IllegalArgumentException> { store.cancelQueued(" ", WorkTransition.Cancelled("stop")) }
        assertFailsWith<IllegalArgumentException> { store.setOrdering(" ", ReverseSequenceOrdering) }
        assertFailsWith<IllegalArgumentException> { store.metrics(" ") }
    }

    @Test
    fun `history for an unknown resource is empty`() = runTest {
        val store = InMemoryCrawlStateStore()
        assertEquals(
            ResourceHistory(lastCompletedAt = null, activeWorkCount = 0, visitCount = 0, nextEligibleAt = null),
            store.history("pipeline", RequestIdentity("unknown")),
        )
    }

    @Test
    fun `idle and metrics observations are pipeline scoped and report the oldest open work`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val store = InMemoryCrawlStateStore(clock = clock)
        assertTrue(store.isIdle())
        store.enqueue(CrawlRequest("old"), RequestIdentity("old"), crawler("p1"))
        clock.current = Instant.EPOCH.plusSeconds(10)
        store.enqueue(CrawlRequest("new"), RequestIdentity("new"), crawler("p1"))

        assertFalse(store.isIdle())
        assertFalse(store.isIdle("p1"))
        assertTrue(store.isIdle("p2"))
        assertEquals(WorkQueueMetrics(2, Instant.EPOCH), store.metrics("p1"))
        assertEquals(WorkQueueMetrics(0, null), store.metrics("p2"))
    }

    @Test
    fun `queued cancellation transitions retry-waiting work and supports failed and skipped outcomes`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val store = InMemoryCrawlStateStore(clock = clock)
        val context = crawler("pipeline")
        store.enqueue(CrawlRequest("waiting"), RequestIdentity("waiting"), context)
        val lease = store.claim("pipeline", 1, "owner", clock.current).single()
        assertEquals(
            LeaseMutationResult.Accepted,
            store.transition(lease, WorkTransition.Retry(clock.current.plusSeconds(60))),
        )
        store.enqueue(CrawlRequest("queued"), RequestIdentity("queued"), context)
        store.enqueue(CrawlRequest("other"), RequestIdentity("other"), crawler("other-pipeline"))

        assertEquals(2, store.cancelQueued("pipeline", WorkTransition.Failed("shutdown failure")))
        assertTrue(store.isIdle("pipeline"))
        assertFalse(store.isIdle("other-pipeline"))
        assertEquals(0, store.history("pipeline", RequestIdentity("waiting")).activeWorkCount)
        assertEquals(0L, store.history("pipeline", RequestIdentity("waiting")).visitCount)
        assertTrue(store.claim("pipeline", 10, "owner", clock.current.plusSeconds(120)).isEmpty())

        store.enqueue(CrawlRequest("skipped"), RequestIdentity("skipped"), context)
        assertEquals(1, store.cancelQueued("pipeline", WorkTransition.Skipped("policy")))
        assertIs<EnqueueResult.Accepted>(store.enqueue(CrawlRequest("skipped"), RequestIdentity("skipped"), context))
        assertEquals(0, store.cancelQueued("absent-pipeline", WorkTransition.Abandoned("none")))
    }

    @Test
    fun `queued cancellation rejects success and retry transitions without mutating work`() = runTest {
        val store = InMemoryCrawlStateStore(clock = MutableClock(Instant.EPOCH))
        store.enqueue(CrawlRequest("queued"), RequestIdentity("queued"), crawler("pipeline"))

        assertFailsWith<IllegalStateException> { store.cancelQueued("pipeline", WorkTransition.Succeeded) }
        assertFailsWith<IllegalStateException> {
            store.cancelQueued("pipeline", WorkTransition.Retry(Instant.EPOCH))
        }

        assertEquals(1, store.activeWorkCount("pipeline"))
        assertEquals(0L, store.history("pipeline", RequestIdentity("queued")).visitCount)
        assertEquals(listOf("queued"), store.claim("pipeline", 1, "owner", Instant.EPOCH).map { it.request.target })
    }

    @Test
    fun `repeated retries advance resource eligibility and success clears it`() = runTest {
        val start = Instant.EPOCH
        val clock = MutableClock(start)
        val store = InMemoryCrawlStateStore(clock = clock)
        val identity = RequestIdentity("resource")
        store.enqueue(CrawlRequest("resource"), identity, crawler("pipeline"))

        val first = store.claim("pipeline", 1, "owner", start).single()
        store.transition(first, WorkTransition.Retry(start.plusSeconds(20)))
        assertEquals(start.plusSeconds(20), store.history("pipeline", identity).nextEligibleAt)

        clock.current = start.plusSeconds(20)
        val second = store.claim("pipeline", 1, "owner", clock.current).single()
        store.transition(second, WorkTransition.Retry(start.plusSeconds(25)))
        assertEquals(start.plusSeconds(25), store.history("pipeline", identity).nextEligibleAt)
        assertTrue(store.claim("pipeline", 1, "owner", start.plusSeconds(24)).isEmpty())

        clock.current = start.plusSeconds(25)
        val third = store.claim("pipeline", 1, "owner", clock.current).single()
        store.transition(third, WorkTransition.Succeeded)
        val history = store.history("pipeline", identity)
        assertNull(history.nextEligibleAt)
        assertEquals(start.plusSeconds(25), history.lastCompletedAt)
        assertEquals(1L, history.visitCount)
    }

    @Test
    fun `resource retention skips resources with active work and evicts the oldest terminal resource`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val store = InMemoryCrawlStateStore(
            maxQueueSize = 2,
            maxRetainedJobs = 2,
            maxRetainedResources = 2,
            clock = clock,
        )
        val context = crawler("pipeline")
        assertIs<EnqueueResult.Accepted>(store.enqueue(CrawlRequest("active"), RequestIdentity("active"), context))
        val activeLease = store.claim("pipeline", 1, "worker", clock.current).single()
        completeWithoutAttempt(store, "done", clock.current)
        assertEquals(1L, store.history("pipeline", RequestIdentity("done")).visitCount)

        assertIs<EnqueueResult.Accepted>(store.enqueue(CrawlRequest("new"), RequestIdentity("new"), context))

        assertEquals(0L, store.history("pipeline", RequestIdentity("done")).visitCount)
        assertEquals(1, store.history("pipeline", RequestIdentity("active")).activeWorkCount)
        assertEquals(1, store.history("pipeline", RequestIdentity("new")).activeWorkCount)
        assertIs<EnqueueResult.AlreadyActive>(store.enqueue(CrawlRequest("active"), RequestIdentity("active"), context))
        assertEquals(LeaseMutationResult.Accepted, store.checkCurrent(activeLease))
        assertEquals(
            listOf("new"),
            store.claim("pipeline", 10, "owner", clock.current).map { it.request.target },
        )
    }

    @Test
    fun `attempt retention evicts finished attempts but never the active attempt`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val store = InMemoryCrawlStateStore(maxQueueSize = 1, maxRetainedJobs = 1, maxRetainedResources = 1, clock = clock)
        store.enqueue(CrawlRequest("resource"), RequestIdentity("resource"), crawler("pipeline"))

        repeat(3) { index ->
            val lease = store.claim("pipeline", 1, "owner", clock.current).single()
            val attempt = assertIs<AttemptStartResult.Started>(store.beginAttempt(lease, clock.current)).attempt
            assertEquals(index + 1, attempt.number)
            assertEquals(LeaseMutationResult.Accepted, store.finishAttempt(lease, attempt, AttemptOutcome.FAILED))
            assertEquals(LeaseMutationResult.Accepted, store.transition(lease, WorkTransition.Retry(clock.current)))
        }

        val lease = store.claim("pipeline", 1, "owner", clock.current).single()
        val active = assertIs<AttemptStartResult.Started>(store.beginAttempt(lease, clock.current)).attempt
        assertEquals(4, active.number)
        assertEquals(LeaseMutationResult.Accepted, store.finishAttempt(lease, active, AttemptOutcome.SUCCEEDED))
        assertEquals(LeaseMutationResult.Accepted, store.transition(lease, WorkTransition.Succeeded))
        assertEquals(1L, store.history("pipeline", RequestIdentity("resource")).visitCount)
    }

    @Test
    fun `attempt retention skips an older active attempt of another job`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val store = InMemoryCrawlStateStore(maxQueueSize = 2, maxRetainedJobs = 2, maxRetainedResources = 2, clock = clock)
        store.enqueue(CrawlRequest("long"), RequestIdentity("long"), crawler("pipeline"))
        val longLease = store.claim("pipeline", 1, "owner", clock.current).single()
        val longAttempt = assertIs<AttemptStartResult.Started>(store.beginAttempt(longLease, clock.current)).attempt

        store.enqueue(CrawlRequest("retrying"), RequestIdentity("retrying"), crawler("pipeline"))
        repeat(3) {
            val lease = store.claim("pipeline", 1, "owner", clock.current).single()
            assertEquals("retrying", lease.request.target)
            val attempt = assertIs<AttemptStartResult.Started>(store.beginAttempt(lease, clock.current)).attempt
            assertEquals(LeaseMutationResult.Accepted, store.finishAttempt(lease, attempt, AttemptOutcome.FAILED))
            assertEquals(LeaseMutationResult.Accepted, store.transition(lease, WorkTransition.Retry(clock.current)))
        }

        assertEquals(LeaseMutationResult.Accepted, store.finishAttempt(longLease, longAttempt, AttemptOutcome.SUCCEEDED))
        assertEquals(LeaseMutationResult.Accepted, store.transition(longLease, WorkTransition.Succeeded))
    }

    @Test
    fun `job retention tolerates terminal jobs whose resource history was already evicted`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val store = InMemoryCrawlStateStore(maxQueueSize = 1, maxRetainedJobs = 2, maxRetainedResources = 1, clock = clock)
        completeWithoutAttempt(store, "a", clock.current)
        completeWithoutAttempt(store, "b", clock.current)
        assertEquals(0L, store.history("pipeline", RequestIdentity("a")).visitCount)

        completeWithoutAttempt(store, "c", clock.current)
        assertEquals(1L, store.history("pipeline", RequestIdentity("c")).visitCount)
        assertEquals(0L, store.history("pipeline", RequestIdentity("b")).visitCount)
        assertIs<EnqueueResult.Accepted>(store.enqueue(CrawlRequest("a"), RequestIdentity("a"), crawler("pipeline")))
    }

    @Test
    fun `active work count and metrics ignore terminal jobs and other pipelines`() = runTest {
        val clock = MutableClock(Instant.EPOCH.plusSeconds(100))
        val store = InMemoryCrawlStateStore(clock = clock)
        completeWithoutAttempt(store, "done", clock.current)
        store.enqueue(CrawlRequest("later"), RequestIdentity("later"), crawler("pipeline"))
        clock.current = Instant.EPOCH.plusSeconds(50)
        store.enqueue(CrawlRequest("earlier-clock"), RequestIdentity("earlier-clock"), crawler("pipeline"))
        store.enqueue(CrawlRequest("other"), RequestIdentity("other"), crawler("other"))

        assertEquals(2, store.activeWorkCount("pipeline"))
        assertEquals(1, store.activeWorkCount("other"))
        assertEquals(WorkQueueMetrics(2, Instant.EPOCH.plusSeconds(50)), store.metrics("pipeline"))
    }

    @Test
    fun `queued abandonment releases admission without recording a visit`() = runTest {
        val store = InMemoryCrawlStateStore(clock = MutableClock(Instant.EPOCH))
        store.enqueue(CrawlRequest("queued"), RequestIdentity("queued"), crawler("pipeline"))
        assertEquals(1, store.cancelQueued("pipeline", WorkTransition.Abandoned("budget")))
        assertEquals(0L, store.history("pipeline", RequestIdentity("queued")).visitCount)
        assertIs<EnqueueResult.Accepted>(store.enqueue(CrawlRequest("queued"), RequestIdentity("queued"), crawler("pipeline")))
    }

    @Test
    fun `the in-memory store accepts custom ordering policies`() {
        assertTrue(InMemoryCrawlStateStore().supports(ReverseSequenceOrdering))
    }

    @Test
    fun `checkpoint quarantine validates input and never replaces the live checkpoint`() = runTest {
        val store = InMemoryCrawlStateStore(maxRetainedDiscoveryState = 1, maxCheckpointPayloadBytes = 1)
        val live = checkpoint(0, byteArrayOf(1))
        assertTrue(store.compareAndSet("pipeline", "discovery", null, live))
        val incompatible = checkpoint(0, byteArrayOf(2))

        repeat(3) { store.quarantine("pipeline", "discovery", incompatible, "incompatible-$it") }
        assertEquals(live, store.load("pipeline", "discovery"))

        assertFailsWith<IllegalArgumentException> { store.quarantine(" ", "discovery", incompatible, "reason") }
        assertFailsWith<IllegalArgumentException> { store.quarantine("pipeline", " ", incompatible, "reason") }
        assertFailsWith<IllegalArgumentException> { store.quarantine("pipeline", "discovery", incompatible, " ") }
        assertFailsWith<IllegalArgumentException> {
            store.quarantine("pipeline", "discovery", checkpoint(0, byteArrayOf(1, 2)), "too large")
        }
        assertEquals(live, store.load("pipeline", "discovery"))
    }

    @Test
    fun `harvest quarantine and CAS validate bounds and never replace the live state`() = runTest {
        val store = InMemoryCrawlStateStore(maxRetainedDiscoveryState = 1, maxCheckpointPayloadBytes = 1)
        val live = harvest(0, byteArrayOf(1))
        assertTrue(store.compareAndSetHarvestState("pipeline", "strategy", null, live))

        repeat(3) { store.quarantineHarvestState("pipeline", "strategy", harvest(0, byteArrayOf(2)), "old-$it") }
        assertEquals(live, store.loadHarvestState("pipeline", "strategy"))

        val oversized = harvest(0, byteArrayOf(1, 2))
        assertFailsWith<IllegalArgumentException> { store.quarantineHarvestState(" ", "strategy", live, "reason") }
        assertFailsWith<IllegalArgumentException> { store.quarantineHarvestState("pipeline", " ", live, "reason") }
        assertFailsWith<IllegalArgumentException> { store.quarantineHarvestState("pipeline", "strategy", live, " ") }
        assertFailsWith<IllegalArgumentException> {
            store.quarantineHarvestState("pipeline", "strategy", oversized, "too large")
        }
        assertFailsWith<IllegalArgumentException> {
            store.compareAndSetHarvestState("pipeline", "strategy", 0, harvest(1, byteArrayOf(1, 2)))
        }
        assertEquals(live, store.loadHarvestState("pipeline", "strategy"))
        assertTrue(store.compareAndSetHarvestState("pipeline", "strategy", 0, harvest(1, byteArrayOf(3))))
    }

    @Test
    fun `pipeline health CAS rejects revision gaps without replacing the stored snapshot`() = runTest {
        val store = InMemoryCrawlStateStore()
        assertTrue(store.compareAndSetPipelineHealth("pipeline", null, PipelineHealthSnapshot()))
        assertFailsWith<IllegalArgumentException> {
            store.compareAndSetPipelineHealth("pipeline", 0, PipelineHealthSnapshot(revision = 2))
        }
        assertFailsWith<IllegalArgumentException> {
            store.compareAndSetPipelineHealth("fresh", null, PipelineHealthSnapshot(revision = 1))
        }
        assertEquals(PipelineHealthSnapshot(), store.loadPipelineHealth("pipeline"))
        assertNull(store.loadPipelineHealth("fresh"))
    }

    private suspend fun completeWithoutAttempt(store: InMemoryCrawlStateStore, target: String, now: Instant) {
        assertIs<EnqueueResult.Accepted>(store.enqueue(CrawlRequest(target), RequestIdentity(target), crawler("pipeline")))
        // Callers ensure no other work is queued, so the claim returns exactly this item.
        val lease = store.claim("pipeline", 10, "completer", now).single()
        assertEquals(target, lease.request.target)
        assertEquals(LeaseMutationResult.Accepted, store.transition(lease, WorkTransition.Succeeded))
    }

    private fun discoveryItem(pipelineId: String, target: String): DiscoveryWorkItem = DiscoveryWorkItem(
        request = CrawlRequest(target),
        identity = RequestIdentity(target),
        context = crawler(pipelineId),
    )

    private fun checkpoint(revision: Long, bytes: ByteArray): DiscoveryCheckpoint = DiscoveryCheckpoint(
        revision = revision,
        codecId = "codec",
        codecVersion = 1,
        payload = CheckpointPayload.from(bytes),
    )

    private fun harvest(revision: Long, bytes: ByteArray): HarvestState = HarvestState(
        revision = revision,
        strategyId = "strategy",
        codecId = "codec",
        codecVersion = 1,
        payload = CheckpointPayload.from(bytes),
    )

    private fun crawler(pipelineId: String): CrawlerContext =
        CrawlerContext("engine", "$pipelineId-run", pipelineId)

    private object ReverseSequenceOrdering : WorkOrderingPolicy {
        override val id: String = "reverse-sequence"

        override fun compare(left: WorkDescriptor, right: WorkDescriptor): Int =
            right.sequence.compareTo(left.sequence)
    }

    private class MutableClock(var current: Instant) : Clock {
        override fun now(): Instant = current
    }
}
