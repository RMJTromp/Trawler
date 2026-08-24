package com.rmjtromp.trawler.state

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.RequestIdentity
import com.rmjtromp.trawler.policy.WorkDescriptor
import com.rmjtromp.trawler.policy.WorkOrderingPolicy
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class InMemoryStateLifecycleTest {
    @Test
    fun `complete lifecycle records retry eligibility and successful visit history`() = runTest {
        val start = Instant.parse("2026-01-01T00:00:00Z")
        val clock = MutableClock(start)
        val store = InMemoryCrawlStateStore(
            leaseDuration = Duration.ofSeconds(10),
            clock = clock,
        )
        val context = crawler("pipeline")
        val identity = RequestIdentity("resource")
        val request = CrawlRequest("resource")

        assertIs<EnqueueResult.Accepted>(store.enqueue(request, identity, context))
        assertEquals(WorkQueueMetrics(1, start), store.metrics("pipeline"))

        val firstLease = store.claim("pipeline", 1, "worker", start).single()
        val firstAttempt = assertIs<AttemptStartResult.Started>(
            store.beginAttempt(firstLease, start),
        ).attempt
        assertEquals(1, firstAttempt.number)
        assertIs<AttemptStartResult.Rejected>(store.beginAttempt(firstLease, start))
        assertEquals(
            LeaseMutationResult.Accepted,
            store.finishAttempt(firstLease, firstAttempt, AttemptOutcome.FAILED),
        )

        val retryAt = start.plusSeconds(20)
        assertEquals(
            LeaseMutationResult.Accepted,
            store.transition(firstLease, WorkTransition.Retry(retryAt)),
        )
        assertEquals(
            com.rmjtromp.trawler.policy.ResourceHistory(
                lastCompletedAt = null,
                nextEligibleAt = retryAt,
                activeWorkCount = 1,
                visitCount = 0,
            ),
            store.history("pipeline", identity),
        )
        assertTrue(store.claim("pipeline", 1, "worker-two", retryAt.minusNanos(1)).isEmpty())

        val retryLease = store.claim("pipeline", 1, "worker-two", retryAt).single()
        assertNotEquals(firstLease.leaseToken, retryLease.leaseToken)
        val secondAttempt = assertIs<AttemptStartResult.Started>(
            store.beginAttempt(retryLease, retryAt),
        ).attempt
        assertEquals(2, secondAttempt.number)
        assertEquals(
            LeaseMutationResult.Accepted,
            store.finishAttempt(retryLease, secondAttempt, AttemptOutcome.SUCCEEDED),
        )
        assertEquals(LeaseMutationResult.Accepted, store.transition(retryLease, WorkTransition.Succeeded))

        assertEquals(
            com.rmjtromp.trawler.policy.ResourceHistory(
                lastCompletedAt = start,
                nextEligibleAt = null,
                activeWorkCount = 0,
                visitCount = 1,
            ),
            store.history("pipeline", identity),
        )
        assertEquals(WorkQueueMetrics(0, null), store.metrics("pipeline"))
        assertTrue(store.isIdle("pipeline"))
    }

    @Test
    fun `failure skip cancellation and abandonment release resources for recrawl`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val store = InMemoryCrawlStateStore(clock = clock)
        val transitions = listOf(
            WorkTransition.Failed("permanent") to AttemptOutcome.FAILED,
            WorkTransition.Skipped("filtered") to AttemptOutcome.CANCELLED,
            WorkTransition.Cancelled("shutdown") to AttemptOutcome.CANCELLED,
            WorkTransition.Abandoned("owner-stopped") to AttemptOutcome.ABANDONED,
        )

        transitions.forEachIndexed { index, (transition, outcome) ->
            val identity = RequestIdentity("resource-$index")
            assertIs<EnqueueResult.Accepted>(
                store.enqueue(CrawlRequest("resource-$index"), identity, crawler("pipeline")),
            )
            val lease = store.claim("pipeline", 1, "worker-$index", Instant.EPOCH).single()
            val attempt = assertIs<AttemptStartResult.Started>(
                store.beginAttempt(lease, Instant.EPOCH),
            ).attempt
            assertEquals(LeaseMutationResult.Accepted, store.finishAttempt(lease, attempt, outcome))
            assertEquals(LeaseMutationResult.Accepted, store.transition(lease, transition))

            val history = store.history("pipeline", identity)
            assertEquals(0, history.activeWorkCount)
            assertEquals(0L, history.visitCount)
            assertNull(history.lastCompletedAt)
            assertIs<EnqueueResult.Accepted>(
                store.enqueue(CrawlRequest("resource-$index"), identity, crawler("pipeline")),
            )
            assertEquals(1, store.cancelQueued("pipeline", WorkTransition.Cancelled("cleanup")))
        }

        assertTrue(store.isIdle("pipeline"))
    }

    @Test
    fun `queued cancellation leaves a leased owner fenced and mutable`() = runTest {
        val now = Instant.EPOCH
        val clock = MutableClock(now)
        val store = InMemoryCrawlStateStore(clock = clock)
        val context = crawler("pipeline")
        val leasedIdentity = RequestIdentity("leased")
        val queuedIdentity = RequestIdentity("queued")
        store.enqueue(CrawlRequest("leased"), leasedIdentity, context)
        store.enqueue(CrawlRequest("queued"), queuedIdentity, context)
        val leased = store.claim("pipeline", 1, "owner", now).single()

        assertEquals(1, store.cancelQueued("pipeline", WorkTransition.Cancelled("shutdown")))
        assertEquals(1, store.activeWorkCount("pipeline"))
        assertEquals(LeaseMutationResult.Accepted, store.renew(leased, now.plusSeconds(5)))
        assertEquals(LeaseMutationResult.Accepted, store.transition(leased, WorkTransition.Succeeded))
        assertEquals(1L, store.history("pipeline", leasedIdentity).visitCount)
        assertEquals(0, store.history("pipeline", queuedIdentity).activeWorkCount)
        assertIs<EnqueueResult.Accepted>(
            store.enqueue(CrawlRequest("queued"), queuedIdentity, context),
        )
    }

    @Test
    fun `custom ordering stays pipeline scoped and does not change other queues`() = runTest {
        val store = InMemoryCrawlStateStore()
        store.setOrdering("priority", object : WorkOrderingPolicy {
            override val id: String = "reverse-sequence"

            override fun compare(left: WorkDescriptor, right: WorkDescriptor): Int =
                right.sequence.compareTo(left.sequence)
        })
        store.enqueue(CrawlRequest("priority-first"), RequestIdentity("priority-first"), crawler("priority"))
        store.enqueue(CrawlRequest("other"), RequestIdentity("other"), crawler("other"))
        store.enqueue(CrawlRequest("priority-second"), RequestIdentity("priority-second"), crawler("priority"))

        assertEquals(
            listOf("priority-second", "priority-first"),
            store.claim("priority", 10, "priority-worker", Instant.EPOCH).map { it.request.target },
        )
        assertEquals(
            listOf("other"),
            store.claim(10, "other-worker", Instant.EPOCH).map { it.request.target },
        )
    }

    @Test
    fun `capacity and retained resource bounds evict terminal history but not active work`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val store = InMemoryCrawlStateStore(
            maxQueueSize = 1,
            maxRetainedJobs = 1,
            maxRetainedResources = 1,
            clock = clock,
        )
        val context = crawler("pipeline")
        val first = RequestIdentity("first")
        val second = RequestIdentity("second")
        val third = RequestIdentity("third")

        assertIs<EnqueueResult.Accepted>(store.enqueue(CrawlRequest("first"), first, context))
        assertIs<EnqueueResult.Rejected>(store.enqueue(CrawlRequest("blocked"), RequestIdentity("blocked"), context))
        val firstLease = store.claim("pipeline", 1, "worker-first", clock.current).single()
        assertEquals(LeaseMutationResult.Accepted, store.transition(firstLease, WorkTransition.Succeeded))
        assertEquals(0, store.activeWorkCount("pipeline"))

        clock.current = clock.current.plusSeconds(1)
        completeWithoutAttempt(store, "pipeline", second, "second", clock.current)
        assertEquals(0L, store.history("pipeline", first).visitCount)
        assertEquals(1L, store.history("pipeline", second).visitCount)

        clock.current = clock.current.plusSeconds(1)
        completeWithoutAttempt(store, "pipeline", third, "third", clock.current)
        assertEquals(0L, store.history("pipeline", second).visitCount)
        assertEquals(1L, store.history("pipeline", third).visitCount)
        assertTrue(store.isIdle("pipeline"))
    }

    @Test
    fun `checkpoint harvest health and override state obey CAS revisions and retention bounds`() = runTest {
        val store = InMemoryCrawlStateStore(maxRetainedDiscoveryState = 1)
        val firstCheckpoint = checkpoint(0, byteArrayOf(1))
        val secondCheckpoint = checkpoint(0, byteArrayOf(2))
        assertTrue(store.compareAndSet("pipeline", "first", null, firstCheckpoint))
        assertTrue(store.compareAndSet("pipeline", "second", null, secondCheckpoint))
        assertNull(store.load("pipeline", "first"))
        assertEquals(secondCheckpoint, store.load("pipeline", "second"))
        assertTrue(store.compareAndSet("pipeline", "second", 0, checkpoint(1, byteArrayOf(3))))
        assertEquals(false, store.compareAndSet("pipeline", "second", 0, checkpoint(1, byteArrayOf(4))))
        assertFailsWith<IllegalArgumentException> {
            store.compareAndSet("pipeline", "second", 1, checkpoint(3, byteArrayOf(4)))
        }

        val firstHarvest = harvest(0, byteArrayOf(1))
        val secondHarvest = harvest(0, byteArrayOf(2))
        assertTrue(store.compareAndSetHarvestState("pipeline", "first", null, firstHarvest))
        assertTrue(store.compareAndSetHarvestState("pipeline", "second", null, secondHarvest))
        assertNull(store.loadHarvestState("pipeline", "first"))
        assertEquals(secondHarvest, store.loadHarvestState("pipeline", "second"))
        assertFailsWith<IllegalArgumentException> {
            store.compareAndSetHarvestState("pipeline", "second", 0, harvest(2, byteArrayOf(3)))
        }

        assertTrue(store.compareAndSetPipelineHealth("pipeline-first", null, PipelineHealthSnapshot()))
        assertTrue(store.compareAndSetPipelineHealth("pipeline-second", null, PipelineHealthSnapshot()))
        assertNull(store.loadPipelineHealth("pipeline-first"))
        assertEquals(PipelineHealthSnapshot(), store.loadPipelineHealth("pipeline-second"))
        val health = PipelineHealthSnapshot(
            revision = 1,
            consecutiveFailures = 2,
            totalSuccesses = 3,
            totalFailures = 4,
            lastObservedAt = Instant.EPOCH,
            circuitOpenUntil = Instant.EPOCH.plusSeconds(10),
        )
        assertTrue(store.compareAndSetPipelineHealth("pipeline-second", 0, health))
        assertEquals(false, store.compareAndSetPipelineHealth("pipeline-second", 0, health.copy(revision = 1)))

        val override = PipelineEligibilityOverride(false, "maintenance", Instant.EPOCH.plusSeconds(5))
        store.setEligibilityOverride("pipeline-first", override)
        store.setEligibilityOverride("pipeline-second", override)
        assertNull(store.loadEligibilityOverride("pipeline-first"))
        assertEquals(override, store.loadEligibilityOverride("pipeline-second"))
        store.setEligibilityOverride("pipeline-second", null)
        assertNull(store.loadEligibilityOverride("pipeline-second"))
        assertFailsWith<IllegalArgumentException> {
            store.setEligibilityOverride(" ", null)
        }
    }

    @Test
    fun `atomic admission commits accepted work and rolls back on capacity or revision conflict`() = runTest {
        val store = InMemoryCrawlStateStore(maxQueueSize = 3)
        val items = listOf(
            discoveryItem("pipeline", "one"),
            discoveryItem("pipeline", "two"),
        )
        val next = checkpoint(0, byteArrayOf(1))
        val advanced = store.enqueueAndCheckpoint("pipeline", "discovery", null, next, items)
        val committed = assertIs<AtomicEnqueueCheckpointResult.Advanced>(advanced)
        assertEquals(2, committed.enqueueResults.count { it is EnqueueResult.Accepted })
        assertEquals(next, store.load("pipeline", "discovery"))
        assertEquals(2, store.activeWorkCount("pipeline"))

        val conflicting = store.enqueueAndCheckpoint(
            pipelineId = "pipeline",
            discoveryId = "discovery",
            expectedRevision = null,
            next = checkpoint(0, byteArrayOf(2)),
            items = listOf(discoveryItem("pipeline", "three")),
        )
        assertEquals(AtomicEnqueueCheckpointResult.RevisionConflict, conflicting)
        assertEquals(2, store.activeWorkCount("pipeline"))
        assertEquals(0, store.history("pipeline", RequestIdentity("three")).activeWorkCount)

        val capacityStore = InMemoryCrawlStateStore(maxQueueSize = 2)
        capacityStore.enqueue(CrawlRequest("existing"), RequestIdentity("existing"), crawler("pipeline"))
        val rejected = capacityStore.enqueueAndCheckpoint(
            pipelineId = "pipeline",
            discoveryId = "capacity",
            expectedRevision = null,
            next = checkpoint(0, byteArrayOf(3)),
            items = listOf(
                discoveryItem("pipeline", "new-one"),
                discoveryItem("pipeline", "new-two"),
            ),
        )
        assertIs<AtomicEnqueueCheckpointResult.Rejected>(rejected)
        assertNull(capacityStore.load("pipeline", "capacity"))
        assertEquals(1, capacityStore.activeWorkCount("pipeline"))
        assertEquals(0, capacityStore.history("pipeline", RequestIdentity("new-one")).activeWorkCount)
        assertEquals(0, capacityStore.history("pipeline", RequestIdentity("new-two")).activeWorkCount)

        assertFailsWith<IllegalArgumentException> {
            capacityStore.enqueueAndCheckpoint(
                "pipeline",
                "empty",
                null,
                checkpoint(0, byteArrayOf(4)),
                emptyList(),
            )
        }
        assertFailsWith<IllegalArgumentException> {
            capacityStore.enqueueAndCheckpoint(
                "pipeline",
                "too-many",
                null,
                checkpoint(0, byteArrayOf(5)),
                listOf(
                    discoveryItem("pipeline", "one"),
                    discoveryItem("pipeline", "two"),
                    discoveryItem("pipeline", "three"),
                ),
            )
        }
    }

    @Test
    fun `atomic admission rejects a cross-pipeline item before changing queue or checkpoint`() = runTest {
        val store = InMemoryCrawlStateStore()
        val error = assertFailsWith<IllegalArgumentException> {
            store.enqueueAndCheckpoint(
                pipelineId = "pipeline-a",
                discoveryId = "discovery",
                expectedRevision = null,
                next = checkpoint(0, byteArrayOf(1)),
                items = listOf(discoveryItem("pipeline-b", "wrong-pipeline")),
            )
        }
        assertEquals(
            "Atomic discovery admission item pipeline must match the operation pipeline",
            error.message,
        )
        assertNull(store.load("pipeline-a", "discovery"))
        assertEquals(0, store.activeWorkCount("pipeline-a"))
        assertEquals(0, store.activeWorkCount("pipeline-b"))
    }

    @Test
    fun `expired active attempts are abandoned and stale writers cannot mutate the fresh lease`() = runTest {
        val start = Instant.EPOCH
        val clock = MutableClock(start)
        val store = InMemoryCrawlStateStore(
            leaseDuration = Duration.ofSeconds(5),
            clock = clock,
        )
        val identity = RequestIdentity("resource")
        store.enqueue(CrawlRequest("resource"), identity, crawler("pipeline"))
        val oldLease = store.claim("pipeline", 1, "old-owner", start).single()
        val oldAttempt = assertIs<AttemptStartResult.Started>(store.beginAttempt(oldLease, start)).attempt

        clock.current = start.plusSeconds(5)
        val freshLease = store.claim("pipeline", 1, "new-owner", clock.current).single()
        assertNotEquals(oldLease.leaseToken, freshLease.leaseToken)
        assertEquals(AttemptStartResult.Stale, store.beginAttempt(oldLease, clock.current))
        assertEquals(LeaseMutationResult.Stale, store.renew(oldLease, clock.current.plusSeconds(1)))
        assertEquals(LeaseMutationResult.Stale, store.finishAttempt(oldLease, oldAttempt, AttemptOutcome.SUCCEEDED))
        assertEquals(LeaseMutationResult.Stale, store.transition(oldLease, WorkTransition.Succeeded))

        val freshAttempt = assertIs<AttemptStartResult.Started>(
            store.beginAttempt(freshLease, clock.current),
        ).attempt
        assertEquals(2, freshAttempt.number)
        assertEquals(LeaseMutationResult.Accepted, store.finishAttempt(freshLease, freshAttempt, AttemptOutcome.SUCCEEDED))
        assertEquals(LeaseMutationResult.Accepted, store.transition(freshLease, WorkTransition.Succeeded))
        assertEquals(1L, store.history("pipeline", identity).visitCount)
    }

    @Test
    fun `concurrent enqueue claim and CAS races are linearizable and bounded`() = runTest {
        val store = InMemoryCrawlStateStore(maxQueueSize = 32)
        val context = crawler("pipeline")
        val duplicateResults = coroutineScope {
            (1..16).map {
                async(start = CoroutineStart.UNDISPATCHED) {
                    store.enqueue(CrawlRequest("same"), RequestIdentity("same"), context)
                }
            }.awaitAll()
        }
        assertEquals(1, duplicateResults.count { it is EnqueueResult.Accepted })
        assertEquals(15, duplicateResults.count { it is EnqueueResult.AlreadyActive })

        (1..6).forEach { index ->
            store.enqueue(CrawlRequest("item-$index"), RequestIdentity("item-$index"), context)
        }
        val claims = coroutineScope {
            listOf(
                async(start = CoroutineStart.UNDISPATCHED) {
                    store.claim("pipeline", 3, "worker-one", Instant.EPOCH)
                },
                async(start = CoroutineStart.UNDISPATCHED) {
                    store.claim("pipeline", 3, "worker-two", Instant.EPOCH)
                },
            ).awaitAll().flatten()
        }
        assertEquals(6, claims.size)
        assertEquals(6, claims.map { it.workItemId }.distinct().size)

        val checkpointStore = InMemoryCrawlStateStore()
        val checkpoint = checkpoint(0, byteArrayOf(1))
        val checkpointResults = coroutineScope {
            listOf(
                async(start = CoroutineStart.UNDISPATCHED) {
                    checkpointStore.compareAndSet("pipeline", "discovery", null, checkpoint)
                },
                async(start = CoroutineStart.UNDISPATCHED) {
                    checkpointStore.compareAndSet("pipeline", "discovery", null, checkpoint)
                },
            ).awaitAll()
        }
        assertEquals(1, checkpointResults.count { it })
        assertEquals(checkpoint, checkpointStore.load("pipeline", "discovery"))

        val atomicStore = InMemoryCrawlStateStore(maxQueueSize = 2)
        val atomicResults = coroutineScope {
            listOf(
                async(start = CoroutineStart.UNDISPATCHED) {
                    atomicStore.enqueueAndCheckpoint(
                        "pipeline",
                        "discovery",
                        null,
                        checkpoint(0, byteArrayOf(1)),
                        listOf(discoveryItem("pipeline", "one")),
                    )
                },
                async(start = CoroutineStart.UNDISPATCHED) {
                    atomicStore.enqueueAndCheckpoint(
                        "pipeline",
                        "discovery",
                        null,
                        checkpoint(0, byteArrayOf(2)),
                        listOf(discoveryItem("pipeline", "two")),
                    )
                },
            ).awaitAll()
        }
        assertEquals(1, atomicResults.count { it is AtomicEnqueueCheckpointResult.Advanced })
        assertEquals(1, atomicResults.count { it == AtomicEnqueueCheckpointResult.RevisionConflict })
        assertEquals(1, atomicStore.activeWorkCount("pipeline"))
    }

    private suspend fun completeWithoutAttempt(
        store: InMemoryCrawlStateStore,
        pipelineId: String,
        identity: RequestIdentity,
        target: String,
        now: Instant,
    ) {
        assertIs<EnqueueResult.Accepted>(store.enqueue(CrawlRequest(target), identity, crawler(pipelineId)))
        val lease = store.claim(pipelineId, 1, "worker-$target", now).single()
        assertEquals(LeaseMutationResult.Accepted, store.transition(lease, WorkTransition.Succeeded))
    }

    private fun discoveryItem(pipelineId: String, target: String): DiscoveryWorkItem = DiscoveryWorkItem(
        request = CrawlRequest(target),
        identity = RequestIdentity(target),
        context = crawler(pipelineId),
    )

    private fun checkpoint(revision: Long, bytes: ByteArray): DiscoveryCheckpoint = DiscoveryCheckpoint(
        revision = revision,
        codecId = "checkpoint-codec",
        codecVersion = 1,
        payload = CheckpointPayload.from(bytes),
    )

    private fun harvest(revision: Long, bytes: ByteArray): HarvestState = HarvestState(
        revision = revision,
        strategyId = "strategy",
        codecId = "harvest-codec",
        codecVersion = 1,
        payload = CheckpointPayload.from(bytes),
    )

    private fun crawler(pipelineId: String): CrawlerContext =
        CrawlerContext("engine", "$pipelineId-run", pipelineId)

    private class MutableClock(var current: Instant) : Clock {
        override fun now(): Instant = current
    }
}
