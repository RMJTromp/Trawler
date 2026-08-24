package com.rmjtromp.trawler.state

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.RequestIdentity
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class InMemoryStateTest {
    @Test
    fun `active work is deduplicated while completed resources can be recrawled`() = runTest {
        val now = Instant.parse("2026-01-01T00:00:00Z")
        val clock = MutableClock(now)
        val store = InMemoryCrawlStateStore(maxQueueSize = 3, clock = clock)
        val context = crawler("pipeline")
        val firstRequest = CrawlRequest("first")
        val secondRequest = CrawlRequest("second")
        val firstIdentity = RequestIdentity("first")
        val secondIdentity = RequestIdentity("second")

        assertIs<EnqueueResult.Accepted>(store.enqueue(firstRequest, firstIdentity, context))
        assertIs<EnqueueResult.Accepted>(store.enqueue(secondRequest, secondIdentity, context))
        val duplicate = store.enqueue(firstRequest, firstIdentity, context)
        assertIs<EnqueueResult.AlreadyActive>(duplicate)

        val claimed = store.claim("pipeline", 2, "worker", now)
        assertEquals(listOf("first", "second"), claimed.map { it.request.target })
        assertEquals(2, store.activeWorkCount("pipeline"))

        assertEquals(LeaseMutationResult.Accepted, store.transition(claimed[0], WorkTransition.Succeeded))
        val history = store.history("pipeline", firstIdentity)
        assertEquals(1L, history.visitCount)
        assertEquals(0, history.activeWorkCount)

        assertIs<EnqueueResult.Accepted>(store.enqueue(firstRequest, firstIdentity, context))
        assertIs<EnqueueResult.AlreadyActive>(store.enqueue(secondRequest, secondIdentity, context))
    }

    @Test
    fun `claims are FIFO and remain pipeline scoped`() = runTest {
        val store = InMemoryCrawlStateStore()
        val first = store.enqueue(CrawlRequest("p1-first"), RequestIdentity("p1-first"), crawler("p1"))
        val second = store.enqueue(CrawlRequest("p2"), RequestIdentity("p2"), crawler("p2"))
        val third = store.enqueue(CrawlRequest("p1-second"), RequestIdentity("p1-second"), crawler("p1"))

        assertIs<EnqueueResult.Accepted>(first)
        assertIs<EnqueueResult.Accepted>(second)
        assertIs<EnqueueResult.Accepted>(third)
        val p1Claims = store.claim("p1", 10, "worker", Instant.EPOCH)
        assertEquals(listOf("p1-first", "p1-second"), p1Claims.map { it.request.target })
        val remaining = store.claim(10, "other-worker", Instant.EPOCH)
        assertEquals(listOf("p2"), remaining.map { it.request.target })
    }

    @Test
    fun `expired leases become reclaimable without counting a lease as an attempt`() = runTest {
        val start = Instant.EPOCH
        val clock = MutableClock(start)
        val store = InMemoryCrawlStateStore(leaseDuration = Duration.ofSeconds(5), clock = clock)
        val context = crawler("pipeline")
        store.enqueue(CrawlRequest("resource"), RequestIdentity("resource"), context)
        val firstLease = store.claim("pipeline", 1, "worker-one", start).single()
        val firstAttempt = assertIs<AttemptStartResult.Started>(store.beginAttempt(firstLease, start)).attempt
        assertEquals(1, firstAttempt.number)

        clock.current = start.plusSeconds(5)
        assertEquals(
            LeaseMutationResult.Stale,
            store.transition(firstLease, WorkTransition.Succeeded),
        )
        assertEquals(
            LeaseMutationResult.Stale,
            store.finishAttempt(firstLease, firstAttempt, AttemptOutcome.SUCCEEDED),
        )

        val reclaimed = store.claim("pipeline", 1, "worker-two", clock.current).single()
        val secondAttempt = assertIs<AttemptStartResult.Started>(
            store.beginAttempt(reclaimed, clock.current),
        ).attempt
        assertEquals(2, secondAttempt.number)
        assertEquals(
            LeaseMutationResult.Accepted,
            store.finishAttempt(reclaimed, secondAttempt, AttemptOutcome.FAILED),
        )
        assertEquals(
            LeaseMutationResult.Accepted,
            store.transition(reclaimed, WorkTransition.Retry(start.plusSeconds(10))),
        )
        assertTrue(store.claim("pipeline", 1, "worker-three", clock.current).isEmpty())
        clock.current = start.plusSeconds(10)
        assertEquals(1, store.claim("pipeline", 1, "worker-three", clock.current).size)
    }

    @Test
    fun `lease ownership and renewal reject stale or non-future mutations`() = runTest {
        val now = Instant.EPOCH
        val clock = MutableClock(now)
        val store = InMemoryCrawlStateStore(clock = clock)
        val context = crawler("pipeline")
        store.enqueue(CrawlRequest("resource"), RequestIdentity("resource"), context)
        val lease = store.claim("pipeline", 1, "owner", now).single()

        assertEquals(
            LeaseMutationResult.Rejected("Lease expiry must be in the future"),
            store.renew(lease, now),
        )
        assertEquals(LeaseMutationResult.Accepted, store.renew(lease, now.plusSeconds(2)))
        val stale = lease.copy(leaseToken = LeaseToken("stale-token"))
        assertEquals(LeaseMutationResult.Stale, store.transition(stale, WorkTransition.Succeeded))
        assertEquals(LeaseMutationResult.Accepted, store.transition(lease, WorkTransition.Succeeded))
    }

    @Test
    fun `checkpoint and harvest state compare-and-set require the expected revision`() = runTest {
        val store = InMemoryCrawlStateStore(maxCheckpointPayloadBytes = 2)
        val checkpoint = DiscoveryCheckpoint(
            revision = 0,
            codecId = "codec",
            codecVersion = 1,
            payload = CheckpointPayload.from(byteArrayOf(1, 2)),
        )
        assertTrue(store.compareAndSet("pipeline", "discovery", null, checkpoint))
        assertEquals(checkpoint, store.load("pipeline", "discovery"))
        assertEquals(false, store.compareAndSet("pipeline", "discovery", null, checkpoint))
        assertTrue(store.compareAndSet("pipeline", "discovery", 0, checkpoint.copy(revision = 1)))
        assertFailsWith<IllegalArgumentException> {
            store.compareAndSet(
                "pipeline",
                "discovery",
                1,
                checkpoint.copy(revision = 2, payload = CheckpointPayload.from(byteArrayOf(1, 2, 3))),
            )
        }

        val harvest = HarvestState(
            revision = 0,
            strategyId = "strategy",
            codecId = "codec",
            codecVersion = 1,
            payload = CheckpointPayload.from(byteArrayOf(1)),
        )
        assertTrue(store.compareAndSetHarvestState("pipeline", "strategy", null, harvest))
        assertEquals(false, store.compareAndSetHarvestState("pipeline", "strategy", null, harvest))
        assertTrue(
            store.compareAndSetHarvestState("pipeline", "strategy", 0, harvest.copy(revision = 1)),
        )
    }

    @Test
    fun `queued cancellation releases resource admission and reports bounded counts`() = runTest {
        val store = InMemoryCrawlStateStore(maxQueueSize = 2)
        val context = crawler("pipeline")
        store.enqueue(CrawlRequest("one"), RequestIdentity("one"), context)
        store.enqueue(CrawlRequest("two"), RequestIdentity("two"), context)

        assertEquals(2, store.cancelQueued("pipeline", WorkTransition.Cancelled("shutdown")))
        assertTrue(store.isIdle("pipeline"))
        assertEquals(0, store.activeWorkCount("pipeline"))
        assertIs<EnqueueResult.Accepted>(store.enqueue(CrawlRequest("one"), RequestIdentity("one"), context))
    }

    @Test
    fun `state store constructor and operation bounds reject invalid values`() = runTest {
        assertFailsWith<IllegalArgumentException> { InMemoryCrawlStateStore(id = " ") }
        assertFailsWith<IllegalArgumentException> { InMemoryCrawlStateStore(maxQueueSize = 0) }
        assertFailsWith<IllegalArgumentException> {
            InMemoryCrawlStateStore(maxQueueSize = 2, maxRetainedJobs = 1)
        }
        assertFailsWith<IllegalArgumentException> {
            InMemoryCrawlStateStore(maxQueueSize = 2, maxRetainedResources = 1)
        }
        assertFailsWith<IllegalArgumentException> { InMemoryCrawlStateStore(maxRetainedDiscoveryState = 0) }
        assertFailsWith<IllegalArgumentException> { InMemoryCrawlStateStore(maxCheckpointPayloadBytes = 0) }
        assertFailsWith<IllegalArgumentException> { InMemoryCrawlStateStore(leaseDuration = Duration.ZERO) }

        val store = InMemoryCrawlStateStore()
        assertFailsWith<IllegalArgumentException> { store.claim(0, "owner", Instant.EPOCH) }
        assertFailsWith<IllegalArgumentException> { store.claim(1, " ", Instant.EPOCH) }
        assertFailsWith<IllegalArgumentException> { store.activeWorkCount(" ") }
    }

    private fun crawler(pipelineId: String): CrawlerContext =
        CrawlerContext("engine", "$pipelineId-run", pipelineId)

    private class MutableClock(var current: Instant) : Clock {
        override fun now(): Instant = current
    }
}
