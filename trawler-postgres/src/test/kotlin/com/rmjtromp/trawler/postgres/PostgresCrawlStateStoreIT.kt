package com.rmjtromp.trawler.postgres

import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.extension.ExtensionStopReason
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.FifoWorkOrderingPolicy
import com.rmjtromp.trawler.policy.RequestIdentity
import com.rmjtromp.trawler.policy.WorkDescriptor
import com.rmjtromp.trawler.policy.WorkOrderingPolicy
import com.rmjtromp.trawler.retention.RetentionCategory
import com.rmjtromp.trawler.state.AtomicEnqueueCheckpointResult
import com.rmjtromp.trawler.state.AttemptOutcome
import com.rmjtromp.trawler.state.AttemptStartResult
import com.rmjtromp.trawler.state.CheckpointPayload
import com.rmjtromp.trawler.state.CrawlStateRetentionRequest
import com.rmjtromp.trawler.state.DiscoveryCheckpoint
import com.rmjtromp.trawler.state.DiscoveryWorkItem
import com.rmjtromp.trawler.state.DurableRunStatus
import com.rmjtromp.trawler.state.EnqueueResult
import com.rmjtromp.trawler.state.ExecutionAttempt
import com.rmjtromp.trawler.state.HarvestState
import com.rmjtromp.trawler.state.LeaseMutationResult
import com.rmjtromp.trawler.state.PipelineEligibilityOverride
import com.rmjtromp.trawler.state.PipelineHealthSnapshot
import com.rmjtromp.trawler.state.WorkItemId
import com.rmjtromp.trawler.state.WorkQueueMetrics
import com.rmjtromp.trawler.state.WorkTransition
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * SQL-semantic coverage for the durable state adapter.
 *
 * Each test owns a fresh PostgreSQL schema. The only shared state is the
 * container itself; two stores deliberately share a schema only in tests
 * whose contract is multi-process coordination.
 */
@Testcontainers
class PostgresCrawlStateStoreIT {
    @Test
    fun `startup migrates state tables into an independent history and lifecycle is terminal`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema)

        assertFailsWith<IllegalStateException> { store.activeWorkCount("pipeline") }
        assertTrue(store.supports(FifoWorkOrderingPolicy))
        val unsupportedOrdering = object : WorkOrderingPolicy {
            override val id: String = "priority"
            override fun compare(left: WorkDescriptor, right: WorkDescriptor): Int = left.priority.compareTo(right.priority)
        }
        assertFalse(store.supports(unsupportedOrdering))
        assertFailsWith<IllegalArgumentException> { runBlocking { store.setOrdering("pipeline", unsupportedOrdering) } }
        store.start(startContext())
        store.start(startContext())

        assertEquals(2, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"flyway_schema_history\""))
        assertEquals(
            2,
            scalarInt(
                schema,
                "SELECT COUNT(*) FROM \"$schema\".\"flyway_schema_history\" " +
                    "WHERE version IN ('1', '2')",
            ),
        )
        assertTrue(tableExists(schema, "jobs"))
        assertTrue(tableExists(schema, "discovery_checkpoints"))
        assertFalse(tableExists(schema, "responses"))
        assertFalse(tableExists(schema, "output_history"))
        assertFalse(tableExists(schema, "outbox"))

        store.stop(stopContext())
        store.stop(stopContext())
        assertFailsWith<IllegalStateException> { store.metrics("pipeline") }
        assertFailsWith<IllegalStateException> { store.start(startContext()) }
    }

    @Test
    fun `failed migration closes resources and the same store can retry after the fault is removed`(): Unit = runBlocking {
        val schema = nextSchema()
        withConnection { connection ->
            connection.createStatement().use { it.execute("CREATE SCHEMA \"$schema\"") }
            connection.createStatement().use {
                it.execute("CREATE TABLE \"$schema\".\"flyway_schema_history\" (bad INTEGER)")
            }
        }
        val store = store(schema)

        assertFailsWith<Throwable> { store.start(startContext()) }
        withConnection { connection ->
            connection.createStatement().use {
                it.execute("DROP TABLE \"$schema\".\"flyway_schema_history\"")
            }
        }

        store.start(startContext())
        assertTrue(tableExists(schema, "jobs"))
        store.stop(stopContext())
    }

    @Test
    fun `concurrent enqueue has one active identity and capacity is transactional`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema, maximumPoolSize = 8, databaseDispatcherParallelism = 8)
        store.start(startContext())
        try {
            val sameIdentity = concurrently(8) { index ->
                store.enqueue(request("same-$index"), RequestIdentity("same"), context("pipeline"))
            }
            assertEquals(1, sameIdentity.count { it is EnqueueResult.Accepted })
            assertEquals(7, sameIdentity.count { it is EnqueueResult.AlreadyActive })
            assertEquals(1, store.activeWorkCount("pipeline"))

            val capacitySchema = nextSchema()
            val capacityStore = store(
                capacitySchema,
                maximumPoolSize = 8,
                databaseDispatcherParallelism = 8,
                maxActiveJobs = 1,
                maxClaimBatch = 1,
            )
            capacityStore.start(startContext())
            try {
                val distinct = concurrently(2) { index ->
                    capacityStore.enqueue(
                        request("capacity-$index"),
                        RequestIdentity("capacity-$index"),
                        context("pipeline"),
                    )
                }
                assertEquals(1, distinct.count { it is EnqueueResult.Accepted })
                assertEquals(1, distinct.count { it is EnqueueResult.Rejected })
                assertEquals(1, capacityStore.activeWorkCount("pipeline"))
            } finally {
                capacityStore.stop(stopContext())
            }
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `claims are FIFO and pipeline scoped while concurrent claimers partition rows`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema, maximumPoolSize = 8, databaseDispatcherParallelism = 8, maxClaimBatch = 4)
        store.start(startContext())
        try {
            val p1 = listOf("p1-first", "p1-second").map { identity ->
                accepted(store.enqueue(request(identity), RequestIdentity(identity), context("p1")))
            }
            val p2 = accepted(store.enqueue(request("p2-only"), RequestIdentity("p2-only"), context("p2")))

            val p1Claims = store.claim("p1", 2, "owner-p1", NOW)
            assertEquals(p1, p1Claims.map { it.workItemId })
            assertTrue(p1Claims.all { it.pipelineId == "p1" })
            assertEquals(p2, store.claim(1, "owner-global", NOW).single().workItemId)

            assertFailsWith<IllegalArgumentException> { store.claim(0, "owner", NOW) }
            assertFailsWith<IllegalArgumentException> { store.claim(5, "owner", NOW) }
            assertFailsWith<IllegalArgumentException> { store.claim(1, " ", NOW) }

            val concurrentIds = (1..4).map { index ->
                accepted(store.enqueue(request("partition-$index"), RequestIdentity("partition-$index"), context("p3")))
            }
            val otherStore = store(schema, maximumPoolSize = 4, databaseDispatcherParallelism = 4, maxClaimBatch = 2)
            otherStore.start(startContext())
            try {
                val claims = coroutineScope {
                    listOf(
                        async(Dispatchers.Default) { otherStore.claim("p3", 2, "owner-a", NOW) },
                        async(Dispatchers.Default) { store.claim("p3", 2, "owner-b", NOW) },
                    ).awaitAll().flatten()
                }
                assertEquals(concurrentIds.toSet(), claims.map { it.workItemId }.toSet())
                assertEquals(concurrentIds.size, claims.map { it.workItemId }.distinct().size)
            } finally {
                otherStore.stop(stopContext())
            }
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `retry eligibility is exclusive before not-before and available at the exact instant`(): Unit = runBlocking {
        val clock = MutableClock(NOW)
        val schema = nextSchema()
        val store = store(schema, clock = clock)
        store.start(startContext())
        try {
            val id = accepted(store.enqueue(request("retry"), RequestIdentity("retry"), context("p")))
            val first = store.claim(1, "owner", NOW).single()
            val retryAt = NOW.plusSeconds(10)
            assertEquals(LeaseMutationResult.Accepted, store.transition(first, WorkTransition.Retry(retryAt)))
            assertEquals(0, store.claim(1, "owner", NOW.plusSeconds(9)).size)
            val second = store.claim(1, "owner", retryAt).single()
            assertEquals(id, second.workItemId)
            assertNotEquals(first.leaseToken, second.leaseToken)
            assertEquals(retryAt, store.history("p", RequestIdentity("retry")).nextEligibleAt)
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `claim does not increment attempts and expiry creates a fresh fenced lease`(): Unit = runBlocking {
        val schema = nextSchema()
        val clock = MutableClock(NOW)
        val firstStore = store(schema, clock = clock)
        val secondStore = store(schema, clock = clock)
        firstStore.start(startContext())
        secondStore.start(startContext())
        try {
            val id = accepted(firstStore.enqueue(request("lease"), RequestIdentity("lease"), context("p")))
            val first = firstStore.claim(1, "owner-a", NOW).single()
            assertEquals(0, jobAttemptCount(schema, id))
            assertEquals(LeaseMutationResult.Accepted, firstStore.renew(first, first.expiresAt.plusSeconds(1)))
            assertEquals(LeaseMutationResult.Rejected("Lease expiry must be in the future"), firstStore.renew(first, NOW))

            val reclaimed = secondStore.claim(1, "owner-b", first.expiresAt.plusSeconds(1)).single()
            assertEquals(id, reclaimed.workItemId)
            assertNotEquals(first.leaseToken, reclaimed.leaseToken)
            assertEquals(0, jobAttemptCount(schema, id))
            assertEquals(LeaseMutationResult.Stale, firstStore.renew(first, reclaimed.expiresAt))
            assertEquals(AttemptStartResult.Stale, firstStore.beginAttempt(first, NOW))
            assertEquals(LeaseMutationResult.Stale, firstStore.transition(first, WorkTransition.Succeeded))
        } finally {
            firstStore.stop(stopContext())
            secondStore.stop(stopContext())
        }
    }

    @Test
    fun `expired active attempt is abandoned and attempt numbering resumes after reclaim`(): Unit = runBlocking {
        val schema = nextSchema()
        val clock = MutableClock(NOW)
        val firstStore = store(schema, clock = clock)
        val secondStore = store(schema, clock = clock)
        firstStore.start(startContext())
        secondStore.start(startContext())
        try {
            val id = accepted(firstStore.enqueue(request("attempt"), RequestIdentity("attempt"), context("p")))
            val first = firstStore.claim(1, "owner-a", NOW).single()
            val attempt = assertIs<AttemptStartResult.Started>(firstStore.beginAttempt(first, NOW)).attempt
            assertEquals(1, attempt.number)
            assertEquals(AttemptStartResult.Stale, firstStore.beginAttempt(first, NOW))
            val wrongAttempt = ExecutionAttempt(
                id = com.rmjtromp.trawler.state.AttemptId("not-the-active-attempt"),
                number = 1,
                startedAt = NOW,
            )
            assertEquals(
                LeaseMutationResult.Stale,
                firstStore.finishAttempt(first, wrongAttempt, AttemptOutcome.FAILED),
            )

            val reclaimed = secondStore.claim(1, "owner-b", first.expiresAt).single()
            assertEquals("ABANDONED", jobAttemptOutcome(schema, id, attempt.id.value))
            assertEquals(
                LeaseMutationResult.Stale,
                firstStore.finishAttempt(first, attempt, AttemptOutcome.FAILED),
            )
            val secondAttempt = assertIs<AttemptStartResult.Started>(
                secondStore.beginAttempt(reclaimed, first.expiresAt.plusMillis(1)),
            ).attempt
            assertEquals(2, secondAttempt.number)
            assertEquals(
                LeaseMutationResult.Accepted,
                secondStore.finishAttempt(reclaimed, secondAttempt, AttemptOutcome.SUCCEEDED),
            )
        } finally {
            firstStore.stop(stopContext())
            secondStore.stop(stopContext())
        }
    }

    @Test
    fun `attempt insert rollback restores the lease row and active attempt fields`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema)
        store.start(startContext())
        try {
            val id = accepted(store.enqueue(request("rollback"), RequestIdentity("rollback"), context("p")))
            val work = store.claim(1, "owner", NOW).single()
            installAttemptFailureTrigger(schema)
            assertFailsWith<Throwable> { store.beginAttempt(work, NOW) }
            assertEquals(0, jobAttemptCount(schema, id))
            assertNull(jobActiveAttempt(schema, id))
            dropAttemptFailureTrigger(schema)

            val started = assertIs<AttemptStartResult.Started>(store.beginAttempt(work, NOW)).attempt
            assertEquals(1, started.number)
        } finally {
            dropAttemptFailureTrigger(schema)
            store.stop(stopContext())
        }
    }

    @Test
    fun `all fenced terminal transitions record the matching attempt outcome and resource truth`(): Unit = runBlocking {
        val schema = nextSchema()
        val clock = MutableClock(NOW)
        val store = store(schema, clock = clock)
        store.start(startContext())
        try {
            val cases = listOf(
                WorkTransition.Succeeded to AttemptOutcome.SUCCEEDED,
                WorkTransition.Retry(NOW.plusSeconds(20)) to AttemptOutcome.ABANDONED,
                WorkTransition.Failed("failed") to AttemptOutcome.FAILED,
                WorkTransition.Skipped("skipped") to AttemptOutcome.ABANDONED,
                WorkTransition.Cancelled("cancelled") to AttemptOutcome.CANCELLED,
                WorkTransition.Abandoned("abandoned") to AttemptOutcome.ABANDONED,
            )
            cases.forEachIndexed { index, (transition, outcome) ->
                val identity = RequestIdentity("transition-$index")
                val id = accepted(store.enqueue(request(identity.value), identity, context("p")))
                val work = store.claim(1, "owner-$index", NOW).single()
                val attempt = assertIs<AttemptStartResult.Started>(store.beginAttempt(work, NOW)).attempt
                assertEquals(LeaseMutationResult.Accepted, store.transition(work, transition))
                assertEquals(outcome.name, jobAttemptOutcome(schema, id, attempt.id.value))
                assertEquals(
                    if (transition is WorkTransition.Retry) 1 else 0,
                    store.history("p", identity).activeWorkCount,
                )
            }

            val successHistory = store.history("p", RequestIdentity("transition-0"))
            assertEquals(1, successHistory.visitCount)
            assertEquals(NOW, successHistory.lastCompletedAt)
            assertNull(successHistory.nextEligibleAt)

            val retryHistory = store.history("p", RequestIdentity("transition-1"))
            assertEquals(NOW.plusSeconds(20), retryHistory.nextEligibleAt)
            assertEquals(0, retryHistory.visitCount)

            val recrawlIdentity = RequestIdentity("recrawl")
            val firstId = accepted(store.enqueue(request("recrawl-1"), recrawlIdentity, context("p")))
            val firstWork = store.claim(1, "recrawl-owner", NOW).single()
            assertEquals(LeaseMutationResult.Accepted, store.transition(firstWork, WorkTransition.Succeeded))
            val secondId = accepted(store.enqueue(request("recrawl-2"), recrawlIdentity, context("p")))
            assertNotEquals(firstId, secondId)
            assertEquals(1, store.history("p", recrawlIdentity).visitCount)
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `queued cancellation is bounded and never mutates leased work`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema, maxCancellationRows = 1)
        store.start(startContext())
        try {
            val leasedId = accepted(store.enqueue(request("leased"), RequestIdentity("leased"), context("p")))
            val leased = store.claim(1, "lease-owner", NOW).single()
            val queuedIds = (1..3).map { index ->
                accepted(store.enqueue(request("queued-$index"), RequestIdentity("queued-$index"), context("p")))
            }
            assertFailsWith<IllegalArgumentException> {
                store.cancelQueued("p", WorkTransition.Retry(NOW.plusSeconds(1)))
            }
            assertEquals(3, store.cancelQueued("p", WorkTransition.Cancelled("shutdown")))
            assertEquals("LEASED", jobStatus(schema, leasedId))
            assertEquals("lease-owner", jobLeaseOwner(schema, leasedId))
            assertEquals(queuedIds.toSet(), terminalJobIds(schema, "CANCELLED"))
            assertEquals(1, store.activeWorkCount("p"))
            assertEquals(LeaseMutationResult.Accepted, store.transition(leased, WorkTransition.Succeeded))
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `metrics report open count and oldest enqueue while terminal work disappears`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema)
        store.start(startContext())
        try {
            assertEquals(WorkQueueMetrics(0, null), store.metrics("p"))
            store.enqueue(request("metric-1"), RequestIdentity("metric-1"), context("p"))
            store.enqueue(request("metric-2"), RequestIdentity("metric-2"), context("p"))
            val metrics = store.metrics("p")
            assertEquals(2, metrics.openWorkCount)
            assertEquals(NOW, metrics.oldestOpenWorkAt)
            val leased = store.claim(1, "owner", NOW).single()
            assertEquals(2, store.metrics("p").openWorkCount)
            store.transition(leased, WorkTransition.Succeeded)
            assertEquals(1, store.metrics("p").openWorkCount)
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `checkpoint harvest health CAS quarantine and overrides preserve revisions`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema, maxCheckpointPayloadBytes = 2)
        store.start(startContext())
        try {
            val checkpoint0 = checkpoint(0, byteArrayOf(1, 2))
            assertNull(store.load("p", "d"))
            assertTrue(store.compareAndSet("p", "d", null, checkpoint0))
            assertEquals(checkpoint0, store.load("p", "d"))
            assertFalse(store.compareAndSet("p", "d", null, checkpoint(0, byteArrayOf(1))))
            assertFailsWith<IllegalArgumentException> {
                store.compareAndSet("p", "d", 0, checkpoint(2, byteArrayOf(1)))
            }
            assertFailsWith<IllegalArgumentException> {
                store.compareAndSet("p", "d", 0, checkpoint(1, byteArrayOf(1, 2, 3)))
            }
            store.quarantine("p", "d", checkpoint(1, byteArrayOf(1)), "old codec")
            assertEquals(1, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"discovery_checkpoint_quarantine\""))

            val harvest0 = HarvestState(0, "strategy", "harvest-codec", 1, CheckpointPayload.from(byteArrayOf(4)))
            assertTrue(store.compareAndSetHarvestState("p", "strategy", null, harvest0))
            assertEquals(harvest0, store.loadHarvestState("p", "strategy"))
            assertFalse(store.compareAndSetHarvestState("p", "strategy", null, harvest0))
            assertFailsWith<IllegalArgumentException> {
                store.compareAndSetHarvestState(
                    "p",
                    "strategy",
                    0,
                    harvest0.copy(revision = 2),
                )
            }
            store.quarantineHarvestState("p", "strategy", harvest0, "legacy")
            assertEquals(1, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"harvest_state_quarantine\""))

            val health0 = PipelineHealthSnapshot(
                revision = 0,
                consecutiveFailures = 2,
                totalSuccesses = 3,
                totalFailures = 4,
                lastObservedAt = NOW,
                circuitOpenUntil = NOW.plusSeconds(5),
            )
            assertTrue(store.compareAndSetPipelineHealth("p", null, health0))
            assertEquals(health0, store.loadPipelineHealth("p"))
            assertFalse(store.compareAndSetPipelineHealth("p", null, health0))

            val override = PipelineEligibilityOverride(false, "maintenance", NOW.plusSeconds(30))
            store.setEligibilityOverride("p", override)
            assertEquals(override, store.loadEligibilityOverride("p"))
            store.setEligibilityOverride("p", null)
            assertNull(store.loadEligibilityOverride("p"))
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `CAS races have one winner and atomic admission rolls back on conflict capacity and pipeline mismatch`(): Unit = runBlocking {
        val schema = nextSchema()
        val first = store(schema, maximumPoolSize = 4, databaseDispatcherParallelism = 4)
        val second = store(schema, maximumPoolSize = 4, databaseDispatcherParallelism = 4)
        first.start(startContext())
        second.start(startContext())
        try {
            val next = checkpoint(0, byteArrayOf(1))
            val casResults = concurrently(2) { index ->
                if (index == 0) first.compareAndSet("p", "d", null, next)
                else second.compareAndSet("p", "d", null, next)
            }
            assertEquals(listOf(false, true), casResults.sorted())

            val health0 = PipelineHealthSnapshot(0, 0, 0, 0, NOW, null)
            assertTrue(first.compareAndSetPipelineHealth("health-race", null, health0))
            val health1 = health0.copy(revision = 1, totalFailures = 1, lastObservedAt = NOW)
            val healthResults = concurrently(2) { index ->
                if (index == 0) first.compareAndSetPipelineHealth("health-race", 0, health1)
                else second.compareAndSetPipelineHealth("health-race", 0, health1)
            }
            assertEquals(listOf(false, true), healthResults.sorted())
            assertEquals(health1, first.loadPipelineHealth("health-race"))

            val harvest0 = HarvestState(0, "harvest-race", "harvest-codec", 1, CheckpointPayload.from(byteArrayOf(1)))
            assertTrue(first.compareAndSetHarvestState("p", "harvest-race", null, harvest0))
            val harvest1 = harvest0.copy(revision = 1, payload = CheckpointPayload.from(byteArrayOf(2)))
            val harvestResults = concurrently(2) { index ->
                if (index == 0) first.compareAndSetHarvestState("p", "harvest-race", 0, harvest1)
                else second.compareAndSetHarvestState("p", "harvest-race", 0, harvest1)
            }
            assertEquals(listOf(false, true), harvestResults.sorted())
            assertEquals(harvest1, first.loadHarvestState("p", "harvest-race"))

            val item = DiscoveryWorkItem(request("atomic"), RequestIdentity("atomic"), context("p"))
            val advanced = first.enqueueAndCheckpoint("p", "atomic-discovery", null, checkpoint(0, byteArrayOf(2)), listOf(item))
            assertIs<AtomicEnqueueCheckpointResult.Advanced>(advanced)
            val before = first.activeWorkCount("p")
            val conflict = first.enqueueAndCheckpoint(
                "p",
                "atomic-discovery",
                null,
                checkpoint(0, byteArrayOf(3)),
                listOf(DiscoveryWorkItem(request("conflict"), RequestIdentity("conflict"), context("p"))),
            )
            assertEquals(AtomicEnqueueCheckpointResult.RevisionConflict, conflict)
            assertEquals(before, first.activeWorkCount("p"))

            assertFailsWith<IllegalArgumentException> {
                first.enqueueAndCheckpoint(
                    "p",
                    "other-discovery",
                    null,
                    checkpoint(0, byteArrayOf(4)),
                    listOf(DiscoveryWorkItem(request("foreign"), RequestIdentity("foreign"), context("other"))),
                )
            }

            val rollbackBefore = first.activeWorkCount("p")
            installAtomicCheckpointConflictTrigger(schema)
            try {
                assertEquals(
                    AtomicEnqueueCheckpointResult.RevisionConflict,
                    first.enqueueAndCheckpoint(
                        "p",
                        "atomic-rollback",
                        null,
                        checkpoint(0, byteArrayOf(6)),
                        listOf(
                            DiscoveryWorkItem(
                                request("rollback"),
                                RequestIdentity("rollback"),
                                context("p"),
                            ),
                        ),
                    ),
                )
            } finally {
                dropAtomicCheckpointConflictTrigger(schema)
            }
            assertEquals(rollbackBefore, first.activeWorkCount("p"))
            assertNull(first.load("p", "atomic-rollback"))
        } finally {
            dropAtomicCheckpointConflictTrigger(schema)
            first.stop(stopContext())
            second.stop(stopContext())
        }

        val capacitySchema = nextSchema()
        val capacity = store(capacitySchema, maxActiveJobs = 1, maxClaimBatch = 1)
        capacity.start(startContext())
        try {
            capacity.enqueue(request("occupied"), RequestIdentity("occupied"), context("p"))
            val rejected = capacity.enqueueAndCheckpoint(
                "p",
                "capacity-discovery",
                null,
                checkpoint(0, byteArrayOf(5)),
                listOf(DiscoveryWorkItem(request("rejected"), RequestIdentity("rejected"), context("p"))),
            )
            assertIs<AtomicEnqueueCheckpointResult.Rejected>(rejected)
            assertNull(capacity.load("p", "capacity-discovery"))
            assertEquals(1, capacity.activeWorkCount("p"))
        } finally {
            capacity.stop(stopContext())
        }
    }

    @Test
    fun `run history completion is terminal and stale recovery is bounded without lease mutation`(): Unit = runBlocking {
        val schema = nextSchema()
        val clock = MutableClock(NOW)
        val store = store(schema, clock = clock, maxRecoveryRows = 1, staleRunAfter = Duration.ofSeconds(10))
        store.start(startContext())
        try {
            val old = NOW.minusSeconds(20)
            store.startEngineRun("engine-old", old, "manual")
            store.startPipelineRun("engine-old", "pipeline-old-1", "p", old)
            store.startPipelineRun("engine-old", "pipeline-old-2", "p", old)
            store.startEngineRun("engine-new", NOW.minusSeconds(10), null)
            store.startPipelineRun("engine-new", "pipeline-new", "p", NOW.minusSeconds(10))

            val leasedId = accepted(store.enqueue(request("lease-preserved"), RequestIdentity("lease-preserved"), context("p")))
            val lease = store.claim(1, "owner", NOW).single()
            val leaseTokenBefore = lease.leaseToken.value

            val recovered = store.recoverStaleRuns(NOW, Duration.ofSeconds(1))
            assertEquals(2, recovered)
            val oldPipelineStatuses = listOf("pipeline-old-1", "pipeline-old-2").map { pipelineRunId ->
                pipelineRunId to runStatus(schema, "pipeline_runs", pipelineRunId, "pipeline_run_id")
            }
            assertEquals(listOf("ABANDONED", "RUNNING"), oldPipelineStatuses.map { it.second }.sorted())
            val recoveredPipelineId = oldPipelineStatuses.single { it.second == "ABANDONED" }.first
            assertEquals("ABANDONED", runStatus(schema, "engine_runs", "engine-old", "engine_run_id"))
            assertEquals("RUNNING", runStatus(schema, "engine_runs", "engine-new", "engine_run_id"))
            store.completePipelineRun(recoveredPipelineId, DurableRunStatus.COMPLETED, NOW, 9)
            assertEquals("ABANDONED", runStatus(schema, "pipeline_runs", recoveredPipelineId, "pipeline_run_id"))
            assertEquals(leaseTokenBefore, jobLeaseToken(schema, leasedId))
            assertEquals("LEASED", jobStatus(schema, leasedId))

            store.completePipelineRun("pipeline-new", DurableRunStatus.FAILED, NOW, 2)
            store.completeEngineRun("engine-new", DurableRunStatus.CANCELLED, NOW, 3)
            assertEquals("FAILED", runStatus(schema, "pipeline_runs", "pipeline-new", "pipeline_run_id"))
            assertEquals("CANCELLED", runStatus(schema, "engine_runs", "engine-new", "engine_run_id"))
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `retention is bounded strict at cutoff terminal-only and foreign-key safe`(): Unit = runBlocking {
        val schema = nextSchema()
        val clock = MutableClock(NOW.minusSeconds(100))
        val store = store(schema, clock = clock, maxRetentionRows = 1)
        store.start(startContext())
        try {
            val old = clock.current
            val withAttempt = accepted(store.enqueue(request("with-attempt"), RequestIdentity("with-attempt"), context("p")))
            val attemptedWork = store.claim(1, "attempt-owner", old).single()
            val attempt = assertIs<AttemptStartResult.Started>(store.beginAttempt(attemptedWork, old)).attempt
            store.finishAttempt(attemptedWork, attempt, AttemptOutcome.FAILED)
            store.transition(attemptedWork, WorkTransition.Failed("failed"))

            val noAttempt = accepted(store.enqueue(request("no-attempt"), RequestIdentity("no-attempt"), context("p")))
            val noAttemptWork = store.claim(1, "no-attempt-owner", old).single()
            store.transition(noAttemptWork, WorkTransition.Skipped("skip"))

            val active = accepted(store.enqueue(request("active"), RequestIdentity("active"), context("p")))
            store.claim(1, "active-owner", old)

            val exactCutoff = old
            assertEquals(
                0,
                store.prune(CrawlStateRetentionRequest(RetentionCategory.COMPLETED_JOBS, exactCutoff, 1)).deleted,
            )
            clock.current = old.plusSeconds(2)
            val attemptsPruned = store.prune(
                CrawlStateRetentionRequest(RetentionCategory.ATTEMPTS, clock.current, 1),
            )
            assertEquals(1, attemptsPruned.deleted)
            assertEquals(0, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"attempts\""))

            val jobsPruned = store.prune(
                CrawlStateRetentionRequest(RetentionCategory.COMPLETED_JOBS, clock.current, 1),
            )
            assertEquals(1, jobsPruned.deleted)
            assertEquals("LEASED", jobStatus(schema, active))
            assertEquals("MISSING", jobStatus(schema, withAttempt))
            assertEquals("SKIPPED", jobStatus(schema, noAttempt))

            val runTime = old.minusSeconds(1)
            store.startEngineRun("retention-engine", runTime, null)
            store.startPipelineRun("retention-engine", "retention-pipeline-1", "p", runTime)
            store.startPipelineRun("retention-engine", "retention-pipeline-2", "p", runTime)
            store.completePipelineRun("retention-pipeline-1", DurableRunStatus.COMPLETED, runTime.plusMillis(1), 0)
            store.completePipelineRun("retention-pipeline-2", DurableRunStatus.COMPLETED, runTime.plusMillis(2), 0)
            store.completeEngineRun("retention-engine", DurableRunStatus.COMPLETED, runTime.plusMillis(3), 0)

            val firstRunPrune = store.prune(
                CrawlStateRetentionRequest(RetentionCategory.RUN_HISTORY, old, 1),
            )
            assertEquals(1, firstRunPrune.deleted)
            assertEquals("COMPLETED", runStatus(schema, "engine_runs", "retention-engine", "engine_run_id"))
            val secondRunPrune = store.prune(
                CrawlStateRetentionRequest(RetentionCategory.RUN_HISTORY, old, 1),
            )
            assertEquals(2, secondRunPrune.deleted)
            assertNull(runStatusOrNull(schema, "engine_runs", "retention-engine", "engine_run_id"))
        } finally {
            store.stop(stopContext())
        }
    }

    private suspend fun <T> concurrently(count: Int, block: suspend (Int) -> T): List<T> {
        val ready = List(count) { CompletableDeferred<Unit>() }
        val gate = CompletableDeferred<Unit>()
        return coroutineScope {
            val jobs = (0 until count).map { index ->
                async(Dispatchers.Default) {
                    ready[index].complete(Unit)
                    gate.await()
                    block(index)
                }
            }
            ready.forEach { it.await() }
            gate.complete(Unit)
            jobs.awaitAll()
        }
    }

    private fun store(
        schema: String,
        clock: Clock = Clock { NOW },
        maximumPoolSize: Int = 4,
        databaseDispatcherParallelism: Int = maximumPoolSize,
        maxActiveJobs: Int = 100,
        maxClaimBatch: Int = 16,
        maxCancellationRows: Int = 4_096,
        maxRecoveryRows: Int = 1_024,
        maxCheckpointPayloadBytes: Int = 2 * 1_024 * 1_024,
        maxRetentionRows: Int = 4_096,
        staleRunAfter: Duration = Duration.ofMinutes(30),
    ): PostgresCrawlStateStore = PostgresCrawlStateStore(
        id = "it-$schema",
        configuration = PostgresStateConfiguration(
            jdbcUrl = postgres.jdbcUrl,
            username = postgres.username,
            password = postgres.password.toCharArray(),
            schema = schema,
            maximumPoolSize = maximumPoolSize,
            databaseDispatcherParallelism = databaseDispatcherParallelism,
            maxActiveJobs = maxActiveJobs,
            maxClaimBatch = maxClaimBatch,
            maxCancellationRows = maxCancellationRows,
            maxRecoveryRows = maxRecoveryRows,
            maxCheckpointPayloadBytes = maxCheckpointPayloadBytes,
            maxRetentionRows = maxRetentionRows,
            staleRunAfter = staleRunAfter,
        ),
        clock = clock,
    )

    private fun request(target: String): CrawlRequest = CrawlRequest("https://example.test/$target")

    private fun context(pipelineId: String): CrawlerContext = CrawlerContext(
        engineRunId = "engine",
        pipelineRunId = "run-$pipelineId",
        pipelineId = pipelineId,
    )

    private fun accepted(result: EnqueueResult): WorkItemId = assertIs<EnqueueResult.Accepted>(result).workItemId

    private fun checkpoint(revision: Long, bytes: ByteArray): DiscoveryCheckpoint = DiscoveryCheckpoint(
        revision = revision,
        codecId = "checkpoint-codec",
        codecVersion = 1,
        payload = CheckpointPayload.from(bytes),
        generationId = "generation",
        strategyId = "strategy",
        completed = false,
    )

    private fun startContext(): ExtensionStartContext =
        ExtensionStartContext(kotlinx.coroutines.CoroutineScope(Dispatchers.Default))

    private fun stopContext(): ExtensionStopContext = ExtensionStopContext(ExtensionStopReason.ENGINE_SHUTDOWN)

    private fun <T> withConnection(block: (Connection) -> T): T =
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use(block)

    private fun scalarInt(schema: String, sql: String): Int = withConnection { connection ->
        connection.createStatement().use { statement ->
            statement.executeQuery(sql).use { result ->
                assertTrue(result.next())
                result.getInt(1)
            }
        }
    }

    private fun tableExists(schema: String, table: String): Boolean = withConnection { connection ->
        connection.prepareStatement(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?)",
        ).use { statement ->
            statement.setString(1, schema)
            statement.setString(2, table)
            statement.executeQuery().use { result ->
                assertTrue(result.next())
                result.getBoolean(1)
            }
        }
    }

    private fun jobAttemptCount(schema: String, id: WorkItemId): Int = scalarInt(
        schema,
        "SELECT attempt_count FROM \"$schema\".\"jobs\" WHERE job_id = '${id.value}'",
    )

    private fun jobActiveAttempt(schema: String, id: WorkItemId): String? = withConnection { connection ->
        connection.prepareStatement("SELECT active_attempt_id FROM \"$schema\".\"jobs\" WHERE job_id = ?").use {
            it.setString(1, id.value)
            it.executeQuery().use { result ->
                assertTrue(result.next())
                result.getString(1)
            }
        }
    }

    private fun jobAttemptOutcome(schema: String, id: WorkItemId, attemptId: String): String? = withConnection { connection ->
        connection.prepareStatement("SELECT outcome FROM \"$schema\".\"attempts\" WHERE attempt_id = ? AND job_id = ?").use {
            it.setString(1, attemptId)
            it.setString(2, id.value)
            it.executeQuery().use { result ->
                assertTrue(result.next())
                result.getString(1)
            }
        }
    }

    private fun jobStatus(schema: String, id: WorkItemId): String = withConnection { connection ->
        connection.prepareStatement("SELECT status FROM \"$schema\".\"jobs\" WHERE job_id = ?").use {
            it.setString(1, id.value)
            it.executeQuery().use { result ->
                if (result.next()) result.getString(1) else "MISSING"
            }
        }
    }

    private fun jobLeaseOwner(schema: String, id: WorkItemId): String? = withConnection { connection ->
        connection.prepareStatement("SELECT lease_owner FROM \"$schema\".\"jobs\" WHERE job_id = ?").use {
            it.setString(1, id.value)
            it.executeQuery().use { result ->
                assertTrue(result.next())
                result.getString(1)
            }
        }
    }

    private fun jobLeaseToken(schema: String, id: WorkItemId): String? = withConnection { connection ->
        connection.prepareStatement("SELECT lease_token FROM \"$schema\".\"jobs\" WHERE job_id = ?").use {
            it.setString(1, id.value)
            it.executeQuery().use { result ->
                assertTrue(result.next())
                result.getString(1)
            }
        }
    }

    private fun terminalJobIds(schema: String, status: String): Set<WorkItemId> = withConnection { connection ->
        connection.prepareStatement("SELECT job_id FROM \"$schema\".\"jobs\" WHERE status = ?").use {
            it.setString(1, status)
            it.executeQuery().use { result ->
                buildSet {
                    while (result.next()) add(WorkItemId(result.getString(1)))
                }
            }
        }
    }

    private fun runStatus(schema: String, table: String, id: String, key: String): String =
        runStatusOrNull(schema, table, id, key) ?: error("Missing $table row $id")

    private fun runStatusOrNull(schema: String, table: String, id: String, key: String): String? = withConnection { connection ->
        connection.prepareStatement("SELECT status FROM \"$schema\".\"$table\" WHERE \"$key\" = ?").use {
            it.setString(1, id)
            it.executeQuery().use { result ->
                if (result.next()) result.getString(1) else null
            }
        }
    }

    private fun installAttemptFailureTrigger(schema: String) {
        withConnection { connection ->
            connection.createStatement().use { statement ->
                statement.execute(
                    """
                    CREATE OR REPLACE FUNCTION \"$schema\".fail_attempt_insert()
                    RETURNS trigger LANGUAGE plpgsql AS
                    'BEGIN RAISE EXCEPTION ''intentional attempt insert failure''; END;'
                    """.trimIndent(),
                )
                statement.execute(
                    "CREATE TRIGGER fail_attempt_insert BEFORE INSERT ON \"$schema\".\"attempts\" " +
                        "FOR EACH ROW EXECUTE FUNCTION \"$schema\".fail_attempt_insert()",
                )
            }
        }
    }

    private fun dropAttemptFailureTrigger(schema: String) {
        withConnection { connection ->
            connection.createStatement().use { statement ->
                statement.execute("DROP TRIGGER IF EXISTS fail_attempt_insert ON \"$schema\".\"attempts\"")
                statement.execute("DROP FUNCTION IF EXISTS \"$schema\".fail_attempt_insert()")
            }
        }
    }

    private fun installAtomicCheckpointConflictTrigger(schema: String) {
        withConnection { connection ->
            connection.createStatement().use { statement ->
                statement.execute(
                    """
                    CREATE OR REPLACE FUNCTION "$schema".conflict_checkpoint()
                    RETURNS trigger LANGUAGE plpgsql AS ${'$'}${'$'}
                    BEGIN
                        INSERT INTO "$schema"."discovery_checkpoints" (
                            pipeline_id, discovery_id, revision, codec_id, codec_version,
                            payload, generation_id, strategy_id, completed, updated_at
                        ) VALUES (NEW.pipeline_id, 'atomic-rollback', 0, 'trigger', 1,
                                  decode('00', 'hex'), NULL, NULL, FALSE, now());
                        RETURN NEW;
                    END;
                    ${'$'}${'$'}
                    """.trimIndent(),
                )
                statement.execute(
                    "CREATE TRIGGER atomic_checkpoint_conflict AFTER INSERT ON \"$schema\".\"jobs\" " +
                        "FOR EACH ROW EXECUTE FUNCTION \"$schema\".conflict_checkpoint()",
                )
            }
        }
    }

    private fun dropAtomicCheckpointConflictTrigger(schema: String) {
        withConnection { connection ->
            connection.createStatement().use { statement ->
                statement.execute("DROP TRIGGER IF EXISTS atomic_checkpoint_conflict ON \"$schema\".\"jobs\"")
                statement.execute("DROP FUNCTION IF EXISTS \"$schema\".conflict_checkpoint()")
            }
        }
    }

    private class MutableClock(var current: Instant) : Clock {
        override fun now(): Instant = current
    }

    private companion object {
        val NOW: Instant = Instant.parse("2026-01-01T00:00:00Z")
        val schemaSequence = AtomicInteger()

        @Container
        @JvmField
        val postgres: PostgreSQLContainer<Nothing> = PostgreSQLContainer<Nothing>("postgres:16.4-alpine")

        fun nextSchema(): String = "it_${schemaSequence.incrementAndGet()}"
    }
}
