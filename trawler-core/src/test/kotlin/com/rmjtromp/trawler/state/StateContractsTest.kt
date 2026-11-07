package com.rmjtromp.trawler.state

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.policy.RequestIdentity
import com.rmjtromp.trawler.retention.RetentionCategory
import java.time.Instant
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class StateContractsTest {
    @Test
    fun `identity and token value types reject blank values and keep the supplied value`() {
        assertEquals("work-1", WorkItemId("work-1").value)
        assertEquals("lease-1", LeaseToken("lease-1").value)
        assertEquals("attempt-1", AttemptId("attempt-1").value)
        assertFailsWith<IllegalArgumentException> { WorkItemId(" ") }
        assertFailsWith<IllegalArgumentException> { LeaseToken("") }
        assertFailsWith<IllegalArgumentException> { AttemptId("\t") }
    }

    @Test
    fun `operational results require non-blank reasons and keep optional identities explicit`() {
        assertNull(EnqueueResult.AlreadyActive().workItemId)
        assertEquals(WorkItemId("work"), EnqueueResult.AlreadyActive(WorkItemId("work")).workItemId)
        assertEquals("full", EnqueueResult.Rejected("full").reason)
        assertEquals("busy", LeaseMutationResult.Rejected("busy").reason)
        assertEquals("busy", AttemptStartResult.Rejected("busy").reason)
        assertEquals("full", AtomicEnqueueCheckpointResult.Rejected("full").reason)

        assertFailsWith<IllegalArgumentException> { EnqueueResult.Rejected(" ") }
        assertFailsWith<IllegalArgumentException> { LeaseMutationResult.Rejected(" ") }
        assertFailsWith<IllegalArgumentException> { AttemptStartResult.Rejected(" ") }
        assertFailsWith<IllegalArgumentException> { AtomicEnqueueCheckpointResult.Rejected(" ") }
    }

    @Test
    fun `work transitions require non-blank reasons for every terminal reasoned transition`() {
        assertEquals("boom", WorkTransition.Failed("boom").reason)
        assertEquals("policy", WorkTransition.Skipped("policy").reason)
        assertEquals("shutdown", WorkTransition.Cancelled("shutdown").reason)
        assertEquals("budget", WorkTransition.Abandoned("budget").reason)
        assertEquals(Instant.EPOCH, WorkTransition.Retry(Instant.EPOCH).notBefore)

        assertFailsWith<IllegalArgumentException> { WorkTransition.Failed(" ") }
        assertFailsWith<IllegalArgumentException> { WorkTransition.Skipped(" ") }
        assertFailsWith<IllegalArgumentException> { WorkTransition.Cancelled(" ") }
        assertFailsWith<IllegalArgumentException> { WorkTransition.Abandoned(" ") }
    }

    @Test
    fun `execution attempts must have a positive number`() {
        val attempt = ExecutionAttempt(AttemptId("attempt"), 1, Instant.EPOCH)
        assertEquals(1, attempt.number)
        assertFailsWith<IllegalArgumentException> { ExecutionAttempt(AttemptId("attempt"), 0, Instant.EPOCH) }
        assertFailsWith<IllegalArgumentException> { ExecutionAttempt(AttemptId("attempt"), -1, Instant.EPOCH) }
    }

    @Test
    fun `leased work defaults to an unscoped pipeline identifier`() {
        val leased = LeasedWork(
            workItemId = WorkItemId("work"),
            request = CrawlRequest("target"),
            identity = RequestIdentity("target"),
            leaseToken = LeaseToken("token"),
            ownerId = "owner",
            expiresAt = Instant.EPOCH,
        )
        assertEquals("", leased.pipelineId)
        assertEquals("scoped", leased.copy(pipelineId = "scoped").pipelineId)
    }

    @Test
    fun `durable run status exposes the documented lifecycle states in order`() {
        assertEquals(
            listOf("RUNNING", "COMPLETED", "ABANDONED", "FAILED", "CANCELLED"),
            DurableRunStatus.entries.map { it.name },
        )
        assertEquals(DurableRunStatus.ABANDONED, DurableRunStatus.valueOf("ABANDONED"))
    }

    @Test
    fun `checkpoint payload is defensively copied and compares by content`() {
        val source = byteArrayOf(1, 2, 3)
        val payload = CheckpointPayload.from(source)
        source[0] = 9
        assertContentEquals(byteArrayOf(1, 2, 3), payload.toByteArray())
        payload.toByteArray()[1] = 9
        assertContentEquals(byteArrayOf(1, 2, 3), payload.toByteArray())
        assertEquals(3, payload.size)

        val same = CheckpointPayload.from(byteArrayOf(1, 2, 3))
        val different = CheckpointPayload.from(byteArrayOf(1, 2))
        assertEquals(payload, same)
        assertEquals(payload.hashCode(), same.hashCode())
        assertNotEquals(payload, different)
        assertFalse(payload.equals("not a payload"))
        assertFalse(payload.equals(null))
        assertEquals("CheckpointPayload(size=3)", payload.toString())
        assertFalse(payload.toString().contains("1, 2, 3"))
    }

    @Test
    fun `discovery checkpoints validate revision codec and optional identifiers`() {
        val payload = CheckpointPayload.from(byteArrayOf(1))
        val checkpoint = DiscoveryCheckpoint(
            revision = 0,
            codecId = "codec",
            codecVersion = 0,
            payload = payload,
            generationId = "generation",
            strategyId = "strategy",
            completed = true,
        )
        assertEquals("generation", checkpoint.generationId)
        assertEquals("strategy", checkpoint.strategyId)
        assertTrue(checkpoint.completed)
        assertNull(DiscoveryCheckpoint(0, "codec", 0, payload).generationId)

        assertFailsWith<IllegalArgumentException> { DiscoveryCheckpoint(-1, "codec", 0, payload) }
        assertFailsWith<IllegalArgumentException> { DiscoveryCheckpoint(0, " ", 0, payload) }
        assertFailsWith<IllegalArgumentException> { DiscoveryCheckpoint(0, "codec", -1, payload) }
        assertFailsWith<IllegalArgumentException> {
            DiscoveryCheckpoint(0, "codec", 0, payload, generationId = " ")
        }
        assertFailsWith<IllegalArgumentException> {
            DiscoveryCheckpoint(0, "codec", 0, payload, strategyId = " ")
        }
    }

    @Test
    fun `checkpoint codec descriptors and resolutions validate their explicit decisions`() {
        assertEquals(0, DiscoveryCheckpointCodecDescriptor("codec", 0).version)
        assertFailsWith<IllegalArgumentException> { DiscoveryCheckpointCodecDescriptor(" ", 0) }
        assertFailsWith<IllegalArgumentException> { DiscoveryCheckpointCodecDescriptor("codec", -1) }

        val checkpoint = DiscoveryCheckpoint(0, "codec", 1, CheckpointPayload.from(byteArrayOf(1)))
        assertEquals(checkpoint, CheckpointResolution.Use(checkpoint).checkpoint)
        assertEquals(checkpoint, CheckpointResolution.Migrated(checkpoint).checkpoint)
        assertNotEquals<CheckpointResolution>(
            CheckpointResolution.Use(checkpoint),
            CheckpointResolution.Migrated(checkpoint),
        )
        assertEquals("version", CheckpointResolution.Reset("version").reason)
        assertEquals("corrupt", CheckpointResolution.Quarantine("corrupt").reason)
        assertFailsWith<IllegalArgumentException> { CheckpointResolution.Reset(" ") }
        assertFailsWith<IllegalArgumentException> { CheckpointResolution.Quarantine(" ") }
    }

    @Test
    fun `discovery work items and atomic results carry operational data only`() {
        val item = DiscoveryWorkItem(
            CrawlRequest("target"),
            RequestIdentity("target"),
            CrawlerContext("engine", "run", "pipeline"),
        )
        val checkpoint = DiscoveryCheckpoint(1, "codec", 1, CheckpointPayload.from(byteArrayOf()))
        val advanced = AtomicEnqueueCheckpointResult.Advanced(
            listOf(EnqueueResult.Accepted(WorkItemId("work"))),
            checkpoint,
        )
        assertEquals("pipeline", item.context.pipelineId)
        assertEquals(checkpoint, advanced.checkpoint)
        assertEquals(listOf<EnqueueResult>(EnqueueResult.Accepted(WorkItemId("work"))), advanced.enqueueResults)
    }

    @Test
    fun `queue metrics reject negative counts and allow an empty queue`() {
        assertEquals(WorkQueueMetrics(0, null), WorkQueueMetrics(openWorkCount = 0, oldestOpenWorkAt = null))
        assertFailsWith<IllegalArgumentException> { WorkQueueMetrics(-1, null) }
    }

    @Test
    fun `crawl-state retention accepts only operational categories and positive batches`() {
        listOf(
            RetentionCategory.COMPLETED_JOBS,
            RetentionCategory.ATTEMPTS,
            RetentionCategory.RUN_HISTORY,
        ).forEach { category ->
            val request = CrawlStateRetentionRequest(category, Instant.EPOCH, 1)
            assertEquals(category, request.category)
            assertEquals(Instant.EPOCH, request.cutoff)
            assertEquals(1, request.maxEntries)
        }
        listOf(
            RetentionCategory.RESPONSES,
            RetentionCategory.OUTPUT_HISTORY,
            RetentionCategory.OUTBOX_HISTORY,
            RetentionCategory.OPERATIONAL_EVENTS,
        ).forEach { category ->
            assertFailsWith<IllegalArgumentException> { CrawlStateRetentionRequest(category, Instant.EPOCH, 1) }
        }
        assertFailsWith<IllegalArgumentException> {
            CrawlStateRetentionRequest(RetentionCategory.ATTEMPTS, Instant.EPOCH, 0)
        }
    }

    @Test
    fun `harvest state and codec descriptors validate revision identity and codec version`() {
        val payload = CheckpointPayload.from(byteArrayOf(1))
        val state = HarvestState(0, "strategy", "codec", 0, payload)
        assertFalse(state.historicalComplete)
        assertTrue(state.copy(historicalComplete = true).historicalComplete)

        assertFailsWith<IllegalArgumentException> { HarvestState(-1, "strategy", "codec", 0, payload) }
        assertFailsWith<IllegalArgumentException> { HarvestState(0, " ", "codec", 0, payload) }
        assertFailsWith<IllegalArgumentException> { HarvestState(0, "strategy", " ", 0, payload) }
        assertFailsWith<IllegalArgumentException> { HarvestState(0, "strategy", "codec", -1, payload) }

        assertEquals(0, HarvestStateCodecDescriptor("codec", 0).version)
        assertFailsWith<IllegalArgumentException> { HarvestStateCodecDescriptor(" ", 0) }
        assertFailsWith<IllegalArgumentException> { HarvestStateCodecDescriptor("codec", -1) }
    }

    @Test
    fun `harvest state resolutions keep explicit migrate reset and quarantine decisions`() {
        val state = HarvestState(0, "strategy", "codec", 1, CheckpointPayload.from(byteArrayOf(1)))
        assertEquals(state, HarvestStateResolution.Use(state).state)
        assertEquals(state, HarvestStateResolution.Migrated(state).state)
        assertNotEquals<HarvestStateResolution>(HarvestStateResolution.Use(state), HarvestStateResolution.Migrated(state))
        assertEquals("version", HarvestStateResolution.Reset("version").reason)
        assertEquals("corrupt", HarvestStateResolution.Quarantine("corrupt").reason)
        assertFailsWith<IllegalArgumentException> { HarvestStateResolution.Reset(" ") }
        assertFailsWith<IllegalArgumentException> { HarvestStateResolution.Quarantine(" ") }
    }

    @Test
    fun `eligibility overrides allow absent reasons but reject blank ones`() {
        val override = PipelineEligibilityOverride(enabled = true)
        assertNull(override.reason)
        assertNull(override.expiresAt)
        assertEquals("maintenance", PipelineEligibilityOverride(false, "maintenance").reason)
        assertFailsWith<IllegalArgumentException> { PipelineEligibilityOverride(false, " ") }
    }

    @Test
    fun `pipeline health snapshots reject negative counters`() {
        val snapshot = PipelineHealthSnapshot()
        assertEquals(0, snapshot.revision)
        assertNull(snapshot.circuitOpenUntil)
        assertFailsWith<IllegalArgumentException> { PipelineHealthSnapshot(revision = -1) }
        assertFailsWith<IllegalArgumentException> { PipelineHealthSnapshot(consecutiveFailures = -1) }
        assertFailsWith<IllegalArgumentException> { PipelineHealthSnapshot(totalSuccesses = -1) }
        assertFailsWith<IllegalArgumentException> { PipelineHealthSnapshot(totalFailures = -1) }
    }
}
