package com.rmjtromp.trawler.postgres

import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.extension.ExtensionStopReason
import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.RequestBody
import com.rmjtromp.trawler.model.RequestMethod
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.FifoWorkOrderingPolicy
import com.rmjtromp.trawler.policy.RequestIdentity
import com.rmjtromp.trawler.policy.ResourceHistory
import com.rmjtromp.trawler.state.AtomicEnqueueCheckpointResult
import com.rmjtromp.trawler.state.AttemptOutcome
import com.rmjtromp.trawler.state.AttemptStartResult
import com.rmjtromp.trawler.state.CheckpointPayload
import com.rmjtromp.trawler.state.DiscoveryCheckpoint
import com.rmjtromp.trawler.state.DiscoveryWorkItem
import com.rmjtromp.trawler.state.DurableRunStatus
import com.rmjtromp.trawler.state.EnqueueResult
import com.rmjtromp.trawler.state.HarvestState
import com.rmjtromp.trawler.state.LeaseMutationResult
import com.rmjtromp.trawler.state.PipelineEligibilityOverride
import com.rmjtromp.trawler.state.PipelineHealthSnapshot
import com.rmjtromp.trawler.state.WorkItemId
import com.rmjtromp.trawler.state.WorkTransition
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * Contract coverage for state-store validation, revision semantics, active
 * deduplication authority, fenced rollback, and cancellation. Each test owns
 * a fresh schema; triggers and external transactions inject faults through
 * direct SQL rather than through the adapter under test.
 */
@Testcontainers
class PostgresCrawlStateStoreContractIT {
    @Test
    fun `ordering accepts only fifo for a named pipeline`(): Unit = runBlocking {
        val store = store(nextSchema())

        store.setOrdering("p", FifoWorkOrderingPolicy)
        assertEquals(
            "Pipeline ID must not be blank",
            assertFailsWith<IllegalArgumentException> { store.setOrdering(" ", FifoWorkOrderingPolicy) }.message,
        )
    }

    @Test
    fun `enqueue validates every request field bound before mutation and claim returns the exact accepted request`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(
            schema,
            configuration = configuration(
                schema,
                maxTargetBytes = 32,
                maxHeaderCount = 1,
                maxHeaderNameBytes = 8,
                maxHeaderValueBytes = 8,
                maxRequestBodyBytes = 2,
                maxAttributeCount = 1,
                maxAttributeNameBytes = 8,
                maxAttributeValueBytes = 8,
            ),
        )
        store.start(startContext())
        try {
            val valid = CrawlRequest(
                target = "https://e.test/ok",
                method = RequestMethod("POST"),
                headers = Headers.of(Header("X-A", "12345678")),
                body = RequestBody.from(byteArrayOf(1, 2)),
                attributes = ContextAttributes.of(ContextAttribute("cursor", "12345678")),
            )
            val id = assertIs<EnqueueResult.Accepted>(store.enqueue(valid, RequestIdentity("valid"), context("p"))).workItemId

            val rejected = listOf(
                "PostgreSQL request target exceeds its configured UTF-8 bound" to
                    valid.copy(target = "https://e.test/" + "x".repeat(32)),
                "PostgreSQL request method exceeds its configured UTF-8 bound" to
                    valid.copy(method = RequestMethod("LONGMETHOD")),
                "Request header count exceeds the PostgreSQL state bound" to
                    valid.copy(headers = Headers.of(Header("X-A", "v"), Header("X-B", "v"))),
                "PostgreSQL request header name exceeds its configured UTF-8 bound" to
                    valid.copy(headers = Headers.of(Header("X-Too-Long", "v"))),
                "PostgreSQL request header value exceeds its configured UTF-8 bound" to
                    valid.copy(headers = Headers.of(Header("X-A", "123456789"))),
                "Request body exceeds the PostgreSQL state bound" to
                    valid.copy(body = RequestBody.from(byteArrayOf(1, 2, 3))),
                "Request attribute count exceeds the PostgreSQL state bound" to
                    valid.copy(attributes = ContextAttributes.of(ContextAttribute("a", "v"), ContextAttribute("b", "v"))),
                "PostgreSQL request attribute name exceeds its configured UTF-8 bound" to
                    valid.copy(attributes = ContextAttributes.of(ContextAttribute("attribute1", "v"))),
                "PostgreSQL request attribute value exceeds its configured UTF-8 bound" to
                    valid.copy(attributes = ContextAttributes.of(ContextAttribute("cursor", "123456789"))),
            )
            rejected.forEachIndexed { index, (message, request) ->
                val error = assertFailsWith<IllegalArgumentException> {
                    store.enqueue(request, RequestIdentity("rejected-$index"), context("p"))
                }
                assertEquals(message, error.message)
            }
            val atomicError = assertFailsWith<IllegalArgumentException> {
                store.enqueueAndCheckpoint(
                    "p",
                    "d",
                    null,
                    checkpoint(0),
                    listOf(DiscoveryWorkItem(rejected.first().second, RequestIdentity("atomic"), context("p"))),
                )
            }
            assertEquals(rejected.first().first, atomicError.message)

            assertEquals(1L, database.long("SELECT COUNT(*) FROM \"$schema\".\"resources\""))
            assertEquals(1L, database.long("SELECT COUNT(*) FROM \"$schema\".\"jobs\""))
            assertNull(store.load("p", "d"))
            val claimed = store.claim(1, "owner", NOW).single()
            assertEquals(id, claimed.workItemId)
            assertEquals(valid, claimed.request)
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `a caller codec that exceeds the encoded request bound is rejected before any transaction`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(
            schema,
            configuration = configuration(schema, maxEncodedRequestBytes = 64),
            codecs = PostgresRequestCodecRegistry(listOf(OversizedCodec(65))),
        )
        store.start(startContext())
        try {
            val direct = assertFailsWith<IllegalArgumentException> {
                store.enqueue(request("oversized"), RequestIdentity("oversized"), context("p"))
            }
            assertEquals("Encoded request exceeds the PostgreSQL state bound", direct.message)
            val atomic = assertFailsWith<IllegalArgumentException> {
                store.enqueueAndCheckpoint(
                    "p",
                    "d",
                    null,
                    checkpoint(0),
                    listOf(DiscoveryWorkItem(request("oversized"), RequestIdentity("oversized"), context("p"))),
                )
            }
            assertEquals("Encoded request exceeds the PostgreSQL state bound", atomic.message)
            assertEquals(0L, database.long("SELECT COUNT(*) FROM \"$schema\".\"resources\""))
            assertNull(store.load("p", "d"))
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `claim refuses persisted requests above the current bounds and leaves them queued and unleased`(): Unit = runBlocking {
        val schema = nextSchema()
        val writer = store(schema)
        val tighterBody = store(schema, id = "tighter-body", configuration = configuration(schema, maxRequestBodyBytes = 2))
        val tighterPayload = store(schema, id = "tighter-payload", configuration = configuration(schema, maxEncodedRequestBytes = 16))
        writer.start(startContext())
        tighterBody.start(startContext())
        tighterPayload.start(startContext())
        try {
            val stored = request("body").copy(body = RequestBody.from(byteArrayOf(1, 2, 3)))
            val id = assertIs<EnqueueResult.Accepted>(writer.enqueue(stored, RequestIdentity("body"), context("p"))).workItemId

            assertEquals(
                "Encoded request body exceeds the PostgreSQL codec bound",
                assertFailsWith<IllegalArgumentException> { tighterBody.claim(1, "body-owner", NOW) }.message,
            )
            assertEquals(
                "PostgreSQL request payload exceeds its configured bound",
                assertFailsWith<IllegalArgumentException> { tighterPayload.claim(1, "payload-owner", NOW) }.message,
            )
            assertEquals("QUEUED", jobColumn(schema, id, "status"))
            assertNull(jobColumn(schema, id, "lease_token"))
            assertEquals("0", jobColumn(schema, id, "attempt_count"))

            val claimed = writer.claim(1, "writer", NOW).single()
            assertEquals(id, claimed.workItemId)
            assertEquals(stored, claimed.request)
        } finally {
            tighterPayload.stop(stopContext())
            tighterBody.stop(stopContext())
            writer.stop(stopContext())
        }
    }

    @Test
    fun `atomic admission validates batches and revisions and charges capacity only for new resources`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema, configuration = configuration(schema, maxActiveJobs = 2, maxClaimBatch = 1, maxCheckpointBatch = 3))
        store.start(startContext())
        try {
            assertEquals(
                "Atomic discovery admission requires work",
                assertFailsWith<IllegalArgumentException> {
                    store.enqueueAndCheckpoint("p", "d", null, checkpoint(0), emptyList())
                }.message,
            )
            assertEquals(
                "Atomic discovery admission batch exceeds the PostgreSQL bound",
                assertFailsWith<IllegalArgumentException> {
                    store.enqueueAndCheckpoint("p", "d", null, checkpoint(0), (1..4).map { item("batch-$it") })
                }.message,
            )

            val existing = assertIs<EnqueueResult.Accepted>(
                store.enqueue(request("existing"), RequestIdentity("existing"), context("p")),
            ).workItemId
            val first = assertIs<AtomicEnqueueCheckpointResult.Advanced>(
                store.enqueueAndCheckpoint(
                    "p",
                    "d",
                    null,
                    checkpoint(0),
                    listOf(item("existing"), item("fresh"), item("fresh", target = "fresh-again")),
                ),
            )
            val freshId = assertIs<EnqueueResult.Accepted>(first.enqueueResults[1]).workItemId
            assertEquals(
                listOf(EnqueueResult.AlreadyActive(existing), EnqueueResult.Accepted(freshId), EnqueueResult.AlreadyActive(freshId)),
                first.enqueueResults,
            )
            assertEquals(2, store.activeWorkCount("p"))

            val atCapacity = assertIs<AtomicEnqueueCheckpointResult.Advanced>(
                store.enqueueAndCheckpoint("p", "d", 0, checkpoint(1), listOf(item("existing"))),
            )
            assertEquals(listOf(EnqueueResult.AlreadyActive(existing)), atCapacity.enqueueResults)
            assertEquals(1L, store.load("p", "d")?.revision)

            assertIs<AtomicEnqueueCheckpointResult.Rejected>(
                store.enqueueAndCheckpoint("p", "d", 1, checkpoint(2), listOf(item("over-capacity"))),
            )
            assertEquals(
                AtomicEnqueueCheckpointResult.RevisionConflict,
                store.enqueueAndCheckpoint("p", "d", 0, checkpoint(1), listOf(item("stale"))),
            )
            val skipped = assertFailsWith<IllegalArgumentException> {
                store.enqueueAndCheckpoint("p", "d", 1, checkpoint(3), listOf(item("skipped")))
            }
            assertEquals("Next checkpoint revision must immediately follow the expected revision", skipped.message)
            assertFailsWith<IllegalArgumentException> {
                store.enqueueAndCheckpoint("p", "new-discovery", null, checkpoint(1), listOf(item("first-skipped")))
            }

            assertEquals(1L, store.load("p", "d")?.revision)
            assertNull(store.load("p", "new-discovery"))
            assertEquals(2, store.activeWorkCount("p"))
            assertEquals(2L, database.long("SELECT COUNT(*) FROM \"$schema\".\"resources\""))
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `checkpoint CAS rejects a stale expected revision and round-trips absent metadata`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema)
        store.start(startContext())
        try {
            val bare = DiscoveryCheckpoint(
                revision = 0,
                codecId = "bare-codec",
                codecVersion = 0,
                payload = CheckpointPayload.from(ByteArray(0)),
                completed = true,
            )
            assertTrue(store.compareAndSet("p", "d", null, bare))
            assertEquals(bare, store.load("p", "d"))
            assertEquals(false, store.compareAndSet("p", "d", 3, bare.copy(revision = 4)))
            assertEquals(bare, store.load("p", "d"))
            val next = checkpoint(1)
            assertTrue(store.compareAndSet("p", "d", 0, next))
            assertEquals(next, store.load("p", "d"))

            store.quarantine("p", "d", bare, "incompatible")
            assertEquals(
                "1",
                database.string(
                    "SELECT COUNT(*) FROM \"$schema\".\"discovery_checkpoint_quarantine\" " +
                        "WHERE generation_id IS NULL AND strategy_id IS NULL AND completed AND reason = 'incompatible'",
                ),
            )
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `harvest state reports absence validates ownership and payload bounds and advances revisions`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema, configuration = configuration(schema, maxCheckpointPayloadBytes = 2))
        store.start(startContext())
        try {
            assertNull(store.loadHarvestState("p", "strategy"))
            val state0 = HarvestState(0, "strategy", "codec", 1, CheckpointPayload.from(byteArrayOf(1, 2)))
            assertEquals(
                "Harvest state strategy does not match its key",
                assertFailsWith<IllegalArgumentException> {
                    store.compareAndSetHarvestState("p", "other", null, state0)
                }.message,
            )
            val oversized = state0.copy(payload = CheckpointPayload.from(byteArrayOf(1, 2, 3)))
            assertEquals(
                "Harvest payload exceeds the PostgreSQL state bound",
                assertFailsWith<IllegalArgumentException> {
                    store.compareAndSetHarvestState("p", "strategy", null, oversized)
                }.message,
            )
            assertEquals(
                "Harvest payload exceeds the PostgreSQL state bound",
                assertFailsWith<IllegalArgumentException> {
                    store.quarantineHarvestState("p", "strategy", oversized, "too large")
                }.message,
            )
            assertNull(store.loadHarvestState("p", "strategy"))

            assertTrue(store.compareAndSetHarvestState("p", "strategy", null, state0))
            val state1 = state0.copy(revision = 1, payload = CheckpointPayload.from(byteArrayOf(9)), historicalComplete = true)
            assertTrue(store.compareAndSetHarvestState("p", "strategy", 0, state1))
            assertEquals(false, store.compareAndSetHarvestState("p", "strategy", 0, state1))
            assertEquals(state1, store.loadHarvestState("p", "strategy"))
            assertEquals(0L, database.long("SELECT COUNT(*) FROM \"$schema\".\"harvest_state_quarantine\""))
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `pipeline health and overrides round-trip absent values and reject non-successor revisions`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema)
        store.start(startContext())
        try {
            assertNull(store.loadPipelineHealth("p"))
            assertFailsWith<IllegalArgumentException> {
                store.compareAndSetPipelineHealth("p", null, PipelineHealthSnapshot(revision = 1))
            }
            val empty = PipelineHealthSnapshot()
            assertTrue(store.compareAndSetPipelineHealth("p", null, empty))
            assertEquals(empty, store.loadPipelineHealth("p"))
            assertEquals(
                "Next pipeline-health revision must immediately follow the expected revision",
                assertFailsWith<IllegalArgumentException> {
                    store.compareAndSetPipelineHealth("p", 0, PipelineHealthSnapshot(revision = 2))
                }.message,
            )
            val observed = PipelineHealthSnapshot(1, 1, 0, 1, NOW, NOW.plusSeconds(30))
            assertTrue(store.compareAndSetPipelineHealth("p", 0, observed))
            assertEquals(observed, store.loadPipelineHealth("p"))

            assertNull(store.loadEligibilityOverride("p"))
            val open = PipelineEligibilityOverride(enabled = true)
            store.setEligibilityOverride("p", open)
            assertEquals(open, store.loadEligibilityOverride("p"))
            val closed = PipelineEligibilityOverride(false, "maintenance", NOW.plusSeconds(60))
            store.setEligibilityOverride("p", closed)
            assertEquals(closed, store.loadEligibilityOverride("p"))
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `a concurrent first writer wins harvest and health inserts that race past the row lock`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema)
        store.start(startContext())
        try {
            installConcurrentInsertTrigger(
                schema,
                "harvest_state",
                """
                INSERT INTO "$schema"."harvest_state" (
                    pipeline_id, strategy_id, revision, codec_id, codec_version, payload, historical_complete, updated_at
                ) VALUES (NEW.pipeline_id, NEW.strategy_id, 0, 'concurrent-codec', 7, decode('07', 'hex'), TRUE, now());
                """,
            )
            installConcurrentInsertTrigger(
                schema,
                "pipeline_health",
                """
                INSERT INTO "$schema"."pipeline_health" (pipeline_id, revision, consecutive_failures, updated_at)
                VALUES (NEW.pipeline_id, 0, 7, now());
                """,
            )
            val mine = HarvestState(0, "strategy", "codec", 1, CheckpointPayload.from(byteArrayOf(1)))
            assertEquals(false, store.compareAndSetHarvestState("p", "strategy", null, mine))
            assertEquals(
                HarvestState(0, "strategy", "concurrent-codec", 7, CheckpointPayload.from(byteArrayOf(7)), true),
                store.loadHarvestState("p", "strategy"),
            )
            assertEquals(false, store.compareAndSetPipelineHealth("p", null, PipelineHealthSnapshot()))
            assertEquals(7, store.loadPipelineHealth("p")?.consecutiveFailures)
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `a revisioned update that loses its locked row reports CAS failure without a partial write`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema)
        store.start(startContext())
        try {
            val state0 = HarvestState(0, "strategy", "codec", 1, CheckpointPayload.from(byteArrayOf(1)))
            val health0 = PipelineHealthSnapshot()
            assertTrue(store.compareAndSetHarvestState("p", "strategy", null, state0))
            assertTrue(store.compareAndSetPipelineHealth("p", null, health0))
            database.execute(
                "CREATE FUNCTION \"$schema\".suppress_update() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END;'",
                "CREATE TRIGGER suppress_harvest BEFORE UPDATE ON \"$schema\".\"harvest_state\" " +
                    "FOR EACH ROW EXECUTE FUNCTION \"$schema\".suppress_update()",
                "CREATE TRIGGER suppress_health BEFORE UPDATE ON \"$schema\".\"pipeline_health\" " +
                    "FOR EACH ROW EXECUTE FUNCTION \"$schema\".suppress_update()",
            )

            assertEquals(false, store.compareAndSetHarvestState("p", "strategy", 0, state0.copy(revision = 1)))
            assertEquals(false, store.compareAndSetPipelineHealth("p", 0, health0.copy(revision = 1, totalSuccesses = 1)))
            assertEquals(state0, store.loadHarvestState("p", "strategy"))
            assertEquals(health0, store.loadPipelineHealth("p"))
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `active deduplication stays authoritative when a concurrent writer bypasses the capacity lock`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema)
        store.start(startContext())
        try {
            database.execute(
                """
                CREATE FUNCTION "$schema".concurrent_job() RETURNS trigger LANGUAGE plpgsql AS ${'$'}${'$'}
                BEGIN
                    IF pg_trigger_depth() = 1 THEN
                        INSERT INTO "$schema"."jobs" (
                            job_id, pipeline_id, resource_id, request_codec_id, request_codec_version,
                            request_payload, status, enqueued_at, updated_at
                        ) VALUES ('concurrent-' || NEW.job_id, NEW.pipeline_id, NEW.resource_id, NEW.request_codec_id,
                                  NEW.request_codec_version, NEW.request_payload, 'QUEUED', NEW.enqueued_at, NEW.updated_at);
                    END IF;
                    RETURN NEW;
                END;
                ${'$'}${'$'}
                """.trimIndent(),
                "CREATE TRIGGER concurrent_job BEFORE INSERT ON \"$schema\".\"jobs\" FOR EACH ROW EXECUTE FUNCTION \"$schema\".concurrent_job()",
            )
            val jobRace = assertIs<EnqueueResult.AlreadyActive>(
                store.enqueue(request("job-race"), RequestIdentity("job-race"), context("p")),
            )
            assertTrue(assertNotNull(jobRace.workItemId).value.startsWith("concurrent-postgres-work-"))
            assertEquals(1, store.history("p", RequestIdentity("job-race")).activeWorkCount)
            database.execute("DROP TRIGGER concurrent_job ON \"$schema\".\"jobs\"")

            database.execute(
                """
                CREATE FUNCTION "$schema".concurrent_resource_job() RETURNS trigger LANGUAGE plpgsql AS ${'$'}${'$'}
                BEGIN
                    INSERT INTO "$schema"."jobs" (
                        job_id, pipeline_id, resource_id, request_codec_id, request_codec_version,
                        request_payload, status, enqueued_at, updated_at
                    ) VALUES ('resource-race-' || NEW.resource_id, NEW.pipeline_id, NEW.resource_id, 'concurrent-codec',
                              1, decode('00', 'hex'), 'QUEUED', now(), now());
                    RETURN NEW;
                END;
                ${'$'}${'$'}
                """.trimIndent(),
                "CREATE TRIGGER concurrent_resource_job AFTER INSERT ON \"$schema\".\"resources\" " +
                    "FOR EACH ROW EXECUTE FUNCTION \"$schema\".concurrent_resource_job()",
            )
            val resourceRace = assertIs<EnqueueResult.AlreadyActive>(
                store.enqueue(request("resource-race"), RequestIdentity("resource-race"), context("p")),
            )
            val resourceId = database.long(
                "SELECT resource_id FROM \"$schema\".\"resources\" WHERE identity_value = 'resource-race'",
            )
            assertEquals(WorkItemId("resource-race-$resourceId"), resourceRace.workItemId)
            assertEquals(1, store.history("p", RequestIdentity("resource-race")).activeWorkCount)
            assertEquals(2, store.activeWorkCount("p"))
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `lost locked rows abort claim reclaim and transition without partial lease or attempt mutation`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema)
        store.start(startContext())
        try {
            database.execute(
                """
                CREATE FUNCTION "$schema".suppress_update() RETURNS trigger LANGUAGE plpgsql AS
                'BEGIN RETURN NULL; END;'
                """.trimIndent(),
            )
            val id = assertIs<EnqueueResult.Accepted>(
                store.enqueue(request("locked"), RequestIdentity("locked"), context("p")),
            ).workItemId

            suppressJobUpdate(schema, "suppress_claim", "OLD.status = 'QUEUED' AND NEW.status = 'LEASED'")
            assertEquals(
                "PostgreSQL claim lost a locked job row",
                assertFailsWith<IllegalArgumentException> { store.claim(1, "owner-a", NOW) }.message,
            )
            assertEquals("QUEUED", jobColumn(schema, id, "status"))
            assertNull(jobColumn(schema, id, "lease_token"))
            database.execute("DROP TRIGGER suppress_claim ON \"$schema\".\"jobs\"")

            val work = store.claim(1, "owner-a", NOW).single()
            val attempt = assertIs<AttemptStartResult.Started>(store.beginAttempt(work, NOW)).attempt
            suppressJobUpdate(schema, "suppress_reclaim", "OLD.status = 'LEASED' AND NEW.status = 'QUEUED'")
            assertEquals(
                "PostgreSQL lease reclaim lost a locked job row",
                assertFailsWith<IllegalArgumentException> { store.claim(1, "owner-b", work.expiresAt) }.message,
            )
            assertNull(attemptOutcome(schema, attempt.id.value))
            assertEquals("owner-a", jobColumn(schema, id, "lease_owner"))
            assertEquals(work.leaseToken.value, jobColumn(schema, id, "lease_token"))
            database.execute("DROP TRIGGER suppress_reclaim ON \"$schema\".\"jobs\"")

            suppressJobUpdate(schema, "suppress_success", "OLD.status = 'LEASED' AND NEW.status = 'SUCCEEDED'")
            assertEquals(LeaseMutationResult.Stale, store.transition(work, WorkTransition.Succeeded))
            assertEquals("LEASED", jobColumn(schema, id, "status"))
            assertNull(attemptOutcome(schema, attempt.id.value))
            assertEquals(0L, store.history("p", RequestIdentity("locked")).visitCount)
            database.execute("DROP TRIGGER suppress_success ON \"$schema\".\"jobs\"")

            assertEquals(LeaseMutationResult.Accepted, store.transition(work, WorkTransition.Succeeded))
            assertEquals(AttemptOutcome.SUCCEEDED.name, attemptOutcome(schema, attempt.id.value))
            assertEquals(1L, store.history("p", RequestIdentity("locked")).visitCount)
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `finishing an attempt closed out of band fails and rolls back the job mutation`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema)
        store.start(startContext())
        try {
            val id = assertIs<EnqueueResult.Accepted>(
                store.enqueue(request("closed"), RequestIdentity("closed"), context("p")),
            ).workItemId
            val work = store.claim(1, "owner", NOW).single()
            val attempt = assertIs<AttemptStartResult.Started>(store.beginAttempt(work, NOW)).attempt
            assertEquals(
                1,
                database.update(
                    "UPDATE \"$schema\".\"attempts\" SET outcome = 'FAILED', finished_at = now() WHERE attempt_id = ?",
                    attempt.id.value,
                ),
            )

            assertEquals(
                "PostgreSQL attempt completion found no open attempt",
                assertFailsWith<IllegalArgumentException> {
                    store.finishAttempt(work, attempt, AttemptOutcome.SUCCEEDED)
                }.message,
            )
            assertEquals(attempt.id.value, jobColumn(schema, id, "active_attempt_id"))
            assertEquals("FAILED", attemptOutcome(schema, attempt.id.value))
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `unknown resources have empty history`(): Unit = runBlocking {
        val store = store(nextSchema())
        store.start(startContext())
        try {
            assertEquals(ResourceHistory(lastCompletedAt = null, activeWorkCount = 0, visitCount = 0), store.history("p", RequestIdentity("never")))
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `queued cancellation rejects non-terminal transitions and stops at the active-job bound`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(
            schema,
            configuration = configuration(schema, maxActiveJobs = 2, maxClaimBatch = 1, maxCancellationRows = 1),
        )
        store.start(startContext())
        try {
            assertEquals(
                "PostgreSQL queued cancellation requires a terminal transition",
                assertFailsWith<IllegalArgumentException> { store.cancelQueued("p", WorkTransition.Succeeded) }.message,
            )
            assertEquals(0, store.cancelQueued("p", WorkTransition.Failed("drained")))
            val ids = listOf("one", "two").map { identity ->
                assertIs<EnqueueResult.Accepted>(store.enqueue(request(identity), RequestIdentity(identity), context("p"))).workItemId
            }
            assertEquals(2, store.cancelQueued("p", WorkTransition.Failed("drained")))
            ids.forEach { id ->
                assertEquals("FAILED", jobColumn(schema, id, "status"))
                assertEquals("drained", jobColumn(schema, id, "reason"))
                assertNotNull(jobColumn(schema, id, "finished_at"))
            }
            assertEquals(0, store.activeWorkCount("p"))
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `stale run recovery validates its threshold and never undercuts the configured minimum`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema, configuration = configuration(schema, staleRunAfter = Duration.ofSeconds(10)))
        store.start(startContext())
        try {
            listOf(Duration.ZERO, Duration.ofSeconds(-1)).forEach { threshold ->
                assertEquals(
                    "Stale-run threshold must be positive",
                    assertFailsWith<IllegalArgumentException> { store.recoverStaleRuns(NOW, threshold) }.message,
                )
            }
            store.startEngineRun("engine", NOW.minusSeconds(20), null)
            assertEquals(0, store.recoverStaleRuns(NOW, Duration.ofSeconds(60)))
            assertEquals("RUNNING", database.string("SELECT status FROM \"$schema\".\"engine_runs\" WHERE engine_run_id = 'engine'"))
            store.startEngineRun("recent", NOW.minusSeconds(5), null)
            assertEquals(1, store.recoverStaleRuns(NOW, Duration.ofSeconds(1)))
            assertEquals("ABANDONED", database.string("SELECT status FROM \"$schema\".\"engine_runs\" WHERE engine_run_id = 'engine'"))
            assertEquals("RUNNING", database.string("SELECT status FROM \"$schema\".\"engine_runs\" WHERE engine_run_id = 'recent'"))
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `durable run completion rejects RUNNING and negative failure counts without mutation`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema)
        store.start(startContext())
        try {
            store.startEngineRun("engine", NOW, "manual")
            store.startPipelineRun("engine", "pipeline-run", "p", NOW)
            val invalid = listOf<Pair<String, suspend () -> Unit>>(
                "A durable run cannot be completed as RUNNING" to {
                    store.completePipelineRun("pipeline-run", DurableRunStatus.RUNNING, NOW, 0)
                },
                "Durable run failure count must not be negative" to {
                    store.completePipelineRun("pipeline-run", DurableRunStatus.COMPLETED, NOW, -1)
                },
                "A durable run cannot be completed as RUNNING" to {
                    store.completeEngineRun("engine", DurableRunStatus.RUNNING, NOW, 0)
                },
                "Durable run failure count must not be negative" to {
                    store.completeEngineRun("engine", DurableRunStatus.FAILED, NOW, -1)
                },
            )
            invalid.forEach { (message, call) ->
                assertEquals(message, assertFailsWith<IllegalArgumentException> { call() }.message)
            }
            assertEquals("RUNNING", database.string("SELECT status FROM \"$schema\".\"engine_runs\""))
            assertEquals("RUNNING", database.string("SELECT status FROM \"$schema\".\"pipeline_runs\""))
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `enqueue cancelled while waiting for the capacity lock rolls back and the store stays usable`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema)
        store.start(startContext())
        try {
            val holder = database.openTransaction(
                "SELECT pg_advisory_xact_lock(hashtext('trawler-active-capacity:$schema'))",
            )
            val pending = try {
                val pending = async(Dispatchers.Default) {
                    store.enqueue(request("cancelled"), RequestIdentity("cancelled"), context("p"))
                }
                database.awaitLockWaiter("%pg_advisory_xact_lock%")
                pending.cancel()
                pending
            } finally {
                holder.commit()
                holder.close()
            }
            assertIs<CancellationException>(runCatching { pending.await() }.exceptionOrNull())
            assertEquals(0L, database.long("SELECT COUNT(*) FROM \"$schema\".\"jobs\""))
            assertEquals(0L, database.long("SELECT COUNT(*) FROM \"$schema\".\"resources\""))

            assertIs<EnqueueResult.Accepted>(store.enqueue(request("cancelled"), RequestIdentity("cancelled"), context("p")))
            assertEquals(1, store.activeWorkCount("p"))
        } finally {
            store.stop(stopContext())
        }
    }

    private fun item(identity: String, target: String = identity): DiscoveryWorkItem =
        DiscoveryWorkItem(request(target), RequestIdentity(identity), context("p"))

    private fun installConcurrentInsertTrigger(schema: String, table: String, insert: String) {
        database.execute(
            """
            CREATE FUNCTION "$schema".concurrent_$table() RETURNS trigger LANGUAGE plpgsql AS ${'$'}${'$'}
            BEGIN
                IF pg_trigger_depth() = 1 THEN
                    ${insert.trimIndent()}
                END IF;
                RETURN NEW;
            END;
            ${'$'}${'$'}
            """.trimIndent(),
            "CREATE TRIGGER concurrent_$table BEFORE INSERT ON \"$schema\".\"$table\" " +
                "FOR EACH ROW EXECUTE FUNCTION \"$schema\".concurrent_$table()",
        )
    }

    private fun suppressJobUpdate(schema: String, name: String, condition: String) {
        database.execute(
            "CREATE TRIGGER $name BEFORE UPDATE ON \"$schema\".\"jobs\" FOR EACH ROW " +
                "WHEN ($condition) EXECUTE FUNCTION \"$schema\".suppress_update()",
        )
    }

    private fun jobColumn(schema: String, id: WorkItemId, column: String): String? =
        database.string("SELECT \"$column\" FROM \"$schema\".\"jobs\" WHERE job_id = ?", id.value)

    private fun attemptOutcome(schema: String, attemptId: String): String? =
        database.string("SELECT outcome FROM \"$schema\".\"attempts\" WHERE attempt_id = ?", attemptId)

    private fun store(
        schema: String,
        id: String = "contract-$schema",
        configuration: PostgresStateConfiguration = configuration(schema),
        codecs: PostgresRequestCodecRegistry = PostgresRequestCodecRegistry(
            listOf(BinaryPostgresRequestCodec(configuration.requestCodecLimits())),
        ),
        clock: Clock = Clock { NOW },
    ): PostgresCrawlStateStore = PostgresCrawlStateStore(id, configuration, codecs, clock)

    private fun configuration(
        schema: String,
        maxActiveJobs: Int = 100,
        maxClaimBatch: Int = 16,
        maxCancellationRows: Int = 4_096,
        maxCheckpointBatch: Int = 64,
        maxCheckpointPayloadBytes: Int = 1_024,
        maxEncodedRequestBytes: Int = 8 * 1_024 * 1_024,
        maxRequestBodyBytes: Int = 8 * 1_024 * 1_024,
        maxTargetBytes: Int = 16 * 1_024,
        maxHeaderCount: Int = 256,
        maxHeaderNameBytes: Int = 1_024,
        maxHeaderValueBytes: Int = 8 * 1_024,
        maxAttributeCount: Int = 256,
        maxAttributeNameBytes: Int = 1_024,
        maxAttributeValueBytes: Int = 8 * 1_024,
        staleRunAfter: Duration = Duration.ofMinutes(30),
    ): PostgresStateConfiguration = PostgresStateConfiguration(
        jdbcUrl = database.jdbcUrl,
        username = database.username,
        password = database.password.toCharArray(),
        schema = schema,
        maximumPoolSize = 2,
        maxActiveJobs = maxActiveJobs,
        maxClaimBatch = maxClaimBatch,
        maxCancellationRows = maxCancellationRows,
        maxCheckpointBatch = maxCheckpointBatch,
        maxCheckpointPayloadBytes = maxCheckpointPayloadBytes,
        maxEncodedRequestBytes = maxEncodedRequestBytes,
        maxRequestBodyBytes = maxRequestBodyBytes,
        maxTargetBytes = maxTargetBytes,
        maxHeaderCount = maxHeaderCount,
        maxHeaderNameBytes = maxHeaderNameBytes,
        maxHeaderValueBytes = maxHeaderValueBytes,
        maxAttributeCount = maxAttributeCount,
        maxAttributeNameBytes = maxAttributeNameBytes,
        maxAttributeValueBytes = maxAttributeValueBytes,
        staleRunAfter = staleRunAfter,
    )

    private fun request(target: String): CrawlRequest = CrawlRequest("https://example.test/$target")

    private fun context(pipelineId: String): CrawlerContext = CrawlerContext("engine", "run-$pipelineId", pipelineId)

    private fun checkpoint(revision: Long): DiscoveryCheckpoint = DiscoveryCheckpoint(
        revision = revision,
        codecId = "checkpoint-codec",
        codecVersion = 1,
        payload = CheckpointPayload.from(byteArrayOf(revision.toByte())),
        generationId = "generation",
        strategyId = "strategy",
    )

    private fun startContext(): ExtensionStartContext = ExtensionStartContext(CoroutineScope(Dispatchers.Default))

    private fun stopContext(): ExtensionStopContext = ExtensionStopContext(ExtensionStopReason.ENGINE_SHUTDOWN)

    /** Caller-owned codec whose output deliberately exceeds the adapter bound. */
    private class OversizedCodec(private val size: Int) : PostgresRequestCodec {
        override val descriptor: PostgresCodecDescriptor = PostgresCodecDescriptor("oversized", 1)

        override fun encode(request: CrawlRequest): ByteArray = ByteArray(size)

        override fun decode(payload: ByteArray): CrawlRequest = error("Oversized payloads are never stored")
    }

    private companion object {
        val NOW: Instant = Instant.parse("2026-01-01T00:00:00Z")
        val schemaSequence = AtomicInteger()

        @Container
        @JvmField
        val postgres: PostgreSQLContainer<Nothing> = PostgreSQLContainer<Nothing>("postgres:16.4-alpine")

        val database: PostgresTestDatabase by lazy { PostgresTestDatabase(postgres) }

        fun nextSchema(): String = "state_contract_${schemaSequence.incrementAndGet()}"
    }
}
