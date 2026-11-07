package com.rmjtromp.trawler.postgres

import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.extension.ExtensionStopReason
import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.model.RequestBody
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.model.ResponseReference
import com.rmjtromp.trawler.output.OutputCodec
import com.rmjtromp.trawler.output.OutputCodecDescriptor
import com.rmjtromp.trawler.output.OutputCodecRegistry
import com.rmjtromp.trawler.output.OutputRetentionRequest
import com.rmjtromp.trawler.pipeline.Output
import com.rmjtromp.trawler.pipeline.OutputResult
import com.rmjtromp.trawler.pipeline.Produced
import com.rmjtromp.trawler.pipeline.ProductionContext
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.response.InMemoryResponseStore
import com.rmjtromp.trawler.response.ResponseCodec
import com.rmjtromp.trawler.response.ResponseCodecDescriptor
import com.rmjtromp.trawler.response.ResponseCodecRegistry
import com.rmjtromp.trawler.response.ResponseLookup
import com.rmjtromp.trawler.response.ResponseReplayQuery
import com.rmjtromp.trawler.retention.RetentionCategory
import com.zaxxer.hikari.pool.HikariPool
import java.nio.ByteBuffer
import java.sql.SQLException
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.flywaydb.core.api.FlywayException
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * Contract coverage for adapter-owned pool/dispatcher lifecycle, cancellation
 * during an open transaction, and the response, output, and outbox paths not
 * exercised by their focused suites. Each test owns a fresh schema.
 */
@Testcontainers
class PostgresAdapterContractIT {
    @Test
    fun `an unreachable database fails pool construction without leaking threads and stays retryable`(): Unit = runBlocking {
        val output = PostgresOutput(
            id = "unreachable-output",
            configuration = PostgresStateConfiguration(
                jdbcUrl = "jdbc:postgresql://127.0.0.1:1/unreachable",
                schema = "unreachable",
                maximumPoolSize = 2,
                connectionTimeout = Duration.ofMillis(250),
            ),
            codecs = intCodecs(),
        )

        repeat(2) {
            val failure = assertFailsWith<HikariPool.PoolInitializationException> { output.start(startContext()) }
            assertNotNull(failure.cause)
        }
        val notStarted = assertIs<OutputResult.Failed>(output.write(listOf(produced(1)), outputContext()))
        assertEquals("PostgreSQL adapter 'unreachable-output' is not started", notStarted.error.message)
        assertNoAdapterThreads("unreachable-output")

        output.stop(stopContext())
        assertEquals(
            "PostgreSQL adapter 'unreachable-output' is closed",
            assertFailsWith<IllegalStateException> { output.start(startContext()) }.message,
        )
    }

    @Test
    fun `the minimum pool starts with credentials embedded in the JDBC URL and no separate username or password`(): Unit = runBlocking {
        val schema = nextSchema()
        val separator = if ('?' in database.jdbcUrl) "&" else "?"
        val configuration = PostgresStateConfiguration(
            jdbcUrl = "${database.jdbcUrl}${separator}user=${database.username}&password=${database.password}",
            schema = schema,
            maximumPoolSize = 2,
        )
        val output = PostgresOutput(id = "url-credentials", configuration = configuration, codecs = intCodecs())

        assertNull(configuration.username)
        assertNull(configuration.passwordCopy())
        assertTrue(configuration.toString().contains("?<redacted>"))
        assertFalse(configuration.toString().contains("password="))
        output.start(startContext())
        try {
            assertEquals(OutputResult.Accepted(1), output.write(listOf(produced(1)), outputContext()))
            assertEquals(1L, database.long("SELECT COUNT(*) FROM \"$schema\".\"output_entries\""))
        } finally {
            output.stop(stopContext())
        }
        assertNoAdapterThreads("url-credentials")
    }

    @Test
    fun `failed migration releases pool threads and the same adapter recovers with repeatable replay activation`(): Unit = runBlocking {
        val schema = nextSchema()
        database.execute(
            "CREATE SCHEMA \"$schema\"",
            "CREATE TABLE \"$schema\".\"flyway_response_schema_history\" (bad INTEGER)",
        )
        val store = PostgresResponseStore(
            id = "migration-$schema",
            configuration = configuration(schema),
            codecs = ResponseCodecRegistry(listOf(BodyCodec())),
            clock = Clock { BASE },
        )

        assertFailsWith<FlywayException> { store.start(startContext()) }
        assertNoAdapterThreads("migration-$schema")
        assertFailsWith<IllegalStateException> { store.find(ResponseLookup.ByTarget("https://example.test/x")) }
        database.execute("DROP TABLE \"$schema\".\"flyway_response_schema_history\"")

        store.start(startContext())
        val reference = assertNotNull(store.put(response("migrated", pipelineId = "p")))
        store.stopForReplay(stopContext())
        assertNoAdapterThreads("migration-$schema")
        store.startForReplay(startContext())
        store.startForReplay(startContext())
        assertEquals(reference, store.find(ResponseLookup.ByReference(reference))?.reference)
        store.stop(stopContext())
        store.stopForReplay(stopContext())
        store.close()
        assertNoAdapterThreads("migration-$schema")
        assertFailsWith<IllegalStateException> { store.startForReplay(startContext()) }
    }

    @Test
    fun `output and outbox writes cancelled inside an open transaction roll back instead of committing`(): Unit = runBlocking {
        val schema = nextSchema()
        val output = PostgresOutput(id = "cancel-output", configuration = configuration(schema), codecs = intCodecs())
        val outbox = PostgresOutboxOutput(
            id = "cancel-outbox",
            configuration = configuration(schema),
            codecs = intCodecs(),
            delegate = RecordingDelegate(),
        )
        output.start(startContext())
        outbox.start(startContext())
        try {
            listOf<Pair<String, suspend () -> OutputResult>>(
                "output_entries" to { output.write(listOf(produced(1)), outputContext()) },
                "outbox_entries" to { outbox.write(listOf(produced(1)), outputContext()) },
            ).forEach { (table, write) ->
                val pending = cancelWhileBlocked(
                    lockSql = "LOCK TABLE \"$schema\".\"$table\" IN ACCESS EXCLUSIVE MODE",
                    waitingQuery = "%INSERT INTO%$table%",
                    operation = write,
                )
                assertIs<CancellationException>(runCatching { pending.await() }.exceptionOrNull())
                assertEquals(0L, database.long("SELECT COUNT(*) FROM \"$schema\".\"$table\""), "$table committed after cancellation")
                assertEquals(OutputResult.Accepted(1), write())
                assertEquals(1L, database.long("SELECT COUNT(*) FROM \"$schema\".\"$table\""))
            }
        } finally {
            outbox.stop(stopContext())
            output.stop(stopContext())
        }
    }

    @Test
    fun `a connection terminated mid-transaction keeps the original failure and rollback failure and the pool recovers`(): Unit = runBlocking {
        val schema = nextSchema()
        val output = PostgresOutput(id = "terminated-output", configuration = configuration(schema), codecs = intCodecs())
        output.start(startContext())
        try {
            val waiting = "%INSERT INTO%output_entries%"
            val holder = database.openTransaction("LOCK TABLE \"$schema\".\"output_entries\" IN ACCESS EXCLUSIVE MODE")
            val result = try {
                val pending = async(Dispatchers.Default) { output.write(listOf(produced(1)), outputContext()) }
                database.awaitLockWaiter(waiting)
                assertEquals(
                    "t",
                    database.string(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE ?",
                        waiting,
                    ),
                )
                pending.await()
            } finally {
                holder.commit()
                holder.close()
            }

            val error = assertIs<SQLException>(assertIs<OutputResult.Failed>(result).error)
            assertEquals("57P01", error.sqlState, "Expected the administrator-termination failure, got: $error")
            val rollbackFailure = assertIs<SQLException>(error.suppressed.single(), "Rollback failure must be retained as suppressed")
            assertEquals("Connection is closed", rollbackFailure.message, "Unexpected rollback failure: $rollbackFailure")
            assertEquals(0L, database.long("SELECT COUNT(*) FROM \"$schema\".\"output_entries\""))

            assertEquals(OutputResult.Accepted(1), output.write(listOf(produced(2)), outputContext()))
            assertEquals(1L, database.long("SELECT COUNT(*) FROM \"$schema\".\"output_entries\""))
        } finally {
            output.stop(stopContext())
        }
    }

    @Test
    fun `lineage is recorded only when the response adapter records table exists beside a lineage table`(): Unit = runBlocking {
        val schema = nextSchema()
        database.execute(
            "CREATE SCHEMA \"$schema\"",
            "CREATE TABLE \"$schema\".\"response_lineage\" (" +
                "response_reference VARCHAR(255), owner_kind VARCHAR(16), owner_id VARCHAR(255), recorded_at TIMESTAMPTZ)",
        )
        val output = PostgresOutput(id = "partial-output", configuration = configuration(schema), codecs = intCodecs())
        val outbox = outbox(schema, id = "partial-outbox")
        output.start(startContext())
        outbox.start(startContext())
        try {
            val reference = ResponseReference("host-reference")
            assertEquals(OutputResult.Accepted(1), output.write(listOf(produced(1, reference)), outputContext()))
            assertEquals(OutputResult.Accepted(1), outbox.write(listOf(produced(2, reference)), outputContext()))

            assertFalse(tableExists(schema, "response_records"))
            assertEquals(0L, database.long("SELECT COUNT(*) FROM \"$schema\".\"response_lineage\""))
            assertEquals(reference.value, database.string("SELECT response_reference FROM \"$schema\".\"output_entries\""))
            assertEquals(reference.value, database.string("SELECT response_reference FROM \"$schema\".\"outbox_entries\""))
        } finally {
            outbox.stop(stopContext())
            output.stop(stopContext())
        }
    }

    @Test
    fun `replay by reference and request ID honors the owner predicate while unowned responses stay ownerless`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = responseStore(schema)
        store.start(startContext())
        try {
            val first = assertNotNull(store.put(response("first", pipelineId = "a", requestId = "req")))
            val second = assertNotNull(store.put(response("second", pipelineId = "b", requestId = "req")))
            val unowned = assertNotNull(store.put(response("unowned", pipelineId = null, requestId = null)))
            val withBody = assertNotNull(
                store.put(response("with-body", pipelineId = "a", requestBody = byteArrayOf(1, 2))),
            )

            assertEquals(listOf(first), store.replay(ResponseReplayQuery.ByReference(first), 10).map { it.reference })
            assertEquals(listOf(first), store.replay(ResponseReplayQuery.ByReference(first), 10, "a").map { it.reference })
            assertEquals(emptyList(), store.replay(ResponseReplayQuery.ByReference(first), 10, "b"))
            assertEquals(listOf(second), store.replay(ResponseReplayQuery.ByRequestId("req"), 10, "b").map { it.reference })
            assertEquals(listOf(first, second), store.history("req", 10).map { it.reference })

            val stored = assertNotNull(store.find(ResponseLookup.ByReference(unowned)))
            assertNull(stored.pipelineId)
            assertNull(database.string("SELECT pipeline_id FROM \"$schema\".\"response_records\" WHERE response_id = ?", unowned.value))
            assertNull(database.string("SELECT request_id FROM \"$schema\".\"response_records\" WHERE response_id = ?", unowned.value))
            assertEquals("a", store.find(ResponseLookup.ByReference(withBody))?.pipelineId)
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `evictBefore applies the strict cutoff within the adapter retention bound`(): Unit = runBlocking {
        val schema = nextSchema()
        val clock = MutableClock(BASE)
        val store = responseStore(schema, clock = clock, configuration = configuration(schema, maxRetentionRows = 1))
        store.start(startContext())
        try {
            store.put(response("old-1", pipelineId = "p"))
            store.put(response("old-2", pipelineId = "p"))
            clock.current = BASE.plusSeconds(1)
            val retained = assertNotNull(store.put(response("at-cutoff", pipelineId = "p")))

            assertEquals(1L, store.evictBefore(clock.current))
            assertEquals(1L, store.evictBefore(clock.current))
            assertEquals(0L, store.evictBefore(clock.current))
            assertEquals(
                listOf(retained),
                store.replay(ResponseReplayQuery.ByPipelineId("p"), 10).map { it.reference },
            )
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `decoded responses are revalidated against body request-body and target bounds`(): Unit = runBlocking {
        val schema = nextSchema()
        val codec = ShapingCodec()
        val store = responseStore(
            schema,
            codecs = ResponseCodecRegistry(listOf(codec)),
            configuration = configuration(schema, maxResponseBodyBytes = 2, maxRequestBodyBytes = 1, maxTargetBytes = 64),
        )
        store.start(startContext())
        try {
            val reference = assertNotNull(store.put(response("shaped", pipelineId = "p")))
            val lookup = ResponseLookup.ByReference(reference)

            codec.shape = { it }
            assertEquals(reference, store.find(lookup)?.reference)
            codec.shape = { it.copy(request = it.request.copy(body = RequestBody.from(byteArrayOf(1)))) }
            assertEquals(1, store.find(lookup)?.response?.request?.body?.size)

            val violations = listOf<Pair<String, (CrawlResponse) -> CrawlResponse>>(
                "Decoded response body exceeds the configured bound" to {
                    it.copy(body = ResponseBody.from(byteArrayOf(1, 2, 3)))
                },
                "Decoded response request body exceeds the configured bound" to {
                    it.copy(request = it.request.copy(body = RequestBody.from(byteArrayOf(1, 2))))
                },
                "decoded response target exceeds the configured PostgreSQL bound" to {
                    it.copy(request = it.request.copy(target = "https://example.test/" + "x".repeat(64)))
                },
            )
            violations.forEach { (message, shape) ->
                codec.shape = shape
                assertEquals(message, assertFailsWith<IllegalArgumentException> { store.find(lookup) }.message)
            }
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `output rejects oversized references and foreign retention and prunes without a response schema`(): Unit = runBlocking {
        val schema = nextSchema()
        val output = PostgresOutput(
            id = "output",
            configuration = configuration(schema, maxIdentifierBytes = 16),
            codecs = intCodecs(),
            clock = Clock { BASE },
        )
        output.start(startContext())
        output.startForReplay(startContext())
        try {
            val failed = assertIs<OutputResult.Failed>(
                output.write(listOf(produced(1, ResponseReference("r".repeat(17)))), outputContext()),
            )
            assertEquals("output response reference exceeds the configured PostgreSQL bound", failed.error.message)
            assertEquals(0L, database.long("SELECT COUNT(*) FROM \"$schema\".\"output_entries\""))

            assertEquals(
                "PostgreSQL output owns only output history retention",
                assertFailsWith<IllegalArgumentException> {
                    output.prune(OutputRetentionRequest(RetentionCategory.OUTBOX_HISTORY, BASE.plusSeconds(1), 10))
                }.message,
            )
            assertEquals(OutputResult.Accepted(2), output.write(listOf(produced(1, ResponseReference("r")), produced(2)), outputContext()))
            assertFalse(tableExists(schema, "response_lineage"))
            assertEquals(
                2L,
                output.prune(OutputRetentionRequest(RetentionCategory.OUTPUT_HISTORY, BASE.plusSeconds(1), 10)).deleted,
            )
            assertEquals(0L, database.long("SELECT COUNT(*) FROM \"$schema\".\"output_entries\""))
        } finally {
            output.stop(stopContext())
        }
    }

    @Test
    fun `every delegate outcome requeues or reports a stale fence when the lease expires during delivery`(): Unit = runBlocking {
        val schema = nextSchema()
        val clock = MutableClock(BASE)
        var expireDuringDelivery = false
        val delegate = RecordingDelegate { items, _ ->
            if (expireDuringDelivery) clock.current = BASE.plusSeconds(10)
            when (items.single().value) {
                1 -> OutputResult.Accepted(2)
                2 -> OutputResult.Skipped("not useful")
                3 -> OutputResult.Failed(IllegalStateException("failed"))
                else -> throw IllegalArgumentException("thrown")
            }
        }
        val outbox = outbox(
            schema,
            delegate = delegate,
            clock = clock,
            configuration = configuration(schema, maxOutboxAttempts = 3, outboxLeaseDuration = Duration.ofSeconds(10)),
        )
        outbox.start(startContext())
        try {
            (1..4).forEach { assertEquals(OutputResult.Accepted(1), outbox.write(listOf(produced(it)), outputContext())) }

            assertEquals(OutboxDeliverySummary(4, 0, 4, 0, 0), outbox.deliverPending("owner", 4, BASE))
            val requeued = outboxRows(schema, "outbox")
            assertTrue(requeued.all { it.status == "PENDING" && it.deliveryAttempts == 1 && it.leaseOwner == null })
            assertEquals(
                listOf(
                    "delegate-ack-count",
                    "delegate-skipped",
                    "delegate-failed:IllegalStateException",
                    "delivery-failed:IllegalArgumentException",
                ),
                requeued.map { it.lastError },
            )

            expireDuringDelivery = true
            assertEquals(OutboxDeliverySummary(4, 0, 0, 0, 4), outbox.deliverPending("owner", 4, BASE))
            val stale = outboxRows(schema, "outbox")
            assertTrue(stale.all { it.status == "LEASED" && it.deliveryAttempts == 2 && it.leaseOwner == "owner" })
            assertEquals(requeued.map { it.lastError }, stale.map { it.lastError })
            assertEquals(listOf(1, 2, 3, 4, 1, 2, 3, 4), delegate.values)
        } finally {
            outbox.stop(stopContext())
        }
    }

    @Test
    fun `anonymous failures and long reasons are recorded as bounded diagnostics`(): Unit = runBlocking {
        val schema = nextSchema()
        val anonymous = outbox(
            schema,
            id = "anonymous",
            delegate = RecordingDelegate { _, _ -> throw object : RuntimeException("anonymous") {} },
        )
        val truncated = outbox(
            schema,
            id = "truncated",
            delegate = RecordingDelegate { _, _ -> OutputResult.Skipped("skip") },
            configuration = configuration(schema, maxReasonBytes = 10),
        )
        anonymous.start(startContext())
        truncated.start(startContext())
        try {
            anonymous.write(listOf(produced(1)), outputContext())
            truncated.write(listOf(produced(2)), outputContext())
            assertEquals(1, anonymous.deliverPending("owner", 1, BASE).retried)
            assertEquals(1, truncated.deliverPending("owner", 1, BASE).retried)
            assertEquals("delivery-failed:failure", outboxRows(schema, "anonymous").single().lastError)
            assertEquals("delegate-s", outboxRows(schema, "truncated").single().lastError)
        } finally {
            truncated.stop(stopContext())
            anonymous.stop(stopContext())
        }
    }

    @Test
    fun `default delivery pass uses the configured batch and injected clock and validates its owner`(): Unit = runBlocking {
        val schema = nextSchema()
        val clock = MutableClock(BASE.plusSeconds(5))
        val observedExpiry = mutableListOf<Timestamp?>()
        val delegate = RecordingDelegate { items, _ ->
            observedExpiry += outboxRows(schema, "outbox").single { decodeInt(it.payload) == items.single().value }.leaseExpiresAt
            OutputResult.Accepted(1)
        }
        val outbox = outbox(
            schema,
            delegate = delegate,
            clock = clock,
            configuration = configuration(
                schema,
                maxIdentifierBytes = 16,
                maxOutboxClaimBatch = 2,
                outboxLeaseDuration = Duration.ofSeconds(10),
            ),
        )
        outbox.start(startContext())
        outbox.startForReplay(startContext())
        try {
            (1..3).forEach { outbox.write(listOf(produced(it)), outputContext()) }
            assertEquals(
                "Outbox delivery owner must not be blank",
                assertFailsWith<IllegalArgumentException> { outbox.deliverPending(" ") }.message,
            )
            assertEquals(
                "outbox delivery owner exceeds the configured PostgreSQL bound",
                assertFailsWith<IllegalArgumentException> { outbox.deliverPending("o".repeat(17)) }.message,
            )

            assertEquals(OutboxDeliverySummary(2, 2, 0, 0, 0), outbox.deliverPending("owner"))
            assertEquals(List(2) { Timestamp.from(BASE.plusSeconds(15)) }, observedExpiry)
            assertEquals(listOf("DELIVERED", "DELIVERED", "PENDING"), outboxRows(schema, "outbox").map { it.status })
        } finally {
            outbox.stop(stopContext())
        }
    }

    @Test
    fun `outbox rejects oversized references and foreign retention and prunes without a response schema`(): Unit = runBlocking {
        val schema = nextSchema()
        val outbox = outbox(schema, configuration = configuration(schema, maxIdentifierBytes = 16))
        outbox.start(startContext())
        try {
            val failed = assertIs<OutputResult.Failed>(
                outbox.write(listOf(produced(1, ResponseReference("r".repeat(17)))), outputContext()),
            )
            assertEquals("outbox response reference exceeds the configured PostgreSQL bound", failed.error.message)
            assertEquals(0L, database.long("SELECT COUNT(*) FROM \"$schema\".\"outbox_entries\""))
            assertEquals(
                "PostgreSQL outbox owns only outbox history retention",
                assertFailsWith<IllegalArgumentException> {
                    outbox.prune(OutputRetentionRequest(RetentionCategory.OUTPUT_HISTORY, BASE.plusSeconds(1), 10))
                }.message,
            )

            outbox.write(listOf(produced(1, ResponseReference("r"))), outputContext())
            assertEquals(1, outbox.deliverPending("owner", 1, BASE).delivered)
            assertFalse(tableExists(schema, "response_lineage"))
            assertEquals(
                1L,
                outbox.prune(OutputRetentionRequest(RetentionCategory.OUTBOX_HISTORY, BASE.plusSeconds(1), 10)).deleted,
            )
            assertEquals(0L, database.long("SELECT COUNT(*) FROM \"$schema\".\"outbox_entries\""))
        } finally {
            outbox.stop(stopContext())
        }
    }

    @Test
    fun `claim refuses an oversized stored payload and a lost claim row without invoking the delegate`(): Unit = runBlocking {
        val schema = nextSchema()
        val delegate = RecordingDelegate()
        val writer = outbox(schema, delegate = delegate)
        val tighter = outbox(
            schema,
            delegate = delegate,
            configuration = configuration(schema, maxOutputPayloadBytes = 3),
        )
        writer.start(startContext())
        tighter.start(startContext())
        try {
            writer.write(listOf(produced(1)), outputContext())
            assertEquals(
                "PostgreSQL outbox payload exceeds its configured bound",
                assertFailsWith<IllegalArgumentException> { tighter.deliverPending("tight", 1, BASE) }.message,
            )

            database.execute(
                "CREATE FUNCTION \"$schema\".suppress_update() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END;'",
                "CREATE TRIGGER suppress_claim BEFORE UPDATE ON \"$schema\".\"outbox_entries\" FOR EACH ROW " +
                    "WHEN (OLD.status = 'PENDING' AND NEW.status = 'LEASED') EXECUTE FUNCTION \"$schema\".suppress_update()",
            )
            assertEquals(
                "Outbox claim was lost before fencing",
                assertFailsWith<IllegalStateException> { writer.deliverPending("owner", 1, BASE) }.message,
            )
            val untouched = outboxRows(schema, "outbox").single()
            assertEquals("PENDING", untouched.status)
            assertEquals(0, untouched.deliveryAttempts)
            assertNull(untouched.leaseToken)
            assertEquals(0, delegate.calls)

            database.execute("DROP TRIGGER suppress_claim ON \"$schema\".\"outbox_entries\"")
            assertEquals(1, writer.deliverPending("owner", 1, BASE).delivered)
            assertEquals(listOf(1), delegate.values)
        } finally {
            tighter.stop(stopContext())
            writer.stop(stopContext())
        }
    }

    /**
     * Starts [operation] while an external transaction holds [lockSql], waits
     * until the adapter's statement is blocked on that lock, cancels the
     * caller, then releases the lock so the adapter finishes its statement
     * under a cancelled job.
     */
    private suspend fun <T> CoroutineScope.cancelWhileBlocked(
        lockSql: String,
        waitingQuery: String,
        operation: suspend () -> T,
    ): Deferred<T> {
        val holder = database.openTransaction(lockSql)
        try {
            val pending = async(Dispatchers.Default) { operation() }
            database.awaitLockWaiter(waitingQuery)
            pending.cancel()
            return pending
        } finally {
            holder.commit()
            holder.close()
        }
    }

    private fun outbox(
        schema: String,
        id: String = "outbox",
        delegate: Output<Int> = RecordingDelegate(),
        clock: Clock = Clock { BASE },
        configuration: PostgresStateConfiguration = configuration(schema),
    ): PostgresOutboxOutput<Int> = PostgresOutboxOutput(id, configuration, intCodecs(), delegate, clock)

    private fun responseStore(
        schema: String,
        codecs: ResponseCodecRegistry = ResponseCodecRegistry(listOf(BodyCodec())),
        clock: Clock = Clock { BASE },
        configuration: PostgresStateConfiguration = configuration(schema),
    ): PostgresResponseStore = PostgresResponseStore("responses-$schema", configuration, codecs, clock)

    private fun configuration(
        schema: String,
        maxIdentifierBytes: Int = 255,
        maxReasonBytes: Int = 4_096,
        maxResponseBodyBytes: Int = 8 * 1_024 * 1_024,
        maxRequestBodyBytes: Int = 8 * 1_024 * 1_024,
        maxTargetBytes: Int = 16 * 1_024,
        maxOutputPayloadBytes: Int = 1_024,
        maxOutboxClaimBatch: Int = 64,
        maxOutboxAttempts: Int = 10,
        maxRetentionRows: Int = 4_096,
        outboxLeaseDuration: Duration = Duration.ofMinutes(5),
    ): PostgresStateConfiguration = PostgresStateConfiguration(
        jdbcUrl = database.jdbcUrl,
        username = database.username,
        password = database.password.toCharArray(),
        schema = schema,
        maximumPoolSize = 2,
        maxIdentifierBytes = maxIdentifierBytes,
        maxReasonBytes = maxReasonBytes,
        maxResponseBodyBytes = maxResponseBodyBytes,
        maxRequestBodyBytes = maxRequestBodyBytes,
        maxTargetBytes = maxTargetBytes,
        maxOutputPayloadBytes = maxOutputPayloadBytes,
        maxOutboxClaimBatch = maxOutboxClaimBatch,
        maxOutboxAttempts = maxOutboxAttempts,
        maxRetentionRows = maxRetentionRows,
        outboxLeaseDuration = outboxLeaseDuration,
    )

    private fun intCodecs(): OutputCodecRegistry<Int> = OutputCodecRegistry(listOf(IntCodec))

    private fun produced(value: Int, responseReference: ResponseReference? = null): Produced<Int> = Produced(
        value = value,
        context = ProductionContext(
            pipelineId = "pipeline",
            engineRunId = "engine",
            pipelineRunId = "pipeline-run",
            requestId = "request-$value",
            responseReference = responseReference,
            originalTarget = "https://example.test/$value",
            finalTarget = null,
            producedAt = BASE,
        ),
    )

    private fun outputContext(): OutputContext = OutputContext(CrawlerContext("engine", "pipeline-run", "pipeline"))

    private fun response(
        target: String,
        pipelineId: String?,
        requestId: String? = "request-$target",
        requestBody: ByteArray? = null,
    ): CrawlResponse = CrawlResponse(
        request = CrawlRequest(
            target = "https://example.test/$target",
            body = requestBody?.let(RequestBody::from),
            attributes = ContextAttributes.from(
                listOfNotNull(
                    pipelineId?.let { ContextAttribute(InMemoryResponseStore.PIPELINE_ID_ATTRIBUTE, it) },
                    requestId?.let { ContextAttribute(InMemoryResponseStore.REQUEST_ID_ATTRIBUTE, it) },
                ),
            ),
        ),
        statusCode = 200,
        headers = Headers.empty(),
        body = ResponseBody.from(byteArrayOf(1)),
        receivedAt = BASE,
    )

    private fun tableExists(schema: String, table: String): Boolean = database.string(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?)",
        schema,
        table,
    ) == "t"

    private data class OutboxRow(
        val status: String,
        val deliveryAttempts: Int,
        val lastError: String?,
        val leaseToken: Long?,
        val leaseOwner: String?,
        val leaseExpiresAt: Timestamp?,
        val payload: ByteArray,
    )

    private fun outboxRows(schema: String, outputId: String): List<OutboxRow> = database.withConnection { connection ->
        connection.prepareStatement(
            "SELECT status, delivery_attempts, last_error, lease_token, lease_owner, lease_expires_at, payload " +
                "FROM \"$schema\".\"outbox_entries\" WHERE output_id = ? ORDER BY sequence",
        ).use { statement ->
            statement.setString(1, outputId)
            statement.executeQuery().use { result ->
                buildList {
                    while (result.next()) {
                        val token = result.getLong("lease_token").takeUnless { result.wasNull() }
                        add(
                            OutboxRow(
                                status = result.getString("status"),
                                deliveryAttempts = result.getInt("delivery_attempts"),
                                lastError = result.getString("last_error"),
                                leaseToken = token,
                                leaseOwner = result.getString("lease_owner"),
                                leaseExpiresAt = result.getTimestamp("lease_expires_at"),
                                payload = result.getBytes("payload"),
                            ),
                        )
                    }
                }
            }
        }
    }

    private fun startContext(): ExtensionStartContext = ExtensionStartContext(CoroutineScope(Dispatchers.Default))

    private fun stopContext(): ExtensionStopContext = ExtensionStopContext(ExtensionStopReason.ENGINE_SHUTDOWN)

    private class RecordingDelegate(
        private val behavior: suspend (List<Produced<Int>>, OutputContext) -> OutputResult = { items, _ ->
            OutputResult.Accepted(items.size)
        },
    ) : Output<Int> {
        override val id: String = "delegate"
        val values: MutableList<Int> = mutableListOf()
        var calls: Int = 0

        override suspend fun write(items: List<Produced<Int>>, context: OutputContext): OutputResult {
            calls++
            values += items.map { it.value }
            return behavior(items, context)
        }
    }

    private object IntCodec : OutputCodec<Int> {
        override val descriptor: OutputCodecDescriptor = OutputCodecDescriptor("it-int", 1)

        override fun encode(item: Produced<Int>): ByteArray = ByteBuffer.allocate(4).putInt(item.value).array()

        override fun decode(payload: ByteArray): Produced<Int> = Produced(
            value = decodeInt(payload),
            context = ProductionContext(
                pipelineId = "pipeline",
                engineRunId = "engine",
                pipelineRunId = "pipeline-run",
                requestId = null,
                responseReference = null,
                originalTarget = null,
                finalTarget = null,
                producedAt = BASE,
            ),
        )
    }

    /** Stores the response body and decodes it into a fixed request shape. */
    private open class BodyCodec : ResponseCodec {
        override val descriptor: ResponseCodecDescriptor = ResponseCodecDescriptor("it-body", 1)

        override fun encode(response: CrawlResponse): ByteArray = response.body.toByteArray()

        override fun decode(payload: ByteArray): CrawlResponse = CrawlResponse(
            request = CrawlRequest("https://decoded.test/response"),
            statusCode = 200,
            headers = Headers.empty(),
            body = ResponseBody.from(payload),
            receivedAt = BASE,
        )
    }

    /** Caller-owned codec whose decoded shape is adjustable to model a misbehaving historical codec. */
    private class ShapingCodec : BodyCodec() {
        var shape: (CrawlResponse) -> CrawlResponse = { it }

        override fun decode(payload: ByteArray): CrawlResponse = shape(super.decode(payload))
    }

    private class MutableClock(var current: Instant) : Clock {
        override fun now(): Instant = current
    }

    private companion object {
        val BASE: Instant = Instant.parse("2026-01-01T00:00:00Z")
        val schemaSequence = AtomicInteger()

        @Container
        @JvmField
        val postgres: PostgreSQLContainer<Nothing> = PostgreSQLContainer<Nothing>("postgres:16.4-alpine")

        val database: PostgresTestDatabase by lazy { PostgresTestDatabase(postgres) }

        fun nextSchema(): String = "adapter_contract_${schemaSequence.incrementAndGet()}"

        fun decodeInt(payload: ByteArray): Int = ByteBuffer.wrap(payload).int
    }
}
