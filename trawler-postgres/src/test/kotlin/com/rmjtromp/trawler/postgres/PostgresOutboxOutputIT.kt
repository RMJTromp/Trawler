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
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.model.ResponseReference
import com.rmjtromp.trawler.output.OutputCodec
import com.rmjtromp.trawler.output.OutputCodecDescriptor
import com.rmjtromp.trawler.output.OutputCodecRegistry
import com.rmjtromp.trawler.output.OutputLifecycle
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
import com.rmjtromp.trawler.response.ResponseRetentionRequest
import com.rmjtromp.trawler.retention.RetentionCategory
import java.nio.ByteBuffer
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/** SQL-semantic coverage for fenced PostgreSQL outbox delivery. */
@Testcontainers
class PostgresOutboxOutputIT {
    @Test
    fun `outbox migration is independent and enqueue stores exact codec metadata atomically`(): Unit = runBlocking {
        val schema = nextSchema()
        val delegate = RecordingDelegate()
        val outbox = outbox(schema, delegate = delegate)

        assertEquals(OutputResult.Accepted(0), outbox.write(emptyList(), outputContext()))
        outbox.start(startContext())
        outbox.start(startContext())
        assertEquals(1, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"flyway_outbox_schema_history\""))
        assertTrue(tableExists(schema, "outbox_entries"))
        assertFalse(tableExists(schema, "response_records"))
        assertFalse(tableExists(schema, "output_entries"))

        val reference = ResponseReference("external-reference")
        assertEquals(
            OutputResult.Accepted(2),
            outbox.write(
                listOf(produced(1), produced(2, responseReference = reference)),
                outputContext(),
            ),
        )
        val rows = outboxRows(schema, "it-outbox")
        assertEquals(2, rows.size)
        assertTrue(rows.all { it.status == "PENDING" && it.codecId == "it-int" && it.codecVersion == 1 })
        assertEquals(listOf(1, 2), rows.map { decodeInt(it.payload) })
        assertEquals(listOf(null, reference.value), rows.map { it.responseReference })
        assertEquals(0, delegate.calls)

        outbox.stopForReplay(stopContext())
        outbox.startForReplay(startContext())
        outbox.stopForReplay(stopContext())
        outbox.start(startContext())
        outbox.stop(stopContext())
        outbox.close()
        outbox.close()
        assertEquals(1, delegate.closeCalls)
        assertFailsWith<IllegalStateException> { outbox.start(startContext()) }
    }

    @Test
    fun `outbox enqueue batch and payload failures roll back every earlier insert`(): Unit = runBlocking {
        val schema = nextSchema()
        val cause = IllegalStateException("second enqueue failed")
        val outbox = outbox(
            schema,
            codec = IntCodec(failOn = 2, failure = cause),
            config = configuration(schema, maxOutputBatchItems = 2, maxOutputPayloadBytes = 4),
        )
        outbox.start(startContext())
        try {
            val oversizedBatch = assertIs<OutputResult.Failed>(
                outbox.write(listOf(produced(1), produced(2), produced(3)), outputContext()),
            )
            assertTrue(oversizedBatch.error is IllegalArgumentException)
            assertEquals(0, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"outbox_entries\""))

            val failedBatch = assertIs<OutputResult.Failed>(
                outbox.write(listOf(produced(1), produced(2)), outputContext()),
            )
            assertSame(cause, failedBatch.error)
            assertEquals(0, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"outbox_entries\""))
        } finally {
            outbox.stop(stopContext())
        }

        val payloadSchema = nextSchema()
        val payloadOutbox = outbox(
            payloadSchema,
            codec = IntCodec(payload = ByteArray(5) { 1 }),
            config = configuration(payloadSchema, maxOutputPayloadBytes = 4),
        )
        payloadOutbox.start(startContext())
        try {
            val result = assertIs<OutputResult.Failed>(payloadOutbox.write(listOf(produced(1)), outputContext()))
            assertTrue(result.error is IllegalArgumentException)
            assertEquals(0, scalarInt(payloadSchema, "SELECT COUNT(*) FROM \"$payloadSchema\".\"outbox_entries\""))
        } finally {
            payloadOutbox.stop(stopContext())
        }
    }

    @Test
    fun `bounded claims are FIFO and delegate observes a committed lease outside the transaction`(): Unit = runBlocking {
        val schema = nextSchema()
        var observedStatus: String? = null
        val delegate = RecordingDelegate { _, _ ->
            observedStatus = scalarString(schema, "SELECT status FROM \"$schema\".\"outbox_entries\" WHERE outbox_id = 'postgres-outbox-1'")
            OutputResult.Accepted(1)
        }
        val outbox = outbox(
            schema,
            delegate = delegate,
            config = configuration(schema, maxOutboxClaimBatch = 2),
        )
        outbox.start(startContext())
        try {
            repeat(3) { index ->
                assertEquals(OutputResult.Accepted(1), outbox.write(listOf(produced(index + 1)), outputContext()))
            }
            val summary = outbox.deliverPending("owner", limit = 99, now = BASE)
            assertEquals(2, summary.claimed)
            assertEquals(2, summary.delivered)
            assertEquals(0, summary.retried)
            assertEquals(0, summary.failed)
            assertEquals(listOf(1, 2), delegate.values)
            assertEquals("LEASED", observedStatus)
            assertEquals(listOf("DELIVERED", "DELIVERED", "PENDING"), outboxRows(schema, "it-outbox").map { it.status })
        } finally {
            outbox.stop(stopContext())
        }
    }

    @Test
    fun `fresh fence tokens prevent stale completion and document the duplicate delivery window`(): Unit = runBlocking {
        val schema = nextSchema()
        val clock = MutableClock(BASE)
        val firstEntered = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        var secondToken: Long? = null
        val firstDelegate = RecordingDelegate { _, _ ->
            firstEntered.complete(Unit)
            releaseFirst.await()
            OutputResult.Accepted(1)
        }
        val secondDelegate = RecordingDelegate { _, _ ->
            secondToken = outboxLeaseToken(schema)
            OutputResult.Accepted(1)
        }
        val config = configuration(
            schema,
            outboxLeaseDuration = Duration.ofSeconds(10),
            maxOutboxAttempts = 3,
        )
        val first = outbox(schema, id = "it-outbox", delegate = firstDelegate, clock = clock, config = config)
        val second = outbox(schema, id = "it-outbox", delegate = secondDelegate, clock = clock, config = config)
        first.start(startContext())
        second.start(startContext())
        try {
            assertEquals(OutputResult.Accepted(1), first.write(listOf(produced(7)), outputContext()))
            val firstJob = async(Dispatchers.Default) { first.deliverPending("owner-a", 1, BASE) }
            try {
                firstEntered.await()
                val firstToken = assertNotNull(outboxLeaseToken(schema))
                assertEquals("owner-a", outboxLeaseOwner(schema))
                assertEquals("LEASED", outboxRows(schema, "it-outbox").single().status)

                clock.current = BASE.plusSeconds(10)
                val secondSummary = second.deliverPending("owner-b", 1, clock.current)
                assertEquals(1, secondSummary.claimed)
                assertEquals(1, secondSummary.delivered)
                assertEquals(0, secondSummary.stale)
                assertNotNull(secondToken)
                assertTrue(firstToken != secondToken)
                assertEquals(listOf(7), firstDelegate.values)
                assertEquals(listOf(7), secondDelegate.values)

                releaseFirst.complete(Unit)
                val firstSummary = firstJob.await()
                assertEquals(1, firstSummary.claimed)
                assertEquals(0, firstSummary.delivered)
                assertEquals(1, firstSummary.stale)
                val row = outboxRows(schema, "it-outbox").single()
                assertEquals("DELIVERED", row.status)
                assertEquals(2, row.deliveryAttempts)
                assertNull(row.leaseToken)
            } finally {
                releaseFirst.complete(Unit)
                firstJob.cancelAndJoin()
            }
        } finally {
            second.stop(stopContext())
            first.stop(stopContext())
        }
    }

    @Test
    fun `skipped failed thrown and invalid acknowledgements retry then exhaust with bounded reasons`(): Unit = runBlocking {
        val schema = nextSchema()
        val thrown = IllegalArgumentException("delegate threw")
        val delegate = RecordingDelegate { items, _ ->
            when (items.single().value) {
                1 -> OutputResult.Skipped("not useful")
                2 -> OutputResult.Failed(IllegalStateException("delegate failed"))
                3 -> throw thrown
                else -> OutputResult.Accepted(0)
            }
        }
        val outbox = outbox(schema, delegate = delegate, config = configuration(schema, maxOutboxAttempts = 1))
        outbox.start(startContext())
        try {
            (1..4).forEach { value -> assertEquals(OutputResult.Accepted(1), outbox.write(listOf(produced(value)), outputContext())) }
            val summary = outbox.deliverPending("owner", limit = 4, now = BASE)
            assertEquals(4, summary.claimed)
            assertEquals(0, summary.delivered)
            assertEquals(0, summary.retried)
            assertEquals(4, summary.failed)
            val rows = outboxRows(schema, "it-outbox")
            assertTrue(rows.all { it.status == "FAILED" && it.deliveryAttempts == 1 })
            assertEquals("delegate-skipped", rows[0].lastError)
            assertTrue(rows[1].lastError.orEmpty().startsWith("delegate-failed:IllegalStateException"))
            assertTrue(rows[2].lastError.orEmpty().startsWith("delivery-failed:IllegalArgumentException"))
            assertEquals("delegate-ack-count", rows[3].lastError)
        } finally {
            outbox.stop(stopContext())
        }

        val retrySchema = nextSchema()
        val retryDelegate = RecordingDelegate { _, _ -> OutputResult.Failed(IllegalStateException("retry me")) }
        val retryOutbox = outbox(
            retrySchema,
            delegate = retryDelegate,
            config = configuration(retrySchema, maxOutboxAttempts = 2),
        )
        retryOutbox.start(startContext())
        try {
            retryOutbox.write(listOf(produced(5)), outputContext())
            assertEquals(1, retryOutbox.deliverPending("owner", 1, BASE).retried)
            assertEquals("PENDING", outboxRows(retrySchema, "it-outbox").single().status)
            assertEquals(1, retryOutbox.deliverPending("owner", 1, BASE).failed)
            assertEquals("FAILED", outboxRows(retrySchema, "it-outbox").single().status)
            assertEquals(2, outboxRows(retrySchema, "it-outbox").single().deliveryAttempts)
        } finally {
            retryOutbox.stop(stopContext())
        }
    }

    @Test
    fun `missing historical codec becomes a delivery failure without calling the delegate`(): Unit = runBlocking {
        val schema = nextSchema()
        val writer = outbox(
            schema,
            codec = IntCodec(descriptor = OutputCodecDescriptor("historical", 1)),
        )
        writer.start(startContext())
        writer.write(listOf(produced(9)), outputContext())
        writer.stop(stopContext())

        val delegate = RecordingDelegate()
        val reader = outbox(
            schema,
            id = "it-outbox",
            delegate = delegate,
            codec = IntCodec(descriptor = OutputCodecDescriptor("new-codec", 2)),
        )
        reader.start(startContext())
        try {
            val summary = reader.deliverPending("owner", 1, BASE)
            assertEquals(1, summary.failed)
            assertEquals(0, delegate.calls)
            assertEquals("FAILED", outboxRows(schema, "it-outbox").single().status)
            assertTrue(outboxRows(schema, "it-outbox").single().lastError.orEmpty().contains("IllegalStateException"))
        } finally {
            reader.stop(stopContext())
        }
    }

    @Test
    fun `cancellation releases a leased claim and terminal retention excludes pending work`(): Unit = runBlocking {
        val schema = nextSchema()
        val clock = MutableClock(BASE)
        val entered = CompletableDeferred<Unit>()
        val blockingDelegate = RecordingDelegate { _, _ ->
            entered.complete(Unit)
            CompletableDeferred<Unit>().await()
            OutputResult.Accepted(1)
        }
        val outbox = outbox(
            schema,
            delegate = blockingDelegate,
            clock = clock,
            config = configuration(schema, maxOutboxAttempts = 1),
        )
        outbox.start(startContext())
        try {
            outbox.write(listOf(produced(1)), outputContext())
            val job = async(Dispatchers.Default) { outbox.deliverPending("owner", 1, BASE) }
            entered.await()
            assertEquals("LEASED", outboxRows(schema, "it-outbox").single().status)
            assertEquals(
                0,
                outbox.prune(OutputRetentionRequest(RetentionCategory.OUTBOX_HISTORY, BASE.plusSeconds(1), 99)).deleted,
            )
            job.cancelAndJoin()
            assertTrue(job.isCancelled)
            val released = outboxRows(schema, "it-outbox").single()
            assertEquals("PENDING", released.status)
            assertNull(released.leaseToken)
            assertNull(released.leaseOwner)
        } finally {
            outbox.stop(stopContext())
        }

        val retentionSchema = nextSchema()
        val retentionClock = MutableClock(BASE)
        val responseStore = responseStore(retentionSchema, retentionClock)
        val retentionDelegate = RecordingDelegate { items, _ ->
            if (items.single().value == 1) OutputResult.Accepted(1) else OutputResult.Failed(IllegalStateException("terminal"))
        }
        val retentionOutbox = outbox(
            retentionSchema,
            delegate = retentionDelegate,
            clock = retentionClock,
            config = configuration(retentionSchema, maxOutboxAttempts = 1, maxRetentionRows = 1),
        )
        responseStore.start(startContext())
        retentionOutbox.start(startContext())
        try {
            val responseReference = assertNotNull(responseStore.put(response("lineage", "pipeline")))
            retentionOutbox.write(
                listOf(
                    produced(1, responseReference = responseReference),
                    produced(2),
                    produced(3),
                ),
                outputContext(),
            )
            val delivery = retentionOutbox.deliverPending("owner", 2, BASE)
            assertEquals(2, delivery.claimed)
            assertEquals(1, delivery.delivered)
            assertEquals(1, delivery.failed)
            assertEquals("PENDING", outboxRows(retentionSchema, "it-outbox")[2].status)
            assertEquals(1, scalarInt(retentionSchema, "SELECT COUNT(*) FROM \"$retentionSchema\".\"response_lineage\""))

            val firstPrune = retentionOutbox.prune(
                OutputRetentionRequest(RetentionCategory.OUTBOX_HISTORY, BASE.plusSeconds(1), 99),
            )
            assertEquals(1, firstPrune.deleted)
            assertEquals(1, outboxRows(retentionSchema, "it-outbox").size)
            assertEquals("FAILED", outboxRows(retentionSchema, "it-outbox").single().status)
            assertEquals(0, scalarInt(retentionSchema, "SELECT COUNT(*) FROM \"$retentionSchema\".\"response_lineage\""))
            assertEquals(1, responseStore.prune(ResponseRetentionRequest(BASE.plusSeconds(1), 99)).deleted)

            val secondPrune = retentionOutbox.prune(
                OutputRetentionRequest(RetentionCategory.OUTBOX_HISTORY, BASE.plusSeconds(1), 99),
            )
            assertEquals(1, secondPrune.deleted)
            assertEquals(1, outboxRows(retentionSchema, "it-outbox").size)
            assertEquals("PENDING", outboxRows(retentionSchema, "it-outbox").single().status)
        } finally {
            retentionOutbox.stop(stopContext())
            responseStore.stop(stopContext())
        }
    }

    private fun outbox(
        schema: String,
        id: String = "it-outbox",
        codec: OutputCodec<Int> = IntCodec(),
        delegate: Output<Int> = RecordingDelegate(),
        clock: Clock = Clock { BASE },
        config: PostgresStateConfiguration = configuration(schema),
    ): PostgresOutboxOutput<Int> = PostgresOutboxOutput(
        id = id,
        configuration = config,
        codecs = OutputCodecRegistry(listOf(codec)),
        delegate = delegate,
        clock = clock,
    )

    private fun responseStore(schema: String, clock: Clock): PostgresResponseStore = PostgresResponseStore(
        id = "it-responses-$schema",
        configuration = configuration(schema),
        codecs = ResponseCodecRegistry(listOf(ResponseMarkerCodec)),
        clock = clock,
    )

    private fun configuration(
        schema: String,
        maxOutputPayloadBytes: Int = 1 * 1_024 * 1_024,
        maxOutputBatchItems: Int = 256,
        maxOutboxClaimBatch: Int = 64,
        maxOutboxAttempts: Int = 10,
        maxRetentionRows: Int = 4_096,
        outboxLeaseDuration: Duration = Duration.ofMinutes(5),
    ): PostgresStateConfiguration = PostgresStateConfiguration(
        jdbcUrl = postgres.jdbcUrl,
        username = postgres.username,
        password = postgres.password.toCharArray(),
        schema = schema,
        maxOutputPayloadBytes = maxOutputPayloadBytes,
        maxOutputBatchItems = maxOutputBatchItems,
        maxOutboxClaimBatch = maxOutboxClaimBatch,
        maxOutboxAttempts = maxOutboxAttempts,
        maxRetentionRows = maxRetentionRows,
        outboxLeaseDuration = outboxLeaseDuration,
    )

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

    private fun outputContext(): OutputContext = OutputContext(
        CrawlerContext("engine", "pipeline-run", "pipeline"),
    )

    private fun response(target: String, pipelineId: String): CrawlResponse = CrawlResponse(
        request = CrawlRequest(
            target = "https://example.test/$target",
            attributes = ContextAttributes.of(
                ContextAttribute(InMemoryResponseStore.PIPELINE_ID_ATTRIBUTE, pipelineId),
            ),
        ),
        statusCode = 200,
        headers = Headers.empty(),
        body = ResponseBody.from(byteArrayOf(1)),
        receivedAt = BASE,
    )

    private fun startContext(): ExtensionStartContext =
        ExtensionStartContext(kotlinx.coroutines.CoroutineScope(Dispatchers.Default))

    private fun stopContext(): ExtensionStopContext = ExtensionStopContext(ExtensionStopReason.ENGINE_SHUTDOWN)

    private data class OutboxRow(
        val status: String,
        val deliveryAttempts: Int,
        val lastError: String?,
        val leaseToken: Long?,
        val leaseOwner: String?,
        val leaseExpiresAt: Timestamp?,
        val responseReference: String?,
        val payload: ByteArray,
        val codecId: String,
        val codecVersion: Int,
    )

    private fun outboxRows(schema: String, outputId: String): List<OutboxRow> = withConnection { connection ->
        connection.prepareStatement(
            "SELECT status, delivery_attempts, last_error, lease_token, lease_owner, lease_expires_at, response_reference, payload, codec_id, codec_version FROM \"$schema\".\"outbox_entries\" WHERE output_id = ? ORDER BY sequence",
        ).use { statement ->
            statement.setString(1, outputId)
            statement.executeQuery().use { result ->
                buildList {
                    while (result.next()) {
                        val token = result.getLong("lease_token").let { value -> if (result.wasNull()) null else value }
                        add(
                            OutboxRow(
                                status = result.getString("status"),
                                deliveryAttempts = result.getInt("delivery_attempts"),
                                lastError = result.getString("last_error"),
                                leaseToken = token,
                                leaseOwner = result.getString("lease_owner"),
                                leaseExpiresAt = result.getTimestamp("lease_expires_at"),
                                responseReference = result.getString("response_reference"),
                                payload = result.getBytes("payload"),
                                codecId = result.getString("codec_id"),
                                codecVersion = result.getInt("codec_version"),
                            ),
                        )
                    }
                }
            }
        }
    }

    private fun outboxLeaseToken(schema: String): Long? = outboxRows(schema, "it-outbox").single().leaseToken

    private fun outboxLeaseOwner(schema: String): String? = outboxRows(schema, "it-outbox").single().leaseOwner

    private fun scalarInt(schema: String, sql: String): Int = withConnection { connection ->
        connection.createStatement().use { statement ->
            statement.executeQuery(sql).use { result ->
                assertTrue(result.next())
                result.getInt(1)
            }
        }
    }

    private fun scalarString(schema: String, sql: String): String? = withConnection { connection ->
        connection.createStatement().use { statement ->
            statement.executeQuery(sql).use { result ->
                assertTrue(result.next())
                result.getString(1)
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

    private fun <T> withConnection(block: (Connection) -> T): T =
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use(block)

    private fun decodeInt(payload: ByteArray): Int = ByteBuffer.wrap(payload).int

    private inner class IntCodec(
        override val descriptor: OutputCodecDescriptor = OutputCodecDescriptor("it-int", 1),
        private val failOn: Int? = null,
        private val failure: Throwable? = null,
        private val payload: ByteArray? = null,
    ) : OutputCodec<Int> {
        override fun encode(item: Produced<Int>): ByteArray {
            if (item.value == failOn) throw requireNotNull(failure)
            return payload?.copyOf() ?: ByteBuffer.allocate(4).putInt(item.value).array()
        }

        override fun decode(payload: ByteArray): Produced<Int> = produced(ByteBuffer.wrap(payload).int)
    }

    private class RecordingDelegate(
        override val id: String = "delegate",
        private val behavior: suspend (List<Produced<Int>>, OutputContext) -> OutputResult = { items, _ ->
            OutputResult.Accepted(items.size)
        },
    ) : Output<Int>, OutputLifecycle {
        val values: MutableList<Int> = mutableListOf()
        var calls: Int = 0
        var closeCalls: Int = 0

        override suspend fun write(items: List<Produced<Int>>, context: OutputContext): OutputResult {
            calls++
            values += items.map { it.value }
            return behavior(items, context)
        }

        override suspend fun close() {
            closeCalls++
        }
    }

    private object ResponseMarkerCodec : ResponseCodec {
        override val descriptor: ResponseCodecDescriptor = ResponseCodecDescriptor("it-response", 1)

        override fun encode(response: CrawlResponse): ByteArray = response.body.toByteArray()

        override fun decode(payload: ByteArray): CrawlResponse = CrawlResponse(
            request = CrawlRequest("https://decoded.test/response"),
            statusCode = 200,
            headers = Headers.empty(),
            body = ResponseBody.from(payload),
            receivedAt = Instant.EPOCH,
        )
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

        fun nextSchema(): String = "outbox_it_${schemaSequence.incrementAndGet()}"
    }
}
