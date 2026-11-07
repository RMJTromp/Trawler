package com.rmjtromp.trawler.postgres

import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.extension.ExtensionStopReason
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.model.ResponseReference
import com.rmjtromp.trawler.output.OutputCodec
import com.rmjtromp.trawler.output.OutputCodecDescriptor
import com.rmjtromp.trawler.output.OutputCodecRegistry
import com.rmjtromp.trawler.output.OutputRetentionRequest
import com.rmjtromp.trawler.pipeline.OutputResult
import com.rmjtromp.trawler.pipeline.Produced
import com.rmjtromp.trawler.pipeline.ProductionContext
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.response.InMemoryResponseStore
import com.rmjtromp.trawler.response.ResponseCodec
import com.rmjtromp.trawler.response.ResponseCodecDescriptor
import com.rmjtromp.trawler.response.ResponseCodecRegistry
import com.rmjtromp.trawler.response.ResponseLookup
import com.rmjtromp.trawler.response.ResponseRetentionRequest
import com.rmjtromp.trawler.postgres.PostgresOutput
import com.rmjtromp.trawler.postgres.PostgresResponseStore
import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.retention.RetentionCategory
import java.nio.ByteBuffer
import java.sql.Connection
import java.sql.DriverManager
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/** SQL-semantic coverage for typed PostgreSQL output history. */
@Testcontainers
class PostgresOutputIT {
    @Test
    fun `output migration is independent and replay lifecycle is restartable`(): Unit = runBlocking {
        val schema = nextSchema()
        val output = output(schema)

        assertEquals(OutputResult.Accepted(0), output.write(emptyList(), outputContext()))
        output.start(startContext())
        output.start(startContext())
        assertEquals(1, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"flyway_output_schema_history\" WHERE type = 'SQL'"))
        assertTrue(tableExists(schema, "output_entries"))
        assertFalse(tableExists(schema, "response_records"))
        assertFalse(tableExists(schema, "outbox_entries"))

        output.stopForReplay(stopContext())
        output.startForReplay(startContext())
        assertIs<OutputResult.Accepted>(output.write(listOf(produced(1)), outputContext()))
        output.stopForReplay(stopContext())
        output.start(startContext())
        output.stop(stopContext())
        output.close()
        assertIs<OutputResult.Failed>(output.write(listOf(produced(2)), outputContext()))
        assertFailsWith<IllegalStateException> { output.start(startContext()) }
    }

    @Test
    fun `output commits bounded batches with exact codec metadata and acknowledgement after persistence`(): Unit = runBlocking {
        val schema = nextSchema()
        val output = output(schema)
        output.start(startContext())
        try {
            val result = output.write(
                listOf(produced(11), produced(12, responseReference = ResponseReference("external-ref"))),
                outputContext(),
            )
            assertEquals(OutputResult.Accepted(2), result)

            val rows = outputRows(schema, "it-output")
            assertEquals(2, rows.size)
            assertEquals(listOf(11, 12), rows.map { decodeInt(it.payload) })
            assertTrue(rows.all { it.codecId == "it-int" && it.codecVersion == 1 })
            assertEquals(listOf(null, "external-ref"), rows.map { it.responseReference })
            assertEquals(2, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"output_entries\""))
        } finally {
            output.stop(stopContext())
        }
    }

    @Test
    fun `output rejects oversized batches and payloads and rolls back a partial codec failure`(): Unit = runBlocking {
        val schema = nextSchema()
        val cause = IllegalStateException("second item failed")
        val output = output(
            schema,
            codec = IntCodec(failOn = 2, failure = cause),
            config = configuration(schema, maxOutputBatchItems = 2, maxOutputPayloadBytes = 4),
        )
        output.start(startContext())
        try {
            val batchFailure = assertIs<OutputResult.Failed>(
                output.write(listOf(produced(1), produced(2), produced(3)), outputContext()),
            )
            assertTrue(batchFailure.error is IllegalArgumentException)
            assertEquals(0, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"output_entries\""))

            val codecFailure = assertIs<OutputResult.Failed>(
                output.write(listOf(produced(1), produced(2)), outputContext()),
            )
            assertOriginalFailure(cause, codecFailure.error)
            assertEquals(0, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"output_entries\""))
        } finally {
            output.stop(stopContext())
        }

        val payloadSchema = nextSchema()
        val payloadOutput = output(
            payloadSchema,
            codec = IntCodec(payload = ByteArray(5) { 3 }),
            config = configuration(payloadSchema, maxOutputPayloadBytes = 4),
        )
        payloadOutput.start(startContext())
        try {
            val result = assertIs<OutputResult.Failed>(payloadOutput.write(listOf(produced(1)), outputContext()))
            assertTrue(result.error is IllegalArgumentException)
            assertEquals(0, scalarInt(payloadSchema, "SELECT COUNT(*) FROM \"$payloadSchema\".\"output_entries\""))
        } finally {
            payloadOutput.stop(stopContext())
        }
    }

    @Test
    fun `output isolation retention and response lineage are transactional and bounded`(): Unit = runBlocking {
        val schema = nextSchema()
        val cutoff = Instant.parse("2026-01-02T00:00:00Z")
        val clock = MutableClock(Instant.parse("2026-01-01T00:00:00Z"))
        val responseStore = responseStore(schema, clock)
        val first = output(
            schema,
            id = "output-a",
            clock = clock,
            config = configuration(schema, maxRetentionRows = 1),
        )
        val second = output(schema, id = "output-b", clock = clock)
        responseStore.start(startContext())
        first.start(startContext())
        second.start(startContext())
        try {
            val responseReference = assertNotNull(responseStore.put(response("retained", "pipeline-a")))
            assertEquals(
                OutputResult.Accepted(1),
                first.write(listOf(produced(1, responseReference = responseReference)), outputContext("pipeline-a")),
            )
            clock.current = cutoff
            assertEquals(
                OutputResult.Accepted(1),
                first.write(listOf(produced(2)), outputContext("pipeline-a")),
            )
            assertEquals(
                OutputResult.Accepted(1),
                second.write(listOf(produced(3)), outputContext("pipeline-b")),
            )

            val responseProtected = responseStore.prune(ResponseRetentionRequest(cutoff, 99))
            assertEquals(0, responseProtected.deleted)
            assertEquals(1, responseProtected.protectedEntries)
            assertEquals(1, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"response_lineage\""))

            val firstPrune = first.prune(OutputRetentionRequest(RetentionCategory.OUTPUT_HISTORY, cutoff, 99))
            assertEquals(1, firstPrune.deleted)
            assertEquals(1, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"output_entries\" WHERE output_id = 'output-a'"))
            assertEquals(0, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"response_lineage\""))

            val responseReleased = responseStore.prune(ResponseRetentionRequest(cutoff, 99))
            assertEquals(1, responseReleased.deleted)
            assertNull(responseStore.find(ResponseLookup.ByReference(responseReference)))

            val exactPrune = first.prune(OutputRetentionRequest(RetentionCategory.OUTPUT_HISTORY, cutoff, 99))
            assertEquals(0, exactPrune.deleted)
            assertEquals(1, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"output_entries\" WHERE output_id = 'output-a'"))

            val secondPrune = second.prune(OutputRetentionRequest(RetentionCategory.OUTPUT_HISTORY, cutoff.plusSeconds(1), 99))
            assertEquals(1, secondPrune.deleted)
            assertFailsWith<IllegalArgumentException> {
                first.prune(OutputRetentionRequest(RetentionCategory.RESPONSES, cutoff, 1))
            }
        } finally {
            second.stop(stopContext())
            first.stop(stopContext())
            responseStore.stop(stopContext())
        }
    }

    @Test
    fun `output stores foreign response references without creating lineage in an independent schema`(): Unit = runBlocking {
        val schema = nextSchema()
        val output = output(schema)
        output.start(startContext())
        try {
            val result = output.write(
                listOf(produced(5, responseReference = ResponseReference("foreign-store-reference"))),
                outputContext(),
            )
            assertEquals(OutputResult.Accepted(1), result)
            val row = outputRows(schema, "it-output").single()
            assertEquals("foreign-store-reference", row.responseReference)
            assertFalse(tableExists(schema, "response_lineage"))
        } finally {
            output.stop(stopContext())
        }
    }

    private fun output(
        schema: String,
        id: String = "it-output",
        codec: OutputCodec<Int> = IntCodec(),
        clock: Clock = Clock { Instant.parse("2026-01-01T00:00:00Z") },
        config: PostgresStateConfiguration = configuration(schema),
    ): PostgresOutput<Int> = PostgresOutput(
        id = id,
        configuration = config,
        codecs = OutputCodecRegistry(listOf(codec)),
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
        maxRetentionRows: Int = 4_096,
    ): PostgresStateConfiguration = PostgresStateConfiguration(
        jdbcUrl = postgres.jdbcUrl,
        username = postgres.username,
        password = postgres.password.toCharArray(),
        schema = schema,
        maxOutputPayloadBytes = maxOutputPayloadBytes,
        maxOutputBatchItems = maxOutputBatchItems,
        maxRetentionRows = maxRetentionRows,
    )

    private fun produced(value: Int, pipelineId: String = "pipeline", responseReference: ResponseReference? = null): Produced<Int> = Produced(
        value = value,
        context = ProductionContext(
            pipelineId = pipelineId,
            engineRunId = "engine",
            pipelineRunId = "run-$pipelineId",
            requestId = "request-$value",
            responseReference = responseReference,
            originalTarget = "https://example.test/$value",
            finalTarget = null,
            producedAt = Instant.parse("2026-01-01T00:00:00Z"),
        ),
    )

    private fun outputContext(pipelineId: String = "pipeline"): OutputContext = OutputContext(
        CrawlerContext("engine", "run-$pipelineId", pipelineId),
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
        body = ResponseBody.from(byteArrayOf(8)),
        receivedAt = Instant.parse("2026-01-01T00:00:00Z"),
    )

    private fun startContext(): ExtensionStartContext =
        ExtensionStartContext(kotlinx.coroutines.CoroutineScope(Dispatchers.Default))

    private fun stopContext(): ExtensionStopContext = ExtensionStopContext(ExtensionStopReason.ENGINE_SHUTDOWN)

    private data class OutputRow(
        val codecId: String,
        val codecVersion: Int,
        val payload: ByteArray,
        val responseReference: String?,
    )

    private fun outputRows(schema: String, outputId: String): List<OutputRow> = withConnection { connection ->
        connection.prepareStatement(
            "SELECT codec_id, codec_version, payload, response_reference FROM \"$schema\".\"output_entries\" WHERE output_id = ? ORDER BY sequence",
        ).use { statement ->
            statement.setString(1, outputId)
            statement.executeQuery().use { result ->
                buildList {
                    while (result.next()) {
                        add(
                            OutputRow(
                                codecId = result.getString("codec_id"),
                                codecVersion = result.getInt("codec_version"),
                                payload = result.getBytes("payload"),
                                responseReference = result.getString("response_reference"),
                            ),
                        )
                    }
                }
            }
        }
    }

    private fun decodeInt(payload: ByteArray): Int = ByteBuffer.wrap(payload).int

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

    private fun <T> withConnection(block: (Connection) -> T): T =
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use(block)

    private inner class IntCodec(
        private val failOn: Int? = null,
        private val failure: Throwable? = null,
        private val payload: ByteArray? = null,
    ) : OutputCodec<Int> {
        override val descriptor: OutputCodecDescriptor = OutputCodecDescriptor("it-int", 1)

        override fun encode(item: Produced<Int>): ByteArray {
            if (item.value == failOn) throw requireNotNull(failure)
            return payload?.copyOf() ?: ByteBuffer.allocate(4).putInt(item.value).array()
        }

        override fun decode(payload: ByteArray): Produced<Int> = produced(ByteBuffer.wrap(payload).int)
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
        val schemaSequence = AtomicInteger()

        @Container
        @JvmField
        val postgres: PostgreSQLContainer<Nothing> = PostgreSQLContainer<Nothing>("postgres:16.4-alpine")

        fun nextSchema(): String = "output_it_${schemaSequence.incrementAndGet()}"
    }
}
