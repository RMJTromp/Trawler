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
import com.rmjtromp.trawler.output.OutputRetentionRequest
import com.rmjtromp.trawler.pipeline.Output
import com.rmjtromp.trawler.pipeline.OutputResult
import com.rmjtromp.trawler.pipeline.Produced
import com.rmjtromp.trawler.pipeline.ProductionContext
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.RequestIdentity
import com.rmjtromp.trawler.response.InMemoryResponseStore
import com.rmjtromp.trawler.response.ResponseCodec
import com.rmjtromp.trawler.response.ResponseCodecDescriptor
import com.rmjtromp.trawler.response.ResponseCodecRegistry
import com.rmjtromp.trawler.response.ResponseLookup
import com.rmjtromp.trawler.response.ResponseReplayQuery
import com.rmjtromp.trawler.response.ResponseRetentionRequest
import com.rmjtromp.trawler.retention.RetentionCategory
import com.rmjtromp.trawler.state.CheckpointPayload
import com.rmjtromp.trawler.state.DiscoveryCheckpoint
import com.rmjtromp.trawler.state.EnqueueResult
import com.rmjtromp.trawler.state.LeaseMutationResult
import com.rmjtromp.trawler.state.WorkTransition
import java.nio.ByteBuffer
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/** Durable composition coverage across the four PostgreSQL-owned boundaries. */
@Testcontainers
class PostgresDurableE2EIT {
    @Test
    fun `state response output and outbox survive restart with fenced reclaim checkpoint resume replay and retention`() =
        runBlocking {
            val schema = nextSchema()
            val clock = MutableClock(BASE)
            val configuration = configuration(schema)
            val firstState = state(configuration, clock, "state-a")
            val responseStore = responseStore(configuration, clock, "responses-a")
            val output = output(configuration, clock, "output-a")
            val delegate = RecordingOutput()
            val outbox = outbox(configuration, clock, "outbox-a", delegate)
            val secondState = state(configuration, clock, "state-b")
            val start = startContext()
            val stop = stopContext()

            try {
                firstState.start(start)
                responseStore.start(start)
                output.start(start)
                outbox.start(start)

            val checkpoint = DiscoveryCheckpoint(
                revision = 0,
                codecId = "durable-checkpoint",
                codecVersion = 1,
                payload = CheckpointPayload.from(byteArrayOf(0)),
                generationId = "generation-1",
                strategyId = "news",
                completed = false,
            )
            assertTrue(firstState.compareAndSet("news", "feed", null, checkpoint))

            val request = CrawlRequest("https://example.test/durable")
            val context = CrawlerContext("engine-1", "pipeline-run-1", "news")
            val workId = assertIs<EnqueueResult.Accepted>(
                firstState.enqueue(request, RequestIdentity("durable-resource"), context),
            ).workItemId
            val firstLease = firstState.claim(1, "worker-a", BASE).single()

            secondState.start(start)
            clock.current = BASE.plusSeconds(11)
            val reclaimed = secondState.claim("news", 1, "worker-b", clock.current).single()
            assertEquals(workId, reclaimed.workItemId)
            assertNotEquals(firstLease.leaseToken, reclaimed.leaseToken)
            assertEquals(
                LeaseMutationResult.Stale,
                firstState.transition(firstLease, WorkTransition.Failed("late worker")),
            )

            val responseReference = assertNotNull(
                responseStore.put(response("durable", "news", byteArrayOf(7, 8, 9))),
            )
            assertEquals(
                LeaseMutationResult.Accepted,
                secondState.transition(reclaimed, WorkTransition.Succeeded),
            )
            assertEquals(
                OutputResult.Accepted(1),
                output.write(listOf(produced(42, responseReference)), outputContext("news")),
            )
            assertEquals(
                OutputResult.Accepted(1),
                outbox.write(listOf(produced(42, responseReference)), outputContext("news")),
            )
            assertEquals(1, outbox.deliverPending("delivery-a", now = clock.current).delivered)
            assertEquals(listOf(42), delegate.values)
            assertEquals(1, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"jobs\""))
            assertEquals(1, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"response_records\""))
            assertEquals(1, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"output_entries\""))
            assertEquals(1, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"outbox_entries\""))
            assertEquals("DELIVERED", scalarString(schema, "SELECT status FROM \"$schema\".\"outbox_entries\""))

            outbox.stop(stop)
            output.stop(stop)
            responseStore.stop(stop)
            secondState.stop(stop)
            firstState.stop(stop)

            val resumedState = state(configuration, clock, "state-a")
            val resumedResponses = responseStore(configuration, clock, "responses-a")
            val resumedOutput = output(configuration, clock, "output-a")
            val resumedOutbox = outbox(configuration, clock, "outbox-a", RecordingOutput())
            try {
                resumedState.start(start)
                resumedResponses.start(start)
                resumedOutput.start(start)
                resumedOutbox.start(start)
                val loaded = assertNotNull(resumedState.load("news", "feed"))
                assertEquals(0, loaded.revision)
                assertEquals(byteArrayOf(0).toList(), loaded.payload.toByteArray().toList())

                val completed = loaded.copy(
                    revision = 1,
                    payload = CheckpointPayload.from(byteArrayOf(1)),
                    completed = true,
                )
                assertTrue(resumedState.compareAndSet("news", "feed", loaded.revision, completed))
                assertEquals(true, assertNotNull(resumedState.load("news", "feed")).completed)

                val stored = assertNotNull(resumedResponses.find(ResponseLookup.ByReference(responseReference)))
                assertEquals(responseReference, stored.reference)
                assertEquals(byteArrayOf(7, 8, 9).toList(), stored.response.body.toByteArray().toList())
                val replay = resumedResponses.replay(
                    ResponseReplayQuery.ByPipelineId("news"),
                    limit = 10,
                    pipelineId = "news",
                )
                assertEquals(listOf(responseReference), replay.map { it.reference })
                assertEquals(byteArrayOf(7, 8, 9).toList(), replay.single().response.body.toByteArray().toList())
                assertEquals(1, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"output_entries\""))
                assertEquals(1, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"outbox_entries\""))

                val cutoff = BASE.plusSeconds(12)
                assertEquals(
                    1,
                    resumedOutput.prune(OutputRetentionRequest(RetentionCategory.OUTPUT_HISTORY, cutoff, 10)).deleted,
                )
                assertEquals(
                    1,
                    resumedOutbox.prune(OutputRetentionRequest(RetentionCategory.OUTBOX_HISTORY, cutoff, 10)).deleted,
                )
                assertEquals(
                    1,
                    resumedResponses.prune(ResponseRetentionRequest(cutoff, 10)).deleted,
                )
                assertEquals(0, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"response_records\""))
            } finally {
                resumedOutbox.stop(stop)
                resumedOutput.stop(stop)
                resumedResponses.stop(stop)
                resumedState.stop(stop)
            }
            } finally {
                outbox.stop(stop)
                output.stop(stop)
                responseStore.stop(stop)
                secondState.stop(stop)
                firstState.stop(stop)
            }
        }

    private fun state(
        configuration: PostgresStateConfiguration,
        clock: Clock,
        id: String,
    ): PostgresCrawlStateStore = PostgresCrawlStateStore(id, configuration, clock = clock)

    private fun responseStore(
        configuration: PostgresStateConfiguration,
        clock: Clock,
        id: String,
    ): PostgresResponseStore = PostgresResponseStore(
        id = id,
        configuration = configuration,
        codecs = ResponseCodecRegistry(listOf(ResponseBodyCodec)),
        clock = clock,
    )

    private fun output(
        configuration: PostgresStateConfiguration,
        clock: Clock,
        id: String,
    ): PostgresOutput<Int> = PostgresOutput(
        id = id,
        configuration = configuration,
        codecs = OutputCodecRegistry(listOf(IntOutputCodec)),
        clock = clock,
    )

    private fun outbox(
        configuration: PostgresStateConfiguration,
        clock: Clock,
        id: String,
        delegate: Output<Int>,
    ): PostgresOutboxOutput<Int> = PostgresOutboxOutput(
        id = id,
        configuration = configuration,
        codecs = OutputCodecRegistry(listOf(IntOutputCodec)),
        delegate = delegate,
        clock = clock,
    )

    private fun configuration(schema: String): PostgresStateConfiguration = PostgresStateConfiguration(
        jdbcUrl = postgres.jdbcUrl,
        username = postgres.username,
        password = postgres.password.toCharArray(),
        schema = schema,
        leaseDuration = Duration.ofSeconds(10),
        outboxLeaseDuration = Duration.ofSeconds(10),
    )

    private fun response(target: String, pipelineId: String, body: ByteArray): CrawlResponse = CrawlResponse(
        request = CrawlRequest(
            target = "https://example.test/$target",
            attributes = ContextAttributes.of(
                ContextAttribute(InMemoryResponseStore.PIPELINE_ID_ATTRIBUTE, pipelineId),
                ContextAttribute(InMemoryResponseStore.REQUEST_ID_ATTRIBUTE, "request-$target"),
            ),
        ),
        statusCode = 200,
        headers = Headers.empty(),
        body = ResponseBody.from(body),
        receivedAt = BASE,
    )

    private fun produced(value: Int, responseReference: ResponseReference? = null): Produced<Int> = Produced(
        value = value,
        context = ProductionContext(
            pipelineId = "news",
            engineRunId = "engine-1",
            pipelineRunId = "pipeline-run-1",
            requestId = "request-durable",
            responseReference = responseReference,
            originalTarget = "https://example.test/durable",
            finalTarget = null,
            producedAt = BASE,
        ),
    )

    private fun outputContext(pipelineId: String): OutputContext = OutputContext(
        CrawlerContext("engine-1", "pipeline-run-1", pipelineId),
    )

    private fun startContext(): ExtensionStartContext =
        ExtensionStartContext(kotlinx.coroutines.CoroutineScope(Dispatchers.Default))

    private fun stopContext(): ExtensionStopContext = ExtensionStopContext(ExtensionStopReason.ENGINE_SHUTDOWN)

    private fun scalarInt(schema: String, sql: String): Int = withConnection { connection ->
        connection.createStatement().use { statement ->
            statement.executeQuery(sql).use { result ->
                assertTrue(result.next())
                result.getInt(1)
            }
        }
    }

    private fun scalarString(schema: String, sql: String): String = withConnection { connection ->
        connection.createStatement().use { statement ->
            statement.executeQuery(sql).use { result ->
                assertTrue(result.next())
                result.getString(1)
            }
        }
    }

    private fun <T> withConnection(block: (Connection) -> T): T =
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use(block)

    private class MutableClock(var current: Instant) : Clock {
        override fun now(): Instant = current
    }

    private class RecordingOutput : Output<Int> {
        override val id: String = "durable-delegate"
        val values: MutableList<Int> = mutableListOf()

        override suspend fun write(items: List<Produced<Int>>, context: OutputContext): OutputResult {
            values += items.map { it.value }
            return OutputResult.Accepted(items.size)
        }
    }

    private object ResponseBodyCodec : ResponseCodec {
        override val descriptor: ResponseCodecDescriptor = ResponseCodecDescriptor("durable-response", 1)

        override fun encode(response: CrawlResponse): ByteArray = response.body.toByteArray()

        override fun decode(payload: ByteArray): CrawlResponse = CrawlResponse(
            request = CrawlRequest("https://decoded.test/durable"),
            statusCode = 200,
            headers = Headers.empty(),
            body = ResponseBody.from(payload),
            receivedAt = BASE,
        )
    }

    private object IntOutputCodec : OutputCodec<Int> {
        override val descriptor: OutputCodecDescriptor = OutputCodecDescriptor("durable-int", 1)

        override fun encode(item: Produced<Int>): ByteArray = ByteBuffer.allocate(4).putInt(item.value).array()

        override fun decode(payload: ByteArray): Produced<Int> = Produced(
            value = ByteBuffer.wrap(payload).int,
            context = ProductionContext(
                pipelineId = "news",
                engineRunId = "decoded-engine",
                pipelineRunId = "decoded-run",
                requestId = "decoded-request",
                responseReference = null,
                originalTarget = "https://decoded.test/durable",
                finalTarget = null,
                producedAt = BASE,
            ),
        )
    }

    private companion object {
        val BASE: Instant = Instant.parse("2026-01-01T00:00:00Z")
        val schemaSequence = AtomicInteger()

        @Container
        @JvmField
        val postgres: PostgreSQLContainer<Nothing> = PostgreSQLContainer<Nothing>("postgres:16.4-alpine")

        fun nextSchema(): String = "durable_e2e_${schemaSequence.incrementAndGet()}"
    }
}
