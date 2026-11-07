package com.rmjtromp.trawler.postgres

import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.extension.ExtensionStopReason
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.output.OutputCodec
import com.rmjtromp.trawler.output.OutputCodecDescriptor
import com.rmjtromp.trawler.output.OutputCodecRegistry
import com.rmjtromp.trawler.pipeline.Output
import com.rmjtromp.trawler.pipeline.OutputResult
import com.rmjtromp.trawler.pipeline.Produced
import com.rmjtromp.trawler.pipeline.ProductionContext
import com.rmjtromp.trawler.policy.RequestIdentity
import com.rmjtromp.trawler.response.ResponseCodec
import com.rmjtromp.trawler.response.ResponseCodecDescriptor
import com.rmjtromp.trawler.response.ResponseCodecRegistry
import java.lang.reflect.Proxy
import java.sql.ResultSet
import java.time.Duration
import java.time.Instant
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

/**
 * Database-free contracts of the adapter-private JDBC helpers and of every
 * adapter before it owns a pool: construction allocates nothing, operations
 * outside the lifecycle fail, and a stop before start is terminal.
 */
class PostgresAdapterSupportTest {
    @Test
    fun `UTF-8 truncation keeps whole code points and never exceeds the byte bound`() {
        assertEquals("delegate", truncatePostgresUtf8("delegate", 8))
        assertEquals("delegate", truncatePostgresUtf8("delegate-skipped", 8))
        assertEquals("a", truncatePostgresUtf8("aéb", 2))
        assertEquals("aé", truncatePostgresUtf8("aéb", 3))
        assertEquals("", truncatePostgresUtf8("é", 1))

        requirePostgresUtf8Bound("é", 2, "value")
        assertEquals(
            "value exceeds the configured PostgreSQL bound",
            assertFailsWith<IllegalArgumentException> { requirePostgresUtf8Bound("é", 1, "value") }.message,
        )
        assertEquals("\"a\"\"b\"", quotePostgresIdentifier("a\"b"))
    }

    @Test
    fun `bounded byte reads reject inconsistent driver sizes null payloads and oversized bytes`() {
        assertContentEquals(byteArrayOf(1, 2), resultSet(byteArrayOf(1, 2)).getBoundedBytes("payload", 2, 2, "test"))

        val invalid = listOf(
            "PostgreSQL test payload exceeds its configured bound" to { resultSet(byteArrayOf(1)).getBoundedBytes("payload", -1, 2, "test") },
            "PostgreSQL test payload exceeds its configured bound" to { resultSet(byteArrayOf(1)).getBoundedBytes("payload", 3, 2, "test") },
            "PostgreSQL test payload is null" to { resultSet(null).getBoundedBytes("payload", 0, 2, "test") },
            "PostgreSQL test payload exceeds its configured bound" to {
                resultSet(byteArrayOf(1, 2, 3)).getBoundedBytes("payload", 1, 2, "test")
            },
        )
        invalid.forEach { (message, read) ->
            assertEquals(message, assertFailsWith<IllegalArgumentException> { read() }.message)
        }
    }

    @Test
    fun `adapters use documented default identities and reject blank or oversized identifiers`() {
        val state = PostgresCrawlStateStore(configuration = configuration())
        val responses = PostgresResponseStore(configuration = configuration(), codecs = responseCodecs())
        val output = PostgresOutput(configuration = configuration(), codecs = outputCodecs())
        val outbox = PostgresOutboxOutput(configuration = configuration(), codecs = outputCodecs(), delegate = PlainOutput())

        assertEquals("postgres-state", state.id)
        assertEquals("postgres-responses", responses.id)
        assertEquals("postgres-output", output.id)
        assertEquals("postgres-outbox", outbox.id)

        val blank = listOf(
            "PostgreSQL response store ID must not be blank" to {
                PostgresResponseStore(id = " ", configuration = configuration(), codecs = responseCodecs())
            },
            "PostgreSQL output ID must not be blank" to {
                PostgresOutput(id = " ", configuration = configuration(), codecs = outputCodecs())
            },
            "PostgreSQL outbox ID must not be blank" to {
                PostgresOutboxOutput(id = " ", configuration = configuration(), codecs = outputCodecs(), delegate = PlainOutput())
            },
            "response store ID exceeds the configured PostgreSQL bound" to {
                PostgresResponseStore(id = "ab", configuration = configuration(maxIdentifierBytes = 1), codecs = responseCodecs())
            },
            "output ID exceeds the configured PostgreSQL bound" to {
                PostgresOutput(id = "ab", configuration = configuration(maxIdentifierBytes = 1), codecs = outputCodecs())
            },
            "outbox ID exceeds the configured PostgreSQL bound" to {
                PostgresOutboxOutput(
                    id = "ab",
                    configuration = configuration(maxIdentifierBytes = 1),
                    codecs = outputCodecs(),
                    delegate = PlainOutput(),
                )
            },
        )
        blank.forEach { (message, construct) ->
            assertEquals(message, assertFailsWith<IllegalArgumentException> { construct() }.message)
        }
    }

    @Test
    fun `operations before start fail without creating a pool and recovery requires a started store`(): Unit = runBlocking {
        val state = PostgresCrawlStateStore(configuration = configuration())
        val responses = PostgresResponseStore(configuration = configuration(), codecs = responseCodecs())
        val output = PostgresOutput(configuration = configuration(), codecs = outputCodecs())
        val outbox = PostgresOutboxOutput(configuration = configuration(), codecs = outputCodecs(), delegate = PlainOutput())

        assertNotStarted("postgres-state") {
            state.enqueue(CrawlRequest("https://example.test/a"), RequestIdentity("a"), CrawlerContext("e", "r", "p"))
        }
        assertNotStarted("postgres-state") { state.recoverStaleRuns(Instant.EPOCH, Duration.ofSeconds(1)) }
        assertNotStarted("postgres-responses") { responses.put(response()) }
        assertNotStarted("postgres-outbox") { outbox.deliverPending("owner") }
        val failed = assertIs<OutputResult.Failed>(output.write(listOf(produced()), outputContext()))
        assertEquals("PostgreSQL adapter 'postgres-output' is not started", failed.error.message)
        assertEquals(0, adapterThreads("postgres-").size)
    }

    @Test
    fun `stop before start is terminal idempotent and never allocates resources`(): Unit = runBlocking {
        val delegate = PlainOutput()
        val outbox = PostgresOutboxOutput(id = "never-started", configuration = configuration(), codecs = outputCodecs(), delegate = delegate)

        outbox.stop(stopContext())
        outbox.stop(stopContext())
        outbox.stopForReplay(stopContext())
        outbox.close()
        outbox.close()

        assertEquals(
            "PostgreSQL adapter 'never-started' is closed",
            assertFailsWith<IllegalStateException> { outbox.start(startContext()) }.message,
        )
        assertEquals(
            "PostgreSQL adapter 'never-started' is closed",
            assertFailsWith<IllegalStateException> { outbox.startForReplay(startContext()) }.message,
        )
        assertEquals(0, delegate.writes)
        assertEquals(0, adapterThreads("never-started-db-").size)
    }

    private suspend fun assertNotStarted(adapterId: String, block: suspend () -> Unit) {
        val error = assertFailsWith<IllegalStateException> { block() }
        assertEquals("PostgreSQL adapter '$adapterId' is not started", error.message)
    }

    private fun adapterThreads(prefix: String): List<Thread> =
        Thread.getAllStackTraces().keys.filter { it.name.startsWith(prefix) && it.name.contains("-db-") && it.isAlive }

    private fun resultSet(bytes: ByteArray?): ResultSet = Proxy.newProxyInstance(
        ResultSet::class.java.classLoader,
        arrayOf(ResultSet::class.java),
    ) { _, method, _ ->
        when (method.name) {
            "getBytes" -> bytes?.copyOf()
            else -> throw UnsupportedOperationException("Unexpected ResultSet call ${method.name}")
        }
    } as ResultSet

    private fun configuration(maxIdentifierBytes: Int = 255): PostgresStateConfiguration = PostgresStateConfiguration(
        jdbcUrl = "jdbc:postgresql://db.invalid/trawler",
        maxIdentifierBytes = maxIdentifierBytes,
    )

    private fun responseCodecs(): ResponseCodecRegistry = ResponseCodecRegistry(listOf(BodyCodec))

    private fun outputCodecs(): OutputCodecRegistry<String> = OutputCodecRegistry(listOf(StringCodec))

    private fun produced(): Produced<String> = Produced(
        value = "value",
        context = ProductionContext(
            pipelineId = "p",
            engineRunId = "e",
            pipelineRunId = "r",
            requestId = null,
            responseReference = null,
            originalTarget = null,
            finalTarget = null,
            producedAt = Instant.EPOCH,
        ),
    )

    private fun outputContext(): OutputContext = OutputContext(CrawlerContext("e", "r", "p"))

    private fun response(): CrawlResponse = CrawlResponse(
        request = CrawlRequest("https://example.test/response"),
        statusCode = 200,
        headers = Headers.empty(),
        body = ResponseBody.from(byteArrayOf(1)),
        receivedAt = Instant.EPOCH,
    )

    private fun startContext(): ExtensionStartContext = ExtensionStartContext(CoroutineScope(Dispatchers.Default))

    private fun stopContext(): ExtensionStopContext = ExtensionStopContext(ExtensionStopReason.ENGINE_SHUTDOWN)

    /** Delegate that deliberately does not implement OutputLifecycle. */
    private class PlainOutput : Output<String> {
        override val id: String = "plain"
        var writes: Int = 0

        override suspend fun write(items: List<Produced<String>>, context: OutputContext): OutputResult {
            writes++
            return OutputResult.Accepted(items.size)
        }
    }

    private object StringCodec : OutputCodec<String> {
        override val descriptor: OutputCodecDescriptor = OutputCodecDescriptor("string", 1)

        override fun encode(item: Produced<String>): ByteArray = item.value.toByteArray()

        override fun decode(payload: ByteArray): Produced<String> = error("Not used without a database")
    }

    private object BodyCodec : ResponseCodec {
        override val descriptor: ResponseCodecDescriptor = ResponseCodecDescriptor("body", 1)

        override fun encode(response: CrawlResponse): ByteArray = response.body.toByteArray()

        override fun decode(payload: ByteArray): CrawlResponse = error("Not used without a database")
    }
}
