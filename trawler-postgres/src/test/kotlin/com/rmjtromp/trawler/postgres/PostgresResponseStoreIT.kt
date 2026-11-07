package com.rmjtromp.trawler.postgres

import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.extension.ExtensionStopReason
import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.RequestBody
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.model.ResponseReference
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.response.InMemoryResponseStore
import com.rmjtromp.trawler.response.ResponseCodec
import com.rmjtromp.trawler.response.ResponseCodecDescriptor
import com.rmjtromp.trawler.response.ResponseCodecRegistry
import com.rmjtromp.trawler.response.ResponseLookup
import com.rmjtromp.trawler.response.ResponseReplayQuery
import com.rmjtromp.trawler.response.ResponseRetentionRequest
import java.sql.Connection
import java.sql.DriverManager
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertContentEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/** Regression coverage for response replay ownership predicates. */
@Testcontainers
class PostgresResponseStoreIT {
    @Test
    fun `response adapter owns an independent migration and replay lifecycle`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema)

        assertFailsWith<IllegalStateException> {
            store.find(ResponseLookup.ByTarget("https://example.test/not-started"))
        }
        store.start(startContext())
        store.start(startContext())
        assertEquals(1, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"flyway_response_schema_history\" WHERE type = 'SQL'"))
        assertTrue(tableExists(schema, "response_records"))
        assertTrue(tableExists(schema, "response_lineage"))
        assertFalse(tableExists(schema, "jobs"))
        assertFalse(tableExists(schema, "output_entries"))
        assertFalse(tableExists(schema, "outbox_entries"))

        store.stopForReplay(stopContext())
        store.startForReplay(startContext())
        val reference = assertNotNull(store.put(response("replay", pipelineId = "p", body = byteArrayOf(7))))
        assertEquals(
            reference,
            store.find(ResponseLookup.ByReference(reference))?.reference,
        )
        store.stopForReplay(stopContext())
        store.start(startContext())
        store.stop(stopContext())
        store.close()
        assertFailsWith<IllegalStateException> {
            store.find(ResponseLookup.ByReference(reference))
        }
        assertFailsWith<IllegalStateException> { store.start(startContext()) }
    }

    @Test
    fun `pipeline replay applies owner predicate before limit`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema)
        store.start(startContext())
        try {
            store.put(response("source-first", pipelineId = "source", body = byteArrayOf(1)))
            store.put(response("source-second", pipelineId = "source", body = byteArrayOf(2)))
            val ownerReference = assertNotNull(store.put(response("owner", pipelineId = "owner", body = byteArrayOf(3))))
            assertEquals(
                emptyList(),
                store.replay(
                    query = ResponseReplayQuery.ByPipelineId("source"),
                    limit = 1,
                    pipelineId = "owner",
                ),
            )
            assertEquals(
                listOf(ownerReference),
                store.replay(
                    query = ResponseReplayQuery.ByPipelineId("owner"),
                    limit = 1,
                    pipelineId = "owner",
                ).map { it.reference },
            )
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `responses preserve codec metadata and newest lookup while replay history stays ordered and bounded`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(schema)
        store.start(startContext())
        try {
            val first = assertNotNull(
                store.put(response("same", requestId = "request", pipelineId = "p", body = byteArrayOf(1))),
            )
            val second = assertNotNull(
                store.put(response("same", requestId = "request", pipelineId = "p", body = byteArrayOf(2))),
            )

            assertEquals(second, store.find(ResponseLookup.ByTarget("https://example.test/same"))?.reference)
            assertEquals(second, store.find(ResponseLookup.ByRequestId("request"))?.reference)
            assertEquals(second, store.find(ResponseLookup.ByReference(second))?.reference)
            assertEquals(
                listOf(first, second),
                store.replay(ResponseReplayQuery.ByRequestId("request"), limit = 8).map { it.reference },
            )
            assertEquals(
                listOf(first),
                store.history("request", limit = 1).map { it.reference },
            )
            assertEquals(
                listOf(first, second),
                store.replay(ResponseReplayQuery.ByPipelineId("p"), limit = 8).map { it.reference },
            )

            val row = responseRow(schema, second)
            assertEquals("it-response", row.codecId)
            assertEquals(1, row.codecVersion)
            assertEquals(1, row.bodySize)
            assertContentEquals(byteArrayOf(2), row.payload)
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `response body request body and encoded payload bounds refuse before persistence`(): Unit = runBlocking {
        val bodySchema = nextSchema()
        val bodyCodec = CountingResponseCodec(byteArrayOf(9))
        val bodyStore = store(
            bodySchema,
            codecs = ResponseCodecRegistry(listOf(bodyCodec)),
            config = configuration(
                bodySchema,
                maxResponseBodyBytes = 2,
                maxRequestBodyBytes = 1,
            ),
        )
        bodyStore.start(startContext())
        try {
            assertNull(bodyStore.put(response("too-large-body", pipelineId = "p", body = byteArrayOf(1, 2, 3))))
            assertEquals(0, bodyCodec.encodeCalls)
            assertNull(
                bodyStore.put(
                    response(
                        "too-large-request-body",
                        pipelineId = "p",
                        body = byteArrayOf(1),
                        requestBody = byteArrayOf(1, 2),
                    ),
                ),
            )
            assertEquals(0, bodyCodec.encodeCalls)
        } finally {
            bodyStore.stop(stopContext())
        }

        val encodedSchema = nextSchema()
        val encodedCodec = CountingResponseCodec(byteArrayOf(1, 2, 3))
        val encodedStore = store(
            encodedSchema,
            codecs = ResponseCodecRegistry(listOf(encodedCodec)),
            config = configuration(encodedSchema, maxEncodedResponseBytes = 2),
        )
        encodedStore.start(startContext())
        try {
            assertNull(encodedStore.put(response("too-large-encoded", pipelineId = "p", body = byteArrayOf(1))))
            assertEquals(1, encodedCodec.encodeCalls)
            assertEquals(0, scalarInt(encodedSchema, "SELECT COUNT(*) FROM \"$encodedSchema\".\"response_records\""))
        } finally {
            encodedStore.stop(stopContext())
        }
    }

    @Test
    fun `response reads reject database payload and stored-body violations before decoding`(): Unit = runBlocking {
        val schema = nextSchema()
        val store = store(
            schema,
            config = configuration(schema, maxResponseBodyBytes = 2, maxEncodedResponseBytes = 2),
        )
        store.start(startContext())
        try {
            val payloadReference = assertNotNull(store.put(response("payload-bound", pipelineId = "p", body = byteArrayOf(1))))
            updateBytes(schema, payloadReference, byteArrayOf(1, 2, 3))
            assertFailsWith<IllegalArgumentException> {
                store.find(ResponseLookup.ByReference(payloadReference))
            }

            val bodyReference = assertNotNull(store.put(response("body-bound", pipelineId = "p", body = byteArrayOf(1))))
            withConnection { connection ->
                connection.prepareStatement(
                    "UPDATE \"$schema\".\"response_records\" SET body_size = 3 WHERE response_id = ?",
                ).use { statement ->
                    statement.setString(1, bodyReference.value)
                    statement.executeUpdate()
                }
            }
            assertFailsWith<IllegalArgumentException> {
                store.find(ResponseLookup.ByReference(bodyReference))
            }
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `response replay limit is positive and capped while historical codec absence is visible`(): Unit = runBlocking {
        val schema = nextSchema()
        val writer = store(schema, config = configuration(schema, maxResponseQueryLimit = 2))
        writer.start(startContext())
        val references = try {
            (1..3).map { index ->
                assertNotNull(writer.put(response("bounded-$index", pipelineId = "p", body = byteArrayOf(index.toByte()))))
            }
        } finally {
            writer.stop(stopContext())
        }

        val reader = store(
            schema,
            codecs = ResponseCodecRegistry(
                listOf(MarkerResponseCodec(ResponseCodecDescriptor("different-codec", 1))),
            ),
            config = configuration(schema, maxResponseQueryLimit = 2),
        )
        reader.start(startContext())
        try {
            val error = assertFailsWith<IllegalStateException> {
                reader.replay(ResponseReplayQuery.ByPipelineId("p"), limit = 99)
            }
            assertTrue(error.message.orEmpty().contains("it-response@1"))
        } finally {
            reader.stop(stopContext())
        }

        val validReader = store(schema, config = configuration(schema, maxResponseQueryLimit = 2))
        validReader.start(startContext())
        try {
            assertEquals(references.take(2), validReader.replay(ResponseReplayQuery.ByPipelineId("p"), 99).map { it.reference })
            assertFailsWith<IllegalArgumentException> {
                validReader.replay(ResponseReplayQuery.ByPipelineId("p"), 0)
            }
        } finally {
            validReader.stop(stopContext())
        }
    }

    @Test
    fun `response retention honors strict cutoff caller protection lineage and adapter batch bound`(): Unit = runBlocking {
        val schema = nextSchema()
        val clock = MutableClock(Instant.parse("2026-01-01T00:00:00Z"))
        val store = store(
            schema,
            clock = clock,
            config = configuration(schema, maxRetentionRows = 1),
        )
        store.start(startContext())
        try {
            val old = assertNotNull(store.put(response("old", pipelineId = "p", body = byteArrayOf(1))))
            clock.current = Instant.parse("2026-01-02T00:00:00Z")
            val protected = assertNotNull(store.put(response("protected", pipelineId = "p", body = byteArrayOf(2))))
            clock.current = Instant.parse("2026-01-03T00:00:00Z")
            val exact = assertNotNull(store.put(response("exact", pipelineId = "p", body = byteArrayOf(3))))
            clock.current = Instant.parse("2026-01-04T00:00:00Z")
            val newer = assertNotNull(store.put(response("newer", pipelineId = "p", body = byteArrayOf(4))))

            val first = store.prune(
                ResponseRetentionRequest(
                    cutoff = Instant.parse("2026-01-03T00:00:00Z"),
                    maxEntries = 99,
                    protectedReferences = setOf(protected),
                ),
            )
            assertEquals(1, first.deleted)
            assertEquals(1, first.protectedEntries)
            assertNull(store.find(ResponseLookup.ByReference(old)))
            assertNotNull(store.find(ResponseLookup.ByReference(protected)))
            assertNotNull(store.find(ResponseLookup.ByReference(exact)))
            assertNotNull(store.find(ResponseLookup.ByReference(newer)))

            val second = store.prune(
                ResponseRetentionRequest(
                    cutoff = Instant.parse("2026-01-05T00:00:00Z"),
                    maxEntries = 99,
                ),
            )
            assertEquals(1, second.deleted)
            assertNotNull(store.find(ResponseLookup.ByReference(newer)))
        } finally {
            store.stop(stopContext())
        }
    }

    @Test
    fun `response adapter migrates into a shared schema that already contains other tables`(): Unit = runBlocking {
        val schema = nextSchema()
        withConnection { connection ->
            connection.createStatement().use {
                it.execute("CREATE SCHEMA \"$schema\"")
                it.execute("CREATE TABLE \"$schema\".\"host_owned\" (id INTEGER PRIMARY KEY)")
                it.execute("INSERT INTO \"$schema\".\"host_owned\" VALUES (7)")
            }
        }
        val store = store(schema)
        store.start(startContext())
        try {
            assertTrue(tableExists(schema, "response_records"))
            assertEquals(
                1,
                scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"flyway_response_schema_history\" WHERE type = 'SQL' AND success"),
            )
            assertEquals(1, scalarInt(schema, "SELECT COUNT(*) FROM \"$schema\".\"host_owned\" WHERE id = 7"))
            assertNotNull(store.put(response("https://example.test/shared", "pipeline", byteArrayOf(1))))
        } finally {
            store.stop(stopContext())
        }
    }

    private fun store(
        schema: String,
        codecs: ResponseCodecRegistry = ResponseCodecRegistry(listOf(MarkerResponseCodec())),
        config: PostgresStateConfiguration = configuration(schema),
        clock: Clock = Clock { Instant.parse("2026-01-01T00:00:00Z") },
    ): PostgresResponseStore = PostgresResponseStore(
        id = "it-$schema",
        configuration = config,
        codecs = codecs,
        clock = clock,
    )

    private fun configuration(
        schema: String,
        maxResponseBodyBytes: Int = 8 * 1_024 * 1_024,
        maxRequestBodyBytes: Int = 8 * 1_024 * 1_024,
        maxEncodedResponseBytes: Int = 8 * 1_024 * 1_024,
        maxResponseQueryLimit: Int = 4_096,
        maxRetentionRows: Int = 4_096,
    ): PostgresStateConfiguration = PostgresStateConfiguration(
        jdbcUrl = postgres.jdbcUrl,
        username = postgres.username,
        password = postgres.password.toCharArray(),
        schema = schema,
        maxResponseBodyBytes = maxResponseBodyBytes,
        maxRequestBodyBytes = maxRequestBodyBytes,
        maxEncodedResponseBytes = maxEncodedResponseBytes,
        maxResponseQueryLimit = maxResponseQueryLimit,
        maxRetentionRows = maxRetentionRows,
    )

    private fun response(
        target: String,
        pipelineId: String,
        body: ByteArray,
        requestId: String = "request-$target",
        requestBody: ByteArray? = null,
    ): CrawlResponse = CrawlResponse(
        request = CrawlRequest(
            target = "https://example.test/$target",
            body = requestBody?.let(RequestBody::from),
            attributes = ContextAttributes.of(
                ContextAttribute(InMemoryResponseStore.PIPELINE_ID_ATTRIBUTE, pipelineId),
                ContextAttribute(InMemoryResponseStore.REQUEST_ID_ATTRIBUTE, requestId),
            ),
        ),
        statusCode = 200,
        headers = Headers.empty(),
        body = ResponseBody.from(body),
        receivedAt = Instant.parse("2026-01-01T00:00:00Z"),
    )

    private fun startContext(): ExtensionStartContext =
        ExtensionStartContext(kotlinx.coroutines.CoroutineScope(Dispatchers.Default))

    private fun stopContext(): ExtensionStopContext = ExtensionStopContext(ExtensionStopReason.ENGINE_SHUTDOWN)

    private companion object {
        val schemaSequence = AtomicInteger()

        @Container
        @JvmField
        val postgres: PostgreSQLContainer<Nothing> = PostgreSQLContainer<Nothing>("postgres:16.4-alpine")

        fun nextSchema(): String = "response_it_${schemaSequence.incrementAndGet()}"
    }

    private class MarkerResponseCodec(
        override val descriptor: ResponseCodecDescriptor = ResponseCodecDescriptor("it-response", 1),
    ) : ResponseCodec {

        override fun encode(response: CrawlResponse): ByteArray = response.body.toByteArray()

        override fun decode(payload: ByteArray): CrawlResponse = CrawlResponse(
            request = CrawlRequest("https://decoded.test/response"),
            statusCode = 200,
            headers = Headers.empty(),
            body = ResponseBody.from(payload),
            receivedAt = Instant.EPOCH,
        )
    }

    private class CountingResponseCodec(
        private val encoded: ByteArray,
    ) : ResponseCodec {
        override val descriptor: ResponseCodecDescriptor = ResponseCodecDescriptor("counting-response", 1)
        var encodeCalls: Int = 0

        override fun encode(response: CrawlResponse): ByteArray {
            encodeCalls++
            return encoded.copyOf()
        }

        override fun decode(payload: ByteArray): CrawlResponse = CrawlResponse(
            request = CrawlRequest("https://decoded.test/counting"),
            statusCode = 200,
            headers = Headers.empty(),
            body = ResponseBody.from(payload),
            receivedAt = Instant.EPOCH,
        )
    }

    private class MutableClock(var current: Instant) : Clock {
        override fun now(): Instant = current
    }

    private data class ResponseRow(
        val codecId: String,
        val codecVersion: Int,
        val bodySize: Int,
        val payload: ByteArray,
    )

    private fun responseRow(schema: String, reference: ResponseReference): ResponseRow = withConnection { connection ->
        connection.prepareStatement(
            "SELECT codec_id, codec_version, body_size, payload FROM \"$schema\".\"response_records\" WHERE response_id = ?",
        ).use { statement ->
            statement.setString(1, reference.value)
            statement.executeQuery().use { result ->
                assertTrue(result.next())
                ResponseRow(
                    codecId = result.getString("codec_id"),
                    codecVersion = result.getInt("codec_version"),
                    bodySize = result.getInt("body_size"),
                    payload = result.getBytes("payload"),
                )
            }
        }
    }

    private fun updateBytes(schema: String, reference: ResponseReference, payload: ByteArray) {
        withConnection { connection ->
            connection.prepareStatement(
                "UPDATE \"$schema\".\"response_records\" SET payload = ? WHERE response_id = ?",
            ).use { statement ->
                statement.setBytes(1, payload)
                statement.setString(2, reference.value)
                statement.executeUpdate()
            }
        }
    }

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
}
