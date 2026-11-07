package com.rmjtromp.trawler.postgres

import java.time.Duration
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class PostgresStateConfigurationTest {
    @Test
    fun `configuration accepts exact boundaries and keeps password ownership defensive`() {
        val suppliedPassword = charArrayOf('s', 'e', 'c', 'r', 'e', 't')
        val configuration = PostgresStateConfiguration(
            jdbcUrl = "jdbc:postgresql://db.example/trawler?user=embedded&password=secret",
            username = "db-user",
            password = suppliedPassword,
            schema = "a".repeat(63),
            maximumPoolSize = 2,
            minimumIdle = 2,
            databaseDispatcherParallelism = 2,
            connectionTimeout = Duration.ofMillis(250),
            statementTimeout = Duration.ofMillis(1),
            leaseDuration = Duration.ofMillis(1),
            staleRunAfter = Duration.ofMillis(1),
            maxActiveJobs = 1,
            maxClaimBatch = 1,
            maxCancellationRows = 1,
            maxRecoveryRows = 1,
            maxCheckpointBatch = 1,
            maxCheckpointPayloadBytes = 1,
            maxEncodedRequestBytes = 1,
            maxRequestBodyBytes = 0,
            maxResponseBodyBytes = 1,
            maxEncodedResponseBytes = 1,
            maxOutputPayloadBytes = 1,
            maxOutputBatchItems = 1,
            maxResponseQueryLimit = 1,
            maxOutboxClaimBatch = 1,
            maxRetentionRows = 1,
            outboxLeaseDuration = Duration.ofMillis(1),
            maxOutboxAttempts = 1,
            maxHeaderCount = 0,
            maxAttributeCount = 0,
            maxHeaderNameBytes = 1,
            maxHeaderValueBytes = 0,
            maxTargetBytes = 1,
            maxAttributeNameBytes = 1,
            maxAttributeValueBytes = 0,
            maxIdentifierBytes = 1,
            maxIdentityBytes = 1,
            maxReasonBytes = 1,
            poolName = "pool-name",
        )

        suppliedPassword[0] = 'x'
        assertContentEquals(charArrayOf('s', 'e', 'c', 'r', 'e', 't'), configuration.passwordCopy())
        val returnedPassword = requireNotNull(configuration.passwordCopy())
        returnedPassword[1] = 'x'
        assertContentEquals(charArrayOf('s', 'e', 'c', 'r', 'e', 't'), configuration.passwordCopy())

        assertEquals(1, configuration.requestCodecLimits().maxEncodedBytes)
        assertEquals(0, configuration.requestCodecLimits().maxBodyBytes)
        assertTrue(configuration.toString().contains("jdbc:postgresql://db.example/trawler?<redacted>"))
        assertFalse(configuration.toString().contains("secret"))
        assertFalse(configuration.toString().contains("db-user"))
    }

    @Test
    fun `configuration rejects every invalid bound and identifier branch`() {
        val invalid = listOf<() -> Unit>(
            { PostgresStateConfiguration("postgres://db") },
            { PostgresStateConfiguration("jdbc:postgresql://db", username = " ") },
            { PostgresStateConfiguration("jdbc:postgresql://db", schema = "") },
            { PostgresStateConfiguration("jdbc:postgresql://db", schema = "1schema") },
            { PostgresStateConfiguration("jdbc:postgresql://db", schema = "bad-name") },
            { PostgresStateConfiguration("jdbc:postgresql://db", schema = "a".repeat(64)) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maximumPoolSize = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maximumPoolSize = 1, databaseDispatcherParallelism = 1) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maximumPoolSize = 2, minimumIdle = -1) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maximumPoolSize = 2, minimumIdle = 3) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maximumPoolSize = 2, databaseDispatcherParallelism = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maximumPoolSize = 2, databaseDispatcherParallelism = 3) },
            { PostgresStateConfiguration("jdbc:postgresql://db", connectionTimeout = Duration.ZERO) },
            { PostgresStateConfiguration("jdbc:postgresql://db", connectionTimeout = Duration.ofMillis(249)) },
            { PostgresStateConfiguration("jdbc:postgresql://db", statementTimeout = Duration.ZERO) },
            { PostgresStateConfiguration("jdbc:postgresql://db", leaseDuration = Duration.ZERO) },
            { PostgresStateConfiguration("jdbc:postgresql://db", staleRunAfter = Duration.ZERO) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxActiveJobs = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxActiveJobs = 1, maxClaimBatch = 2) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxClaimBatch = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxCancellationRows = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxRecoveryRows = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxCheckpointBatch = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxCheckpointPayloadBytes = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxEncodedRequestBytes = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxRequestBodyBytes = -1) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxResponseBodyBytes = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxEncodedResponseBytes = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxOutputPayloadBytes = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxOutputBatchItems = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxResponseQueryLimit = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxOutboxClaimBatch = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxRetentionRows = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", outboxLeaseDuration = Duration.ZERO) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxOutboxAttempts = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxHeaderCount = -1) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxAttributeCount = -1) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxHeaderNameBytes = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxHeaderValueBytes = -1) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxTargetBytes = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxAttributeNameBytes = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxAttributeValueBytes = -1) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxIdentifierBytes = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxIdentifierBytes = 256) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxIdentityBytes = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maxReasonBytes = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", poolName = "") },
        )

        invalid.forEach { candidate -> assertFailsWith<IllegalArgumentException> { candidate() } }

        val validOneByteIdentifier = PostgresStateConfiguration(
            jdbcUrl = "jdbc:postgresql://db",
            maxIdentifierBytes = 1,
            schema = "a",
        )
        assertEquals("a", validOneByteIdentifier.schema)
    }

    @Test
    fun `state store enforces database column bounds and UTF-8 identifiers`() {
        val base = configuration()

        assertFailsWith<IllegalArgumentException> {
            PostgresCrawlStateStore(id = " ", configuration = base)
        }
        assertFailsWith<IllegalArgumentException> {
            PostgresCrawlStateStore(
                id = "é",
                configuration = configuration(maxIdentifierBytes = 1),
            )
        }
        assertFailsWith<IllegalArgumentException> {
            PostgresCrawlStateStore(
                configuration = configuration(maxIdentityBytes = 4_097),
            )
        }
        assertFailsWith<IllegalArgumentException> {
            PostgresCrawlStateStore(
                configuration = configuration(maxReasonBytes = 4_097),
            )
        }

        PostgresCrawlStateStore(
            id = "é",
            configuration = configuration(maxIdentifierBytes = 2),
        )
    }

    @Test
    fun `configuration without credentials keeps no password and renders an unredacted plain URL`() {
        val configuration = PostgresStateConfiguration(jdbcUrl = "jdbc:postgresql://db/trawler", schema = "_private")

        assertNull(configuration.username)
        assertNull(configuration.passwordCopy())
        assertEquals("_private", configuration.schema)
        assertEquals(
            "PostgresStateConfiguration(jdbcUrl=jdbc:postgresql://db/trawler, schema=_private, " +
                "maximumPoolSize=8, databaseDispatcherParallelism=8)",
            configuration.toString(),
        )
    }

    @Test
    fun `pool and dispatcher bounds report which limit was violated`() {
        val cases = listOf<Pair<String, () -> Unit>>(
            "PostgreSQL pool size must be at least 2 for Flyway migration" to {
                PostgresStateConfiguration("jdbc:postgresql://db", maximumPoolSize = 1, databaseDispatcherParallelism = 1)
            },
            "PostgreSQL dispatcher parallelism must be positive" to {
                PostgresStateConfiguration("jdbc:postgresql://db", maximumPoolSize = 2, databaseDispatcherParallelism = 0)
            },
            "PostgreSQL dispatcher parallelism must not exceed the pool size" to {
                PostgresStateConfiguration("jdbc:postgresql://db", maximumPoolSize = 2, databaseDispatcherParallelism = 3)
            },
        )
        cases.forEach { (message, candidate) ->
            assertEquals(message, assertFailsWith<IllegalArgumentException> { candidate() }.message)
        }
        assertEquals(1, PostgresStateConfiguration("jdbc:postgresql://db", maximumPoolSize = 2, databaseDispatcherParallelism = 1).databaseDispatcherParallelism)
    }

    @Test
    fun `every timeout rejects negative durations with its own message`() {
        val negative = Duration.ofMillis(-1)
        val cases = mapOf<String, () -> Unit>(
            "connection timeout" to { PostgresStateConfiguration("jdbc:postgresql://db", connectionTimeout = negative) },
            "statement timeout" to { PostgresStateConfiguration("jdbc:postgresql://db", statementTimeout = negative) },
            "lease duration" to { PostgresStateConfiguration("jdbc:postgresql://db", leaseDuration = negative) },
            "stale-run threshold" to { PostgresStateConfiguration("jdbc:postgresql://db", staleRunAfter = negative) },
            "outbox lease duration" to {
                PostgresStateConfiguration("jdbc:postgresql://db", outboxLeaseDuration = negative)
            },
        )
        cases.forEach { (label, candidate) ->
            val error = assertFailsWith<IllegalArgumentException> { candidate() }
            assertEquals("PostgreSQL $label must be positive", error.message)
        }
    }

    @Test
    fun `request codec limits and descriptors validate every field at the exact boundary`() {
        val exact = PostgresRequestCodecLimits(
            maxEncodedBytes = 1,
            maxBodyBytes = 0,
            maxHeaderCount = 0,
            maxAttributeCount = 0,
            maxHeaderNameBytes = 1,
            maxHeaderValueBytes = 0,
            maxTargetBytes = 1,
            maxAttributeNameBytes = 1,
            maxAttributeValueBytes = 0,
        )
        val invalid = listOf<Pair<String, () -> Unit>>(
            "Encoded request bound must be positive" to { exact.copy(maxEncodedBytes = 0) },
            "Request body bound must not be negative" to { exact.copy(maxBodyBytes = -1) },
            "Header count bound must not be negative" to { exact.copy(maxHeaderCount = -1) },
            "Attribute count bound must not be negative" to { exact.copy(maxAttributeCount = -1) },
            "Header name bound must be positive" to { exact.copy(maxHeaderNameBytes = 0) },
            "Header value bound must not be negative" to { exact.copy(maxHeaderValueBytes = -1) },
            "Request target bound must be positive" to { exact.copy(maxTargetBytes = 0) },
            "Attribute name bound must be positive" to { exact.copy(maxAttributeNameBytes = 0) },
            "Attribute value bound must not be negative" to { exact.copy(maxAttributeValueBytes = -1) },
            "PostgreSQL codec ID must not be blank" to { PostgresCodecDescriptor(" ", 0) },
            "PostgreSQL codec version must not be negative" to { PostgresCodecDescriptor("codec", -1) },
        )
        invalid.forEach { (message, candidate) ->
            assertEquals(message, assertFailsWith<IllegalArgumentException> { candidate() }.message)
        }
        assertEquals(0, PostgresCodecDescriptor("codec", 0).version)
    }

    private fun configuration(
        maxIdentifierBytes: Int = 255,
        maxIdentityBytes: Int = 4 * 1_024,
        maxReasonBytes: Int = 4 * 1_024,
    ): PostgresStateConfiguration = PostgresStateConfiguration(
        jdbcUrl = "jdbc:postgresql://db",
        maxIdentifierBytes = maxIdentifierBytes,
        maxIdentityBytes = maxIdentityBytes,
        maxReasonBytes = maxReasonBytes,
    )
}
