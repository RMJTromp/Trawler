package com.rmjtromp.trawler.postgres

import java.time.Duration
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
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
            { PostgresStateConfiguration("jdbc:postgresql://db", maximumPoolSize = 2, minimumIdle = -1) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maximumPoolSize = 2, minimumIdle = 3) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maximumPoolSize = 1, databaseDispatcherParallelism = 0) },
            { PostgresStateConfiguration("jdbc:postgresql://db", maximumPoolSize = 1, databaseDispatcherParallelism = 2) },
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
