package com.rmjtromp.trawler.postgres

import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.extension.ReplayLifecycle
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.output.OutputCodecDescriptor
import com.rmjtromp.trawler.output.OutputCodecRegistry
import com.rmjtromp.trawler.output.OutputLifecycle
import com.rmjtromp.trawler.output.OutputRetentionRequest
import com.rmjtromp.trawler.output.OutputRetentionStore
import com.rmjtromp.trawler.pipeline.Output
import com.rmjtromp.trawler.pipeline.OutputResult
import com.rmjtromp.trawler.pipeline.Produced
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.SystemClock
import com.rmjtromp.trawler.retention.RetentionCategory
import com.rmjtromp.trawler.retention.RetentionSummary
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

/** Summary of one bounded, explicitly requested outbox delivery pass. */
public data class OutboxDeliverySummary(
    public val claimed: Int,
    public val delivered: Int,
    public val retried: Int,
    public val failed: Int,
    public val stale: Int,
)

/**
 * PostgreSQL output-owned durable outbox decorator.
 *
 * [write] durably enqueues typed `Produced<T>` values and acknowledges only
 * after commit. The host explicitly calls [deliverPending]; this class starts
 * no background worker and therefore cannot hide retries or network work in a
 * plugin. Claims use PostgreSQL row locks and unique fencing tokens. A
 * delegate acknowledgement followed by process loss can produce a duplicate
 * on the next claim; the adapter makes no exactly-once claim.
 *
 * The delegate receives the decoded typed value through the normal [Output]
 * contract. Delegate calls happen outside the database transaction, and every
 * terminal update is fenced by the claim token. Register this object as an
 * engine extension so migration and cleanup are deterministic. The delegate's
 * lifecycle is not started implicitly: when it implements
 * [ExtensionLifecycle], register it as a preceding engine extension as well.
 * This keeps lifecycle ownership explicit and avoids hidden adapter startup.
 */
public class PostgresOutboxOutput<T : Any>(
    override val id: String = "postgres-outbox",
    public val configuration: PostgresStateConfiguration,
    public val codecs: OutputCodecRegistry<T>,
    private val delegate: Output<T>,
    private val clock: Clock = SystemClock,
) : Output<T>,
    OutputRetentionStore,
    OutputLifecycle,
    ExtensionLifecycle,
    ReplayLifecycle {

    private val resources = PostgresAdapterResources(
        id,
        configuration,
        "classpath:db/migration/outbox",
        "flyway_outbox_schema_history",
    )
    private val delegateClosed = AtomicBoolean(false)

    override val retentionCategories: Set<RetentionCategory> = setOf(RetentionCategory.OUTBOX_HISTORY)

    init {
        require(id.isNotBlank()) { "PostgreSQL outbox ID must not be blank" }
        requirePostgresUtf8Bound(id, configuration.maxIdentifierBytes, "outbox ID")
    }

    override suspend fun start(context: ExtensionStartContext) {
        resources.start()
    }

    override suspend fun stop(context: ExtensionStopContext) {
        resources.stop()
    }

    override suspend fun startForReplay(context: ExtensionStartContext) {
        resources.start()
    }

    override suspend fun stopForReplay(context: ExtensionStopContext) {
        resources.stopForReplay()
    }

    override suspend fun close() {
        try {
            resources.stop()
        } finally {
            if (delegateClosed.compareAndSet(false, true)) {
                (delegate as? OutputLifecycle)?.close()
            }
        }
    }

    override suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult {
        if (items.isEmpty()) return OutputResult.Accepted(0)
        if (items.size > configuration.maxOutputBatchItems) {
            return OutputResult.Failed(
                IllegalArgumentException("PostgreSQL outbox batch exceeds the configured bound"),
            )
        }
        return try {
            resources.transaction { connection ->
                val now = clock.now()
                items.forEach { item ->
                    val codec = codecs.active()
                    val payload = codec.encode(item)
                    require(payload.size <= configuration.maxOutputPayloadBytes) {
                        "Encoded PostgreSQL outbox payload exceeds the configured bound"
                    }
                    requirePostgresUtf8Bound(codec.descriptor.id, configuration.maxIdentifierBytes, "outbox codec ID")
                    requirePostgresUtf8Bound(item.context.pipelineId, configuration.maxIdentifierBytes, "outbox pipeline ID")
                    item.context.responseReference?.let {
                        requirePostgresUtf8Bound(it.value, configuration.maxIdentifierBytes, "outbox response reference")
                    }
                    val sequence = nextSequence(connection)
                    val entryId = "postgres-outbox-$sequence"
                    connection.prepareBounded(
                        """
                        INSERT INTO ${resources.table("outbox_entries")} (
                            outbox_id, sequence, output_id, pipeline_id, created_at,
                            status, codec_id, codec_version, payload, response_reference
                        ) VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?)
                        """.trimIndent(),
                        configuration,
                    ).use { statement ->
                        statement.setString(1, entryId)
                        statement.setLong(2, sequence)
                        statement.setString(3, id)
                        statement.setString(4, item.context.pipelineId)
                        statement.setTimestamp(5, resources.timestamp(now))
                        statement.setString(6, codec.descriptor.id)
                        statement.setInt(7, codec.descriptor.version)
                        statement.setBytes(8, payload)
                        statement.setString(9, item.context.responseReference?.value)
                        statement.executeUpdate()
                    }
                    insertLineageIfPresent(connection, item.context.responseReference?.value, entryId, now)
                }
            }
            OutputResult.Accepted(items.size)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Throwable) {
            OutputResult.Failed(error)
        }
    }

    /**
     * Claims and delivers at most [limit] rows for [ownerId]. Expired leases
     * are reclaimable; a delivery call never holds a database transaction
     * while invoking [delegate].
     */
    public suspend fun deliverPending(
        ownerId: String,
        limit: Int = configuration.maxOutboxClaimBatch,
        now: Instant = clock.now(),
    ): OutboxDeliverySummary {
        require(ownerId.isNotBlank()) { "Outbox delivery owner must not be blank" }
        requirePostgresUtf8Bound(ownerId, configuration.maxIdentifierBytes, "outbox delivery owner")
        val boundedLimit = resources.boundLimit(limit).coerceAtMost(configuration.maxOutboxClaimBatch)
        val claims = resources.transaction { connection -> claim(connection, ownerId, boundedLimit, now) }
        var delivered = 0
        var retried = 0
        var failed = 0
        var stale = 0
        for (claim in claims) {
            try {
                val produced = codecs.decode(claim.descriptor, claim.payload)
                val result = delegate.write(
                    listOf(produced),
                    OutputContext(produced.toCrawlerContext()),
                )
                when (result) {
                    is OutputResult.Accepted -> {
                        if (result.itemCount != 1) {
                            val outcome = markFailure(claim, ownerId, "delegate-ack-count")
                            if (outcome == DeliveryMutation.REQUEUED) retried++
                            if (outcome == DeliveryMutation.FAILED) failed++
                            if (outcome == DeliveryMutation.STALE) stale++
                        } else if (markDelivered(claim, ownerId)) {
                            delivered++
                        } else {
                            stale++
                        }
                    }

                    is OutputResult.Skipped -> {
                        val outcome = markFailure(claim, ownerId, "delegate-skipped")
                        if (outcome == DeliveryMutation.REQUEUED) retried++
                        if (outcome == DeliveryMutation.FAILED) failed++
                        if (outcome == DeliveryMutation.STALE) stale++
                    }

                    is OutputResult.Failed -> {
                        val outcome = markFailure(claim, ownerId, "delegate-failed:${safeFailureName(result.error)}")
                        if (outcome == DeliveryMutation.REQUEUED) retried++
                        if (outcome == DeliveryMutation.FAILED) failed++
                        if (outcome == DeliveryMutation.STALE) stale++
                    }
                }
            } catch (cancelled: CancellationException) {
                withContext(NonCancellable) { releaseClaim(claim, ownerId) }
                throw cancelled
            } catch (error: Throwable) {
                val outcome = markFailure(claim, ownerId, "delivery-failed:${safeFailureName(error)}")
                if (outcome == DeliveryMutation.REQUEUED) retried++
                if (outcome == DeliveryMutation.FAILED) failed++
                if (outcome == DeliveryMutation.STALE) stale++
            }
        }
        return OutboxDeliverySummary(claims.size, delivered, retried, failed, stale)
    }

    override suspend fun prune(request: OutputRetentionRequest): RetentionSummary {
        require(request.category == RetentionCategory.OUTBOX_HISTORY) {
            "PostgreSQL outbox owns only outbox history retention"
        }
        return resources.transaction { connection ->
            val limit = resources.boundRetentionLimit(request.maxEntries)
            val entries = connection.prepareBounded(
                """
                SELECT outbox_id
                FROM ${resources.table("outbox_entries")}
                WHERE output_id = ?
                  AND created_at < ?
                  AND status IN ('DELIVERED', 'FAILED')
                ORDER BY sequence ASC
                LIMIT ?
                """.trimIndent(),
                configuration,
            ).use { statement ->
                statement.setString(1, id)
                statement.setTimestamp(2, resources.timestamp(request.cutoff))
                statement.setInt(3, limit)
                statement.executeQuery().use { result ->
                    buildList {
                        while (result.next()) add(result.getString("outbox_id"))
                    }
                }
            }
            entries.forEach { entryId ->
                deleteLineage(connection, entryId)
                connection.prepareBounded(
                    "DELETE FROM ${resources.table("outbox_entries")} WHERE outbox_id = ? AND output_id = ?",
                    configuration,
                ).use { statement ->
                    statement.setString(1, entryId)
                    statement.setString(2, id)
                    statement.executeUpdate()
                }
            }
            RetentionSummary(RetentionCategory.OUTBOX_HISTORY, entries.size.toLong())
        }
    }

    private fun claim(
        connection: java.sql.Connection,
        ownerId: String,
        limit: Int,
        now: Instant,
    ): List<OutboxClaim> {
        val rows = connection.prepareBounded(
            """
            SELECT outbox_id, codec_id, codec_version,
                   octet_length(payload) AS payload_size, payload, delivery_attempts
            FROM ${resources.table("outbox_entries")}
            WHERE output_id = ?
              AND (status = 'PENDING' OR (status = 'LEASED' AND lease_expires_at <= ?))
            ORDER BY sequence ASC
            LIMIT ?
            FOR UPDATE SKIP LOCKED
            """.trimIndent(),
            configuration,
        ).use { statement ->
            statement.setString(1, id)
            statement.setTimestamp(2, resources.timestamp(now))
            statement.setInt(3, limit)
            statement.executeQuery().use { result ->
                buildList {
                    while (result.next()) add(
                        ClaimCandidate(
                            entryId = result.getString("outbox_id"),
                            descriptor = OutputCodecDescriptor(
                                result.getString("codec_id"),
                                result.getInt("codec_version"),
                            ),
                            payload = result.getBoundedBytes(
                                "payload",
                                result.getInt("payload_size"),
                                configuration.maxOutputPayloadBytes,
                                "outbox",
                            ),
                            deliveryAttempts = result.getInt("delivery_attempts") + 1,
                        ),
                    )
                }
            }
        }
        return rows.map { row ->
            val token = nextFenceToken(connection)
            val expiresAt = now.plus(configuration.outboxLeaseDuration)
            val updated = connection.prepareBounded(
                """
                UPDATE ${resources.table("outbox_entries")}
                SET status = 'LEASED', lease_token = ?, lease_owner = ?,
                    lease_expires_at = ?, delivery_attempts = ?
                WHERE outbox_id = ?
                  AND output_id = ?
                  AND (status = 'PENDING' OR (status = 'LEASED' AND lease_expires_at <= ?))
                """.trimIndent(),
                configuration,
            ).use { statement ->
                statement.setLong(1, token)
                statement.setString(2, ownerId)
                statement.setTimestamp(3, resources.timestamp(expiresAt))
                statement.setInt(4, row.deliveryAttempts)
                statement.setString(5, row.entryId)
                statement.setString(6, id)
                statement.setTimestamp(7, resources.timestamp(now))
                statement.executeUpdate()
            }
            check(updated == 1) { "Outbox claim was lost before fencing" }
            OutboxClaim(row.entryId, token, row.descriptor, row.payload, row.deliveryAttempts)
        }
    }

    private suspend fun markDelivered(claim: OutboxClaim, ownerId: String): Boolean =
        resources.transaction { connection ->
            val mutationNow = clock.now()
            connection.prepareBounded(
                """
                UPDATE ${resources.table("outbox_entries")}
                SET status = 'DELIVERED', delivered_at = ?, lease_token = NULL,
                    lease_owner = NULL, lease_expires_at = NULL, last_error = NULL
                WHERE outbox_id = ? AND output_id = ? AND status = 'LEASED'
                  AND lease_token = ? AND lease_owner = ? AND lease_expires_at > ?
                """.trimIndent(),
                configuration,
            ).use { statement ->
                statement.setTimestamp(1, resources.timestamp(mutationNow))
                statement.setString(2, claim.entryId)
                statement.setString(3, id)
                statement.setLong(4, claim.fenceToken)
                statement.setString(5, ownerId)
                statement.setTimestamp(6, resources.timestamp(mutationNow))
                statement.executeUpdate() == 1
            }
        }

    private suspend fun markFailure(claim: OutboxClaim, ownerId: String, reason: String): DeliveryMutation =
        resources.transaction { connection ->
            val mutationNow = clock.now()
            val status = if (claim.deliveryAttempts >= configuration.maxOutboxAttempts) "FAILED" else "PENDING"
            val updated = connection.prepareBounded(
                """
                UPDATE ${resources.table("outbox_entries")}
                SET status = ?, lease_token = NULL, lease_owner = NULL,
                    lease_expires_at = NULL, last_error = ?
                WHERE outbox_id = ? AND output_id = ? AND status = 'LEASED'
                  AND lease_token = ? AND lease_owner = ? AND lease_expires_at > ?
                """.trimIndent(),
                configuration,
            ).use { statement ->
                statement.setString(1, status)
                statement.setString(2, truncatePostgresUtf8(reason, configuration.maxReasonBytes))
                statement.setString(3, claim.entryId)
                statement.setString(4, id)
                statement.setLong(5, claim.fenceToken)
                statement.setString(6, ownerId)
                statement.setTimestamp(7, resources.timestamp(mutationNow))
                statement.executeUpdate()
            }
            if (updated == 0) DeliveryMutation.STALE
            else if (status == "FAILED") DeliveryMutation.FAILED
            else DeliveryMutation.REQUEUED
        }

    private suspend fun releaseClaim(claim: OutboxClaim, ownerId: String) {
        resources.transaction { connection ->
            val mutationNow = clock.now()
            connection.prepareBounded(
                """
                UPDATE ${resources.table("outbox_entries")}
                SET status = 'PENDING', lease_token = NULL, lease_owner = NULL,
                    lease_expires_at = NULL
                WHERE outbox_id = ? AND output_id = ? AND status = 'LEASED'
                  AND lease_token = ? AND lease_owner = ? AND lease_expires_at > ?
                """.trimIndent(),
                configuration,
            ).use { statement ->
                statement.setString(1, claim.entryId)
                statement.setString(2, id)
                statement.setLong(3, claim.fenceToken)
                statement.setString(4, ownerId)
                statement.setTimestamp(5, resources.timestamp(mutationNow))
                statement.executeUpdate()
            }
        }
    }

    private fun nextSequence(connection: java.sql.Connection): Long = connection.prepareBounded(
        "SELECT nextval('${resources.sequence("outbox_sequence")}')",
        configuration,
    ).use { statement ->
        statement.executeQuery().use { result ->
            require(result.next()) { "PostgreSQL outbox sequence returned no value" }
            result.getLong(1)
        }
    }

    private fun nextFenceToken(connection: java.sql.Connection): Long = connection.prepareBounded(
        "SELECT nextval('${resources.sequence("outbox_fence_sequence")}')",
        configuration,
    ).use { statement ->
        statement.executeQuery().use { result ->
            require(result.next()) { "PostgreSQL outbox fence sequence returned no value" }
            result.getLong(1)
        }
    }

    private fun safeFailureName(error: Throwable): String =
        error::class.simpleName?.take(128)?.ifBlank { "failure" } ?: "failure"

    private fun insertLineageIfPresent(
        connection: java.sql.Connection,
        responseReference: String?,
        ownerId: String,
        recordedAt: Instant,
    ) {
        if (responseReference == null ||
            !resources.tableExists(connection, "response_lineage") ||
            !resources.tableExists(connection, "response_records")
        ) return
        connection.prepareBounded(
            """
            INSERT INTO ${resources.table("response_lineage")} (
                response_reference, owner_kind, owner_id, recorded_at
            )
            SELECT ?, 'OUTBOX', ?, ?
            WHERE EXISTS (
                SELECT 1 FROM ${resources.table("response_records")} WHERE response_id = ?
            )
            ON CONFLICT DO NOTHING
            """.trimIndent(),
            configuration,
        ).use { statement ->
            statement.setString(1, responseReference)
            statement.setString(2, ownerId)
            statement.setTimestamp(3, resources.timestamp(recordedAt))
            statement.setString(4, responseReference)
            statement.executeUpdate()
        }
    }

    private fun deleteLineage(connection: java.sql.Connection, ownerId: String) {
        if (!resources.tableExists(connection, "response_lineage")) return
        connection.prepareBounded(
            "DELETE FROM ${resources.table("response_lineage")} WHERE owner_kind = 'OUTBOX' AND owner_id = ?",
            configuration,
        ).use { statement ->
            statement.setString(1, ownerId)
            statement.executeUpdate()
        }
    }

    private fun Produced<T>.toCrawlerContext(): CrawlerContext = CrawlerContext(
        engineRunId = context.engineRunId,
        pipelineRunId = context.pipelineRunId,
        pipelineId = context.pipelineId,
        requestId = context.requestId,
        attributes = context.attributes,
    )

    private data class ClaimCandidate(
        val entryId: String,
        val descriptor: OutputCodecDescriptor,
        val payload: ByteArray,
        val deliveryAttempts: Int,
    )

    private data class OutboxClaim(
        val entryId: String,
        val fenceToken: Long,
        val descriptor: OutputCodecDescriptor,
        val payload: ByteArray,
        val deliveryAttempts: Int,
    )

    private enum class DeliveryMutation {
        REQUEUED,
        FAILED,
        STALE,
    }
}
