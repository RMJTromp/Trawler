package com.rmjtromp.trawler.postgres

import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.extension.ReplayLifecycle
import com.rmjtromp.trawler.model.OutputContext
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

/**
 * PostgreSQL typed output history.
 *
 * The caller-owned [codecs] encode and decode `Produced<T>`; this adapter does
 * not define a user-data row shape or serializer. A successful write returns
 * only after its transaction commits. That is durable acceptance by this
 * adapter, not exactly-once delivery to any outside system. The output owns
 * its bounded pool/dispatcher and must be registered as an engine extension.
 */
public class PostgresOutput<T : Any>(
    override val id: String = "postgres-output",
    public val configuration: PostgresStateConfiguration,
    public val codecs: OutputCodecRegistry<T>,
    private val clock: Clock = SystemClock,
) : Output<T>,
    OutputRetentionStore,
    OutputLifecycle,
    ExtensionLifecycle,
    ReplayLifecycle {

    private val resources = PostgresAdapterResources(
        id,
        configuration,
        "classpath:db/migration/output",
        "flyway_output_schema_history",
    )

    override val retentionCategories: Set<RetentionCategory> = setOf(RetentionCategory.OUTPUT_HISTORY)

    init {
        require(id.isNotBlank()) { "PostgreSQL output ID must not be blank" }
        requirePostgresUtf8Bound(id, configuration.maxIdentifierBytes, "output ID")
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
        resources.stop()
    }

    override suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult {
        if (items.isEmpty()) return OutputResult.Accepted(0)
        if (items.size > configuration.maxOutputBatchItems) {
            return OutputResult.Failed(
                IllegalArgumentException("PostgreSQL output batch exceeds the configured bound"),
            )
        }
        return try {
            resources.transaction { connection ->
                val now = clock.now()
                items.forEach { item ->
                    val encoded = codecs.active().let { codec ->
                        val payload = codec.encode(item)
                        require(payload.size <= configuration.maxOutputPayloadBytes) {
                            "Encoded PostgreSQL output payload exceeds the configured bound"
                        }
                        EncodedOutput(codec.descriptor, payload)
                    }
                    requirePostgresUtf8Bound(encoded.descriptor.id, configuration.maxIdentifierBytes, "output codec ID")
                    requirePostgresUtf8Bound(item.context.pipelineId, configuration.maxIdentifierBytes, "output pipeline ID")
                    item.context.responseReference?.let {
                        requirePostgresUtf8Bound(it.value, configuration.maxIdentifierBytes, "output response reference")
                    }
                    val sequence = nextSequence(connection)
                    val entryId = "postgres-output-$sequence"
                    connection.prepareBounded(
                        """
                        INSERT INTO ${resources.table("output_entries")} (
                            output_entry_id, sequence, output_id, pipeline_id,
                            produced_at, stored_at, codec_id, codec_version,
                            payload, response_reference
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """.trimIndent(),
                        configuration,
                    ).use { statement ->
                        statement.setString(1, entryId)
                        statement.setLong(2, sequence)
                        statement.setString(3, id)
                        statement.setString(4, item.context.pipelineId)
                        statement.setTimestamp(5, resources.timestamp(item.context.producedAt))
                        statement.setTimestamp(6, resources.timestamp(now))
                        statement.setString(7, encoded.descriptor.id)
                        statement.setInt(8, encoded.descriptor.version)
                        statement.setBytes(9, encoded.payload)
                        statement.setString(10, item.context.responseReference?.value)
                        statement.executeUpdate()
                    }
                    insertLineageIfPresent(connection, item.context.responseReference?.value, "OUTPUT", entryId, now)
                }
            }
            OutputResult.Accepted(items.size)
        } catch (cancelled: kotlinx.coroutines.CancellationException) {
            throw cancelled
        } catch (error: Throwable) {
            OutputResult.Failed(error)
        }
    }

    override suspend fun prune(request: OutputRetentionRequest): RetentionSummary {
        require(request.category == RetentionCategory.OUTPUT_HISTORY) {
            "PostgreSQL output owns only output history retention"
        }
        return resources.transaction { connection ->
            val limit = resources.boundLimit(request.maxEntries)
            val entries = connection.prepareBounded(
                """
                SELECT output_entry_id
                FROM ${resources.table("output_entries")}
                WHERE output_id = ? AND stored_at < ?
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
                        while (result.next()) add(result.getString("output_entry_id"))
                    }
                }
            }
            entries.forEach { entryId ->
                deleteLineage(connection, "OUTPUT", entryId)
                connection.prepareBounded(
                    "DELETE FROM ${resources.table("output_entries")} WHERE output_entry_id = ? AND output_id = ?",
                    configuration,
                ).use { statement ->
                    statement.setString(1, entryId)
                    statement.setString(2, id)
                    statement.executeUpdate()
                }
            }
            RetentionSummary(RetentionCategory.OUTPUT_HISTORY, entries.size.toLong())
        }
    }

    private fun nextSequence(connection: java.sql.Connection): Long = connection.prepareBounded(
        "SELECT nextval('${resources.sequence("output_sequence")}')",
        configuration,
    ).use { statement ->
        statement.executeQuery().use { result ->
            require(result.next()) { "PostgreSQL output sequence returned no value" }
            result.getLong(1)
        }
    }

    private fun insertLineageIfPresent(
        connection: java.sql.Connection,
        responseReference: String?,
        ownerKind: String,
        ownerId: String,
        recordedAt: java.time.Instant,
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
            SELECT ?, ?, ?, ?
            WHERE EXISTS (
                SELECT 1 FROM ${resources.table("response_records")} WHERE response_id = ?
            )
            ON CONFLICT DO NOTHING
            """.trimIndent(),
            configuration,
        ).use { statement ->
            statement.setString(1, responseReference)
            statement.setString(2, ownerKind)
            statement.setString(3, ownerId)
            statement.setTimestamp(4, resources.timestamp(recordedAt))
            statement.setString(5, responseReference)
            statement.executeUpdate()
        }
    }

    private fun deleteLineage(connection: java.sql.Connection, ownerKind: String, ownerId: String) {
        if (!resources.tableExists(connection, "response_lineage")) return
        connection.prepareBounded(
            "DELETE FROM ${resources.table("response_lineage")} WHERE owner_kind = ? AND owner_id = ?",
            configuration,
        ).use { statement ->
            statement.setString(1, ownerKind)
            statement.setString(2, ownerId)
            statement.executeUpdate()
        }
    }

    private data class EncodedOutput(
        val descriptor: com.rmjtromp.trawler.output.OutputCodecDescriptor,
        val payload: ByteArray,
    )
}
