package com.rmjtromp.trawler.postgres

import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.extension.ReplayLifecycle
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.ResponseReference
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.SystemClock
import com.rmjtromp.trawler.response.EncodedResponse
import com.rmjtromp.trawler.response.ResponseCodecRegistry
import com.rmjtromp.trawler.response.ResponseHistoryStore
import com.rmjtromp.trawler.response.ResponseLookup
import com.rmjtromp.trawler.response.ResponseReplayQuery
import com.rmjtromp.trawler.response.ResponseReplayStore
import com.rmjtromp.trawler.response.ResponseRetentionRequest
import com.rmjtromp.trawler.response.ResponseRetentionResult
import com.rmjtromp.trawler.response.ResponseStore
import com.rmjtromp.trawler.response.ResponseStoreCapability
import com.rmjtromp.trawler.response.ResponseStoreLifecycle
import com.rmjtromp.trawler.response.StoredResponse
import java.sql.ResultSet
import java.time.Instant

/**
 * PostgreSQL response history owned independently from crawl state.
 *
 * Every payload is encoded by the caller's [codecs] and stored with its exact
 * codec ID/version. Rows are append-only until this adapter's bounded
 * retention operation removes an unprotected row. Replay and lookup execute
 * only database reads; they never create crawl jobs or network calls. The
 * adapter owns a Hikari pool and bounded dispatcher through
 * [PostgresAdapterResources]. Register this object as an engine extension so
 * migration and cleanup occur in the engine lifecycle.
 */
public class PostgresResponseStore(
    override val id: String = "postgres-responses",
    public val configuration: PostgresStateConfiguration,
    public val codecs: ResponseCodecRegistry,
    private val clock: Clock = SystemClock,
) : ResponseStore,
    ResponseReplayStore,
    ResponseHistoryStore,
    com.rmjtromp.trawler.response.ResponseRetentionStore,
    ResponseStoreLifecycle,
    ExtensionLifecycle,
    ReplayLifecycle {

    private val resources = PostgresAdapterResources(
        id,
        configuration,
        "classpath:db/migration/response",
        "flyway_response_schema_history",
    )

    override val capabilities: Set<ResponseStoreCapability> = setOf(
        ResponseStoreCapability.LOOKUP,
        ResponseStoreCapability.CACHE,
        ResponseStoreCapability.HISTORY,
        ResponseStoreCapability.REPLAY,
        ResponseStoreCapability.TTL_EVICTION,
        ResponseStoreCapability.RANGE_QUERY,
    )

    init {
        require(id.isNotBlank()) { "PostgreSQL response store ID must not be blank" }
        requirePostgresUtf8Bound(id, configuration.maxIdentifierBytes, "response store ID")
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

    override suspend fun put(response: CrawlResponse): ResponseReference? {
        resources.requireStarted()
        if (response.body.size > configuration.maxResponseBodyBytes) return null
        response.request.body?.let {
            if (it.size > configuration.maxRequestBodyBytes) return null
        }
        requirePostgresUtf8Bound(response.request.target, configuration.maxTargetBytes, "response target")
        val encoded: EncodedResponse = codecs.encode(response)
        if (encoded.size > configuration.maxEncodedResponseBytes) return null
        requirePostgresUtf8Bound(encoded.descriptor.id, configuration.maxIdentifierBytes, "response codec ID")
        val requestId = response.request.attributes[com.rmjtromp.trawler.response.InMemoryResponseStore.REQUEST_ID_ATTRIBUTE]
        val pipelineId = response.request.attributes[com.rmjtromp.trawler.response.InMemoryResponseStore.PIPELINE_ID_ATTRIBUTE]
        requestId?.let { requirePostgresUtf8Bound(it, configuration.maxIdentifierBytes, "response request ID") }
        pipelineId?.let { requirePostgresUtf8Bound(it, configuration.maxIdentifierBytes, "response pipeline ID") }
        val now = clock.now()
        return resources.transaction { connection ->
            val sequence = nextSequence(connection)
            val reference = ResponseReference("postgres-response-$sequence")
            connection.prepareBounded(
                """
                INSERT INTO ${resources.table("response_records")} (
                    response_id, sequence, pipeline_id, request_id, target,
                    received_at, stored_at, codec_id, codec_version, body_size, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """.trimIndent(),
                configuration,
            ).use { statement ->
                statement.setString(1, reference.value)
                statement.setLong(2, sequence)
                statement.setString(3, pipelineId)
                statement.setString(4, requestId)
                statement.setString(5, response.request.target)
                statement.setTimestamp(6, resources.timestamp(response.receivedAt))
                statement.setTimestamp(7, resources.timestamp(now))
                statement.setString(8, encoded.descriptor.id)
                statement.setInt(9, encoded.descriptor.version)
                statement.setInt(10, response.body.size)
                statement.setBytes(11, encoded.toByteArray())
                statement.executeUpdate()
            }
            reference
        }
    }

    override suspend fun find(query: ResponseLookup): StoredResponse? = resources.database { connection ->
        when (query) {
            is ResponseLookup.ByReference -> {
                requirePostgresUtf8Bound(query.reference.value, configuration.maxIdentifierBytes, "response reference")
                selectOne(connection, "response_id = ?", listOf(SqlArgument.Text(query.reference.value)))
            }

            is ResponseLookup.ByRequestId -> {
                requirePostgresUtf8Bound(query.requestId, configuration.maxIdentifierBytes, "response request ID")
                selectOne(
                    connection,
                    "request_id = ? ORDER BY sequence DESC",
                    listOf(SqlArgument.Text(query.requestId)),
                )
            }

            is ResponseLookup.ByTarget -> {
                requirePostgresUtf8Bound(query.target, configuration.maxTargetBytes, "response target")
                selectOne(
                    connection,
                    "target = ? ORDER BY sequence DESC",
                    listOf(SqlArgument.Text(query.target)),
                )
            }
        }?.let(::decode)
    }

    override suspend fun replay(query: ResponseReplayQuery, limit: Int, pipelineId: String?): List<StoredResponse> =
        resources.database { connection ->
            val boundedLimit = resources.boundLimit(limit)
            pipelineId?.let { requirePostgresUtf8Bound(it, configuration.maxIdentifierBytes, "replay pipeline ID") }
            val ownershipPredicate = pipelineId?.let { " AND pipeline_id = ?" }.orEmpty()
            val rows = when (query) {
                is ResponseReplayQuery.ByReference -> {
                    requirePostgresUtf8Bound(query.reference.value, configuration.maxIdentifierBytes, "response reference")
                    selectMany(
                        connection,
                        "response_id = ?$ownershipPredicate ORDER BY sequence ASC LIMIT ?",
                        buildList {
                            add(SqlArgument.Text(query.reference.value))
                            pipelineId?.let { add(SqlArgument.Text(it)) }
                            add(SqlArgument.Integer(boundedLimit))
                        },
                    )
                }

                is ResponseReplayQuery.ByRequestId -> {
                    requirePostgresUtf8Bound(query.requestId, configuration.maxIdentifierBytes, "response request ID")
                    selectMany(
                        connection,
                        "request_id = ?$ownershipPredicate ORDER BY sequence ASC LIMIT ?",
                        buildList {
                            add(SqlArgument.Text(query.requestId))
                            pipelineId?.let { add(SqlArgument.Text(it)) }
                            add(SqlArgument.Integer(boundedLimit))
                        },
                    )
                }

                is ResponseReplayQuery.ByPipelineId -> {
                    requirePostgresUtf8Bound(query.pipelineId, configuration.maxIdentifierBytes, "response pipeline ID")
                    selectMany(
                        connection,
                        "pipeline_id = ? ORDER BY sequence ASC LIMIT ?",
                        listOf(SqlArgument.Text(query.pipelineId), SqlArgument.Integer(boundedLimit)),
                    )
                }
            }
            rows.map(::decode)
        }

    override suspend fun history(requestId: String, limit: Int): List<StoredResponse> =
        replay(ResponseReplayQuery.ByRequestId(requestId), limit)

    override suspend fun evictBefore(cutoff: Instant): Long =
        prune(ResponseRetentionRequest(cutoff, configuration.maxRetentionRows)).deleted

    override suspend fun prune(request: ResponseRetentionRequest): ResponseRetentionResult =
        resources.transaction { connection ->
            val limit = resources.boundLimit(request.maxEntries)
            val protectedValues = request.protectedReferences.map { it.value }
            protectedValues.forEach {
                requirePostgresUtf8Bound(it, configuration.maxIdentifierBytes, "protected response reference")
            }
            val callerExclusion = protectedValues
                .takeIf(List<String>::isNotEmpty)
                ?.joinToString(", ") { "?" }
                ?.let { "AND r.response_id NOT IN ($it)" }
                .orEmpty()
            val protectedDeletion = protectedValues
                .takeIf(List<String>::isNotEmpty)
                ?.joinToString(", ") { "?" }
                ?.let { "AND r.response_id NOT IN ($it)" }
                .orEmpty()
            val candidates = connection.prepareBounded(
                """
                SELECT r.response_id
                FROM ${resources.table("response_records")} r
                WHERE r.stored_at < ?
                  $callerExclusion
                  AND NOT EXISTS (
                      SELECT 1 FROM ${resources.table("response_lineage")} l
                      WHERE l.response_reference = r.response_id
                  )
                ORDER BY r.sequence ASC
                LIMIT ?
                """.trimIndent(),
                configuration,
            ).use { statement ->
                var index = 1
                statement.setTimestamp(index++, resources.timestamp(request.cutoff))
                protectedValues.forEach { statement.setString(index++, it) }
                statement.setInt(index, limit)
                statement.executeQuery().use { result ->
                    buildList {
                        while (result.next()) add(result.getString("response_id"))
                    }
                }
            }
            var deleted = 0L
            candidates.forEach { reference ->
                deleted += connection.prepareBounded(
                    """
                    DELETE FROM ${resources.table("response_records")} r
                    WHERE r.response_id = ?
                      $protectedDeletion
                      AND NOT EXISTS (
                          SELECT 1 FROM ${resources.table("response_lineage")} l
                          WHERE l.response_reference = r.response_id
                      )
                    """.trimIndent(),
                    configuration,
                ).use { statement ->
                    var index = 1
                    statement.setString(index++, reference)
                    protectedValues.forEach { statement.setString(index++, it) }
                    statement.executeUpdate().toLong()
                }
            }
            val protectedCount = connection.prepareBounded(
                """
                SELECT COUNT(*)
                FROM ${resources.table("response_records")} r
                WHERE r.stored_at < ?
                  AND (
                      ${if (protectedValues.isEmpty()) "FALSE" else "r.response_id IN (${protectedValues.joinToString(", ") { "?" }})"}
                      OR EXISTS (
                          SELECT 1 FROM ${resources.table("response_lineage")} l
                          WHERE l.response_reference = r.response_id
                      )
                  )
                """.trimIndent(),
                configuration,
            ).use { statement ->
                var index = 1
                statement.setTimestamp(index++, resources.timestamp(request.cutoff))
                protectedValues.forEach { statement.setString(index++, it) }
                statement.executeQuery().use { result ->
                    require(result.next()) { "PostgreSQL response retention count returned no row" }
                    result.getLong(1)
                }
            }
            ResponseRetentionResult(deleted, protectedCount)
        }

    private fun nextSequence(connection: java.sql.Connection): Long = connection.prepareBounded(
        "SELECT nextval('${resources.sequence("response_sequence")}')",
        configuration,
    ).use { statement ->
        statement.executeQuery().use { result ->
            require(result.next()) { "PostgreSQL response sequence returned no value" }
            result.getLong(1)
        }
    }

    private fun selectOne(
        connection: java.sql.Connection,
        predicate: String,
        arguments: List<SqlArgument>,
    ): ResponseRow? = selectMany(connection, "$predicate LIMIT 1", arguments).firstOrNull()

    private fun selectMany(
        connection: java.sql.Connection,
        predicate: String,
        arguments: List<SqlArgument>,
    ): List<ResponseRow> {
        val sql = """
            SELECT response_id, sequence, pipeline_id, codec_id, codec_version, body_size,
                   octet_length(payload) AS payload_size, payload
            FROM ${resources.table("response_records")}
            WHERE $predicate
        """.trimIndent()
        return connection.prepareBounded(sql, configuration).use { statement ->
            arguments.forEachIndexed { index, argument ->
                when (argument) {
                    is SqlArgument.Text -> statement.setString(index + 1, argument.value)
                    is SqlArgument.Integer -> statement.setInt(index + 1, argument.value)
                    is SqlArgument.TimestampValue -> statement.setTimestamp(index + 1, argument.value)
                }
            }
            statement.executeQuery().use { result ->
                buildList {
                    while (result.next()) add(readRow(result))
                }
            }
        }
    }

    private fun readRow(result: ResultSet): ResponseRow {
        val payload = result.getBoundedBytes(
            "payload",
            result.getInt("payload_size"),
            configuration.maxEncodedResponseBytes,
            "response",
        )
        return ResponseRow(
            reference = ResponseReference(result.getString("response_id")),
            sequence = result.getLong("sequence"),
            pipelineId = result.getString("pipeline_id"),
            descriptor = com.rmjtromp.trawler.response.ResponseCodecDescriptor(
                result.getString("codec_id"),
                result.getInt("codec_version"),
            ),
            bodySize = result.getInt("body_size"),
            payload = payload,
        )
    }

    private fun decode(row: ResponseRow): StoredResponse {
        require(row.bodySize in 0..configuration.maxResponseBodyBytes) {
            "Stored response body exceeds the configured bound"
        }
        val response = codecs.decode(row.descriptor, row.payload)
        require(response.body.size <= configuration.maxResponseBodyBytes) {
            "Decoded response body exceeds the configured bound"
        }
        require((response.request.body?.size ?: 0) <= configuration.maxRequestBodyBytes) {
            "Decoded response request body exceeds the configured bound"
        }
        requirePostgresUtf8Bound(response.request.target, configuration.maxTargetBytes, "decoded response target")
        return StoredResponse(row.reference, response, row.pipelineId)
    }

    private data class ResponseRow(
        val reference: ResponseReference,
        val sequence: Long,
        val pipelineId: String?,
        val descriptor: com.rmjtromp.trawler.response.ResponseCodecDescriptor,
        val bodySize: Int,
        val payload: ByteArray,
    )

    private sealed interface SqlArgument {
        data class Text(val value: String) : SqlArgument
        data class Integer(val value: Int) : SqlArgument
        data class TimestampValue(val value: java.sql.Timestamp) : SqlArgument
    }
}
