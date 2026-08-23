package com.rmjtromp.trawler.postgres

import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.RequestIdentity
import com.rmjtromp.trawler.policy.ResourceHistory
import com.rmjtromp.trawler.policy.SystemClock
import com.rmjtromp.trawler.policy.WorkOrderingPolicy
import com.rmjtromp.trawler.state.AttemptOutcome
import com.rmjtromp.trawler.state.AttemptStartResult
import com.rmjtromp.trawler.state.AttemptStore
import com.rmjtromp.trawler.state.CrawlStateCapability
import com.rmjtromp.trawler.state.CrawlStateStore
import com.rmjtromp.trawler.state.DurableRunStatus
import com.rmjtromp.trawler.state.EnqueueResult
import com.rmjtromp.trawler.state.ExecutionAttempt
import com.rmjtromp.trawler.state.LeaseMutationResult
import com.rmjtromp.trawler.state.LeaseStore
import com.rmjtromp.trawler.state.LeasedWork
import com.rmjtromp.trawler.state.PipelineScopedWorkQueueStore
import com.rmjtromp.trawler.state.ResourceHistoryStore
import com.rmjtromp.trawler.state.RunHistoryStore
import com.rmjtromp.trawler.state.WorkCancellationStore
import com.rmjtromp.trawler.state.WorkItemId
import com.rmjtromp.trawler.state.WorkOrderingStore
import com.rmjtromp.trawler.state.WorkQueueStatus
import com.rmjtromp.trawler.state.WorkTransition
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.flywaydb.core.Flyway
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * Durable PostgreSQL operational state for Trawler.
 *
 * The adapter owns one Hikari pool, one fixed dispatcher, and its Flyway
 * migration lifecycle. It implements only crawl state and durable run
 * summaries: request bytes are stored through [PostgresRequestCodecRegistry],
 * while produced values, response bodies, checkpoints, and output payloads
 * remain outside this module. All JDBC work runs on the adapter-owned bounded
 * dispatcher and every statement receives a database timeout.
 *
 * Configure the store as both the engine's `stateStore` and an
 * `ExtensionLifecycle`. Startup migrates the configured schema and recovers
 * stale run summaries; shutdown closes the pool and dispatcher. Calls made
 * after shutdown fail, and cancellation stops awaiting the bounded adapter
 * operation while the JDBC statement timeout bounds the underlying call.
 */
public class PostgresCrawlStateStore(
    override val id: String = "postgres-state",
    public val configuration: PostgresStateConfiguration,
    requestCodecs: PostgresRequestCodecRegistry = PostgresRequestCodecRegistry(
        listOf(BinaryPostgresRequestCodec(configuration.requestCodecLimits())),
    ),
    private val clock: Clock = SystemClock,
) : CrawlStateStore,
    PipelineScopedWorkQueueStore,
    LeaseStore,
    AttemptStore,
    ResourceHistoryStore,
    WorkCancellationStore,
    WorkOrderingStore,
    WorkQueueStatus,
    RunHistoryStore,
    ExtensionLifecycle {

    private val requestCodecs: PostgresRequestCodecRegistry = requestCodecs
    private val schemaSql: String = quoteIdentifier(configuration.schema)
    private val lifecycleMutex = Mutex()
    private val started = java.util.concurrent.atomic.AtomicBoolean(false)
    private val threadSequence = AtomicLong(0)
    private val executor = Executors.newFixedThreadPool(
        configuration.databaseDispatcherParallelism,
        PostgresThreadFactory(id, threadSequence),
    )
    private val databaseDispatcher: CoroutineDispatcher = executor.asCoroutineDispatcher()
    private val dataSource: HikariDataSource = HikariDataSource(
        HikariConfig().apply {
            jdbcUrl = configuration.jdbcUrl
            configuration.username?.let { username = it }
            configuration.passwordCopy()?.let { passwordChars ->
                try {
                    password = passwordChars.concatToString()
                } finally {
                    passwordChars.fill('\u0000')
                }
            }
            maximumPoolSize = configuration.maximumPoolSize
            minimumIdle = configuration.minimumIdle
            connectionTimeout = configuration.connectionTimeout.toMillis()
            validationTimeout = configuration.connectionTimeout.toMillis().coerceAtMost(5_000)
            poolName = configuration.poolName
        },
    )

    override val capabilities: Set<CrawlStateCapability> = setOf(
        CrawlStateCapability.WORK_QUEUE,
        CrawlStateCapability.DURABLE,
        CrawlStateCapability.TRANSACTIONAL,
        CrawlStateCapability.MULTI_PROCESS,
        CrawlStateCapability.LEASE_FENCING,
        CrawlStateCapability.QUERYABLE_HISTORY,
        CrawlStateCapability.RESOURCE_HISTORY,
        CrawlStateCapability.ATTEMPT_HISTORY,
        CrawlStateCapability.PIPELINE_SCOPED_WORK,
        CrawlStateCapability.CANCELLATION,
        CrawlStateCapability.WORK_ORDERING,
        CrawlStateCapability.WORK_STATUS,
        CrawlStateCapability.RUN_HISTORY,
        CrawlStateCapability.RUN_RECOVERY,
    )

    init {
        require(id.isNotBlank()) { "PostgreSQL state store ID must not be blank" }
        requireUtf8Bound(id, configuration.maxIdentifierBytes, "state store ID")
        require(configuration.maxIdentityBytes <= 4_096) {
            "PostgreSQL identity bound must fit the migrated resource column"
        }
        require(configuration.maxReasonBytes <= 4_096) {
            "PostgreSQL reason bound must fit the migrated reason columns"
        }
    }

    /** Applies adapter-owned migrations and recovers stale run summaries. */
    override suspend fun start(context: ExtensionStartContext) {
        lifecycleMutex.withLock {
            if (started.get()) return
            try {
                withContext(NonCancellable) {
                    migrateInternal()
                    recoverStaleRunsInternal(clock.now(), configuration.staleRunAfter)
                    started.set(true)
                }
            } catch (error: Throwable) {
                closeResources()
                throw error
            }
        }
    }

    /** Stops new operations and closes the adapter-owned pool and dispatcher. */
    override suspend fun stop(context: ExtensionStopContext) {
        lifecycleMutex.withLock {
            if (!started.compareAndSet(true, false)) {
                closeResources()
                return
            }
            withContext(NonCancellable) { closeResources() }
        }
    }

    /** Returns whether the adapter can apply the selected ordering exactly. */
    override fun supports(policy: WorkOrderingPolicy): Boolean = policy.id == "fifo"

    /** Installs FIFO ordering for one pipeline; other orderings are rejected. */
    override suspend fun setOrdering(pipelineId: String, policy: WorkOrderingPolicy) {
        require(pipelineId.isNotBlank()) { "Pipeline ID must not be blank" }
        require(supports(policy)) {
            "PostgreSQL state supports only the fifo ordering policy; received '${policy.id}'"
        }
    }

    override suspend fun enqueue(
        request: CrawlRequest,
        identity: RequestIdentity,
        context: CrawlerContext,
    ): EnqueueResult {
        requireIdentifier(context.pipelineId, "pipeline ID")
        requireUtf8Bound(identity.value, configuration.maxIdentityBytes, "request identity")
        validateRequest(request)
        val encoded = requestCodecs.encode(request)
        require(encoded.size <= configuration.maxEncodedRequestBytes) {
            "Encoded request exceeds the PostgreSQL state bound"
        }
        requireUtf8Bound(encoded.descriptor.id, configuration.maxIdentifierBytes, "request codec ID")
        val now = clock.now()
        return transaction { connection ->
            val resourceId = upsertResource(connection, context.pipelineId, identity.value, now)
            val existing = findActiveJob(connection, resourceId)
            if (existing != null) {
                return@transaction EnqueueResult.AlreadyActive(existing)
            }
            lockEnqueueCapacity(connection)
            val activeCount = connection.prepare(
                "SELECT COUNT(*) FROM ${table("jobs")} WHERE status IN ('QUEUED', 'LEASED', 'RETRY_WAIT')",
            ).use { statement ->
                statement.executeQuery().use { result ->
                    require(result.next()) { "PostgreSQL active-job count returned no row" }
                    result.getLong(1)
                }
            }
            if (activeCount >= configuration.maxActiveJobs) {
                return@transaction EnqueueResult.Rejected(
                    "PostgreSQL active work capacity ${configuration.maxActiveJobs} is full",
                )
            }
            val workItemId = WorkItemId("postgres-work-${UUID.randomUUID()}")
            val inserted = connection.prepare(
                """
                INSERT INTO ${table("jobs")} (
                    job_id, pipeline_id, resource_id, request_codec_id,
                    request_codec_version, request_payload, status, enqueued_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'QUEUED', ?, ?)
                ON CONFLICT (resource_id) WHERE status IN ('QUEUED', 'LEASED', 'RETRY_WAIT')
                DO NOTHING
                RETURNING job_id
                """.trimIndent(),
            ).use { statement ->
                statement.setString(1, workItemId.value)
                statement.setString(2, context.pipelineId)
                statement.setLong(3, resourceId)
                statement.setString(4, encoded.descriptor.id)
                statement.setInt(5, encoded.descriptor.version)
                statement.setBytes(6, encoded.toByteArray())
                statement.setTimestamp(7, timestamp(now))
                statement.setTimestamp(8, timestamp(now))
                statement.executeQuery().use { result -> result.next() }
            }
            if (inserted) {
                EnqueueResult.Accepted(workItemId)
            } else {
                EnqueueResult.AlreadyActive(findActiveJob(connection, resourceId))
            }
        }
    }

    override suspend fun claim(limit: Int, ownerId: String, now: Instant): List<LeasedWork> =
        claimInternal(null, limit, ownerId, now)

    override suspend fun claim(
        pipelineId: String,
        limit: Int,
        ownerId: String,
        now: Instant,
    ): List<LeasedWork> = claimInternal(pipelineId, limit, ownerId, now)

    private suspend fun claimInternal(
        pipelineId: String?,
        limit: Int,
        ownerId: String,
        now: Instant,
    ): List<LeasedWork> {
        require(limit in 1..configuration.maxClaimBatch) {
            "PostgreSQL claim limit must be between one and ${configuration.maxClaimBatch}"
        }
        requireIdentifier(ownerId, "lease owner ID")
        pipelineId?.let { requireIdentifier(it, "pipeline ID") }
        return transaction { connection ->
            reclaimExpiredLeases(connection, pipelineId, now)
            val sql = buildString {
                append(
                    """
                    SELECT j.job_id, j.pipeline_id, r.identity_value,
                           j.request_codec_id, j.request_codec_version,
                           j.request_payload, j.enqueued_at, j.sequence
                    FROM ${table("jobs")} j
                    JOIN ${table("resources")} r ON r.resource_id = j.resource_id
                    WHERE j.status IN ('QUEUED', 'RETRY_WAIT')
                      AND (j.not_before IS NULL OR j.not_before <= ?)
                    """.trimIndent(),
                )
                if (pipelineId != null) append(" AND j.pipeline_id = ?")
                append(" ORDER BY j.sequence LIMIT ? FOR UPDATE SKIP LOCKED")
            }
            connection.prepare(sql).use { statement ->
                var index = 1
                statement.setTimestamp(index++, timestamp(now))
                pipelineId?.let { statement.setString(index++, it) }
                statement.setInt(index, limit)
                statement.executeQuery().use { result ->
                    val claimed = mutableListOf<LeasedWork>()
                    while (result.next()) {
                        val jobId = WorkItemId(result.getString("job_id"))
                        val token = nextLeaseToken(connection)
                        val expiresAt = now.plus(configuration.leaseDuration)
                        updateLease(connection, jobId, token, ownerId, expiresAt, now)
                        val descriptor = PostgresCodecDescriptor(
                            result.getString("request_codec_id"),
                            result.getInt("request_codec_version"),
                        )
                        val request = requestCodecs.decode(descriptor, result.getBytes("request_payload"))
                        validateRequest(request)
                        claimed += LeasedWork(
                            workItemId = jobId,
                            request = request,
                            identity = RequestIdentity(result.getString("identity_value")),
                            leaseToken = token,
                            ownerId = ownerId,
                            expiresAt = expiresAt,
                            pipelineId = result.getString("pipeline_id"),
                        )
                    }
                    claimed
                }
            }
        }
    }

    private fun upsertResource(
        connection: Connection,
        pipelineId: String,
        identity: String,
        now: Instant,
    ): Long = connection.prepare(
        """
        INSERT INTO ${table("resources")} (
            pipeline_id, identity_value, created_at, updated_at
        ) VALUES (?, ?, ?, ?)
        ON CONFLICT (pipeline_id, identity_value)
        DO UPDATE SET updated_at = EXCLUDED.updated_at
        RETURNING resource_id
        """.trimIndent(),
    ).use { statement ->
        statement.setString(1, pipelineId)
        statement.setString(2, identity)
        statement.setTimestamp(3, timestamp(now))
        statement.setTimestamp(4, timestamp(now))
        statement.executeQuery().use { result ->
            require(result.next()) { "PostgreSQL resource upsert returned no resource ID" }
            result.getLong(1)
        }
    }

    private fun updateLease(
        connection: Connection,
        jobId: WorkItemId,
        token: com.rmjtromp.trawler.state.LeaseToken,
        ownerId: String,
        expiresAt: Instant,
        now: Instant,
    ) {
        connection.prepare(
            """
            UPDATE ${table("jobs")}
            SET status = 'LEASED', lease_token = ?, lease_owner = ?,
                lease_expires_at = ?, updated_at = ?
            WHERE job_id = ? AND status IN ('QUEUED', 'RETRY_WAIT')
            """.trimIndent(),
        ).use { statement ->
            statement.setString(1, token.value)
            statement.setString(2, ownerId)
            statement.setTimestamp(3, timestamp(expiresAt))
            statement.setTimestamp(4, timestamp(now))
            statement.setString(5, jobId.value)
            require(statement.executeUpdate() == 1) { "PostgreSQL claim lost a locked job row" }
        }
    }

    private fun nextLeaseToken(connection: Connection): com.rmjtromp.trawler.state.LeaseToken =
        connection.prepare("SELECT nextval('${qualifiedSequence("lease_token_sequence")}')").use { statement ->
            statement.executeQuery().use { result ->
                require(result.next()) { "PostgreSQL lease token sequence returned no value" }
                com.rmjtromp.trawler.state.LeaseToken("postgres-lease-${result.getLong(1)}")
            }
        }

    override suspend fun renew(
        work: LeasedWork,
        expiresAt: Instant,
    ): LeaseMutationResult {
        val now = clock.now()
        if (!expiresAt.isAfter(now)) {
            return LeaseMutationResult.Rejected("Lease expiry must be in the future")
        }
        return transaction { connection ->
            val updated = connection.prepare(
                """
                UPDATE ${table("jobs")}
                SET lease_expires_at = ?, updated_at = ?
                WHERE job_id = ? AND status = 'LEASED'
                  AND lease_token = ? AND lease_owner = ?
                  AND lease_expires_at > ?
                """.trimIndent(),
            ).use { statement ->
                statement.setTimestamp(1, timestamp(expiresAt))
                statement.setTimestamp(2, timestamp(now))
                statement.setString(3, work.workItemId.value)
                statement.setString(4, work.leaseToken.value)
                statement.setString(5, work.ownerId)
                statement.setTimestamp(6, timestamp(now))
                statement.executeUpdate()
            }
            if (updated == 1) LeaseMutationResult.Accepted else LeaseMutationResult.Stale
        }
    }

    override suspend fun transition(
        work: LeasedWork,
        transition: WorkTransition,
    ): LeaseMutationResult = transaction { connection ->
        val now = clock.now()
        val transitionData = transitionData(transition, now)
        val row = connection.prepare(
            """
            SELECT resource_id, active_attempt_id
            FROM ${table("jobs")}
            WHERE job_id = ? AND status = 'LEASED'
              AND lease_token = ? AND lease_owner = ?
              AND lease_expires_at > ?
            FOR UPDATE
            """.trimIndent(),
        ).use { statement ->
            statement.setString(1, work.workItemId.value)
            statement.setString(2, work.leaseToken.value)
            statement.setString(3, work.ownerId)
            statement.setTimestamp(4, timestamp(now))
            statement.executeQuery().use { result ->
                if (result.next()) {
                    TransitionRow(
                        resourceId = result.getLong("resource_id"),
                        activeAttemptId = result.getString("active_attempt_id"),
                    )
                } else {
                    null
                }
            }
        } ?: return@transaction LeaseMutationResult.Stale

        val updated = connection.prepare(
            """
            UPDATE ${table("jobs")}
            SET status = ?, not_before = ?, lease_token = NULL, lease_owner = NULL,
                lease_expires_at = NULL, active_attempt_id = NULL,
                finished_at = ?, reason = ?, updated_at = ?
            WHERE job_id = ? AND status = 'LEASED'
              AND lease_token = ? AND lease_owner = ?
              AND lease_expires_at > ?
            """.trimIndent(),
        ).use { statement ->
            statement.setString(1, transitionData.status)
            if (transitionData.notBefore == null) {
                statement.setNull(2, java.sql.Types.TIMESTAMP_WITH_TIMEZONE)
            } else {
                statement.setTimestamp(2, timestamp(transitionData.notBefore))
            }
            if (transitionData.finishedAt == null) {
                statement.setNull(3, java.sql.Types.TIMESTAMP_WITH_TIMEZONE)
            } else {
                statement.setTimestamp(3, timestamp(transitionData.finishedAt))
            }
            if (transitionData.reason == null) {
                statement.setNull(4, java.sql.Types.VARCHAR)
            } else {
                statement.setString(4, transitionData.reason)
            }
            statement.setTimestamp(5, timestamp(now))
            statement.setString(6, work.workItemId.value)
            statement.setString(7, work.leaseToken.value)
            statement.setString(8, work.ownerId)
            statement.setTimestamp(9, timestamp(now))
            statement.executeUpdate()
        }
        if (updated != 1) return@transaction LeaseMutationResult.Stale

        row.activeAttemptId?.let { attemptId ->
            connection.prepare(
                """
                UPDATE ${table("attempts")}
                SET outcome = ?, finished_at = ?, reason = ?
                WHERE attempt_id = ? AND outcome IS NULL
                """.trimIndent(),
            ).use { statement ->
                statement.setString(1, transitionData.attemptOutcome.name)
                statement.setTimestamp(2, timestamp(now))
                statement.setString(3, transitionData.reason)
                statement.setString(4, attemptId)
                statement.executeUpdate()
            }
        }

        when (transition) {
            WorkTransition.Succeeded -> connection.prepare(
                """
                UPDATE ${table("resources")}
                SET last_completed_at = ?, next_eligible_at = NULL,
                    visit_count = visit_count + 1, updated_at = ?
                WHERE resource_id = ?
                """.trimIndent(),
            ).use { statement ->
                statement.setTimestamp(1, timestamp(now))
                statement.setTimestamp(2, timestamp(now))
                statement.setLong(3, row.resourceId)
                statement.executeUpdate()
            }

            is WorkTransition.Retry -> connection.prepare(
                """
                UPDATE ${table("resources")}
                SET next_eligible_at = CASE
                    WHEN next_eligible_at IS NULL OR next_eligible_at < ? THEN ?
                    ELSE next_eligible_at
                END,
                    updated_at = ?
                WHERE resource_id = ?
                """.trimIndent(),
            ).use { statement ->
                statement.setTimestamp(1, timestamp(transition.notBefore))
                statement.setTimestamp(2, timestamp(transition.notBefore))
                statement.setTimestamp(3, timestamp(now))
                statement.setLong(4, row.resourceId)
                statement.executeUpdate()
            }

            is WorkTransition.Failed,
            is WorkTransition.Skipped,
            is WorkTransition.Cancelled,
            is WorkTransition.Abandoned,
            -> Unit
        }
        LeaseMutationResult.Accepted
    }

    override suspend fun beginAttempt(
        work: LeasedWork,
        startedAt: Instant,
    ): AttemptStartResult = transaction { connection ->
        val attemptId = nextAttemptId(connection)
        val number = connection.prepare(
            """
            UPDATE ${table("jobs")}
            SET attempt_count = attempt_count + 1, active_attempt_id = ?, updated_at = ?
            WHERE job_id = ? AND status = 'LEASED'
              AND lease_token = ? AND lease_owner = ?
              AND lease_expires_at > ? AND active_attempt_id IS NULL
            RETURNING attempt_count
            """.trimIndent(),
        ).use { statement ->
            val now = clock.now()
            statement.setString(1, attemptId.value)
            statement.setTimestamp(2, timestamp(now))
            statement.setString(3, work.workItemId.value)
            statement.setString(4, work.leaseToken.value)
            statement.setString(5, work.ownerId)
            statement.setTimestamp(6, timestamp(now))
            statement.executeQuery().use { result ->
                if (result.next()) result.getInt("attempt_count") else null
            }
        } ?: return@transaction AttemptStartResult.Stale

        val attempt = ExecutionAttempt(
            id = attemptId,
            number = number,
            startedAt = startedAt,
        )
        connection.prepare(
            """
            INSERT INTO ${table("attempts")} (
                attempt_id, job_id, pipeline_id, attempt_number,
                lease_token, lease_owner, started_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
        ).use { statement ->
            statement.setString(1, attempt.id.value)
            statement.setString(2, work.workItemId.value)
            statement.setString(3, work.pipelineId)
            statement.setInt(4, attempt.number)
            statement.setString(5, work.leaseToken.value)
            statement.setString(6, work.ownerId)
            statement.setTimestamp(7, timestamp(startedAt))
            statement.executeUpdate()
        }
        AttemptStartResult.Started(attempt)
    }

    override suspend fun finishAttempt(
        work: LeasedWork,
        attempt: ExecutionAttempt,
        outcome: AttemptOutcome,
    ): LeaseMutationResult = transaction { connection ->
        val now = clock.now()
        val updated = connection.prepare(
            """
            UPDATE ${table("jobs")}
            SET active_attempt_id = NULL, updated_at = ?
            WHERE job_id = ? AND status = 'LEASED'
              AND lease_token = ? AND lease_owner = ?
              AND lease_expires_at > ? AND active_attempt_id = ?
            RETURNING job_id
            """.trimIndent(),
        ).use { statement ->
            statement.setTimestamp(1, timestamp(now))
            statement.setString(2, work.workItemId.value)
            statement.setString(3, work.leaseToken.value)
            statement.setString(4, work.ownerId)
            statement.setTimestamp(5, timestamp(now))
            statement.setString(6, attempt.id.value)
            statement.executeQuery().use { result -> result.next() }
        }
        if (!updated) return@transaction LeaseMutationResult.Stale
        connection.prepare(
            """
            UPDATE ${table("attempts")}
            SET outcome = ?, finished_at = ?
            WHERE attempt_id = ? AND job_id = ? AND outcome IS NULL
            """.trimIndent(),
        ).use { statement ->
            statement.setString(1, outcome.name)
            statement.setTimestamp(2, timestamp(now))
            statement.setString(3, attempt.id.value)
            statement.setString(4, work.workItemId.value)
            require(statement.executeUpdate() == 1) { "PostgreSQL attempt completion found no open attempt" }
        }
        LeaseMutationResult.Accepted
    }

    override suspend fun history(
        pipelineId: String,
        identity: RequestIdentity,
    ): ResourceHistory = database { connection ->
        requireIdentifier(pipelineId, "pipeline ID")
        requireUtf8Bound(identity.value, configuration.maxIdentityBytes, "request identity")
        connection.prepare(
            """
            SELECT r.last_completed_at, r.next_eligible_at, r.visit_count,
                   (SELECT COUNT(*) FROM ${table("jobs")} j
                    WHERE j.resource_id = r.resource_id
                      AND j.status IN ('QUEUED', 'LEASED', 'RETRY_WAIT')) AS active_count
            FROM ${table("resources")} r
            WHERE r.pipeline_id = ? AND r.identity_value = ?
            """.trimIndent(),
        ).use { statement ->
            statement.setString(1, pipelineId)
            statement.setString(2, identity.value)
            statement.executeQuery().use { result ->
                if (!result.next()) {
                    ResourceHistory(
                        lastCompletedAt = null,
                        activeWorkCount = 0,
                        visitCount = 0,
                    )
                } else {
                    ResourceHistory(
                        lastCompletedAt = result.instant("last_completed_at"),
                        nextEligibleAt = result.instant("next_eligible_at"),
                        activeWorkCount = result.getInt("active_count"),
                        visitCount = result.getLong("visit_count"),
                    )
                }
            }
        }
    }

    override suspend fun activeWorkCount(pipelineId: String): Int = database { connection ->
        requireIdentifier(pipelineId, "pipeline ID")
        connection.prepare(
            """
            SELECT COUNT(*) FROM ${table("jobs")}
            WHERE pipeline_id = ? AND status IN ('QUEUED', 'LEASED', 'RETRY_WAIT')
            """.trimIndent(),
        ).use { statement ->
            statement.setString(1, pipelineId)
            statement.executeQuery().use { result ->
                require(result.next()) { "PostgreSQL active-work count returned no row" }
                val count = result.getLong(1)
                require(count <= Int.MAX_VALUE) { "PostgreSQL active-work count exceeds the core bound" }
                count.toInt()
            }
        }
    }

    override suspend fun cancelQueued(
        pipelineId: String,
        transition: WorkTransition,
    ): Int {
        requireIdentifier(pipelineId, "pipeline ID")
        require(transition !is WorkTransition.Retry && transition !== WorkTransition.Succeeded) {
            "PostgreSQL queued cancellation requires a terminal transition"
        }
        val now = clock.now()
        val data = transitionData(transition, now)
        return transaction { connection ->
            var total = 0
            while (total < configuration.maxActiveJobs) {
                val batchLimit = minOf(
                    configuration.maxCancellationRows,
                    configuration.maxActiveJobs - total,
                )
                val cancelled = connection.prepare(
                    """
                    WITH candidates AS (
                        SELECT job_id
                        FROM ${table("jobs")}
                        WHERE pipeline_id = ? AND status IN ('QUEUED', 'RETRY_WAIT')
                        ORDER BY sequence
                        LIMIT ?
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE ${table("jobs")} j
                    SET status = ?, not_before = NULL,
                        lease_token = NULL, lease_owner = NULL,
                        lease_expires_at = NULL, active_attempt_id = NULL,
                        finished_at = ?, reason = ?, updated_at = ?
                    FROM candidates
                    WHERE j.job_id = candidates.job_id
                    """.trimIndent(),
                ).use { statement ->
                    statement.setString(1, pipelineId)
                    statement.setInt(2, batchLimit)
                    statement.setString(3, data.status)
                    statement.setTimestamp(4, timestamp(data.finishedAt ?: now))
                    statement.setString(5, data.reason)
                    statement.setTimestamp(6, timestamp(now))
                    statement.executeUpdate()
                }
                total += cancelled
                if (cancelled < batchLimit) break
            }
            total
        }
    }

    override suspend fun recoverStaleRuns(
        now: Instant,
        staleAfter: Duration,
    ): Int {
        require(!staleAfter.isNegative && !staleAfter.isZero) {
            "Stale-run threshold must be positive"
        }
        ensureStarted()
        return recoverStaleRunsInternal(now, staleAfter)
    }

    override suspend fun startEngineRun(
        engineRunId: String,
        startedAt: Instant,
        reason: String?,
    ) {
        requireIdentifier(engineRunId, "engine run ID")
        reason?.let { requireUtf8Bound(it, configuration.maxReasonBytes, "engine run reason") }
        transaction { connection ->
            connection.prepare(
                """
                INSERT INTO ${table("engine_runs")} (
                    engine_run_id, status, started_at, last_observed_at, reason
                ) VALUES (?, 'RUNNING', ?, ?, ?)
                """.trimIndent(),
            ).use { statement ->
                statement.setString(1, engineRunId)
                statement.setTimestamp(2, timestamp(startedAt))
                statement.setTimestamp(3, timestamp(startedAt))
                if (reason == null) statement.setNull(4, java.sql.Types.VARCHAR) else statement.setString(4, reason)
                statement.executeUpdate()
            }
        }
    }

    override suspend fun startPipelineRun(
        engineRunId: String,
        pipelineRunId: String,
        pipelineId: String,
        startedAt: Instant,
    ) {
        requireIdentifier(engineRunId, "engine run ID")
        requireIdentifier(pipelineRunId, "pipeline run ID")
        requireIdentifier(pipelineId, "pipeline ID")
        transaction { connection ->
            connection.prepare(
                """
                INSERT INTO ${table("pipeline_runs")} (
                    pipeline_run_id, engine_run_id, pipeline_id, status,
                    started_at, last_observed_at
                ) VALUES (?, ?, ?, 'RUNNING', ?, ?)
                """.trimIndent(),
            ).use { statement ->
                statement.setString(1, pipelineRunId)
                statement.setString(2, engineRunId)
                statement.setString(3, pipelineId)
                statement.setTimestamp(4, timestamp(startedAt))
                statement.setTimestamp(5, timestamp(startedAt))
                statement.executeUpdate()
            }
        }
    }

    override suspend fun completePipelineRun(
        pipelineRunId: String,
        status: DurableRunStatus,
        completedAt: Instant,
        failureCount: Long,
    ) {
        completeRunArguments(pipelineRunId, status, failureCount, "pipeline run ID")
        transaction { connection ->
            connection.prepare(
                """
                UPDATE ${table("pipeline_runs")}
                SET status = ?, completed_at = ?, last_observed_at = ?, failure_count = ?
                WHERE pipeline_run_id = ? AND status = 'RUNNING'
                """.trimIndent(),
            ).use { statement ->
                statement.setString(1, status.name)
                statement.setTimestamp(2, timestamp(completedAt))
                statement.setTimestamp(3, timestamp(completedAt))
                statement.setLong(4, failureCount)
                statement.setString(5, pipelineRunId)
                statement.executeUpdate()
            }
        }
    }

    override suspend fun completeEngineRun(
        engineRunId: String,
        status: DurableRunStatus,
        completedAt: Instant,
        failureCount: Long,
    ) {
        completeRunArguments(engineRunId, status, failureCount, "engine run ID")
        transaction { connection ->
            connection.prepare(
                """
                UPDATE ${table("engine_runs")}
                SET status = ?, completed_at = ?, last_observed_at = ?, failure_count = ?
                WHERE engine_run_id = ? AND status = 'RUNNING'
                """.trimIndent(),
            ).use { statement ->
                statement.setString(1, status.name)
                statement.setTimestamp(2, timestamp(completedAt))
                statement.setTimestamp(3, timestamp(completedAt))
                statement.setLong(4, failureCount)
                statement.setString(5, engineRunId)
                statement.executeUpdate()
            }
        }
    }

    private suspend fun recoverStaleRunsInternal(now: Instant, staleAfter: Duration): Int =
        transactionInternal { connection ->
            val cutoff = now.minus(staleAfter)
            recoverStalePipelineRuns(connection, cutoff, now) +
                recoverStaleEngineRuns(connection, cutoff, now)
        }

    private fun recoverStalePipelineRuns(
        connection: Connection,
        cutoff: Instant,
        now: Instant,
    ): Int = connection.prepare(
        """
        WITH stale AS (
            SELECT pipeline_run_id
            FROM ${table("pipeline_runs")}
            WHERE status = 'RUNNING' AND last_observed_at < ?
            ORDER BY last_observed_at
            LIMIT ?
            FOR UPDATE SKIP LOCKED
        )
        UPDATE ${table("pipeline_runs")} p
        SET status = 'ABANDONED', completed_at = ?, last_observed_at = ?,
            recovery_reason = 'stale run recovered'
        FROM stale
        WHERE p.pipeline_run_id = stale.pipeline_run_id
        """.trimIndent(),
    ).use { statement ->
        statement.setTimestamp(1, timestamp(cutoff))
        statement.setInt(2, configuration.maxRecoveryRows)
        statement.setTimestamp(3, timestamp(now))
        statement.setTimestamp(4, timestamp(now))
        statement.executeUpdate()
    }

    private fun recoverStaleEngineRuns(
        connection: Connection,
        cutoff: Instant,
        now: Instant,
    ): Int = connection.prepare(
        """
        WITH stale AS (
            SELECT engine_run_id
            FROM ${table("engine_runs")}
            WHERE status = 'RUNNING' AND last_observed_at < ?
            ORDER BY last_observed_at
            LIMIT ?
            FOR UPDATE SKIP LOCKED
        )
        UPDATE ${table("engine_runs")} e
        SET status = 'ABANDONED', completed_at = ?, last_observed_at = ?,
            recovery_reason = 'stale run recovered'
        FROM stale
        WHERE e.engine_run_id = stale.engine_run_id
        """.trimIndent(),
    ).use { statement ->
        statement.setTimestamp(1, timestamp(cutoff))
        statement.setInt(2, configuration.maxRecoveryRows)
        statement.setTimestamp(3, timestamp(now))
        statement.setTimestamp(4, timestamp(now))
        statement.executeUpdate()
    }

    private fun reclaimExpiredLeases(
        connection: Connection,
        pipelineId: String?,
        now: Instant,
    ) {
        val sql = buildString {
            append(
                """
                SELECT job_id, active_attempt_id
                FROM ${table("jobs")}
                WHERE status = 'LEASED' AND lease_expires_at <= ?
                """.trimIndent(),
            )
            if (pipelineId != null) append(" AND pipeline_id = ?")
            append(" ORDER BY lease_expires_at, sequence LIMIT ? FOR UPDATE SKIP LOCKED")
        }
        val expired = connection.prepare(sql).use { statement ->
            var index = 1
            statement.setTimestamp(index++, timestamp(now))
            pipelineId?.let { statement.setString(index++, it) }
            statement.setInt(index, configuration.maxClaimBatch)
            statement.executeQuery().use { result ->
                val rows = mutableListOf<ExpiredLease>()
                while (result.next()) {
                    rows += ExpiredLease(
                        jobId = result.getString("job_id"),
                        activeAttemptId = result.getString("active_attempt_id"),
                    )
                }
                rows
            }
        }
        expired.forEach { lease ->
            lease.activeAttemptId?.let { attemptId ->
                connection.prepare(
                    """
                    UPDATE ${table("attempts")}
                    SET outcome = 'ABANDONED', finished_at = ?, reason = 'lease expired'
                    WHERE attempt_id = ? AND outcome IS NULL
                    """.trimIndent(),
                ).use { statement ->
                    statement.setTimestamp(1, timestamp(now))
                    statement.setString(2, attemptId)
                    statement.executeUpdate()
                }
            }
            connection.prepare(
                """
                UPDATE ${table("jobs")}
                SET status = 'QUEUED', lease_token = NULL, lease_owner = NULL,
                    lease_expires_at = NULL, active_attempt_id = NULL, updated_at = ?
                WHERE job_id = ? AND status = 'LEASED' AND lease_expires_at <= ?
                """.trimIndent(),
            ).use { statement ->
                statement.setTimestamp(1, timestamp(now))
                statement.setString(2, lease.jobId)
                statement.setTimestamp(3, timestamp(now))
                require(statement.executeUpdate() == 1) {
                    "PostgreSQL lease reclaim lost a locked job row"
                }
            }
        }
    }

    private fun findActiveJob(connection: Connection, resourceId: Long): WorkItemId? = connection.prepare(
        """
        SELECT job_id FROM ${table("jobs")}
        WHERE resource_id = ? AND status IN ('QUEUED', 'LEASED', 'RETRY_WAIT')
        ORDER BY sequence LIMIT 1
        """.trimIndent(),
    ).use { statement ->
        statement.setLong(1, resourceId)
        statement.executeQuery().use { result ->
            if (result.next()) WorkItemId(result.getString("job_id")) else null
        }
    }

    private fun lockEnqueueCapacity(connection: Connection) {
        connection.prepare("SELECT pg_advisory_xact_lock(hashtext(?))").use { statement ->
            statement.setString(1, "trawler-active-capacity:${configuration.schema}")
            statement.execute()
        }
    }

    private fun transitionData(transition: WorkTransition, now: Instant): TransitionData = when (transition) {
        WorkTransition.Succeeded -> TransitionData(
            status = "SUCCEEDED",
            notBefore = null,
            finishedAt = now,
            reason = null,
            attemptOutcome = AttemptOutcome.SUCCEEDED,
        )

        is WorkTransition.Retry -> TransitionData(
            status = "RETRY_WAIT",
            notBefore = transition.notBefore,
            finishedAt = null,
            reason = null,
            attemptOutcome = AttemptOutcome.ABANDONED,
        )

        is WorkTransition.Failed -> TransitionData(
            status = "FAILED",
            notBefore = null,
            finishedAt = now,
            reason = transition.reason,
            attemptOutcome = AttemptOutcome.FAILED,
        )

        is WorkTransition.Skipped -> TransitionData(
            status = "SKIPPED",
            notBefore = null,
            finishedAt = now,
            reason = transition.reason,
            attemptOutcome = AttemptOutcome.ABANDONED,
        )

        is WorkTransition.Cancelled -> TransitionData(
            status = "CANCELLED",
            notBefore = null,
            finishedAt = now,
            reason = transition.reason,
            attemptOutcome = AttemptOutcome.CANCELLED,
        )

        is WorkTransition.Abandoned -> TransitionData(
            status = "ABANDONED",
            notBefore = null,
            finishedAt = now,
            reason = transition.reason,
            attemptOutcome = AttemptOutcome.ABANDONED,
        )
    }.also { data ->
        data.reason?.let { requireUtf8Bound(it, configuration.maxReasonBytes, "work transition reason") }
    }

    private fun completeRunArguments(
        runId: String,
        status: DurableRunStatus,
        failureCount: Long,
        label: String,
    ) {
        requireIdentifier(runId, label)
        require(status != DurableRunStatus.RUNNING) { "A durable run cannot be completed as RUNNING" }
        require(failureCount >= 0) { "Durable run failure count must not be negative" }
    }

    private fun nextAttemptId(connection: Connection): com.rmjtromp.trawler.state.AttemptId =
        connection.prepare("SELECT nextval('${qualifiedSequence("attempt_sequence")}')").use { statement ->
            statement.executeQuery().use { result ->
                require(result.next()) { "PostgreSQL attempt sequence returned no value" }
                com.rmjtromp.trawler.state.AttemptId("postgres-attempt-${result.getLong(1)}")
            }
        }

    private suspend fun migrateInternal() {
        withContext(databaseDispatcher) {
            Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .schemas(configuration.schema)
                .defaultSchema(configuration.schema)
                .table("flyway_schema_history")
                .createSchemas(true)
                .load()
                .migrate()
        }
    }

    private suspend fun <T> database(block: (Connection) -> T): T {
        ensureStarted()
        return databaseInternal(block)
    }

    private suspend fun <T> databaseInternal(block: (Connection) -> T): T =
        withContext(databaseDispatcher) {
            dataSource.connection.use { connection ->
                block(connection)
            }
        }

    private suspend fun <T> transaction(block: (Connection) -> T): T {
        ensureStarted()
        return transactionInternal(block)
    }

    private suspend fun <T> transactionInternal(block: (Connection) -> T): T =
        withContext(databaseDispatcher) {
            dataSource.connection.use { connection ->
                connection.autoCommit = false
                try {
                    val result = block(connection)
                    connection.commit()
                    result
                } catch (error: Throwable) {
                    try {
                        connection.rollback()
                    } catch (rollbackFailure: Throwable) {
                        error.addSuppressed(rollbackFailure)
                    }
                    throw error
                }
            }
        }

    private fun ensureStarted() {
        check(started.get()) {
            "PostgreSQL state store '$id' is not started"
        }
    }

    private fun closeResources() {
        dataSource.close()
        executor.shutdownNow()
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS)
        } catch (interrupted: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    private fun requireIdentifier(value: String, label: String) {
        require(value.isNotBlank()) { "PostgreSQL $label must not be blank" }
        requireUtf8Bound(value, configuration.maxIdentifierBytes, label)
    }

    private fun validateRequest(request: CrawlRequest) {
        requireUtf8Bound(request.target, configuration.maxTargetBytes, "request target")
        requireUtf8Bound(request.method.value, configuration.maxHeaderValueBytes, "request method")
        require(request.headers.entries.size <= configuration.maxHeaderCount) {
            "Request header count exceeds the PostgreSQL state bound"
        }
        request.headers.entries.forEach { header ->
            requireUtf8Bound(header.name, configuration.maxHeaderNameBytes, "request header name")
            requireUtf8Bound(header.value, configuration.maxHeaderValueBytes, "request header value")
        }
        request.body?.let { body ->
            require(body.size <= configuration.maxRequestBodyBytes) {
                "Request body exceeds the PostgreSQL state bound"
            }
        }
        require(request.attributes.entries.size <= configuration.maxAttributeCount) {
            "Request attribute count exceeds the PostgreSQL state bound"
        }
        request.attributes.entries.forEach { attribute ->
            requireUtf8Bound(attribute.name, configuration.maxAttributeNameBytes, "request attribute name")
            requireUtf8Bound(attribute.value, configuration.maxAttributeValueBytes, "request attribute value")
        }
    }

    private data class ExpiredLease(
        val jobId: String,
        val activeAttemptId: String?,
    )

    private data class TransitionRow(
        val resourceId: Long,
        val activeAttemptId: String?,
    )

    private data class TransitionData(
        val status: String,
        val notBefore: Instant?,
        val finishedAt: Instant?,
        val reason: String?,
        val attemptOutcome: AttemptOutcome,
    )

    private fun table(name: String): String = "$schemaSql.${quoteIdentifier(name)}"

    private fun qualifiedSequence(name: String): String = "$schemaSql.${quoteIdentifier(name)}"

    private fun timestamp(value: Instant): Timestamp = Timestamp.from(value)

    private fun quoteIdentifier(value: String): String = "\"${value.replace("\"", "\"\"")}\""

    private fun ResultSet.instant(column: String): Instant? = getTimestamp(column)?.toInstant()

    private fun Connection.prepare(sql: String): PreparedStatement =
        prepareStatement(sql).also { it.queryTimeout = statementTimeoutSeconds }

    private val statementTimeoutSeconds: Int
        get() = ((configuration.statementTimeout.toMillis() + 999L) / 1_000L)
            .coerceIn(1L, Int.MAX_VALUE.toLong())
            .toInt()

    private class PostgresThreadFactory(
        private val storeId: String,
        private val sequence: AtomicLong,
    ) : ThreadFactory {
        override fun newThread(runnable: Runnable): Thread = Thread(
            runnable,
            "$storeId-db-${sequence.incrementAndGet()}",
        ).apply {
            isDaemon = true
        }
    }
}

private fun requireUtf8Bound(value: String, maximumBytes: Int, label: String) {
    require(value.toByteArray(Charsets.UTF_8).size <= maximumBytes) {
        "PostgreSQL $label exceeds its configured UTF-8 bound"
    }
}
