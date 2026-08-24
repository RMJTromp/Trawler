package com.rmjtromp.trawler.postgres

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.ExecutorCoroutineDispatcher
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
import java.time.Instant
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Adapter-private JDBC ownership shared by the response and output adapters.
 *
 * Each selected adapter owns its own bounded pool and dispatcher; this helper
 * does not become a universal storage abstraction. Migration, transaction,
 * cancellation, and close boundaries stay with the owning adapter. Replay
 * activation closes and later recreates its pool/dispatcher, while [stop] is
 * terminal. A caller must register the adapter as an
 * [com.rmjtromp.trawler.extension.ExtensionLifecycle] before using it through
 * an engine.
 */
internal class PostgresAdapterResources(
    private val adapterId: String,
    private val configuration: PostgresStateConfiguration,
    private val migrationLocation: String,
    private val migrationHistoryTable: String,
) {
    private val lifecycleMutex = Mutex()
    private val started = AtomicBoolean(false)
    private val closed = AtomicBoolean(false)
    private val threadSequence = AtomicLong(0)
    private var executor: ExecutorService? = null
    private var dispatcher: ExecutorCoroutineDispatcher? = null
    private var dataSource: HikariDataSource? = null

    suspend fun start() {
        lifecycleMutex.withLock {
            check(!closed.get()) { "PostgreSQL adapter '$adapterId' is closed" }
            if (started.get()) return
            initializeResources()
            val activeDispatcher = checkNotNull(dispatcher)
            val activeDataSource = checkNotNull(dataSource)
            try {
                withContext(NonCancellable) {
                    withContext(activeDispatcher) {
                        Flyway.configure()
                            .dataSource(activeDataSource)
                            .locations(migrationLocation)
                            .schemas(configuration.schema)
                            .defaultSchema(configuration.schema)
                            .table(migrationHistoryTable)
                            .createSchemas(true)
                            .load()
                            .migrate()
                    }
                    started.set(true)
                }
            } catch (error: Throwable) {
                closeResources()
                throw error
            }
        }
    }

    /** Stops one replay activation while leaving this owner reusable. */
    suspend fun stopForReplay() = withContext(NonCancellable) {
        lifecycleMutex.withLock {
            if (closed.get()) return@withLock
            closeResources()
        }
    }

    suspend fun stop() = withContext(NonCancellable) {
        lifecycleMutex.withLock {
            if (!closed.compareAndSet(false, true)) return@withLock
            closeResources()
        }
    }

    suspend fun <T> database(block: (Connection) -> T): T {
        ensureStarted()
        val activeDispatcher = checkNotNull(dispatcher)
        val activeDataSource = checkNotNull(dataSource)
        return withContext(activeDispatcher) {
            activeDataSource.connection.use { connection -> block(connection) }
        }
    }

    suspend fun <T> transaction(block: (Connection) -> T): T {
        ensureStarted()
        val activeDispatcher = checkNotNull(dispatcher)
        val activeDataSource = checkNotNull(dataSource)
        return withContext(activeDispatcher) {
            activeDataSource.connection.use { connection ->
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
    }

    fun table(name: String): String =
        "${quotePostgresIdentifier(configuration.schema)}.${quotePostgresIdentifier(name)}"

    fun tableExists(connection: Connection, name: String): Boolean =
        connection.prepareBounded(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = ? AND table_name = ?
            )
            """.trimIndent(),
            configuration,
        ).use { statement ->
            statement.setString(1, configuration.schema)
            statement.setString(2, name)
            statement.executeQuery().use { result ->
                result.next() && result.getBoolean(1)
            }
        }

    fun sequence(name: String): String = table(name)

    fun timestamp(value: Instant): Timestamp = Timestamp.from(value)

    fun requireStarted() {
        ensureStarted()
    }

    fun boundLimit(requested: Int): Int {
        require(requested > 0) { "Query limit must be positive" }
        return requested.coerceAtMost(configuration.maxResponseQueryLimit)
    }

    fun boundRetentionLimit(requested: Int): Int {
        require(requested > 0) { "Retention batch must be positive" }
        return requested.coerceAtMost(configuration.maxRetentionRows)
    }

    private fun ensureStarted() {
        check(started.get() && !closed.get()) {
            "PostgreSQL adapter '$adapterId' is not started"
        }
    }

    private fun initializeResources() {
        if (executor != null && dispatcher != null && dataSource != null) return
        val newExecutor = Executors.newFixedThreadPool(
            configuration.databaseDispatcherParallelism,
            AdapterThreadFactory(adapterId, threadSequence),
        )
        var newDataSource: HikariDataSource? = null
        try {
            newDataSource = HikariDataSource(
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
                    poolName = "${configuration.poolName}-${adapterId.sanitizePoolName()}"
                },
            )
            executor = newExecutor
            dispatcher = newExecutor.asCoroutineDispatcher()
            dataSource = newDataSource
        } catch (error: Throwable) {
            try {
                newDataSource?.close()
            } catch (closeFailure: Throwable) {
                error.addSuppressed(closeFailure)
            }
            newExecutor.shutdownNow()
            throw error
        }
    }

    private fun closeResources() {
        started.set(false)
        dataSource?.let {
            dataSource = null
            it.close()
        }
        executor?.let {
            executor = null
            it.shutdownNow()
            try {
                it.awaitTermination(5, TimeUnit.SECONDS)
            } catch (interrupted: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
        dispatcher?.let {
            dispatcher = null
            it.close()
        }
    }

    private fun String.sanitizePoolName(): String =
        replace(Regex("[^A-Za-z0-9_]+"), "_").take(48).ifBlank { "adapter" }

    private class AdapterThreadFactory(
        private val adapterId: String,
        private val sequence: AtomicLong,
    ) : ThreadFactory {
        override fun newThread(runnable: Runnable): Thread = Thread(
            runnable,
            "$adapterId-db-${sequence.incrementAndGet()}",
        ).apply { isDaemon = true }
    }
}

internal fun Connection.prepareBounded(
    sql: String,
    configuration: PostgresStateConfiguration,
): PreparedStatement = prepareStatement(sql).also { statement ->
    statement.queryTimeout = ((configuration.statementTimeout.toMillis() + 999L) / 1_000L)
        .coerceIn(1L, Int.MAX_VALUE.toLong())
        .toInt()
}

internal fun quotePostgresIdentifier(value: String): String =
    "\"${value.replace("\"", "\"\"")}\""

internal fun requirePostgresUtf8Bound(value: String, maxBytes: Int, label: String) {
    require(value.toByteArray(Charsets.UTF_8).size <= maxBytes) {
        "$label exceeds the configured PostgreSQL bound"
    }
}

internal fun truncatePostgresUtf8(value: String, maxBytes: Int): String {
    if (value.toByteArray(Charsets.UTF_8).size <= maxBytes) return value
    var end = value.length
    while (end > 0 && value.substring(0, end).toByteArray(Charsets.UTF_8).size > maxBytes) end--
    return value.substring(0, end)
}

internal fun ResultSet.getBoundedBytes(
    column: String,
    reportedSize: Int,
    maximum: Int,
    label: String,
): ByteArray {
    require(reportedSize >= 0 && reportedSize <= maximum) {
        "PostgreSQL $label payload exceeds its configured bound"
    }
    return requireNotNull(getBytes(column)) { "PostgreSQL $label payload is null" }.also { bytes ->
        require(bytes.size <= maximum) {
            "PostgreSQL $label payload exceeds its configured bound"
        }
    }
}
