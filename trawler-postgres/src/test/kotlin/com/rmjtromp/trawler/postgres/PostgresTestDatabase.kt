package com.rmjtromp.trawler.postgres

import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertFalse
import kotlin.test.fail
import org.testcontainers.containers.PostgreSQLContainer

/**
 * Direct-SQL access to a Testcontainers database for assertions and fault
 * injection that bypass the adapter under test.
 *
 * Every connection is opened per call and closed before returning, except
 * [openTransaction], whose caller owns the returned connection.
 */
internal class PostgresTestDatabase(private val container: PostgreSQLContainer<*>) {
    val jdbcUrl: String get() = container.jdbcUrl
    val username: String get() = container.username
    val password: String get() = container.password

    fun <T> withConnection(block: (Connection) -> T): T =
        DriverManager.getConnection(container.jdbcUrl, container.username, container.password).use(block)

    fun execute(vararg statements: String) {
        withConnection { connection ->
            connection.createStatement().use { statement -> statements.forEach(statement::execute) }
        }
    }

    fun update(sql: String, vararg arguments: Any?): Int = withConnection { connection ->
        connection.prepareStatement(sql).use { statement ->
            arguments.forEachIndexed { index, value -> statement.setObject(index + 1, value) }
            statement.executeUpdate()
        }
    }

    /** First column of the first row, or `null` when the query returns no row or SQL NULL. */
    fun string(sql: String, vararg arguments: Any?): String? = withConnection { connection ->
        connection.prepareStatement(sql).use { statement ->
            arguments.forEachIndexed { index, value -> statement.setObject(index + 1, value) }
            statement.executeQuery().use { result -> if (result.next()) result.getString(1) else null }
        }
    }

    fun long(sql: String, vararg arguments: Any?): Long =
        requireNotNull(string(sql, *arguments)) { "Query returned no value: $sql" }.toLong()

    /**
     * Opens a connection with an uncommitted transaction after running
     * [statements]. The caller must commit or roll back and close it.
     */
    fun openTransaction(vararg statements: String): Connection {
        val connection = DriverManager.getConnection(container.jdbcUrl, container.username, container.password)
        try {
            connection.autoCommit = false
            connection.createStatement().use { statement -> statements.forEach(statement::execute) }
            return connection
        } catch (error: Throwable) {
            connection.close()
            throw error
        }
    }

    /**
     * Establishes that another backend is blocked on a heavyweight lock while
     * running a statement matching [queryPattern]. The happens-before edge is
     * the server's own lock table, not wall-clock timing; the poll count only
     * bounds a failure.
     */
    fun awaitLockWaiter(queryPattern: String, maxPolls: Int = 100_000) {
        withConnection { connection ->
            connection.prepareStatement(
                """
                SELECT COUNT(*) FROM pg_stat_activity
                WHERE wait_event_type = 'Lock' AND query LIKE ? AND pid <> pg_backend_pid()
                """.trimIndent(),
            ).use { statement ->
                statement.setString(1, queryPattern)
                repeat(maxPolls) {
                    statement.executeQuery().use { result ->
                        result.next()
                        if (result.getLong(1) > 0) return@withConnection
                    }
                }
            }
            fail("No backend became blocked on a lock for '$queryPattern' within $maxPolls polls")
        }
    }
}

/**
 * Joins (bounded) every live adapter-owned database thread for [adapterId] and
 * asserts none remain. A terminated executor may still be unwinding its
 * worker threads, so a bounded join is the deterministic check.
 */
internal fun assertNoAdapterThreads(adapterId: String) {
    val threads = Thread.getAllStackTraces().keys.filter { it.name.startsWith("$adapterId-db-") }
    threads.forEach { it.join(5_000) }
    assertFalse(threads.any(Thread::isAlive), "Adapter '$adapterId' leaked live database threads")
}
