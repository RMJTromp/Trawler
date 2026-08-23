package com.rmjtromp.trawler.postgres

import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.RequestBody
import com.rmjtromp.trawler.model.RequestMethod
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.nio.charset.StandardCharsets
import java.time.Duration

/**
 * Immutable bounds and connection settings owned by the PostgreSQL adapter.
 *
 * The password is copied on construction and never appears in [toString].
 * The adapter owns the resulting Hikari pool and its bounded blocking
 * dispatcher. Hosts remain responsible for secret sourcing, authorization,
 * TLS settings in the JDBC URL, and database retention operations.
 */
public class PostgresStateConfiguration(
    public val jdbcUrl: String,
    public val username: String? = null,
    password: CharArray? = null,
    public val schema: String = "trawler",
    public val maximumPoolSize: Int = 8,
    public val minimumIdle: Int = 0,
    public val databaseDispatcherParallelism: Int = maximumPoolSize,
    public val connectionTimeout: Duration = Duration.ofSeconds(30),
    public val statementTimeout: Duration = Duration.ofSeconds(30),
    public val leaseDuration: Duration = Duration.ofMinutes(5),
    public val staleRunAfter: Duration = Duration.ofMinutes(30),
    public val maxActiveJobs: Int = 100_000,
    public val maxClaimBatch: Int = 64,
    public val maxCancellationRows: Int = 4_096,
    public val maxRecoveryRows: Int = 1_024,
    public val maxCheckpointBatch: Int = 64,
    public val maxCheckpointPayloadBytes: Int = 2 * 1024 * 1024,
    public val maxEncodedRequestBytes: Int = 8 * 1_024 * 1_024,
    public val maxRequestBodyBytes: Int = 8 * 1_024 * 1_024,
    public val maxResponseBodyBytes: Int = 8 * 1_024 * 1_024,
    public val maxEncodedResponseBytes: Int = 8 * 1_024 * 1_024,
    public val maxOutputPayloadBytes: Int = 1 * 1_024 * 1_024,
    public val maxOutputBatchItems: Int = 256,
    public val maxResponseQueryLimit: Int = 4_096,
    public val maxOutboxClaimBatch: Int = 64,
    public val maxRetentionRows: Int = 4_096,
    public val outboxLeaseDuration: Duration = Duration.ofMinutes(5),
    public val maxOutboxAttempts: Int = 10,
    public val maxHeaderCount: Int = 256,
    public val maxAttributeCount: Int = 256,
    public val maxHeaderNameBytes: Int = 1_024,
    public val maxHeaderValueBytes: Int = 8 * 1_024,
    public val maxTargetBytes: Int = 16 * 1_024,
    public val maxAttributeNameBytes: Int = 1_024,
    public val maxAttributeValueBytes: Int = 8 * 1_024,
    public val maxIdentifierBytes: Int = 255,
    public val maxIdentityBytes: Int = 4 * 1_024,
    public val maxReasonBytes: Int = 4 * 1_024,
    public val poolName: String = "trawler-postgres",
) {
    private val passwordChars: CharArray? = password?.copyOf()

    init {
        require(jdbcUrl.startsWith("jdbc:postgresql:")) {
            "PostgreSQL state JDBC URL must start with jdbc:postgresql:"
        }
        require(username == null || username.isNotBlank()) {
            "PostgreSQL state username must not be blank when present"
        }
        require(isSafeIdentifier(schema)) {
            "PostgreSQL state schema must be a simple SQL identifier"
        }
        require(maximumPoolSize > 0) { "PostgreSQL pool size must be positive" }
        require(minimumIdle in 0..maximumPoolSize) {
            "PostgreSQL minimum idle must be between zero and the pool size"
        }
        require(databaseDispatcherParallelism > 0) {
            "PostgreSQL dispatcher parallelism must be positive"
        }
        require(databaseDispatcherParallelism <= maximumPoolSize) {
            "PostgreSQL dispatcher parallelism must not exceed the pool size"
        }
        requireTimeout(connectionTimeout, "connection timeout")
        require(connectionTimeout.toMillis() >= 250) {
            "PostgreSQL connection timeout must be at least 250 milliseconds for HikariCP"
        }
        requireTimeout(statementTimeout, "statement timeout")
        requireTimeout(leaseDuration, "lease duration")
        requireTimeout(staleRunAfter, "stale-run threshold")
        require(maxActiveJobs > 0) { "PostgreSQL active-job bound must be positive" }
        require(maxClaimBatch > 0) { "PostgreSQL claim batch must be positive" }
        require(maxClaimBatch <= maxActiveJobs) {
            "PostgreSQL claim batch must not exceed the active-job bound"
        }
        require(maxCancellationRows > 0) { "PostgreSQL cancellation batch must be positive" }
        require(maxRecoveryRows > 0) { "PostgreSQL recovery batch must be positive" }
        require(maxCheckpointBatch > 0) { "PostgreSQL checkpoint batch must be positive" }
        require(maxCheckpointPayloadBytes > 0) { "Checkpoint payload bound must be positive" }
        require(maxEncodedRequestBytes > 0) { "Encoded request bound must be positive" }
        require(maxRequestBodyBytes >= 0) { "Request body bound must not be negative" }
        require(maxResponseBodyBytes > 0) { "Response body bound must be positive" }
        require(maxEncodedResponseBytes > 0) { "Encoded response bound must be positive" }
        require(maxOutputPayloadBytes > 0) { "Output payload bound must be positive" }
        require(maxOutputBatchItems > 0) { "Output batch bound must be positive" }
        require(maxResponseQueryLimit > 0) { "Response query bound must be positive" }
        require(maxOutboxClaimBatch > 0) { "Outbox claim batch must be positive" }
        require(maxRetentionRows > 0) { "Retention row bound must be positive" }
        requireTimeout(outboxLeaseDuration, "outbox lease duration")
        require(maxOutboxAttempts > 0) { "Outbox attempt bound must be positive" }
        require(maxHeaderCount >= 0) { "Header count bound must not be negative" }
        require(maxAttributeCount >= 0) { "Attribute count bound must not be negative" }
        require(maxHeaderNameBytes > 0) { "Header name bound must be positive" }
        require(maxHeaderValueBytes >= 0) { "Header value bound must not be negative" }
        require(maxTargetBytes > 0) { "Request target bound must be positive" }
        require(maxAttributeNameBytes > 0) { "Attribute name bound must be positive" }
        require(maxAttributeValueBytes >= 0) { "Attribute value bound must not be negative" }
        require(maxIdentifierBytes in 1..255) {
            "PostgreSQL identifier bound must be between one and 255 bytes"
        }
        require(maxIdentityBytes > 0) { "Identity bound must be positive" }
        require(maxReasonBytes > 0) { "Reason bound must be positive" }
        require(isSafeIdentifier(poolName.replace('-', '_'))) {
            "PostgreSQL pool name must contain only simple identifier characters"
        }
    }

    /** Returns a defensive password copy for pool construction. */
    public fun passwordCopy(): CharArray? = passwordChars?.copyOf()

    /** Returns bounds used by the default versioned request codec. */
    public fun requestCodecLimits(): PostgresRequestCodecLimits = PostgresRequestCodecLimits(
        maxEncodedBytes = maxEncodedRequestBytes,
        maxBodyBytes = maxRequestBodyBytes,
        maxHeaderCount = maxHeaderCount,
        maxAttributeCount = maxAttributeCount,
        maxHeaderNameBytes = maxHeaderNameBytes,
        maxHeaderValueBytes = maxHeaderValueBytes,
        maxTargetBytes = maxTargetBytes,
        maxAttributeNameBytes = maxAttributeNameBytes,
        maxAttributeValueBytes = maxAttributeValueBytes,
    )

    override fun toString(): String =
        "PostgresStateConfiguration(jdbcUrl=${redactedJdbcUrl(jdbcUrl)}, schema=$schema, " +
            "maximumPoolSize=$maximumPoolSize, databaseDispatcherParallelism=$databaseDispatcherParallelism)"
}

private fun requireTimeout(value: Duration, label: String) {
    require(!value.isNegative && !value.isZero) { "PostgreSQL $label must be positive" }
}

private fun isSafeIdentifier(value: String): Boolean =
    value.isNotBlank() && value.length <= 63 &&
        value.first().let { it == '_' || it.isLetter() } &&
        value.drop(1).all { it == '_' || it.isLetterOrDigit() }

private fun redactedJdbcUrl(value: String): String =
    value.substringBefore('?') + if ('?' in value) "?<redacted>" else ""

/** Versioned bounds for the built-in binary request codec. */
public data class PostgresRequestCodecLimits(
    public val maxEncodedBytes: Int,
    public val maxBodyBytes: Int,
    public val maxHeaderCount: Int,
    public val maxAttributeCount: Int,
    public val maxHeaderNameBytes: Int,
    public val maxHeaderValueBytes: Int,
    public val maxTargetBytes: Int,
    public val maxAttributeNameBytes: Int,
    public val maxAttributeValueBytes: Int,
) {
    init {
        require(maxEncodedBytes > 0) { "Encoded request bound must be positive" }
        require(maxBodyBytes >= 0) { "Request body bound must not be negative" }
        require(maxHeaderCount >= 0) { "Header count bound must not be negative" }
        require(maxAttributeCount >= 0) { "Attribute count bound must not be negative" }
        require(maxHeaderNameBytes > 0) { "Header name bound must be positive" }
        require(maxHeaderValueBytes >= 0) { "Header value bound must not be negative" }
        require(maxTargetBytes > 0) { "Request target bound must be positive" }
        require(maxAttributeNameBytes > 0) { "Attribute name bound must be positive" }
        require(maxAttributeValueBytes >= 0) { "Attribute value bound must not be negative" }
    }
}

/** Codec identity persisted beside one encoded request payload. */
public data class PostgresCodecDescriptor(
    public val id: String,
    public val version: Int,
) {
    init {
        require(id.isNotBlank()) { "PostgreSQL codec ID must not be blank" }
        require(version >= 0) { "PostgreSQL codec version must not be negative" }
    }
}

/** Caller/extension-owned request codec boundary; no serializer is required. */
public interface PostgresRequestCodec {
    /** Stable codec identity stored with every encoded request. */
    public val descriptor: PostgresCodecDescriptor

    /** Encodes one bounded core request without network or database access. */
    public fun encode(request: CrawlRequest): ByteArray

    /** Decodes one payload written by [descriptor], enforcing adapter bounds. */
    public fun decode(payload: ByteArray): CrawlRequest
}

/** Immutable codec output with defensive byte-array ownership. */
public class EncodedPostgresRequest private constructor(
    public val descriptor: PostgresCodecDescriptor,
    private val bytes: ByteArray,
) {
    /** Encoded payload size in bytes. */
    public val size: Int
        get() = bytes.size

    /** Returns a defensive payload copy. */
    public fun toByteArray(): ByteArray = bytes.copyOf()

    public companion object {
        /** Creates an immutable encoded request snapshot. */
        public fun from(descriptor: PostgresCodecDescriptor, bytes: ByteArray): EncodedPostgresRequest =
            EncodedPostgresRequest(descriptor, bytes.copyOf())
    }
}

/**
 * Immutable registry for active and historical request codecs.
 *
 * Codecs are selected by explicit ID/version. A missing historical codec
 * rejects claim/decode rather than silently interpreting bytes with a newer
 * schema. The registry is bounded by its caller-provided list and performs no
 * serialization of produced values.
 */
public class PostgresRequestCodecRegistry(
    codecs: List<PostgresRequestCodec> = listOf(BinaryPostgresRequestCodec()),
    active: PostgresCodecDescriptor? = null,
) {
    private val codecs: List<PostgresRequestCodec> = codecs.toList()
    private val activeDescriptor: PostgresCodecDescriptor = active ?: this.codecs.firstOrNull()?.descriptor
        ?: error("At least one PostgreSQL request codec is required")

    init {
        require(this.codecs.isNotEmpty()) { "At least one PostgreSQL request codec is required" }
        require(this.codecs.map { it.descriptor }.distinct().size == this.codecs.size) {
            "PostgreSQL request codec ID/version pairs must be unique"
        }
        require(this.codecs.any { it.descriptor == activeDescriptor }) {
            "Active PostgreSQL request codec is not registered"
        }
    }

    /** Encodes with the explicitly active codec. */
    public fun encode(request: CrawlRequest): EncodedPostgresRequest {
        val codec = codec(activeDescriptor)
        return EncodedPostgresRequest.from(codec.descriptor, codec.encode(request))
    }

    /** Decodes with the exact persisted codec ID and version. */
    public fun decode(descriptor: PostgresCodecDescriptor, payload: ByteArray): CrawlRequest =
        codec(descriptor).decode(payload.copyOf())

    private fun codec(descriptor: PostgresCodecDescriptor): PostgresRequestCodec =
        codecs.firstOrNull { it.descriptor == descriptor }
            ?: throw IllegalStateException(
                "No PostgreSQL request codec is registered for ${descriptor.id}@${descriptor.version}",
            )
}

/**
 * JDK-only version-1 request codec. It encodes only core request state and is
 * intentionally not a general-purpose serializer or produced-data codec.
 */
public class BinaryPostgresRequestCodec(
    private val limits: PostgresRequestCodecLimits = PostgresRequestCodecLimits(
        maxEncodedBytes = 8 * 1_024 * 1_024,
        maxBodyBytes = 8 * 1_024 * 1_024,
        maxHeaderCount = 256,
        maxAttributeCount = 256,
        maxHeaderNameBytes = 1_024,
        maxHeaderValueBytes = 8 * 1_024,
        maxTargetBytes = 16 * 1_024,
        maxAttributeNameBytes = 1_024,
        maxAttributeValueBytes = 8 * 1_024,
    ),
) : PostgresRequestCodec {
    override val descriptor: PostgresCodecDescriptor = PostgresCodecDescriptor("trawler-binary-request", 1)

    override fun encode(request: CrawlRequest): ByteArray {
        require(request.headers.entries.size <= limits.maxHeaderCount) {
            "Request header count exceeds the PostgreSQL codec bound"
        }
        require(request.attributes.entries.size <= limits.maxAttributeCount) {
            "Request attribute count exceeds the PostgreSQL codec bound"
        }
        request.body?.let { require(it.size <= limits.maxBodyBytes) { "Request body exceeds the PostgreSQL codec bound" } }
        val bytes = ByteArrayOutputStream(minOf(limits.maxEncodedBytes, 4_096))
        DataOutputStream(bytes).use { output ->
            output.writeInt(MAGIC)
            output.writeInt(FORMAT_VERSION)
            writeString(output, request.target, limits.maxTargetBytes)
            writeString(output, request.method.value, limits.maxHeaderValueBytes)
            output.writeInt(request.headers.entries.size)
            request.headers.entries.forEach { header ->
                writeString(output, header.name, limits.maxHeaderNameBytes)
                writeString(output, header.value, limits.maxHeaderValueBytes)
            }
            val body = request.body?.toByteArray()
            output.writeInt(body?.size ?: -1)
            body?.let(output::write)
            output.writeInt(request.attributes.entries.size)
            request.attributes.entries.forEach { attribute ->
                writeString(output, attribute.name, limits.maxAttributeNameBytes)
                writeString(output, attribute.value, limits.maxAttributeValueBytes)
            }
        }
        return bytes.toByteArray().also {
            require(it.size <= limits.maxEncodedBytes) {
                "Encoded request exceeds the PostgreSQL codec bound"
            }
        }
    }

    override fun decode(payload: ByteArray): CrawlRequest {
        require(payload.size <= limits.maxEncodedBytes) {
            "Encoded request exceeds the PostgreSQL codec bound"
        }
        try {
            DataInputStream(ByteArrayInputStream(payload)).use { input ->
                require(input.readInt() == MAGIC) { "Unsupported PostgreSQL request codec payload" }
                require(input.readInt() == FORMAT_VERSION) { "Unsupported PostgreSQL request codec format" }
                val target = readString(input, limits.maxTargetBytes)
                val method = readString(input, limits.maxHeaderValueBytes)
                val headerCount = readCount(input, limits.maxHeaderCount, "header")
                val headers = ArrayList<Header>(headerCount)
                repeat(headerCount) {
                    headers += Header(
                        readString(input, limits.maxHeaderNameBytes),
                        readString(input, limits.maxHeaderValueBytes),
                    )
                }
                val bodySize = input.readInt()
                require(bodySize >= -1 && bodySize <= limits.maxBodyBytes) {
                    "Encoded request body exceeds the PostgreSQL codec bound"
                }
                val body = if (bodySize < 0) null else ByteArray(bodySize).also(input::readFully)
                val attributeCount = readCount(input, limits.maxAttributeCount, "attribute")
                val attributes = ArrayList<ContextAttribute>(attributeCount)
                repeat(attributeCount) {
                    attributes += ContextAttribute(
                        readString(input, limits.maxAttributeNameBytes),
                        readString(input, limits.maxAttributeValueBytes),
                    )
                }
                require(input.available() == 0) { "Encoded request contains trailing bytes" }
                return CrawlRequest(
                    target = target,
                    method = RequestMethod(method),
                    headers = Headers.from(headers),
                    body = body?.let(RequestBody::from),
                    attributes = ContextAttributes.from(attributes),
                )
            }
        } catch (error: EOFException) {
            throw IllegalArgumentException("Truncated PostgreSQL request codec payload", error)
        }
    }

    private fun writeString(output: DataOutputStream, value: String, maxBytes: Int) {
        val bytes = value.toByteArray(StandardCharsets.UTF_8)
        require(bytes.size <= maxBytes) { "Request codec string exceeds its configured bound" }
        output.writeInt(bytes.size)
        output.write(bytes)
    }

    private fun readString(input: DataInputStream, maxBytes: Int): String {
        val size = input.readInt()
        require(size >= 0 && size <= maxBytes) { "Request codec string exceeds its configured bound" }
        return ByteArray(size).also(input::readFully).toString(StandardCharsets.UTF_8)
    }

    private fun readCount(input: DataInputStream, max: Int, label: String): Int {
        val count = input.readInt()
        require(count in 0..max) { "Encoded request $label count exceeds its configured bound" }
        return count
    }

    private companion object {
        const val MAGIC: Int = 0x5452_5752
        const val FORMAT_VERSION: Int = 1
    }
}
