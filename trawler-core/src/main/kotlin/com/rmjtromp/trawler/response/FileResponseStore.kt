package com.rmjtromp.trawler.response

import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.ResponseReference
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Bounded JDK file response history using caller-supplied [ResponseCodec]s.
 *
 * The store uses a fixed ring of response slots. A successful atomic rename
 * replaces one oldest slot, so the number of response files and retained
 * payloads never exceeds [maxResponses]. The store has lookup, cache,
 * history, replay, and retention capabilities but does not create crawl jobs.
 * File operations and codec calls run on a fixed adapter-owned dispatcher;
 * cancellation stops waiting for the operation, while an in-flight file
 * system call remains bounded by the operating system. [close] is idempotent.
 *
 * The root is selected by the host. References and temporary names are
 * generated only from bounded numeric slots, and no caller target is used as
 * a path component. The host remains responsible for directory permissions,
 * encryption, backup, and privacy of raw response bodies.
 */
public class FileResponseStore(
    override val id: String = "file-responses",
    root: Path,
    public val codecs: ResponseCodecRegistry,
    public val maxResponses: Int = 1_024,
    public val maxBodyBytes: Int = 8 * 1_024 * 1_024,
    public val maxEncodedResponseBytes: Int = 8 * 1_024 * 1_024,
    public val dispatcherParallelism: Int = 1,
) : ResponseStore,
    ResponseReplayStore,
    ResponseHistoryStore,
    ResponseRetentionStore,
    ResponseStoreLifecycle {

    /** Normalized host-selected directory; it is never derived from a target. */
    public val root: Path = root.toAbsolutePath().normalize()

    private val mutex = Mutex()
    private val closed = AtomicBoolean(false)
    private val threadSequence = AtomicLong(0)
    private val dispatcher: ExecutorCoroutineDispatcher
    private val entries = LinkedHashMap<Long, FileEntry>()
    private var loaded = false
    private var nextSequence = 0L

    override val capabilities: Set<ResponseStoreCapability> = setOf(
        ResponseStoreCapability.LOOKUP,
        ResponseStoreCapability.CACHE,
        ResponseStoreCapability.HISTORY,
        ResponseStoreCapability.REPLAY,
        ResponseStoreCapability.TTL_EVICTION,
        ResponseStoreCapability.RANGE_QUERY,
    )

    init {
        require(id.isNotBlank()) { "File response store ID must not be blank" }
        require(maxResponses > 0) { "File response capacity must be positive" }
        require(maxBodyBytes > 0) { "File response body limit must be positive" }
        require(maxEncodedResponseBytes > 0) { "File encoded response limit must be positive" }
        require(dispatcherParallelism > 0) { "File response dispatcher parallelism must be positive" }
        dispatcher = Executors.newFixedThreadPool(
            dispatcherParallelism,
            FileThreadFactory(id, threadSequence),
        ).asCoroutineDispatcher()
    }

    override suspend fun put(response: CrawlResponse): ResponseReference? = onFileDispatcher {
        if (response.body.size > maxBodyBytes) return@onFileDispatcher null
        val encoded = codecs.encode(response)
        if (encoded.size > maxEncodedResponseBytes) return@onFileDispatcher null
        val sequence = nextSequence
            .takeIf { it < Long.MAX_VALUE }
            ?.plus(1L)
            ?: error("File response sequence exhausted")
        val slot = slotFor(sequence)
        val path = slotPath(slot)
        val temporary = temporaryPath(slot)
        val entry = FileEntry(
            sequence = sequence,
            slot = slot,
            codec = encoded.descriptor,
            requestId = response.request.attributes[InMemoryResponseStore.REQUEST_ID_ATTRIBUTE],
            pipelineId = response.request.attributes[InMemoryResponseStore.PIPELINE_ID_ATTRIBUTE],
            target = response.request.target,
            receivedAt = response.receivedAt,
            payloadSize = encoded.size,
        )
        try {
            writeRecord(temporary, entry, encoded.toByteArray())
            moveIntoPlace(temporary, path)
        } finally {
            Files.deleteIfExists(temporary)
        }
        sequenceForSlot(slot)?.let(entries::remove)
        entries[sequence] = entry
        nextSequence = sequence
        ResponseReference(referenceFor(sequence))
    }

    override suspend fun find(query: ResponseLookup): StoredResponse? = onFileDispatcher {
        val entry = when (query) {
            is ResponseLookup.ByReference -> entries.values.firstOrNull { it.reference == query.reference }
            is ResponseLookup.ByRequestId -> entries.values
                .filter { it.requestId == query.requestId }
                .maxByOrNull(FileEntry::sequence)

            is ResponseLookup.ByTarget -> entries.values
                .filter { it.target == query.target }
                .maxByOrNull(FileEntry::sequence)
        } ?: return@onFileDispatcher null
        decode(entry)
    }

    override suspend fun replay(query: ResponseReplayQuery, limit: Int, pipelineId: String?): List<StoredResponse> = onFileDispatcher {
        require(limit > 0) { "Replay limit must be positive" }
        require(pipelineId == null || pipelineId.isNotBlank()) { "Replay pipeline ID must not be blank" }
        entries.values.asSequence()
            .sortedBy(FileEntry::sequence)
            .filter { entry -> (pipelineId == null || entry.pipelineId == pipelineId) && matches(entry, query) }
            .take(limit)
            .map(::decode)
            .toList()
    }

    override suspend fun history(requestId: String, limit: Int): List<StoredResponse> = onFileDispatcher {
        require(requestId.isNotBlank()) { "Response request ID must not be blank" }
        require(limit > 0) { "History limit must be positive" }
        entries.values.asSequence()
            .sortedBy(FileEntry::sequence)
            .filter { it.requestId == requestId }
            .take(limit)
            .map(::decode)
            .toList()
    }

    override suspend fun evictBefore(cutoff: Instant): Long =
        prune(ResponseRetentionRequest(cutoff, maxResponses)).deleted

    override suspend fun prune(request: ResponseRetentionRequest): ResponseRetentionResult = onFileDispatcher {
        val candidates = entries.values.filter { it.receivedAt.isBefore(request.cutoff) }
        val protected = candidates.count { it.reference in request.protectedReferences }
        val removable = candidates.asSequence()
            .filterNot { it.reference in request.protectedReferences }
            .sortedBy(FileEntry::sequence)
            .take(request.maxEntries)
            .toList()
        removable.forEach { entry ->
            Files.deleteIfExists(slotPath(entry.slot))
            entries.remove(entry.sequence)
        }
        ResponseRetentionResult(removable.size.toLong(), protected.toLong())
    }

    /** Closes the bounded file dispatcher and rejects later operations. */
    override suspend fun close() {
        if (!closed.compareAndSet(false, true)) return
        withContext(NonCancellable) {
            withContext(dispatcher) { mutex.withLock { Unit } }
            dispatcher.close()
        }
    }

    private suspend fun <T> onFileDispatcher(action: suspend () -> T): T {
        // Checked before dispatch: a closed executor rejects the task with a
        // CancellationException, which callers would mistake for cancellation.
        checkOpen()
        return withContext(dispatcher) {
            mutex.withLock {
                checkOpen()
                loadIfNeeded()
                action()
            }
        }
    }

    private fun checkOpen() {
        check(!closed.get()) { "File response store '$id' is closed" }
    }

    private fun loadIfNeeded() {
        if (loaded) return
        Files.createDirectories(root)
        for (slot in 0 until maxResponses) {
            Files.deleteIfExists(temporaryPath(slot))
            val path = slotPath(slot)
            if (!Files.exists(path)) continue
            val entry = readRecord(path, includePayload = false).entry
            require(entry.slot == slot) { "File response slot header does not match its path" }
            entries[entry.sequence] = entry
            nextSequence = maxOf(nextSequence, entry.sequence)
        }
        loaded = true
    }

    private fun decode(entry: FileEntry): StoredResponse {
        val record = readRecord(slotPath(entry.slot), includePayload = true)
        require(record.entry == entry) { "File response slot changed while it was being read" }
        val response = codecs.decode(entry.codec, record.payload ?: error("Missing response payload"))
        require(response.body.size <= maxBodyBytes) { "Decoded file response exceeds the configured body bound" }
        return StoredResponse(entry.reference, response, entry.pipelineId)
    }

    private fun matches(entry: FileEntry, query: ResponseReplayQuery): Boolean = when (query) {
        is ResponseReplayQuery.ByReference -> entry.reference == query.reference
        is ResponseReplayQuery.ByRequestId -> entry.requestId == query.requestId
        is ResponseReplayQuery.ByPipelineId -> entry.pipelineId == query.pipelineId
    }

    private fun writeRecord(path: Path, entry: FileEntry, payload: ByteArray) {
        Files.newOutputStream(
            path,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE,
        ).use { stream ->
            DataOutputStream(stream).use { output ->
                output.writeInt(MAGIC)
                output.writeInt(FORMAT_VERSION)
                output.writeLong(entry.sequence)
                output.writeInt(entry.slot)
                writeString(output, entry.codec.id, MAX_IDENTIFIER_BYTES)
                output.writeInt(entry.codec.version)
                writeNullableString(output, entry.requestId, MAX_METADATA_BYTES)
                writeNullableString(output, entry.pipelineId, MAX_METADATA_BYTES)
                writeString(output, entry.target, MAX_TARGET_BYTES)
                output.writeLong(entry.receivedAt.epochSecond)
                output.writeInt(entry.receivedAt.nano)
                output.writeInt(payload.size)
                output.write(payload)
            }
        }
    }

    private fun readRecord(path: Path, includePayload: Boolean): ReadRecord = try {
        Files.newInputStream(path, StandardOpenOption.READ).use { stream ->
            DataInputStream(stream).use { input ->
                require(input.readInt() == MAGIC) { "Unsupported file response record" }
                require(input.readInt() == FORMAT_VERSION) { "Unsupported file response record version" }
                val sequence = input.readLong()
                require(sequence > 0) { "File response sequence must be positive" }
                val slot = input.readInt()
                require(slot in 0 until maxResponses) { "File response slot is outside the configured bound" }
                val descriptor = ResponseCodecDescriptor(
                    id = readString(input, MAX_IDENTIFIER_BYTES),
                    version = input.readInt(),
                )
                val requestId = readNullableString(input, MAX_METADATA_BYTES)
                val pipelineId = readNullableString(input, MAX_METADATA_BYTES)
                val target = readString(input, MAX_TARGET_BYTES)
                val receivedAt = Instant.ofEpochSecond(input.readLong(), input.readInt().toLong())
                val payloadSize = input.readInt()
                require(payloadSize >= 0 && payloadSize <= maxEncodedResponseBytes) {
                    "File response payload exceeds the configured bound"
                }
                val payload = if (includePayload) {
                    ByteArray(payloadSize).also(input::readFully)
                } else {
                    input.skipNBytes(payloadSize.toLong())
                    null
                }
                require(input.available() == 0) { "File response record contains trailing bytes" }
                ReadRecord(
                    FileEntry(sequence, slot, descriptor, requestId, pipelineId, target, receivedAt, payloadSize),
                    payload,
                )
            }
        }
    } catch (error: EOFException) {
        throw IllegalArgumentException("Truncated file response record", error)
    }

    private fun writeNullableString(output: DataOutputStream, value: String?, maxBytes: Int) {
        if (value == null) output.writeInt(-1) else writeString(output, value, maxBytes)
    }

    private fun readNullableString(input: DataInputStream, maxBytes: Int): String? {
        val size = input.readInt()
        if (size == -1) return null
        require(size in 0..maxBytes) { "File response metadata exceeds its bound" }
        return ByteArray(size).also(input::readFully).toString(Charsets.UTF_8)
    }

    private fun writeString(output: DataOutputStream, value: String, maxBytes: Int) {
        val bytes = value.toByteArray(Charsets.UTF_8)
        require(bytes.size <= maxBytes) { "File response metadata exceeds its bound" }
        output.writeInt(bytes.size)
        output.write(bytes)
    }

    private fun readString(input: DataInputStream, maxBytes: Int): String {
        val size = input.readInt()
        require(size in 0..maxBytes) { "File response metadata exceeds its bound" }
        return ByteArray(size).also(input::readFully).toString(Charsets.UTF_8)
    }

    private fun moveIntoPlace(temporary: Path, target: Path) {
        try {
            Files.move(
                temporary,
                target,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING,
            )
        } catch (_: AtomicMoveNotSupportedException) {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING)
        }
    }

    private fun sequenceForSlot(slot: Int): Long? = entries.values
        .firstOrNull { it.slot == slot }
        ?.sequence

    private fun slotFor(sequence: Long): Int = ((sequence - 1L) % maxResponses.toLong()).toInt()

    private fun slotPath(slot: Int): Path = root.resolve("response-${slot.toString().padStart(8, '0')}.bin")

    private fun temporaryPath(slot: Int): Path =
        root.resolve("response-${slot.toString().padStart(8, '0')}.tmp")

    private fun referenceFor(sequence: Long): String = "file-response-$sequence"

    private data class FileEntry(
        val sequence: Long,
        val slot: Int,
        val codec: ResponseCodecDescriptor,
        val requestId: String?,
        val pipelineId: String?,
        val target: String,
        val receivedAt: Instant,
        val payloadSize: Int,
    ) {
        val reference: ResponseReference
            get() = ResponseReference("file-response-$sequence")
    }

    private data class ReadRecord(
        val entry: FileEntry,
        val payload: ByteArray?,
    )

    private class FileThreadFactory(
        private val storeId: String,
        private val sequence: AtomicLong,
    ) : ThreadFactory {
        override fun newThread(runnable: Runnable): Thread = Thread(
            runnable,
            "$storeId-file-${sequence.incrementAndGet()}",
        ).apply { isDaemon = true }
    }

    private companion object {
        const val MAGIC: Int = 0x5452_4652
        const val FORMAT_VERSION: Int = 1
        const val MAX_IDENTIFIER_BYTES: Int = 255
        const val MAX_METADATA_BYTES: Int = 4 * 1_024
        const val MAX_TARGET_BYTES: Int = 64 * 1_024
    }
}
