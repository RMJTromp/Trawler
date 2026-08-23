package com.rmjtromp.trawler.output

import com.rmjtromp.trawler.pipeline.Output
import com.rmjtromp.trawler.pipeline.OutputResult
import com.rmjtromp.trawler.pipeline.Produced
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.retention.RetentionCategory
import com.rmjtromp.trawler.retention.RetentionSummary
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
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
 * Bounded JDK file output for typed [Produced] values.
 *
 * The caller codec supplies every user-data representation. The output uses a
 * fixed ring of framed segment files for deterministic rotation; a segment is
 * replaced only after its replacement has been written and renamed. Each
 * write waits for the OS file channel to be forced before returning
 * [OutputResult.Accepted], so the output's guarantee is durable acceptance to
 * the configured filesystem, not external exactly-once delivery. A process
 * failure after acceptance can still leave a caller-specific duplicate window
 * at the surrounding crawl boundary. A failed frame append is truncated back
 * to its previous segment boundary before the failure is returned; an
 * unrecoverable truncation failure remains visible as part of that failure.
 */
public class FileOutput<T : Any>(
    override val id: String = "file-output",
    root: Path,
    public val codec: OutputCodec<T>,
    public val maxSegments: Int = 64,
    public val maxSegmentBytes: Long = 16L * 1_024 * 1_024,
    public val maxItemsPerSegment: Int = 1_024,
    public val maxEncodedItemBytes: Int = 1_024 * 1_024,
    public val dispatcherParallelism: Int = 1,
) : Output<T>, OutputLifecycle, OutputRetentionStore {

    /** Normalized host-selected directory; no produced value becomes a path. */
    public val root: Path = root.toAbsolutePath().normalize()

    private val mutex = Mutex()
    private val closed = AtomicBoolean(false)
    private val threadSequence = AtomicLong(0)
    private val dispatcher: ExecutorCoroutineDispatcher
    private val segments = LinkedHashMap<Long, Segment>()
    private var loaded = false
    private var nextSequence = 0L
    private var current: Segment? = null

    override val retentionCategories: Set<RetentionCategory> = setOf(RetentionCategory.OUTPUT_HISTORY)

    init {
        require(id.isNotBlank()) { "File output ID must not be blank" }
        require(maxSegments > 0) { "File output segment bound must be positive" }
        require(maxSegmentBytes >= SEGMENT_HEADER_BYTES + FRAME_BYTES) {
            "File output segment size must fit its bounded segment header"
        }
        require(maxItemsPerSegment > 0) { "File output item-per-segment bound must be positive" }
        require(maxEncodedItemBytes > 0) { "File output item-size bound must be positive" }
        require(maxEncodedItemBytes.toLong() + FRAME_BYTES <= maxSegmentBytes - SEGMENT_HEADER_BYTES) {
            "File output item bound must fit within one configured segment"
        }
        require(dispatcherParallelism > 0) { "File output dispatcher parallelism must be positive" }
        dispatcher = Executors.newFixedThreadPool(
            dispatcherParallelism,
            FileThreadFactory(id, threadSequence),
        ).asCoroutineDispatcher()
    }

    override suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult = onFileDispatcher {
        if (items.isEmpty()) return@onFileDispatcher OutputResult.Accepted(0)
        try {
            items.asSequence().chunked(maxItemsPerSegment).forEach { batch ->
                batch.map { item ->
                    codec.encode(item).also { bytes ->
                        require(bytes.size <= maxEncodedItemBytes) {
                            "Encoded file output item exceeds the configured bound"
                        }
                    }
                }.forEach(::append)
            }
            OutputResult.Accepted(items.size)
        } catch (cancelled: kotlinx.coroutines.CancellationException) {
            throw cancelled
        } catch (error: Throwable) {
            OutputResult.Failed(error)
        }
    }

    override suspend fun prune(request: OutputRetentionRequest): RetentionSummary = onFileDispatcher {
        require(request.category == RetentionCategory.OUTPUT_HISTORY) {
            "File output owns only output history retention"
        }
        val candidates = segments.values
            .filter { Files.getLastModifiedTime(it.path).toInstant().isBefore(request.cutoff) }
            .sortedBy(Segment::sequence)
            .take(request.maxEntries)
        candidates.forEach { segment ->
            Files.deleteIfExists(segment.path)
            segments.remove(segment.sequence)
            if (current?.sequence == segment.sequence) current = null
        }
        RetentionSummary(RetentionCategory.OUTPUT_HISTORY, candidates.size.toLong())
    }

    /** Closes the bounded file dispatcher and rejects later writes. */
    override suspend fun close() {
        if (!closed.compareAndSet(false, true)) return
        withContext(NonCancellable) {
            withContext(dispatcher) { mutex.withLock { Unit } }
            dispatcher.close()
        }
    }

    private suspend fun <R> onFileDispatcher(action: suspend () -> R): R =
        withContext(dispatcher) {
            mutex.withLock {
                check(!closed.get()) { "File output '$id' is closed" }
                loadIfNeeded()
                action()
            }
        }

    private fun append(payload: ByteArray) {
        var segment = current
        if (segment == null ||
            segment.codec != codec.descriptor ||
            segment.items >= maxItemsPerSegment ||
            segment.bytes + FRAME_BYTES + payload.size > maxSegmentBytes
        ) {
            segment = createSegment()
            current = segment
        }
        val previousBytes = segment.bytes
        val previousItems = segment.items
        val frame = ByteBuffer.allocate(FRAME_BYTES.toInt() + payload.size)
        frame.putInt(payload.size)
        frame.put(payload)
        frame.flip()
        try {
            FileChannel.open(
                segment.path,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND,
            ).use { channel ->
                while (frame.hasRemaining()) channel.write(frame)
                channel.force(true)
            }
            segment.bytes += FRAME_BYTES + payload.size
            segment.items++
        } catch (error: Throwable) {
            try {
                FileChannel.open(segment.path, StandardOpenOption.WRITE).use { channel ->
                    channel.truncate(previousBytes)
                    channel.force(true)
                }
            } catch (repairFailure: Throwable) {
                error.addSuppressed(repairFailure)
            }
            segment.bytes = previousBytes
            segment.items = previousItems
            throw error
        }
    }

    private fun createSegment(): Segment {
        val sequence = nextSequence
            .takeIf { it < Long.MAX_VALUE }
            ?.plus(1L)
            ?: error("File output sequence exhausted")
        val slot = ((sequence - 1L) % maxSegments.toLong()).toInt()
        val path = segmentPath(slot)
        val temporary = temporaryPath(slot)
        try {
            Files.newOutputStream(
                temporary,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE,
            ).use { stream ->
                DataOutputStream(stream).use { output ->
                    output.writeInt(MAGIC)
                    output.writeInt(FORMAT_VERSION)
                    output.writeLong(sequence)
                    writeString(output, codec.descriptor.id, MAX_IDENTIFIER_BYTES)
                    output.writeInt(codec.descriptor.version)
                }
            }
            moveIntoPlace(temporary, path)
        } finally {
            Files.deleteIfExists(temporary)
        }
        segments.remove(sequenceForSlot(slot))
        val segment = Segment(
            sequence = sequence,
            slot = slot,
            path = path,
            codec = codec.descriptor,
            bytes = Files.size(path),
            items = 0,
        )
        segments[sequence] = segment
        nextSequence = sequence
        return segment
    }

    private fun loadIfNeeded() {
        if (loaded) return
        Files.createDirectories(root)
        for (slot in 0 until maxSegments) {
            Files.deleteIfExists(temporaryPath(slot))
            val path = segmentPath(slot)
            if (!Files.exists(path)) continue
            val segment = readSegment(path, slot)
            segments[segment.sequence] = segment
            nextSequence = maxOf(nextSequence, segment.sequence)
        }
        current = segments.values.maxByOrNull(Segment::sequence)
            ?.takeIf { it.codec == codec.descriptor && it.items < maxItemsPerSegment }
        loaded = true
    }

    private fun readSegment(path: Path, slot: Int): Segment = try {
        require(Files.size(path) <= maxSegmentBytes) {
            "File output segment exceeds the configured segment bound"
        }
        Files.newInputStream(path, StandardOpenOption.READ).use { stream ->
            DataInputStream(stream).use { input ->
                require(input.readInt() == MAGIC) { "Unsupported file output segment" }
                require(input.readInt() == FORMAT_VERSION) { "Unsupported file output segment version" }
                val sequence = input.readLong()
                require(sequence > 0) { "File output segment sequence must be positive" }
                val descriptor = OutputCodecDescriptor(
                    id = readString(input, MAX_IDENTIFIER_BYTES),
                    version = input.readInt(),
                )
                var items = 0
                while (input.available() > 0) {
                    val size = input.readInt()
                    require(size in 0..maxEncodedItemBytes) {
                        "File output frame exceeds the configured item bound"
                    }
                    input.skipNBytes(size.toLong())
                    items++
                    require(items <= maxItemsPerSegment) {
                        "File output segment exceeds the configured item bound"
                    }
                }
                Segment(sequence, slot, path, descriptor, Files.size(path), items)
            }
        }
    } catch (error: EOFException) {
        throw IllegalArgumentException("Truncated file output segment", error)
    }

    private fun writeString(output: DataOutputStream, value: String, maxBytes: Int) {
        val bytes = value.toByteArray(Charsets.UTF_8)
        require(bytes.size <= maxBytes) { "File output codec ID exceeds its bound" }
        output.writeInt(bytes.size)
        output.write(bytes)
    }

    private fun readString(input: DataInputStream, maxBytes: Int): String {
        val size = input.readInt()
        require(size in 0..maxBytes) { "File output codec ID exceeds its bound" }
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

    private fun sequenceForSlot(slot: Int): Long? = segments.values.firstOrNull { it.slot == slot }?.sequence

    private fun segmentPath(slot: Int): Path = root.resolve("segment-${slot.toString().padStart(8, '0')}.bin")

    private fun temporaryPath(slot: Int): Path = root.resolve("segment-${slot.toString().padStart(8, '0')}.tmp")

    private data class Segment(
        val sequence: Long,
        val slot: Int,
        val path: Path,
        val codec: OutputCodecDescriptor,
        var bytes: Long,
        var items: Int,
    )

    private class FileThreadFactory(
        private val outputId: String,
        private val sequence: AtomicLong,
    ) : ThreadFactory {
        override fun newThread(runnable: Runnable): Thread = Thread(
            runnable,
            "$outputId-file-${sequence.incrementAndGet()}",
        ).apply { isDaemon = true }
    }

    private companion object {
        const val MAGIC: Int = 0x5452_464F
        const val FORMAT_VERSION: Int = 1
        const val FRAME_BYTES: Long = 4
        const val MAX_IDENTIFIER_BYTES: Int = 255
        const val SEGMENT_HEADER_BYTES: Long = 4L + 4L + 8L + 4L + MAX_IDENTIFIER_BYTES + 4L
    }
}
