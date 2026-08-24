package com.rmjtromp.trawler.output

import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.pipeline.OutputResult
import com.rmjtromp.trawler.pipeline.Produced
import com.rmjtromp.trawler.pipeline.ProductionContext
import com.rmjtromp.trawler.retention.RetentionCategory
import java.io.DataInputStream
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.FileTime
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class FileOutputIT {
    @TempDir
    lateinit var root: Path

    @Test
    fun `file output rotates by item bytes and codec descriptor`() = runTest {
        val countRoot = root.resolve("count")
        val countOutput = FileOutput(
            root = countRoot,
            codec = IntCodec("int-count"),
            maxSegments = 8,
            maxSegmentBytes = 400,
            maxItemsPerSegment = 2,
            maxEncodedItemBytes = 4,
        )
        assertEquals(OutputResult.Accepted(5), countOutput.write((1..5).map(::produced), context))
        countOutput.close()
        assertEquals(3, segmentFiles(countRoot).size)
        assertEquals(
            listOf(listOf(1, 2), listOf(3, 4), listOf(5)),
            segmentFiles(countRoot).map { readSegment(it).payloads.map(::decodeInt) },
        )

        val byteRoot = root.resolve("bytes")
        val byteCodec = IntCodec("x".repeat(255))
        val byteOutput = FileOutput(
            root = byteRoot,
            codec = byteCodec,
            maxSegments = 8,
            maxSegmentBytes = 287,
            maxItemsPerSegment = 20,
            maxEncodedItemBytes = 4,
        )
        assertEquals(OutputResult.Accepted(2), byteOutput.write(listOf(produced(1), produced(2)), context))
        byteOutput.close()
        assertEquals(2, segmentFiles(byteRoot).size)

        val codecRoot = root.resolve("codecs")
        val firstCodec = IntCodec("first")
        val firstOutput = FileOutput(
            root = codecRoot,
            codec = firstCodec,
            maxSegments = 4,
            maxSegmentBytes = 400,
            maxEncodedItemBytes = 4,
        )
        firstOutput.write(listOf(produced(1)), context)
        firstOutput.close()
        val secondCodec = IntCodec("second")
        val secondOutput = FileOutput(
            root = codecRoot,
            codec = secondCodec,
            maxSegments = 4,
            maxSegmentBytes = 400,
            maxEncodedItemBytes = 4,
        )
        secondOutput.write(listOf(produced(2)), context)
        secondOutput.close()
        assertEquals(listOf("first", "second"), segmentFiles(codecRoot).map { readSegment(it).descriptor.id })
    }

    @Test
    fun `file output ring replacement reopens and appends complete frames`() = runTest {
        val ringOutput = FileOutput(
            root = root.resolve("ring"),
            codec = IntCodec("ring"),
            maxSegments = 2,
            maxSegmentBytes = 400,
            maxItemsPerSegment = 1,
            maxEncodedItemBytes = 4,
        )
        ringOutput.write((1..3).map(::produced), context)
        ringOutput.close()
        val ringFiles = segmentFiles(root.resolve("ring"))
        assertEquals(listOf(2L, 3L), ringFiles.map { readSegment(it).sequence }.sorted())

        val reopenedRing = FileOutput(
            root = root.resolve("ring"),
            codec = IntCodec("ring"),
            maxSegments = 2,
            maxSegmentBytes = 400,
            maxItemsPerSegment = 1,
            maxEncodedItemBytes = 4,
        )
        reopenedRing.write(listOf(produced(4)), context)
        reopenedRing.close()
        assertEquals(
            listOf(3L, 4L),
            segmentFiles(root.resolve("ring")).map { readSegment(it).sequence }.sorted(),
        )

        val appendRoot = root.resolve("append")
        val first = FileOutput(
            root = appendRoot,
            codec = IntCodec("append"),
            maxSegments = 2,
            maxSegmentBytes = 400,
            maxItemsPerSegment = 3,
            maxEncodedItemBytes = 4,
        )
        first.write(listOf(produced(1)), context)
        first.close()
        val second = FileOutput(
            root = appendRoot,
            codec = IntCodec("append"),
            maxSegments = 2,
            maxSegmentBytes = 400,
            maxItemsPerSegment = 3,
            maxEncodedItemBytes = 4,
        )
        second.write(listOf(produced(2), produced(3)), context)
        second.close()
        assertEquals(listOf(listOf(1, 2, 3)), segmentFiles(appendRoot).map { readSegment(it).payloads.map(::decodeInt) })
    }

    @Test
    fun `file output forces accepted frames prunes by age and closes idempotently`() = runTest {
        val output = FileOutput(
            root = root,
            codec = IntCodec("retention"),
            maxSegments = 4,
            maxSegmentBytes = 400,
            maxItemsPerSegment = 1,
            maxEncodedItemBytes = 4,
        )
        assertEquals(OutputResult.Accepted(0), output.write(emptyList(), context))
        assertEquals(OutputResult.Accepted(2), output.write(listOf(produced(1), produced(2)), context))
        val files = segmentFiles(root)
        assertTrue(files.all { Files.size(it) > 0 })
        Files.setLastModifiedTime(files[0], FileTime.from(Instant.EPOCH))
        Files.setLastModifiedTime(files[1], FileTime.from(Instant.EPOCH.plusSeconds(1)))

        assertEquals(setOf(RetentionCategory.OUTPUT_HISTORY), output.retentionCategories)
        assertEquals(
            1L,
            output.prune(
                OutputRetentionRequest(
                    category = RetentionCategory.OUTPUT_HISTORY,
                    cutoff = Instant.EPOCH.plusSeconds(1),
                    maxEntries = 1,
                ),
            ).deleted,
        )
        assertEquals(1, segmentFiles(root).size)
        assertEquals(2, readSegment(segmentFiles(root).single()).payloads.single().let(::decodeInt))
        assertFailsWith<IllegalArgumentException> {
            output.prune(
                OutputRetentionRequest(
                    category = RetentionCategory.OUTBOX_HISTORY,
                    cutoff = Instant.EPOCH.plusSeconds(2),
                    maxEntries = 1,
                ),
            )
        }

        output.close()
        output.close()
        assertFailsWith<IllegalStateException> {
            output.write(listOf(produced(3)), context)
        }
    }

    @Test
    fun `file output cancellation and codec or encoded failures preserve causes and leave no partial segment`() = runTest {
        val output = FileOutput(root = root.resolve("cancel"), codec = IntCodec("cancel"))
        val cancelled = async(start = CoroutineStart.LAZY) {
            output.write(listOf(produced(1)), context)
        }
        cancelled.cancel()
        assertFailsWith<CancellationException> { cancelled.await() }
        assertEquals(OutputResult.Accepted(1), output.write(listOf(produced(1)), context))
        output.close()

        val cause = IllegalStateException("codec failed")
        val failingRoot = root.resolve("failure")
        val failing = FileOutput(root = failingRoot, codec = ThrowingCodec(cause))
        val failed = assertIs<OutputResult.Failed>(failing.write(listOf(produced(1)), context))
        assertSame(cause, failed.error)
        assertFalse(Files.exists(failingRoot.resolve("segment-00000000.bin")))
        assertTrue(Files.list(failingRoot).use { paths -> paths.noneMatch { it.fileName.toString().endsWith(".tmp") } })
        failing.close()

        val oversizedRoot = root.resolve("oversized")
        val oversized = FileOutput(
            root = oversizedRoot,
            codec = IntCodec("oversized"),
            maxEncodedItemBytes = 3,
            maxSegmentBytes = 300,
        )
        val oversizedResult = assertIs<OutputResult.Failed>(oversized.write(listOf(produced(1)), context))
        assertTrue(oversizedResult.error.message!!.contains("exceeds the configured bound"))
        assertTrue(Files.list(oversizedRoot).use { paths -> paths.noneMatch { it.fileName.toString().endsWith(".bin") } })
        oversized.close()
    }

    @Test
    fun `file output rejects corrupt truncated oversized and invalid frames on reopen`() = runTest {
        val codec = IntCodec("x".repeat(255))
        val writer = FileOutput(
            root = root,
            codec = codec,
            maxSegments = 2,
            maxSegmentBytes = 287,
            maxItemsPerSegment = 2,
            maxEncodedItemBytes = 4,
        )
        writer.write(listOf(produced(1)), context)
        writer.close()
        val path = segmentFiles(root).single()
        val valid = Files.readAllBytes(path)

        val corrupt = valid.copyOf().also { ByteBuffer.wrap(it).putInt(0, 0) }
        val corruptError = reopenMalformed(path, corrupt, codec, maxSegmentBytes = 287) { write(emptyList(), context) }
        assertTrue(corruptError.message!!.contains("Unsupported file output segment"))

        val truncated = valid.copyOf(valid.size - 1)
        val truncatedError = reopenMalformed(path, truncated, codec, maxSegmentBytes = 287) { write(emptyList(), context) }
        assertTrue(truncatedError.message!!.contains("Truncated file output segment"))

        val frameOffset = 4 + 4 + 8 + 4 + 255 + 4
        val oversizedFrame = valid.copyOf().also { ByteBuffer.wrap(it).putInt(frameOffset, 5) }
        val oversizedError = reopenMalformed(path, oversizedFrame, codec, maxSegmentBytes = 287) { write(emptyList(), context) }
        assertTrue(oversizedError.message!!.contains("frame exceeds the configured item bound"))

        val invalidVersion = valid.copyOf().also { ByteBuffer.wrap(it).putInt(4, 2) }
        val versionError = reopenMalformed(path, invalidVersion, codec, maxSegmentBytes = 287) { write(emptyList(), context) }
        assertTrue(versionError.message!!.contains("Unsupported file output segment version"))

        val invalidSequence = valid.copyOf().also { ByteBuffer.wrap(it).putLong(8, 0L) }
        val sequenceError = reopenMalformed(path, invalidSequence, codec, maxSegmentBytes = 287) { write(emptyList(), context) }
        assertTrue(sequenceError.message!!.contains("sequence must be positive"))

        val tooLargeFile = valid + byteArrayOf(7)
        val fileSizeError = reopenMalformed(path, tooLargeFile, codec, maxSegmentBytes = 287) { write(emptyList(), context) }
        assertTrue(fileSizeError.message!!.contains("exceeds the configured segment bound"))
    }

    @Test
    fun `file output rolls back a failed append and remains usable after repair`() = runTest {
        val output = FileOutput(
            root = root,
            codec = IntCodec("rollback"),
            maxSegments = 2,
            maxSegmentBytes = 400,
            maxItemsPerSegment = 3,
            maxEncodedItemBytes = 4,
        )
        output.write(listOf(produced(1)), context)
        val path = root.resolve("segment-00000000.bin")
        val original = Files.readAllBytes(path)

        Files.delete(path)
        Files.createDirectory(path)
        val failed = assertIs<OutputResult.Failed>(output.write(listOf(produced(2)), context))
        assertTrue(failed.error.suppressed.isNotEmpty())

        Files.delete(path)
        Files.write(path, original, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
        assertEquals(OutputResult.Accepted(1), output.write(listOf(produced(3)), context))
        output.close()

        assertEquals(listOf(1, 3), readSegment(path).payloads.map(::decodeInt))
    }

    @Test
    fun `file output validates every configured bound`() {
        val codec = IntCodec("bounds")
        assertFailsWith<IllegalArgumentException> { FileOutput(root = root, codec = codec, id = " ") }
        assertFailsWith<IllegalArgumentException> { FileOutput(root = root, codec = codec, maxSegments = 0) }
        assertFailsWith<IllegalArgumentException> { FileOutput(root = root, codec = codec, maxSegmentBytes = 282) }
        assertFailsWith<IllegalArgumentException> { FileOutput(root = root, codec = codec, maxItemsPerSegment = 0) }
        assertFailsWith<IllegalArgumentException> { FileOutput(root = root, codec = codec, maxEncodedItemBytes = 0) }
        assertFailsWith<IllegalArgumentException> {
            FileOutput(root = root, codec = codec, maxSegmentBytes = 287, maxEncodedItemBytes = 5)
        }
        assertFailsWith<IllegalArgumentException> { FileOutput(root = root, codec = codec, dispatcherParallelism = 0) }
        assertFailsWith<IllegalArgumentException> { OutputRetentionRequest(RetentionCategory.RESPONSES, Instant.EPOCH, 1) }
    }

    private suspend fun <T> reopenMalformed(
        path: Path,
        bytes: ByteArray,
        codec: OutputCodec<Int>,
        maxSegmentBytes: Long,
        action: suspend FileOutput<Int>.() -> T,
    ): IllegalArgumentException {
        Files.write(path, bytes, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
        val output = FileOutput(
            root = path.parent,
            codec = codec,
            maxSegments = 2,
            maxSegmentBytes = maxSegmentBytes,
            maxItemsPerSegment = 2,
            maxEncodedItemBytes = 4,
        )
        return try {
            action(output)
            error("Expected malformed file output segment to be rejected")
        } catch (error: IllegalArgumentException) {
            error
        } finally {
            output.close()
        }
    }

    private fun segmentFiles(directory: Path): List<Path> = if (!Files.exists(directory)) {
        emptyList()
    } else {
        Files.list(directory).use { paths ->
            paths.filter { it.fileName.toString().endsWith(".bin") }.sorted().toList()
        }
    }

    private fun readSegment(path: Path): SegmentSnapshot =
        Files.newInputStream(path).use { stream ->
            DataInputStream(stream).use { input ->
                input.readInt()
                input.readInt()
                val sequence = input.readLong()
                val identifierSize = input.readInt()
                val identifier = ByteArray(identifierSize).also(input::readFully).toString(Charsets.UTF_8)
                val descriptor = OutputCodecDescriptor(identifier, input.readInt())
                val payloads = buildList {
                    while (input.available() > 0) {
                        val size = input.readInt()
                        add(ByteArray(size).also(input::readFully))
                    }
                }
                SegmentSnapshot(sequence, descriptor, payloads)
            }
        }

    private fun decodeInt(payload: ByteArray): Int = ByteBuffer.wrap(payload).int

    private fun produced(value: Int): Produced<Int> = Produced(
        value = value,
        context = ProductionContext(
            pipelineId = "pipeline",
            engineRunId = "engine",
            pipelineRunId = "pipeline-run",
            requestId = "request-$value",
            responseReference = null,
            originalTarget = "target-$value",
            finalTarget = null,
            producedAt = Instant.EPOCH,
        ),
    )

    private val context: OutputContext = OutputContext(CrawlerContext("engine", "pipeline-run", "pipeline"))

    private inner class IntCodec(id: String) : OutputCodec<Int> {
        override val descriptor: OutputCodecDescriptor = OutputCodecDescriptor(id, 1)

        override fun encode(item: Produced<Int>): ByteArray = ByteBuffer.allocate(4).putInt(item.value).array()

        override fun decode(payload: ByteArray): Produced<Int> = produced(ByteBuffer.wrap(payload).int)
    }

    private class ThrowingCodec(private val cause: IllegalStateException) : OutputCodec<Int> {
        override val descriptor: OutputCodecDescriptor = OutputCodecDescriptor("throwing", 1)

        override fun encode(item: Produced<Int>): ByteArray = throw cause

        override fun decode(payload: ByteArray): Produced<Int> = error("decode must not be called")
    }

    private data class SegmentSnapshot(
        val sequence: Long,
        val descriptor: OutputCodecDescriptor,
        val payloads: List<ByteArray>,
    )
}
