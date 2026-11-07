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

    @Test
    fun `codec cancellation propagates and leaves the output usable`() = runTest {
        val cancellation = CancellationException("encode cancelled")
        val cancelling = FileOutput(root = root.resolve("cancelling"), codec = CancellingCodec(cancellation))
        try {
            val thrown = assertFailsWith<CancellationException> {
                cancelling.write(listOf(produced(1)), context)
            }
            assertEquals(cancellation.message, thrown.message)
            assertTrue(segmentFiles(root.resolve("cancelling")).isEmpty())
            assertEquals(OutputResult.Accepted(0), cancelling.write(emptyList(), context))
        } finally {
            cancelling.close()
        }
    }

    @Test
    fun `pruning the current segment forces a new segment for later writes`() = runTest {
        val output = FileOutput(
            root = root,
            codec = IntCodec("prune-current"),
            maxSegments = 4,
            maxSegmentBytes = 400,
            maxItemsPerSegment = 2,
            maxEncodedItemBytes = 4,
        )
        try {
            assertEquals(OutputResult.Accepted(3), output.write((1..3).map(::produced), context))
            val files = segmentFiles(root)
            assertEquals(2, files.size)
            files.forEach { Files.setLastModifiedTime(it, FileTime.from(Instant.EPOCH)) }
            val request = OutputRetentionRequest(RetentionCategory.OUTPUT_HISTORY, Instant.EPOCH.plusSeconds(1), 1)

            assertEquals(1L, output.prune(request).deleted)
            assertEquals(listOf(listOf(3)), segmentFiles(root).map { readSegment(it).payloads.map(::decodeInt) })
            assertEquals(1L, output.prune(request).deleted)
            assertTrue(segmentFiles(root).isEmpty())
            assertEquals(0L, output.prune(request).deleted)

            assertEquals(OutputResult.Accepted(1), output.write(listOf(produced(4)), context))
            val rewritten = segmentFiles(root).map(::readSegment)
            assertEquals(listOf(3L), rewritten.map { it.sequence })
            assertEquals(listOf(listOf(4)), rewritten.map { it.payloads.map(::decodeInt) })
        } finally {
            output.close()
        }

        segmentFiles(root).forEach { Files.setLastModifiedTime(it, FileTime.from(Instant.EPOCH)) }
        val otherCodec = FileOutput(root = root, codec = IntCodec("other-codec"), maxSegments = 4, maxSegmentBytes = 400, maxEncodedItemBytes = 4)
        try {
            val request = OutputRetentionRequest(RetentionCategory.OUTPUT_HISTORY, Instant.EPOCH.plusSeconds(1), 5)
            assertEquals(1L, otherCodec.prune(request).deleted)
            assertTrue(segmentFiles(root).isEmpty())
        } finally {
            otherCodec.close()
        }
    }

    @Test
    fun `an exhausted persisted sequence fails the write without creating a segment`() = runTest {
        val writer = FileOutput(root = root, codec = IntCodec("old-codec"), maxSegments = 2)
        writer.write(listOf(produced(1)), context)
        writer.close()
        val path = segmentFiles(root).single()
        val patched = Files.readAllBytes(path).also { ByteBuffer.wrap(it).putLong(8, Long.MAX_VALUE) }
        Files.write(path, patched, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)

        val reopened = FileOutput(root = root, codec = IntCodec("new-codec"), maxSegments = 2)
        try {
            val failed = assertIs<OutputResult.Failed>(reopened.write(listOf(produced(2)), context))
            assertEquals("File output sequence exhausted", failed.error.message)
            assertEquals(listOf(path), segmentFiles(root))
            assertEquals(Long.MAX_VALUE, readSegment(path).sequence)
        } finally {
            reopened.close()
        }
    }

    @Test
    fun `reopened segments exceeding item or identifier bounds are rejected`() = runTest {
        val codec = IntCodec("bounded")
        val writer = FileOutput(
            root = root,
            codec = codec,
            maxSegments = 2,
            maxSegmentBytes = 400,
            maxItemsPerSegment = 2,
            maxEncodedItemBytes = 4,
        )
        writer.write(listOf(produced(1), produced(2)), context)
        writer.close()
        val path = segmentFiles(root).single()
        val valid = Files.readAllBytes(path)
        val frameOffset = 4 + 4 + 8 + 4 + "bounded".length + 4

        val tooManyItems = FileOutput(
            root = root,
            codec = codec,
            maxSegments = 2,
            maxSegmentBytes = 400,
            maxItemsPerSegment = 1,
            maxEncodedItemBytes = 4,
        )
        try {
            val error = assertFailsWith<IllegalArgumentException> { tooManyItems.write(emptyList(), context) }
            assertEquals("File output segment exceeds the configured item bound", error.message)
        } finally {
            tooManyItems.close()
        }

        val negativeFrame = valid.copyOf().also { ByteBuffer.wrap(it).putInt(frameOffset, -1) }
        assertEquals(
            "File output frame exceeds the configured item bound",
            reopenMalformed(path, negativeFrame, codec, maxSegmentBytes = 400) { write(emptyList(), context) }.message,
        )
        val oversizedIdentifier = valid.copyOf().also { ByteBuffer.wrap(it).putInt(16, 256) }
        assertEquals(
            "File output codec ID exceeds its bound",
            reopenMalformed(path, oversizedIdentifier, codec, maxSegmentBytes = 400) { write(emptyList(), context) }.message,
        )
        val negativeIdentifier = valid.copyOf().also { ByteBuffer.wrap(it).putInt(16, -1) }
        assertEquals(
            "File output codec ID exceeds its bound",
            reopenMalformed(path, negativeIdentifier, codec, maxSegmentBytes = 400) { write(emptyList(), context) }.message,
        )
    }

    @Test
    fun `codec identifiers beyond the header bound fail before any segment is created`() = runTest {
        val output = FileOutput(root = root, codec = IntCodec("x".repeat(256)), maxSegmentBytes = 2_048, maxEncodedItemBytes = 4)
        try {
            val failed = assertIs<OutputResult.Failed>(output.write(listOf(produced(1)), context))
            assertIs<IllegalArgumentException>(failed.error)
            assertEquals("File output codec ID exceeds its bound", failed.error.message)
            assertTrue(Files.list(root).use { paths -> paths.noneMatch { it.fileName.toString().endsWith(".bin") } })
            assertTrue(Files.list(root).use { paths -> paths.noneMatch { it.fileName.toString().endsWith(".tmp") } })
        } finally {
            output.close()
        }
    }

    @Test
    fun `operations after close fail as closed-resource errors rather than cancellation`() = runTest {
        val output = FileOutput(root = root, codec = IntCodec("closed"))
        assertEquals(OutputResult.Accepted(1), output.write(listOf(produced(1)), context))
        output.close()

        val writeError = assertFailsWith<IllegalStateException> { output.write(listOf(produced(2)), context) }
        assertFalse(writeError is CancellationException, "closed write surfaced as $writeError")
        assertEquals("File output 'file-output' is closed", writeError.message)
        val pruneError = assertFailsWith<IllegalStateException> {
            output.prune(OutputRetentionRequest(RetentionCategory.OUTPUT_HISTORY, Instant.EPOCH, 1))
        }
        assertFalse(pruneError is CancellationException, "closed prune surfaced as $pruneError")
        assertEquals(1, segmentFiles(root).size)
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

    private class CancellingCodec(private val cancellation: CancellationException) : OutputCodec<Int> {
        override val descriptor: OutputCodecDescriptor = OutputCodecDescriptor("cancelling", 1)

        override fun encode(item: Produced<Int>): ByteArray = throw cancellation

        override fun decode(payload: ByteArray): Produced<Int> = error("decode must not be called")
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
