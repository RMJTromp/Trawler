package com.rmjtromp.trawler.response

import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.ResponseBody
import java.io.EOFException
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class FileResponseStoreIT {
    @TempDir
    lateinit var root: Path

    @Test
    fun `file response store enforces body and encoded bounds and recovers ring slots`() = runTest {
        val codec = BodyCodec()
        val registry = ResponseCodecRegistry(listOf(codec))
        val store = FileResponseStore(
            root = root,
            codecs = registry,
            maxResponses = 2,
            maxBodyBytes = 10,
            maxEncodedResponseBytes = 2,
        )
        val first = response("first", byteArrayOf(1, 2))
        val second = response("second", byteArrayOf(3))
        val tooLarge = response("too-large", byteArrayOf(1, 2, 3))
        val firstReference = store.put(first)!!
        assertNull(store.put(tooLarge))
        val secondReference = store.put(second)!!
        val thirdReference = store.put(response("third", byteArrayOf(4)))!!

        assertNull(store.find(ResponseLookup.ByReference(firstReference)))
        assertContentEquals(
            byteArrayOf(4),
            store.find(ResponseLookup.ByReference(thirdReference))!!.response.body.toByteArray(),
        )
        store.close()
        store.close()

        val reopened = FileResponseStore(
            root = root,
            codecs = registry,
            maxResponses = 2,
            maxBodyBytes = 10,
            maxEncodedResponseBytes = 2,
        )
        try {
            assertContentEquals(
                byteArrayOf(3),
                reopened.find(ResponseLookup.ByReference(secondReference))!!.response.body.toByteArray(),
            )
            assertTrue(Files.list(root).use { paths -> paths.count() <= 2 })
        } finally {
            reopened.close()
        }
    }

    @Test
    fun `lookup history and replay preserve newest and insertion ordering`() = runTest {
        val store = FileResponseStore(
            root = root,
            codecs = ResponseCodecRegistry(listOf(BodyCodec())),
            maxResponses = 8,
        )
        val first = store.put(response("shared-target", byteArrayOf(1), "shared", "pipeline-b", Instant.EPOCH))!!
        val second = store.put(response("shared-target", byteArrayOf(2), "shared", "pipeline-a", Instant.EPOCH.plusSeconds(1)))!!
        val third = store.put(response("pipeline-a-2", byteArrayOf(3), "shared", "pipeline-a", Instant.EPOCH.plusSeconds(2)))!!
        val fourth = store.put(response("shared-target", byteArrayOf(4), "latest", "pipeline-a", Instant.EPOCH.plusSeconds(3)))!!

        assertEquals(fourth, store.find(ResponseLookup.ByTarget("shared-target"))!!.reference)
        assertEquals(third, store.find(ResponseLookup.ByRequestId("shared"))!!.reference)
        assertEquals(
            listOf(first, second, third),
            store.history("shared", limit = 3).map { it.reference },
        )
        assertEquals(
            listOf(second, third),
            store.replay(
                query = ResponseReplayQuery.ByRequestId("shared"),
                limit = 2,
                pipelineId = "pipeline-a",
            ).map { it.reference },
        )
        assertEquals(listOf(fourth), store.replay(ResponseReplayQuery.ByReference(fourth), 1).map { it.reference })

        val retention = store.prune(
            ResponseRetentionRequest(
                cutoff = Instant.EPOCH.plusSeconds(2),
                maxEntries = 10,
                protectedReferences = setOf(first),
            ),
        )
        assertEquals(1L, retention.deleted)
        assertEquals(1L, retention.protectedEntries)
        assertNotNull(store.find(ResponseLookup.ByReference(first)))
        assertNull(store.find(ResponseLookup.ByReference(second)))
        assertNotNull(store.find(ResponseLookup.ByReference(third)))

        assertEquals(2L, store.evictBefore(Instant.EPOCH.plusSeconds(3)))
        assertNull(store.find(ResponseLookup.ByReference(first)))
        assertNull(store.find(ResponseLookup.ByReference(third)))
        assertNotNull(store.find(ResponseLookup.ByReference(fourth)))
        store.close()
    }

    @Test
    fun `body refusal occurs before encoding and encoded refusal leaves no temporary files`() = runTest {
        val codec = CountingCodec()
        val store = FileResponseStore(
            root = root,
            codecs = ResponseCodecRegistry(listOf(codec)),
            maxBodyBytes = 2,
            maxEncodedResponseBytes = 3,
        )
        assertNull(store.put(response("body", byteArrayOf(1, 2, 3))))
        assertEquals(0, codec.encodeCalls)
        assertNull(store.put(response("encoded", byteArrayOf(1, 2))))
        assertEquals(1, codec.encodeCalls)
        assertTrue(Files.list(root).use { paths -> paths.noneMatch { it.fileName.toString().endsWith(".tmp") } })
        store.close()
    }

    @Test
    fun `exact historical codec versions are selected and missing history is visible`() = runTest {
        val versionOne = MarkerCodec(ResponseCodecDescriptor("marker", 1), 11)
        val firstStore = FileResponseStore(
            root = root,
            codecs = ResponseCodecRegistry(listOf(versionOne)),
        )
        val firstReference = firstStore.put(response("old", byteArrayOf(1)))!!
        firstStore.close()

        val versionTwo = MarkerCodec(ResponseCodecDescriptor("marker", 2), 22)
        val secondStore = FileResponseStore(
            root = root,
            codecs = ResponseCodecRegistry(
                codecs = listOf(versionOne, versionTwo),
                active = versionTwo.descriptor,
            ),
        )
        val secondReference = secondStore.put(response("new", byteArrayOf(2)))!!
        assertContentEquals(
            byteArrayOf(1),
            secondStore.find(ResponseLookup.ByReference(firstReference))!!.response.body.toByteArray(),
        )
        assertContentEquals(
            byteArrayOf(2),
            secondStore.find(ResponseLookup.ByReference(secondReference))!!.response.body.toByteArray(),
        )
        secondStore.close()

        val missingHistory = FileResponseStore(
            root = root,
            codecs = ResponseCodecRegistry(listOf(versionTwo)),
        )
        try {
            val error = assertFailsWith<IllegalStateException> {
                missingHistory.find(ResponseLookup.ByReference(firstReference))
            }
            assertTrue(error.message!!.contains("marker@1"))
        } finally {
            missingHistory.close()
        }
    }

    @Test
    fun `concurrent puts remain bounded and cancellation does not poison the store`() = runTest {
        val store = FileResponseStore(
            root = root,
            codecs = ResponseCodecRegistry(listOf(BodyCodec())),
            maxResponses = 3,
        )
        val cancelled = async(start = CoroutineStart.LAZY) {
            store.put(response("cancelled", byteArrayOf(0)))
        }
        cancelled.cancel()
        assertFailsWith<CancellationException> { cancelled.await() }

        val references = (1..8).map { value ->
            async(start = CoroutineStart.UNDISPATCHED) {
                store.put(response("target-$value", byteArrayOf(value.toByte())))!!
            }
        }.awaitAll()
        assertEquals(8, references.size)
        assertEquals(3, store.replay(ResponseReplayQuery.ByPipelineId("pipeline"), limit = 10).size)
        store.close()
        store.close()
        assertFailsWith<IllegalStateException> {
            store.find(ResponseLookup.ByTarget("target-8"))
        }
    }

    @Test
    fun `malformed records are rejected as corrupt truncated trailing invalid or oversized`() = runTest {
        val codec = BodyCodec()
        val registry = ResponseCodecRegistry(listOf(codec))
        val writer = FileResponseStore(root = root, codecs = registry, maxEncodedResponseBytes = 10)
        val reference = writer.put(response("target", byteArrayOf(1, 2, 3)))!!
        writer.close()
        val path = root.resolve("response-00000000.bin")
        val valid = Files.readAllBytes(path)

        val corrupt = valid.copyOf().also { it[0] = 0 }
        val corruptError = reopenWith(path, corrupt, registry, maxEncodedResponseBytes = 10) {
            find(ResponseLookup.ByReference(reference))
        }
        assertTrue(corruptError.message!!.contains("Unsupported file response record"))

        val truncated = valid.copyOf(valid.size - 1)
        val truncatedError = reopenWith(path, truncated, registry, maxEncodedResponseBytes = 10) {
            find(ResponseLookup.ByReference(reference))
        }
        assertTrue(
            truncatedError.cause?.cause is EOFException,
            "message=${truncatedError.message}; cause=${truncatedError.cause?.cause}",
        )

        val trailing = valid + byteArrayOf(99)
        val trailingError = reopenWith(path, trailing, registry, maxEncodedResponseBytes = 10) {
            find(ResponseLookup.ByReference(reference))
        }
        assertTrue(trailingError.message!!.contains("trailing bytes"))

        val invalidSequence = valid.copyOf().also { ByteBuffer.wrap(it).putLong(8, 0L) }
        val invalidSequenceError = reopenWith(path, invalidSequence, registry, maxEncodedResponseBytes = 10) {
            find(ResponseLookup.ByReference(reference))
        }
        assertTrue(invalidSequenceError.message!!.contains("sequence must be positive"))

        val oversizedError = reopenWith(path, valid, registry, maxEncodedResponseBytes = 2) {
            find(ResponseLookup.ByReference(reference))
        }
        assertTrue(oversizedError.message!!.contains("payload exceeds the configured bound"))
    }

    @Test
    fun `abandoned temporary files are removed and codec failures leave no temporary record`() = runTest {
        val registry = ResponseCodecRegistry(listOf(BodyCodec()))
        val temporary = root.resolve("response-00000000.tmp")
        Files.createDirectories(root)
        Files.write(temporary, byteArrayOf(1, 2, 3))
        val store = FileResponseStore(root = root, codecs = registry)
        try {
            assertNull(store.find(ResponseLookup.ByTarget("missing")))
            assertFalse(Files.exists(temporary))
        } finally {
            store.close()
        }
        assertFailsWith<IllegalStateException> {
            store.find(ResponseLookup.ByTarget("missing"))
        }

        val cause = IllegalStateException("codec failure")
        val failing = FileResponseStore(
            root = root.resolve("codec-failure"),
            codecs = ResponseCodecRegistry(listOf(ThrowingCodec(cause))),
        )
        try {
            val thrown = assertFailsWith<IllegalStateException> {
                failing.put(response("failure", byteArrayOf(1)))
            }
            assertEquals(cause::class, thrown::class)
            assertEquals(cause.message, thrown.message)
            assertTrue(Files.list(failing.root).use { paths -> paths.noneMatch { it.fileName.toString().endsWith(".tmp") } })
        } finally {
            failing.close()
        }
    }

    @Test
    fun `response store validates all positive bounds`() {
        val registry = ResponseCodecRegistry(listOf(BodyCodec()))
        assertFailsWith<IllegalArgumentException> {
            FileResponseStore(root = root, codecs = registry, maxResponses = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            FileResponseStore(root = root, codecs = registry, maxBodyBytes = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            FileResponseStore(root = root, codecs = registry, maxEncodedResponseBytes = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            FileResponseStore(root = root, codecs = registry, dispatcherParallelism = 0)
        }
        assertFailsWith<IllegalArgumentException> { FileResponseStore(id = " ", root = root, codecs = registry) }
        assertFailsWith<IllegalArgumentException> { ResponseRetentionRequest(Instant.EPOCH, 0) }
    }

    private suspend fun <T> reopenWith(
        path: Path,
        bytes: ByteArray,
        registry: ResponseCodecRegistry,
        maxEncodedResponseBytes: Int,
        action: suspend FileResponseStore.() -> T,
    ): IllegalArgumentException {
        Files.write(path, bytes, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
        val store = FileResponseStore(
            root = path.parent,
            codecs = registry,
            maxEncodedResponseBytes = maxEncodedResponseBytes,
        )
        return try {
            action(store)
            error("Expected malformed file response record to be rejected")
        } catch (error: IllegalArgumentException) {
            error
        } finally {
            store.close()
        }
    }

    private fun response(
        target: String,
        body: ByteArray,
        requestId: String = "request-$target",
        pipelineId: String = "pipeline",
        receivedAt: Instant = Instant.EPOCH,
    ): CrawlResponse = CrawlResponse(
        request = CrawlRequest(
            target = target,
            attributes = ContextAttributes.of(
                ContextAttribute(InMemoryResponseStore.PIPELINE_ID_ATTRIBUTE, pipelineId),
                ContextAttribute(InMemoryResponseStore.REQUEST_ID_ATTRIBUTE, requestId),
            ),
        ),
        statusCode = 200,
        headers = Headers.empty(),
        body = ResponseBody.from(body),
        receivedAt = receivedAt,
    )

    private class BodyCodec : ResponseCodec {
        override val descriptor: ResponseCodecDescriptor = ResponseCodecDescriptor("body", 1)

        override fun encode(response: CrawlResponse): ByteArray = response.body.toByteArray()

        override fun decode(payload: ByteArray): CrawlResponse = CrawlResponse(
            request = CrawlRequest("decoded"),
            statusCode = 200,
            headers = Headers.empty(),
            body = ResponseBody.from(payload),
            receivedAt = Instant.EPOCH,
        )
    }

    private inner class CountingCodec : ResponseCodec {
        override val descriptor: ResponseCodecDescriptor = ResponseCodecDescriptor("counting", 1)
        var encodeCalls: Int = 0

        override fun encode(response: CrawlResponse): ByteArray {
            encodeCalls++
            return byteArrayOf(1, 2, 3, 4)
        }

        override fun decode(payload: ByteArray): CrawlResponse = response("decoded", payload)
    }

    private inner class MarkerCodec(
        override val descriptor: ResponseCodecDescriptor,
        private val marker: Byte,
    ) : ResponseCodec {
        override fun encode(response: CrawlResponse): ByteArray =
            byteArrayOf(marker) + response.body.toByteArray()

        override fun decode(payload: ByteArray): CrawlResponse {
            require(payload.firstOrNull() == marker) { "Unexpected marker" }
            return response("decoded", payload.drop(1).toByteArray())
        }
    }

    private class ThrowingCodec(private val cause: IllegalStateException) : ResponseCodec {
        override val descriptor: ResponseCodecDescriptor = ResponseCodecDescriptor("throwing", 1)

        override fun encode(response: CrawlResponse): ByteArray = throw cause

        override fun decode(payload: ByteArray): CrawlResponse = error("decode must not be called")
    }
}
