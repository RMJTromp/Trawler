package com.rmjtromp.trawler.response

import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.ResponseBody
import java.io.EOFException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class FileResponseStoreTest {
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
    fun `malformed records are rejected as corrupt truncated trailing or oversized`() = runTest {
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

        Files.write(path, valid, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
        Files.write(path, byteArrayOf(99), StandardOpenOption.WRITE, StandardOpenOption.APPEND)
        val trailingError = reopenExisting(path, registry, maxEncodedResponseBytes = 10) {
            find(ResponseLookup.ByReference(reference))
        }
        assertTrue(trailingError.message!!.contains("trailing bytes"))

        Files.write(path, valid, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
        val oversizedError = reopenExisting(path, registry, maxEncodedResponseBytes = 2) {
            find(ResponseLookup.ByReference(reference))
        }
        assertTrue(oversizedError.message!!.contains("payload exceeds the configured bound"))
    }

    @Test
    fun `abandoned temporary files are removed and closed stores reject later operations`() = runTest {
        val registry = ResponseCodecRegistry(listOf(BodyCodec()))
        val temporary = root.resolve("response-00000000.tmp")
        Files.createDirectories(root)
        Files.write(temporary, byteArrayOf(1, 2, 3))
        val store = FileResponseStore(root = root, codecs = registry)
        try {
            assertNull(store.find(ResponseLookup.ByTarget("missing")))
            assertTrue(!Files.exists(temporary))
        } finally {
            store.close()
        }
        assertFailsWith<IllegalStateException> {
            store.find(ResponseLookup.ByTarget("missing"))
        }
    }

    private suspend fun <T> reopenWith(
        path: Path,
        bytes: ByteArray,
        registry: ResponseCodecRegistry,
        maxEncodedResponseBytes: Int,
        action: suspend FileResponseStore.() -> T,
    ): IllegalArgumentException {
        Files.write(path, bytes, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
        return reopenExisting(path, registry, maxEncodedResponseBytes, action)
    }

    private suspend fun <T> reopenExisting(
        path: Path,
        registry: ResponseCodecRegistry,
        maxEncodedResponseBytes: Int,
        action: suspend FileResponseStore.() -> T,
    ): IllegalArgumentException {
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

    private fun response(target: String, body: ByteArray): CrawlResponse = CrawlResponse(
        request = CrawlRequest(
            target = target,
            attributes = ContextAttributes.of(
                ContextAttribute(InMemoryResponseStore.PIPELINE_ID_ATTRIBUTE, "pipeline"),
                ContextAttribute(InMemoryResponseStore.REQUEST_ID_ATTRIBUTE, "request-$target"),
            ),
        ),
        statusCode = 200,
        headers = Headers.empty(),
        body = ResponseBody.from(body),
        receivedAt = Instant.EPOCH,
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
}
