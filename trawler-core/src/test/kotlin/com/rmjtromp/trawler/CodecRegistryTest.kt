package com.rmjtromp.trawler

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.output.OutputCodec
import com.rmjtromp.trawler.output.OutputCodecDescriptor
import com.rmjtromp.trawler.output.OutputCodecRegistry
import com.rmjtromp.trawler.pipeline.Produced
import com.rmjtromp.trawler.pipeline.ProductionContext
import com.rmjtromp.trawler.response.EncodedResponse
import com.rmjtromp.trawler.response.ResponseCodec
import com.rmjtromp.trawler.response.ResponseCodecDescriptor
import com.rmjtromp.trawler.response.ResponseCodecRegistry
import java.time.Instant
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class CodecRegistryTest {
    @Test
    fun `response registry selects active version and decodes exact historical versions`() {
        val v1 = ResponseRecorder(ResponseCodecDescriptor("body", 1), 1)
        val v2 = ResponseRecorder(ResponseCodecDescriptor("body", 2), 2)
        val registry = ResponseCodecRegistry(listOf(v1, v2), active = v2.descriptor)

        val encoded = registry.encode(response())
        assertEquals(v2.descriptor, encoded.descriptor)
        assertEquals(1, v2.encodeCalls)
        assertEquals(0, v1.encodeCalls)

        val v1Bytes = byteArrayOf(7, 8)
        val decoded = registry.decode(v1.descriptor, v1Bytes)
        assertEquals("decoded-1", decoded.request.target)
        assertEquals(1, v1.decodeCalls)
        assertContentEquals(byteArrayOf(7, 8), v1Bytes)
    }

    @Test
    fun `response encoded bytes are defensive and missing historical codecs fail visibly`() {
        val descriptor = ResponseCodecDescriptor("body", 1)
        val registry = ResponseCodecRegistry(listOf(ResponseRecorder(descriptor, 1)))
        val source = byteArrayOf(1, 2, 3)
        val encoded = EncodedResponse.from(descriptor, source)
        source[0] = 9
        val read = encoded.toByteArray()
        read[1] = 9

        assertContentEquals(byteArrayOf(1, 2, 3), encoded.toByteArray())
        assertEquals(3, encoded.size)
        assertFailsWith<IllegalStateException> {
            registry.decode(ResponseCodecDescriptor("body", 2), byteArrayOf())
        }
    }

    @Test
    fun `response codec registry rejects empty duplicate and unregistered active selections`() {
        val v1 = ResponseRecorder(ResponseCodecDescriptor("body", 1), 1)
        val v1Duplicate = ResponseRecorder(ResponseCodecDescriptor("body", 1), 9)

        assertFailsWith<IllegalStateException> { ResponseCodecRegistry(emptyList()) }
        assertFailsWith<IllegalArgumentException> { ResponseCodecRegistry(listOf(v1, v1Duplicate)) }
        assertFailsWith<IllegalArgumentException> {
            ResponseCodecRegistry(listOf(v1), active = ResponseCodecDescriptor("body", 2))
        }
        assertFailsWith<IllegalArgumentException> { ResponseCodecDescriptor(" ", 1) }
        assertFailsWith<IllegalArgumentException> { ResponseCodecDescriptor("body", -1) }
    }

    @Test
    fun `output registry selects exact active codec and copies decode input`() {
        val v1 = OutputRecorder(OutputCodecDescriptor("value", 1), 1)
        val v2 = OutputRecorder(OutputCodecDescriptor("value", 2), 2)
        val registry = OutputCodecRegistry(listOf(v1, v2), active = v2.descriptor)
        val item = produced("value")

        assertSame(v2, registry.active())
        assertContentEquals(byteArrayOf(2, 'v'.code.toByte()), v2.encode(item))
        assertEquals(1, v2.encodeCalls)

        val payload = byteArrayOf(4, 5)
        val decoded = registry.decode(v1.descriptor, payload)
        assertEquals("decoded-1", decoded.value)
        assertEquals(1, v1.decodeCalls)
        assertContentEquals(byteArrayOf(4, 5), payload)
    }

    @Test
    fun `output codec registry rejects duplicate active and missing historical versions`() {
        val v1 = OutputRecorder(OutputCodecDescriptor("value", 1), 1)
        assertFailsWith<IllegalStateException> { OutputCodecRegistry<String>(emptyList()) }
        assertFailsWith<IllegalArgumentException> {
            OutputCodecRegistry(listOf(v1, OutputRecorder(v1.descriptor, 2)))
        }
        assertFailsWith<IllegalArgumentException> {
            OutputCodecRegistry(listOf(v1), active = OutputCodecDescriptor("value", 2))
        }
        assertFailsWith<IllegalStateException> {
            OutputCodecRegistry(listOf(v1)).decode(OutputCodecDescriptor("value", 2), byteArrayOf())
        }
        assertFailsWith<IllegalArgumentException> { OutputCodecDescriptor(" ", 1) }
        assertFailsWith<IllegalArgumentException> { OutputCodecDescriptor("value", -1) }
    }

    @Test
    fun `codec-owned corruption truncation trailing and oversized payloads preserve failure categories`() {
        val codec = StrictResponseCodec(ResponseCodecDescriptor("strict", 1), maxPayload = 4)
        val registry = ResponseCodecRegistry(listOf(codec))

        assertFailsWith<IllegalArgumentException> { registry.decode(codec.descriptor, byteArrayOf()) }
        assertFailsWith<IllegalArgumentException> { registry.decode(codec.descriptor, byteArrayOf(0x00, 0x00)) }
        assertFailsWith<IllegalArgumentException> {
            registry.decode(codec.descriptor, byteArrayOf(0x52, 0x01, 0x01, 0x02))
        }
        assertFailsWith<IllegalArgumentException> {
            registry.decode(codec.descriptor, byteArrayOf(0x52, 0x05, 1, 2, 3, 4, 5))
        }
        assertTrue(codec.failures.containsAll(setOf("truncated", "corrupt", "trailing", "oversized")))
    }

    private fun response(): CrawlResponse = CrawlResponse(
        request = CrawlRequest("response"),
        statusCode = 200,
        headers = Headers.empty(),
        body = ResponseBody.from(byteArrayOf(1)),
        receivedAt = Instant.EPOCH,
    )

    private fun produced(value: String): Produced<String> = Produced(
        value = value,
        context = ProductionContext(
            pipelineId = "pipeline",
            engineRunId = "engine",
            pipelineRunId = "pipeline-run",
            requestId = null,
            responseReference = null,
            originalTarget = null,
            finalTarget = null,
            producedAt = Instant.EPOCH,
        ),
    )

    private class ResponseRecorder(
        override val descriptor: ResponseCodecDescriptor,
        private val tag: Int,
    ) : ResponseCodec {
        var encodeCalls: Int = 0
        var decodeCalls: Int = 0

        override fun encode(response: CrawlResponse): ByteArray {
            encodeCalls++
            return byteArrayOf(tag.toByte())
        }

        override fun decode(payload: ByteArray): CrawlResponse {
            decodeCalls++
            payload[0] = 99
            return codecResponse().copy(request = CrawlRequest("decoded-$tag"))
        }
    }

    private class OutputRecorder(
        override val descriptor: OutputCodecDescriptor,
        private val tag: Int,
    ) : OutputCodec<String> {
        var encodeCalls: Int = 0
        var decodeCalls: Int = 0

        override fun encode(item: Produced<String>): ByteArray {
            encodeCalls++
            return byteArrayOf(tag.toByte(), item.value.first().code.toByte())
        }

        override fun decode(payload: ByteArray): Produced<String> {
            decodeCalls++
            payload[0] = 99
            return codecProduced("decoded-$tag")
        }
    }

    private class StrictResponseCodec(
        override val descriptor: ResponseCodecDescriptor,
        private val maxPayload: Int,
    ) : ResponseCodec {
        val failures = mutableSetOf<String>()

        override fun encode(response: CrawlResponse): ByteArray = byteArrayOf(0x52)

        override fun decode(payload: ByteArray): CrawlResponse {
            if (payload.size < 2) {
                failures += "truncated"
                throw IllegalArgumentException("truncated")
            }
            if (payload[0] != 0x52.toByte()) {
                failures += "corrupt"
                throw IllegalArgumentException("corrupt")
            }
            val declared = payload[1].toInt()
            if (declared > maxPayload) {
                failures += "oversized"
                throw IllegalArgumentException("oversized")
            }
            if (payload.size < declared + 2) {
                failures += "truncated"
                throw IllegalArgumentException("truncated")
            }
            if (payload.size > declared + 2) {
                failures += "trailing"
                throw IllegalArgumentException("trailing")
            }
            return codecResponse()
        }
    }
}

private fun codecResponse(): CrawlResponse = CrawlResponse(
    request = CrawlRequest("response"),
    statusCode = 200,
    headers = Headers.empty(),
    body = ResponseBody.from(byteArrayOf(1)),
    receivedAt = Instant.EPOCH,
)

private fun codecProduced(value: String): Produced<String> = Produced(
    value = value,
    context = ProductionContext(
        pipelineId = "pipeline",
        engineRunId = "engine",
        pipelineRunId = "pipeline-run",
        requestId = null,
        responseReference = null,
        originalTarget = null,
        finalTarget = null,
        producedAt = Instant.EPOCH,
    ),
)
