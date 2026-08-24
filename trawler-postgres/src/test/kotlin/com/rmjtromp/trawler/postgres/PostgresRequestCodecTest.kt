package com.rmjtromp.trawler.postgres

import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.RequestBody
import com.rmjtromp.trawler.model.RequestMethod
import java.io.EOFException
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class PostgresRequestCodecTest {
    @Test
    fun `binary codec round trips ordered duplicate headers attributes and body`() {
        val request = request()
        val codec = BinaryPostgresRequestCodec(limits())

        val payload = codec.encode(request)
        assertEquals(request, codec.decode(payload))

        val encoded = EncodedPostgresRequest.from(codec.descriptor, payload)
        val returned = encoded.toByteArray()
        returned[0] = (returned[0].toInt() xor 0x7f).toByte()
        assertContentEquals(payload, encoded.toByteArray())
        assertEquals(payload.size, encoded.size)

        val source = byteArrayOf(1, 2, 3)
        val snapshot = EncodedPostgresRequest.from(codec.descriptor, source)
        source[0] = 9
        assertContentEquals(byteArrayOf(1, 2, 3), snapshot.toByteArray())
    }

    @Test
    fun `registry selects exact active and historical versions and rejects duplicates or missing codecs`() {
        val v1 = TaggedCodec(PostgresCodecDescriptor("request", 1), 1)
        val v2 = TaggedCodec(PostgresCodecDescriptor("request", 2), 2)
        val registry = PostgresRequestCodecRegistry(listOf(v1, v2), active = v2.descriptor)

        assertEquals(v2.descriptor, registry.encode(request()).descriptor)
        assertEquals("decoded-v1", registry.decode(v1.descriptor, byteArrayOf(1)).target)
        assertEquals("decoded-v2", registry.decode(v2.descriptor, byteArrayOf(2)).target)

        assertFailsWith<IllegalArgumentException> {
            PostgresRequestCodecRegistry(
                listOf(v1, TaggedCodec(v1.descriptor, 9)),
            )
        }
        assertFailsWith<IllegalArgumentException> {
            PostgresRequestCodecRegistry(listOf(v1), active = PostgresCodecDescriptor("request", 3))
        }
        assertFailsWith<IllegalStateException> {
            PostgresRequestCodecRegistry(emptyList())
        }
        assertFailsWith<IllegalStateException> {
            registry.decode(PostgresCodecDescriptor("missing", 1), byteArrayOf(1))
        }

        val mutating = MutatingCodec(PostgresCodecDescriptor("mutating", 1))
        val defensiveRegistry = PostgresRequestCodecRegistry(listOf(mutating))
        val input = byteArrayOf(7)
        defensiveRegistry.decode(mutating.descriptor, input)
        assertContentEquals(byteArrayOf(7), input)
    }

    @Test
    fun `binary codec accepts exact field bounds and rejects every value above them`() {
        BinaryPostgresRequestCodec(
            limits(maxBodyBytes = 0, maxHeaderCount = 0, maxAttributeCount = 0),
        ).encode(CrawlRequest("target"))

        val exactTarget = BinaryPostgresRequestCodec(limits(maxTargetBytes = 2))
        exactTarget.encode(CrawlRequest("é"))
        assertEncodeRejected(limits(maxTargetBytes = 2), CrawlRequest("éa"))

        val exactMethod = BinaryPostgresRequestCodec(limits(maxHeaderValueBytes = 3))
        exactMethod.encode(CrawlRequest("target", method = RequestMethod("GET")))
        assertEncodeRejected(limits(maxHeaderValueBytes = 3), CrawlRequest("target", method = RequestMethod("POST")))

        val exactHeaderName = BinaryPostgresRequestCodec(limits(maxHeaderNameBytes = 2))
        exactHeaderName.encode(CrawlRequest("target", headers = Headers.of(Header("é", "v"))))
        assertEncodeRejected(
            limits(maxHeaderNameBytes = 2),
            CrawlRequest("target", headers = Headers.of(Header("éa", "v"))),
        )

        val exactHeaderValue = BinaryPostgresRequestCodec(limits(maxHeaderValueBytes = 2))
        exactHeaderValue.encode(
            CrawlRequest("target", method = RequestMethod("G"), headers = Headers.of(Header("h", "é"))),
        )
        assertEncodeRejected(
            limits(maxHeaderValueBytes = 2),
            CrawlRequest("target", method = RequestMethod("G"), headers = Headers.of(Header("h", "éa"))),
        )

        val exactBody = BinaryPostgresRequestCodec(limits(maxBodyBytes = 2))
        exactBody.encode(CrawlRequest("target", body = RequestBody.from(byteArrayOf(1, 2))))
        assertEncodeRejected(
            limits(maxBodyBytes = 2),
            CrawlRequest("target", body = RequestBody.from(byteArrayOf(1, 2, 3))),
        )

        val exactAttributeName = BinaryPostgresRequestCodec(limits(maxAttributeNameBytes = 2))
        exactAttributeName.encode(
            CrawlRequest("target", attributes = ContextAttributes.of(ContextAttribute("é", "v"))),
        )
        assertEncodeRejected(
            limits(maxAttributeNameBytes = 2),
            CrawlRequest("target", attributes = ContextAttributes.of(ContextAttribute("éa", "v"))),
        )

        val exactAttributeValue = BinaryPostgresRequestCodec(limits(maxAttributeValueBytes = 2))
        exactAttributeValue.encode(
            CrawlRequest("target", attributes = ContextAttributes.of(ContextAttribute("a", "é"))),
        )
        assertEncodeRejected(
            limits(maxAttributeValueBytes = 2),
            CrawlRequest("target", attributes = ContextAttributes.of(ContextAttribute("a", "éa"))),
        )

        val exactHeaderCount = BinaryPostgresRequestCodec(limits(maxHeaderCount = 1))
        exactHeaderCount.encode(CrawlRequest("target", headers = Headers.of(Header("h", "v"))))
        assertEncodeRejected(
            limits(maxHeaderCount = 1),
            CrawlRequest("target", headers = Headers.of(Header("h", "v"), Header("h2", "v"))),
        )

        val exactAttributeCount = BinaryPostgresRequestCodec(limits(maxAttributeCount = 1))
        exactAttributeCount.encode(
            CrawlRequest("target", attributes = ContextAttributes.of(ContextAttribute("a", "v"))),
        )
        assertEncodeRejected(
            limits(maxAttributeCount = 1),
            CrawlRequest(
                "target",
                attributes = ContextAttributes.of(ContextAttribute("a", "v"), ContextAttribute("b", "v")),
            ),
        )
    }

    @Test
    fun `encoded payload bound accepts exact bytes and rejects oversized input on encode and decode`() {
        val request = request()
        val unbounded = BinaryPostgresRequestCodec(limits(maxEncodedBytes = 1_000_000))
        val payload = unbounded.encode(request)
        val exact = BinaryPostgresRequestCodec(limits(maxEncodedBytes = payload.size))
        assertContentEquals(payload, exact.encode(request))
        assertEquals(request, exact.decode(payload))

        val over = BinaryPostgresRequestCodec(limits(maxEncodedBytes = payload.size - 1))
        assertFailsWith<IllegalArgumentException> { over.encode(request) }
        assertFailsWith<IllegalArgumentException> { over.decode(payload) }
    }

    @Test
    fun `decoder rejects corrupt truncated trailing and field-over-bound payloads`() {
        val request = request()
        val wide = BinaryPostgresRequestCodec(limits(maxEncodedBytes = 1_000_000))
        val payload = wide.encode(request)

        val truncated = assertFailsWith<IllegalArgumentException> {
            wide.decode(payload.copyOf(payload.size - 1))
        }
        assertTrue(truncated.cause is EOFException)
        assertFailsWith<IllegalArgumentException> { wide.decode(payload + byteArrayOf(0)) }

        val badMagic = payload.copyOf().also { it[0] = 0 }
        assertFailsWith<IllegalArgumentException> { wide.decode(badMagic) }
        val badVersion = payload.copyOf().also { it[7] = 2 }
        assertFailsWith<IllegalArgumentException> { wide.decode(badVersion) }

        val oversizedTarget = wide.encode(CrawlRequest("éa"))
        assertFailsWith<IllegalArgumentException> {
            BinaryPostgresRequestCodec(limits(maxTargetBytes = 2)).decode(oversizedTarget)
        }
        val oversizedHeader = wide.encode(
            CrawlRequest("target", headers = Headers.of(Header("éa", "v"))),
        )
        assertFailsWith<IllegalArgumentException> {
            BinaryPostgresRequestCodec(limits(maxHeaderNameBytes = 2)).decode(oversizedHeader)
        }
        val oversizedBody = wide.encode(
            CrawlRequest("target", body = RequestBody.from(byteArrayOf(1, 2, 3))),
        )
        assertFailsWith<IllegalArgumentException> {
            BinaryPostgresRequestCodec(limits(maxBodyBytes = 2)).decode(oversizedBody)
        }
        val oversizedAttributes = wide.encode(
            CrawlRequest(
                "target",
                attributes = ContextAttributes.of(ContextAttribute("a", "éa")),
            ),
        )
        assertFailsWith<IllegalArgumentException> {
            BinaryPostgresRequestCodec(limits(maxAttributeValueBytes = 2)).decode(oversizedAttributes)
        }

        val oneHeader = wide.encode(CrawlRequest("target", headers = Headers.of(Header("h", "v"))))
        assertFailsWith<IllegalArgumentException> {
            BinaryPostgresRequestCodec(limits(maxHeaderCount = 0)).decode(oneHeader)
        }
        val oneAttribute = wide.encode(
            CrawlRequest("target", attributes = ContextAttributes.of(ContextAttribute("a", "v"))),
        )
        assertFailsWith<IllegalArgumentException> {
            BinaryPostgresRequestCodec(limits(maxAttributeCount = 0)).decode(oneAttribute)
        }
    }

    private fun request(): CrawlRequest = CrawlRequest(
        target = "https://example.test/é",
        method = RequestMethod("POST"),
        headers = Headers.of(
            Header("X-Duplicate", "one"),
            Header("X-Duplicate", "two"),
        ),
        body = RequestBody.from(byteArrayOf(1, 2, 3)),
        attributes = ContextAttributes.of(ContextAttribute("cursor", "α")),
    )

    private fun limits(
        maxEncodedBytes: Int = 1_000_000,
        maxBodyBytes: Int = 1_000,
        maxHeaderCount: Int = 10,
        maxAttributeCount: Int = 10,
        maxHeaderNameBytes: Int = 100,
        maxHeaderValueBytes: Int = 100,
        maxTargetBytes: Int = 100,
        maxAttributeNameBytes: Int = 100,
        maxAttributeValueBytes: Int = 100,
    ): PostgresRequestCodecLimits = PostgresRequestCodecLimits(
        maxEncodedBytes = maxEncodedBytes,
        maxBodyBytes = maxBodyBytes,
        maxHeaderCount = maxHeaderCount,
        maxAttributeCount = maxAttributeCount,
        maxHeaderNameBytes = maxHeaderNameBytes,
        maxHeaderValueBytes = maxHeaderValueBytes,
        maxTargetBytes = maxTargetBytes,
        maxAttributeNameBytes = maxAttributeNameBytes,
        maxAttributeValueBytes = maxAttributeValueBytes,
    )

    private fun assertEncodeRejected(
        limits: PostgresRequestCodecLimits,
        request: CrawlRequest,
    ) {
        assertFailsWith<IllegalArgumentException> {
            BinaryPostgresRequestCodec(limits).encode(request)
        }
    }

    private class TaggedCodec(
        override val descriptor: PostgresCodecDescriptor,
        private val marker: Int,
    ) : PostgresRequestCodec {
        override fun encode(request: CrawlRequest): ByteArray = byteArrayOf(marker.toByte())

        override fun decode(payload: ByteArray): CrawlRequest {
            require(payload.contentEquals(byteArrayOf(marker.toByte())))
            return CrawlRequest("decoded-v$marker")
        }
    }

    private class MutatingCodec(
        override val descriptor: PostgresCodecDescriptor,
    ) : PostgresRequestCodec {
        override fun encode(request: CrawlRequest): ByteArray = byteArrayOf(0)

        override fun decode(payload: ByteArray): CrawlRequest {
            payload[0] = 0
            return CrawlRequest("decoded")
        }
    }
}
