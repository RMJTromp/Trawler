package com.rmjtromp.trawler.model

import java.time.Instant
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class CoreModelsTest {
    @Test
    fun `request and response bodies own defensive byte snapshots`() {
        val requestBytes = byteArrayOf(1, 2, 3)
        val requestBody = RequestBody.from(requestBytes)
        requestBytes[0] = 9

        val requestRead = requestBody.toByteArray()
        requestRead[1] = 8

        assertContentEquals(byteArrayOf(1, 2, 3), requestBody.toByteArray())
        assertEquals(3, requestBody.size)

        val responseBytes = byteArrayOf(4, 5, 6)
        val responseBody = ResponseBody.from(responseBytes)
        responseBytes[2] = 0

        val responseRead = responseBody.toByteArray()
        responseRead[0] = 0

        assertContentEquals(byteArrayOf(4, 5, 6), responseBody.toByteArray())
        assertEquals(3, responseBody.size)
    }

    @Test
    fun `attribute and header collections snapshot inputs and reject mutation`() {
        val sourceAttributes = mutableListOf(ContextAttribute("trace", "one"))
        val attributes = ContextAttributes.from(sourceAttributes)
        sourceAttributes += ContextAttribute("later", "two")

        assertEquals(listOf(ContextAttribute("trace", "one")), attributes.entries)
        assertEquals("one", attributes["trace"])
        assertEquals("one", attributes.plus(ContextAttribute("trace", "replacement"))["trace"])

        @Suppress("UNCHECKED_CAST")
        val mutableAttributes = attributes.entries as MutableList<ContextAttribute>
        assertFailsWith<UnsupportedOperationException> {
            mutableAttributes += ContextAttribute("forbidden", "value")
        }

        val sourceHeaders = mutableListOf(Header("X-Test", "one"))
        val headers = Headers.from(sourceHeaders)
        sourceHeaders += Header("X-Test", "two")

        assertEquals(listOf("one"), headers["x-test"])
        assertEquals(listOf(Header("X-Test", "one")), headers.entries)
        assertNotEquals(headers, headers.plus(Header("X-Test", "two")))

        @Suppress("UNCHECKED_CAST")
        val mutableHeaders = headers.entries as MutableList<Header>
        assertFailsWith<UnsupportedOperationException> {
            mutableHeaders += Header("forbidden", "value")
        }
    }

    @Test
    fun `response status and final-target boundaries are enforced`() {
        val request = CrawlRequest("opaque-resource")
        val receivedAt = Instant.parse("2026-01-01T00:00:00Z")

        assertTrue(
            CrawlResponse(
                request = request,
                statusCode = 0,
                headers = Headers.empty(),
                body = ResponseBody.empty(),
                receivedAt = receivedAt,
            ).statusCode == 0,
        )
        assertTrue(
            CrawlResponse(
                request = request,
                statusCode = 999,
                headers = Headers.empty(),
                body = ResponseBody.empty(),
                receivedAt = receivedAt,
            ).statusCode == 999,
        )

        assertFailsWith<IllegalArgumentException> {
            CrawlResponse(
                request = request,
                statusCode = -1,
                headers = Headers.empty(),
                body = ResponseBody.empty(),
                receivedAt = receivedAt,
            )
        }
        assertFailsWith<IllegalArgumentException> {
            CrawlResponse(
                request = request,
                statusCode = 1_000,
                headers = Headers.empty(),
                body = ResponseBody.empty(),
                receivedAt = receivedAt,
            )
        }
        assertFailsWith<IllegalArgumentException> {
            CrawlResponse(
                request = request,
                statusCode = null,
                headers = Headers.empty(),
                body = ResponseBody.empty(),
                receivedAt = receivedAt,
                finalTarget = " ",
            )
        }
    }

    @Test
    fun `core identifiers and required context fields reject blank values`() {
        assertFailsWith<IllegalArgumentException> { ContextAttribute(" ", "value") }
        assertFailsWith<IllegalArgumentException> { Header("", "value") }
        assertFailsWith<IllegalArgumentException> { RequestMethod("") }
        assertFailsWith<IllegalArgumentException> { CrawlRequest(" ") }
        assertFailsWith<IllegalArgumentException> { ResponseReference("") }
        assertFailsWith<IllegalArgumentException> {
            CrawlerContext("engine", "pipeline-run", " ")
        }
        assertFailsWith<IllegalArgumentException> {
            CrawlerContext("engine", "pipeline-run", "pipeline", requestId = " ")
        }
    }

    @Test
    fun `value-equal collections and bodies compare by content and render without secrets`() {
        val attributes = ContextAttributes.of(ContextAttribute("trace", "one"))
        assertEquals(attributes, ContextAttributes.from(listOf(ContextAttribute("trace", "one"))))
        assertEquals(attributes.hashCode(), ContextAttributes.from(listOf(ContextAttribute("trace", "one"))).hashCode())
        assertNotEquals(attributes, ContextAttributes.empty())
        assertNotEquals<Any>(attributes, "trace=one")
        assertEquals(null, attributes["missing"])
        assertEquals("ContextAttributes(entries=[ContextAttribute(name=trace, value=one)])", attributes.toString())

        val headers = Headers.of(Header("Accept", "text/plain"))
        assertEquals(headers, Headers.from(listOf(Header("Accept", "text/plain"))))
        assertEquals(headers.hashCode(), Headers.from(listOf(Header("Accept", "text/plain"))).hashCode())
        assertNotEquals<Any>(headers, listOf(Header("Accept", "text/plain")))
        assertEquals(emptyList(), headers["missing"])
        assertTrue(headers.toString().startsWith("Headers(entries="))

        val requestBody = RequestBody.from(byteArrayOf(1, 2))
        assertEquals(requestBody, RequestBody.from(byteArrayOf(1, 2)))
        assertEquals(requestBody.hashCode(), RequestBody.from(byteArrayOf(1, 2)).hashCode())
        assertNotEquals(requestBody, RequestBody.from(byteArrayOf(1, 3)))
        assertNotEquals<Any>(requestBody, ResponseBody.from(byteArrayOf(1, 2)))
        assertEquals("RequestBody(size=2)", requestBody.toString())
        assertEquals(0, RequestBody.empty().size)
        assertEquals(RequestBody.empty(), RequestBody.from(ByteArray(0)))

        val responseBody = ResponseBody.from(byteArrayOf(7))
        assertEquals(responseBody, ResponseBody.from(byteArrayOf(7)))
        assertEquals(responseBody.hashCode(), ResponseBody.from(byteArrayOf(7)).hashCode())
        assertNotEquals(responseBody, ResponseBody.from(byteArrayOf(8)))
        assertNotEquals<Any>(responseBody, requestBody)
        assertEquals("ResponseBody(size=1)", responseBody.toString())
        assertEquals(0, ResponseBody.empty().size)
    }

    @Test
    fun `crawler context validates every optional identifier only when present`() {
        val context = CrawlerContext("engine", "pipeline-run", "pipeline", requestId = "request", attemptId = "attempt")
        assertEquals("attempt", context.attemptId)
        assertEquals(null, CrawlerContext("engine", "pipeline-run", "pipeline").attemptId)
        listOf(
            "Engine run ID must not be blank" to { CrawlerContext(" ", "pipeline-run", "pipeline") },
            "Pipeline run ID must not be blank" to { CrawlerContext("engine", "", "pipeline") },
            "Pipeline ID must not be blank" to { CrawlerContext("engine", "pipeline-run", "") },
            "Request ID must not be blank when present" to {
                CrawlerContext("engine", "pipeline-run", "pipeline", requestId = "")
            },
            "Attempt ID must not be blank when present" to {
                CrawlerContext("engine", "pipeline-run", "pipeline", attemptId = " ")
            },
            "Attempt ID must not be blank when present" to {
                CrawlerContext("engine", "pipeline-run", "pipeline", attemptId = "")
            },
        ).forEach { (message, construct) ->
            assertEquals(message, assertFailsWith<IllegalArgumentException> { construct() }.message)
        }
    }

    @Test
    fun `request and response defaults are transport neutral`() {
        val request = CrawlRequest("opaque")
        assertEquals(RequestMethod.GET, request.method)
        assertEquals("GET", request.method.value)
        assertEquals(Headers.empty(), request.headers)
        assertEquals(null, request.body)
        assertEquals(ContextAttributes.empty(), request.attributes)
        val response = CrawlResponse(request, null, Headers.empty(), ResponseBody.empty(), Instant.EPOCH, "final")
        assertEquals(null, response.statusCode)
        assertEquals("final", response.finalTarget)
        assertEquals("reference", ResponseReference("reference").value)
    }
}
