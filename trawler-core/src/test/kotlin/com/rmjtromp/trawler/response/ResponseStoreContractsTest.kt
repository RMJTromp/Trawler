package com.rmjtromp.trawler.response

import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.model.ResponseReference
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class ResponseStoreContractsTest {
    @Test
    fun `lookup and replay queries reject blank identifiers with their own messages`() {
        assertInvalid("Response request ID must not be blank") { ResponseLookup.ByRequestId(" ") }
        assertInvalid("Response target must not be blank") { ResponseLookup.ByTarget("") }
        assertInvalid("Replay request ID must not be blank") { ResponseReplayQuery.ByRequestId(" ") }
        assertInvalid("Replay pipeline ID must not be blank") { ResponseReplayQuery.ByPipelineId("") }

        assertEquals("request", ResponseLookup.ByRequestId("request").requestId)
        assertEquals("target", ResponseLookup.ByTarget("target").target)
        assertEquals(ResponseReference("ref"), ResponseReplayQuery.ByReference(ResponseReference("ref")).reference)
    }

    @Test
    fun `stored response derives engine pipeline ownership and rejects blank explicit ownership`() {
        val reference = ResponseReference("ref")
        val owned = response(
            ContextAttributes.of(ContextAttribute(InMemoryResponseStore.PIPELINE_ID_ATTRIBUTE, "pipeline")),
        )
        assertEquals("pipeline", StoredResponse(reference, owned).pipelineId)
        assertNull(StoredResponse(reference, response(ContextAttributes.empty())).pipelineId)
        assertEquals("explicit", StoredResponse(reference, owned, pipelineId = "explicit").pipelineId)
        assertNull(StoredResponse(reference, owned, pipelineId = null).pipelineId)
        assertInvalid("Stored response pipeline ID must not be blank when present") {
            StoredResponse(reference, owned, pipelineId = " ")
        }
    }

    @Test
    fun `retention request snapshots protected references and bounds their count`() {
        val source = mutableSetOf(ResponseReference("a"))
        val request = ResponseRetentionRequest(Instant.EPOCH, maxEntries = 1, protectedReferences = source)
        source += ResponseReference("b")
        assertEquals(setOf(ResponseReference("a")), request.protectedReferences)
        @Suppress("UNCHECKED_CAST")
        val mutable = request.protectedReferences as MutableSet<ResponseReference>
        assertFailsWith<UnsupportedOperationException> { mutable += ResponseReference("c") }
        assertTrue(ResponseRetentionRequest(Instant.EPOCH, maxEntries = 1).protectedReferences.isEmpty())

        val atBound = (1..4_096).map { ResponseReference("ref-$it") }.toSet()
        assertEquals(4_096, ResponseRetentionRequest(Instant.EPOCH, 1, atBound).protectedReferences.size)
        assertInvalid("Response retention protected-reference bound is 4096") {
            ResponseRetentionRequest(Instant.EPOCH, 1, atBound + ResponseReference("overflow"))
        }
        assertInvalid("Response retention batch must be positive") {
            ResponseRetentionRequest(Instant.EPOCH, maxEntries = -1)
        }
    }

    @Test
    fun `replay callers that omit ownership request an unfiltered adapter replay`() = runTest {
        val recorded = mutableListOf<Triple<ResponseReplayQuery, Int, String?>>()
        val store: ResponseReplayStore = object : ResponseReplayStore {
            override suspend fun replay(
                query: ResponseReplayQuery,
                limit: Int,
                pipelineId: String?,
            ): List<StoredResponse> {
                recorded += Triple(query, limit, pipelineId)
                return emptyList()
            }
        }
        val query = ResponseReplayQuery.ByPipelineId("pipeline")

        assertTrue(store.replay(query, 3).isEmpty())
        assertTrue(store.replay(query, 4, "pipeline").isEmpty())
        assertEquals(listOf(Triple(query, 3, null), Triple<ResponseReplayQuery, Int, String?>(query, 4, "pipeline")), recorded)
    }

    private fun response(attributes: ContextAttributes): CrawlResponse = CrawlResponse(
        request = CrawlRequest(target = "target", attributes = attributes),
        statusCode = 200,
        headers = Headers.empty(),
        body = ResponseBody.from(byteArrayOf(1)),
        receivedAt = Instant.EPOCH,
    )

    private fun assertInvalid(expectedMessage: String, create: () -> Any) {
        val error = assertFailsWith<IllegalArgumentException> { create() }
        assertEquals(expectedMessage, error.message)
    }
}
