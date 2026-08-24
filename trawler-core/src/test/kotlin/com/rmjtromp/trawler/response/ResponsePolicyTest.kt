package com.rmjtromp.trawler.response

import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.ResponseBody
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class ResponsePolicyTest {
    @Test
    fun `capture policies distinguish optional disabled and required retention`() {
        val context = ResponseCaptureContext(
            crawler = crawler("pipeline-a"),
            request = request("target", "pipeline-a"),
            response = response("target", "pipeline-a", Instant.EPOCH),
        )

        assertEquals(ResponseCaptureDecision.Store, CaptureResponsesWhenConfigured.decide(context))
        assertEquals(false, CaptureResponsesWhenConfigured.requiresStore)
        val skipped = DoNotCaptureResponses.decide(context)
        assertIs<ResponseCaptureDecision.Skip>(skipped)
        assertEquals("response-capture-disabled", skipped.reason)
        assertEquals(ResponseCaptureDecision.Store, RequireResponseCapture.decide(context))
        assertEquals(true, RequireResponseCapture.requiresStore)
    }

    @Test
    fun `latest target cache enforces pipeline ownership and exact age boundary`() = runTest {
        val now = Instant.parse("2026-01-01T00:00:10Z")
        val store = InMemoryResponseStore()
        val cachedRequest = request("target", "pipeline-a")
        store.put(response("target", "pipeline-a", now.minusSeconds(10)))
        val policy = LatestTargetResponseCachePolicy(Duration.ofSeconds(10))

        val exactAge = policy.lookup(
            ResponseCacheContext(crawler("pipeline-a"), cachedRequest, now),
            store,
        )
        assertIs<ResponseCacheDecision.Hit>(exactAge)

        val tooOld = policy.lookup(
            ResponseCacheContext(crawler("pipeline-a"), cachedRequest, now.plusSeconds(1)),
            store,
        )
        assertEquals(ResponseCacheDecision.Miss, tooOld)

        val wrongPipeline = policy.lookup(
            ResponseCacheContext(crawler("pipeline-b"), request("target", "pipeline-b"), now),
            store,
        )
        assertEquals(ResponseCacheDecision.Miss, wrongPipeline)

        val current = policy.lookup(
            ResponseCacheContext(crawler("pipeline-a"), cachedRequest, now.minusSeconds(10)),
            store,
        )
        assertIs<ResponseCacheDecision.Hit>(current)
    }

    @Test
    fun `cache policies validate age and no-cache never consults a store`() = runTest {
        assertFailsWith<IllegalArgumentException> { LatestTargetResponseCachePolicy(Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { LatestTargetResponseCachePolicy(Duration.ofNanos(-1)) }

        val store = RecordingResponseStore()
        val context = ResponseCacheContext(crawler("pipeline"), request("target", "pipeline"), Instant.EPOCH)
        assertEquals(ResponseCacheDecision.Miss, NoResponseCachePolicy.lookup(context, store))
        assertEquals(0, store.findCalls)
    }

    @Test
    fun `in-memory response retention is bounded and age comparisons are explicit`() = runTest {
        val now = Instant.parse("2026-01-01T00:00:00Z")
        val store = InMemoryResponseStore(maxResponses = 2, maxBodyBytes = 2)
        val first = response("first", "pipeline", now, body = byteArrayOf(1, 2))
        val second = response("second", "pipeline", now.plusSeconds(1), body = byteArrayOf(2))
        val third = response("third", "pipeline", now.plusSeconds(2), body = byteArrayOf(3))

        val firstReference = assertNotNull(store.put(first))
        assertNull(store.put(response("too-large", "pipeline", now, body = byteArrayOf(1, 2, 3))))
        store.put(second)
        store.put(third)
        assertNull(store.find(ResponseLookup.ByReference(firstReference)))

        assertEquals(1L, store.evictBefore(now.plusSeconds(2)))
        assertEquals(1L, store.evictBefore(now.plusSeconds(3)))
        assertFailsWith<IllegalArgumentException> { store.replay(ResponseReplayQuery.ByPipelineId("pipeline"), 0) }
        assertFailsWith<IllegalArgumentException> { store.history(" ", 1) }
    }

    @Test
    fun `replay applies pipeline ownership before the limit and retention protects references`() = runTest {
        val now = Instant.EPOCH
        val store = InMemoryResponseStore(maxResponses = 4)
        val a1 = store.put(response("a1", "pipeline-a", now))!!
        store.put(response("b1", "pipeline-b", now.plusSeconds(1)))
        store.put(response("a2", "pipeline-a", now.plusSeconds(2)))

        val replay = store.replay(ResponseReplayQuery.ByPipelineId("pipeline-a"), limit = 1, pipelineId = "pipeline-a")
        assertEquals(listOf("a1"), replay.map { it.response.request.target })
        assertTrue(store.history("missing", 1).isEmpty())

        val protected = store.prune(
            ResponseRetentionRequest(
                cutoff = now.plusSeconds(3),
                maxEntries = 10,
                protectedReferences = setOf(a1),
            ),
        )
        assertEquals(2L, protected.deleted)
        assertEquals(1L, protected.protectedEntries)
        assertTrue(store.find(ResponseLookup.ByReference(a1)) != null)
    }

    @Test
    fun `response lookup helper uses the opaque request target`() = runTest {
        val store = InMemoryResponseStore()
        store.put(response("target", "pipeline", Instant.EPOCH))
        assertEquals("target", store.latestByRequestTarget(request("target", "pipeline"))!!.response.request.target)
        assertFailsWith<IllegalArgumentException> { ResponseRetentionRequest(Instant.EPOCH, 0) }
    }

    private fun crawler(pipelineId: String): CrawlerContext =
        CrawlerContext("engine", "$pipelineId-run", pipelineId)

    private fun request(target: String, pipelineId: String): CrawlRequest = CrawlRequest(
        target = target,
        attributes = ContextAttributes.of(
            ContextAttribute(InMemoryResponseStore.PIPELINE_ID_ATTRIBUTE, pipelineId),
            ContextAttribute(InMemoryResponseStore.REQUEST_ID_ATTRIBUTE, "request-$target"),
        ),
    )

    private fun response(
        target: String,
        pipelineId: String,
        receivedAt: Instant,
        body: ByteArray = byteArrayOf(1),
    ): CrawlResponse = CrawlResponse(
        request = request(target, pipelineId),
        statusCode = 200,
        headers = Headers.empty(),
        body = ResponseBody.from(body),
        receivedAt = receivedAt,
    )

    private class RecordingResponseStore : ResponseStore {
        override val id: String = "recording"
        override val capabilities: Set<ResponseStoreCapability> = emptySet()
        var findCalls: Int = 0

        override suspend fun put(response: CrawlResponse) = null

        override suspend fun find(query: ResponseLookup): StoredResponse? {
            findCalls++
            return null
        }
    }
}
