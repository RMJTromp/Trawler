package com.rmjtromp.trawler.response

import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.ResponseBody
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class InMemoryResponseStoreTest {
    @Test
    fun `ring eviction and newest lookups preserve bounded history ordering`() = runTest {
        val start = Instant.parse("2026-01-01T00:00:00Z")
        val store = InMemoryResponseStore(maxResponses = 3, maxBodyBytes = 2)
        val first = store.put(response("same", "pipeline", start, "request-1"))!!
        val second = store.put(response("same", "pipeline", start.plusSeconds(1), "request-1"))!!
        val third = store.put(response("other", "pipeline", start.plusSeconds(2), "request-2"))!!

        assertNull(store.put(response("refused", "pipeline", start, "request-refused", byteArrayOf(1, 2, 3))))
        val fourth = store.put(response("same", "pipeline", start.plusSeconds(3), "request-1"))!!
        assertNull(store.find(ResponseLookup.ByReference(first)))
        assertEquals(fourth, store.find(ResponseLookup.ByTarget("same"))!!.reference)
        assertEquals(fourth, store.find(ResponseLookup.ByRequestId("request-1"))!!.reference)
        assertEquals(listOf(second, fourth), store.history("request-1", 10).map { it.reference })
        assertEquals(listOf(second), store.history("request-1", 1).map { it.reference })
        assertEquals(listOf(second, third, fourth), store.replay(
            ResponseReplayQuery.ByPipelineId("pipeline"),
            limit = 10,
        ).map { it.reference })
    }

    @Test
    fun `replay filters pipeline ownership before applying its limit and keeps insertion order`() = runTest {
        val store = InMemoryResponseStore(maxResponses = 8)
        val firstOther = store.put(response("other-first", "pipeline-b", Instant.EPOCH, "b-1"))!!
        val firstOwned = store.put(response("owned-first", "pipeline-a", Instant.EPOCH.plusSeconds(1), "a-1"))!!
        val secondOwned = store.put(response("owned-second", "pipeline-a", Instant.EPOCH.plusSeconds(2), "a-2"))!!
        store.put(response("other-second", "pipeline-b", Instant.EPOCH.plusSeconds(3), "b-2"))

        val replay = store.replay(
            query = ResponseReplayQuery.ByPipelineId("pipeline-a"),
            limit = 2,
            pipelineId = "pipeline-a",
        )
        assertEquals(listOf(firstOwned, secondOwned), replay.map { it.reference })
        assertEquals(firstOther, store.find(ResponseLookup.ByReference(firstOther))!!.reference)
    }

    @Test
    fun `pipeline replay pages with an exclusive cursor that survives pruning of the cursor response`() = runTest {
        val store = InMemoryResponseStore(maxResponses = 8)
        val first = store.put(response("a", "pipeline", Instant.EPOCH))!!
        store.put(response("other", "other-pipeline", Instant.EPOCH))!!
        val second = store.put(response("b", "pipeline", Instant.EPOCH.plusSeconds(10)))!!
        val third = store.put(response("c", "pipeline", Instant.EPOCH.plusSeconds(20)))!!

        val page1 = store.replay(ResponseReplayQuery.ByPipelineId("pipeline"), limit = 2, pipelineId = "pipeline")
        assertEquals(listOf(first, second), page1.map { it.reference })
        val page2 = store.replay(
            ResponseReplayQuery.ByPipelineId("pipeline", after = page1.last().reference),
            limit = 2,
            pipelineId = "pipeline",
        )
        assertEquals(listOf(third), page2.map { it.reference })
        assertEquals(
            emptyList(),
            store.replay(ResponseReplayQuery.ByPipelineId("pipeline", after = third), limit = 2, pipelineId = "pipeline"),
        )

        assertEquals(2L, store.evictBefore(Instant.EPOCH.plusSeconds(5)))
        assertEquals(
            listOf(third),
            store.replay(ResponseReplayQuery.ByPipelineId("pipeline", after = second), limit = 2).map { it.reference },
        )
        assertEquals(
            listOf(second, third),
            store.replay(ResponseReplayQuery.ByPipelineId("pipeline", after = first), limit = 2).map { it.reference },
            "A pruned cursor still orders by the sequence it encodes",
        )
    }

    @Test
    fun `latest-per-target replay skips superseded responses and composes with the cursor and limit`() = runTest {
        val store = InMemoryResponseStore(maxResponses = 8)
        val oldA = store.put(response("a", "pipeline", Instant.EPOCH))!!
        val onlyB = store.put(response("b", "pipeline", Instant.EPOCH))!!
        val otherA = store.put(response("a", "other-pipeline", Instant.EPOCH))!!
        val newA = store.put(response("a", "pipeline", Instant.EPOCH))!!
        val onlyC = store.put(response("c", "pipeline", Instant.EPOCH))!!

        val latest = ResponseReplayQuery.ByPipelineId("pipeline", versions = ResponseReplayVersions.LATEST_PER_TARGET)
        assertEquals(listOf(onlyB, newA, onlyC), store.replay(latest, limit = 8).map { it.reference })
        assertEquals(listOf(onlyB), store.replay(latest, limit = 1).map { it.reference }, "Superseded rows do not consume the limit")
        assertEquals(
            listOf(newA, onlyC),
            store.replay(latest.copy(after = onlyB), limit = 8).map { it.reference },
        )
        assertEquals(
            listOf(oldA, onlyB, newA, onlyC),
            store.replay(ResponseReplayQuery.ByPipelineId("pipeline"), limit = 8).map { it.reference },
        )
        assertEquals(
            listOf(otherA),
            store.replay(latest.copy(pipelineId = "other-pipeline"), limit = 8).map { it.reference },
            "Latest-version selection is scoped to the selected pipeline",
        )
    }

    @Test
    fun `pipeline replay rejects cursors this store did not issue`() = runTest {
        val store = InMemoryResponseStore()
        store.put(response("a", "pipeline", Instant.EPOCH))
        listOf("file-response-1", "memory-response-", "memory-response-0", "memory-response-x").forEach { value ->
            assertEquals(
                "Replay cursor was not issued by in-memory response store 'memory-responses'",
                assertFailsWith<IllegalArgumentException>(value) {
                    store.replay(
                        ResponseReplayQuery.ByPipelineId("pipeline", after = com.rmjtromp.trawler.model.ResponseReference(value)),
                        limit = 1,
                    )
                }.message,
            )
        }
    }

    @Test
    fun `eviction and pruning use an exclusive age cutoff and bounded protected deletion`() = runTest {
        assertFailsWith<IllegalArgumentException> { InMemoryResponseStore(maxResponses = 0) }
        assertFailsWith<IllegalArgumentException> { InMemoryResponseStore(maxBodyBytes = 0) }
        val start = Instant.parse("2026-01-01T00:00:00Z")
        val evictStore = InMemoryResponseStore(maxResponses = 4)
        val old = evictStore.put(response("old", "pipeline", start))!!
        val exact = evictStore.put(response("exact", "pipeline", start.plusSeconds(1)))!!
        assertEquals(1L, evictStore.evictBefore(start.plusSeconds(1)))
        assertNull(evictStore.find(ResponseLookup.ByReference(old)))
        assertNotNull(evictStore.find(ResponseLookup.ByReference(exact)))

        val pruneStore = InMemoryResponseStore(maxResponses = 4)
        val protected = pruneStore.put(response("protected", "pipeline", start))!!
        val removable = pruneStore.put(response("removable", "pipeline", start.plusSeconds(1)))!!
        val cutoff = pruneStore.put(response("cutoff", "pipeline", start.plusSeconds(2)))!!
        val newer = pruneStore.put(response("newer", "pipeline", start.plusSeconds(3)))!!
        val result = pruneStore.prune(
            ResponseRetentionRequest(
                cutoff = start.plusSeconds(2),
                maxEntries = 1,
                protectedReferences = setOf(protected),
            ),
        )
        assertEquals(ResponseRetentionResult(deleted = 1, protectedEntries = 1), result)
        assertNotNull(pruneStore.find(ResponseLookup.ByReference(protected)))
        assertNull(pruneStore.find(ResponseLookup.ByReference(removable)))
        assertNotNull(pruneStore.find(ResponseLookup.ByReference(cutoff)))
        assertNotNull(pruneStore.find(ResponseLookup.ByReference(newer)))
    }

    @Test
    fun `concurrent puts remain bounded and return distinct owned references`() = runTest {
        val store = InMemoryResponseStore(maxResponses = 3)
        val references = coroutineScope {
            (1..12).map { index ->
                async(start = CoroutineStart.UNDISPATCHED) {
                    store.put(response("target-$index", "pipeline", Instant.ofEpochSecond(index.toLong())))
                }
            }.awaitAll()
        }
        assertTrue(references.all { it != null })
        assertEquals(12, references.filterNotNull().distinct().size)
        val retained = store.replay(ResponseReplayQuery.ByPipelineId("pipeline"), limit = 20)
        assertEquals(3, retained.size)
        assertEquals(3, retained.map { it.reference }.distinct().size)
    }

    @Test
    fun `explicit no-response store retains nothing and advertises no capability`() = runTest {
        assertEquals("no-response", NoResponseStore.id)
        assertTrue(NoResponseStore.capabilities.isEmpty())
        assertNull(NoResponseStore.put(response("target", "pipeline", Instant.EPOCH)))
        assertNull(NoResponseStore.find(ResponseLookup.ByTarget("target")))
        assertNull(NoResponseStore.find(ResponseLookup.ByRequestId("request-target")))
    }

    @Test
    fun `lookups miss unknown keys and responses without engine metadata are not attributed`() = runTest {
        val store = InMemoryResponseStore(id = "custom-memory")
        assertEquals("custom-memory", store.id)
        assertEquals(
            setOf(
                ResponseStoreCapability.LOOKUP,
                ResponseStoreCapability.CACHE,
                ResponseStoreCapability.HISTORY,
                ResponseStoreCapability.REPLAY,
                ResponseStoreCapability.TTL_EVICTION,
            ),
            store.capabilities,
        )
        val anonymous = store.put(
            CrawlResponse(
                request = CrawlRequest("anonymous"),
                statusCode = 200,
                headers = Headers.empty(),
                body = ResponseBody.from(byteArrayOf(1)),
                receivedAt = Instant.EPOCH,
            ),
        )!!
        val owned = store.put(response("owned", "pipeline", Instant.EPOCH, "request-owned"))!!

        assertNull(store.find(ResponseLookup.ByReference(com.rmjtromp.trawler.model.ResponseReference("missing"))))
        assertNull(store.find(ResponseLookup.ByTarget("missing")))
        assertNull(store.find(ResponseLookup.ByRequestId("missing")))
        assertNull(store.find(ResponseLookup.ByReference(anonymous))!!.pipelineId)
        assertEquals("pipeline", store.find(ResponseLookup.ByReference(owned))!!.pipelineId)
        assertTrue(store.history("missing", 1).isEmpty())
        assertEquals(
            listOf(owned),
            store.replay(ResponseReplayQuery.ByPipelineId("pipeline"), limit = 10).map { it.reference },
        )
    }

    @Test
    fun `replay supports reference and request queries under an ownership predicate`() = runTest {
        val store = InMemoryResponseStore()
        val first = store.put(response("first", "pipeline-a", Instant.EPOCH, "shared"))!!
        val second = store.put(response("second", "pipeline-b", Instant.EPOCH, "shared"))!!

        assertEquals(
            listOf(first),
            store.replay(ResponseReplayQuery.ByReference(first), limit = 5).map { it.reference },
        )
        assertTrue(store.replay(ResponseReplayQuery.ByReference(first), limit = 5, pipelineId = "pipeline-b").isEmpty())
        assertEquals(
            listOf(first, second),
            store.replay(ResponseReplayQuery.ByRequestId("shared"), limit = 5).map { it.reference },
        )
        assertEquals(
            listOf(second),
            store.replay(ResponseReplayQuery.ByRequestId("shared"), limit = 5, pipelineId = "pipeline-b")
                .map { it.reference },
        )
        assertTrue(store.replay(ResponseReplayQuery.ByRequestId("other"), limit = 5).isEmpty())
    }

    @Test
    fun `invalid store identity and query arguments are rejected before reading state`() = runTest {
        val store = InMemoryResponseStore()
        assertFailsWith<IllegalArgumentException> { InMemoryResponseStore(id = " ") }
        assertEquals(
            "Replay pipeline ID must not be blank",
            assertFailsWith<IllegalArgumentException> {
                store.replay(ResponseReplayQuery.ByPipelineId("pipeline"), limit = 1, pipelineId = " ")
            }.message,
        )
        assertEquals(
            "History limit must be positive",
            assertFailsWith<IllegalArgumentException> { store.history("request", 0) }.message,
        )
        assertEquals(
            "Response request ID must not be blank",
            assertFailsWith<IllegalArgumentException> { store.history(" ", 1) }.message,
        )
    }

    private fun response(
        target: String,
        pipelineId: String,
        receivedAt: Instant,
        requestId: String = "request-$target",
        body: ByteArray = byteArrayOf(1),
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
}
