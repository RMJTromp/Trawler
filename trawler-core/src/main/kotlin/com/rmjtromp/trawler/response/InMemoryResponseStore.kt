package com.rmjtromp.trawler.response

import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.ResponseReference
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Instant

/**
 * Explicit no-op response store. A missing response store and this object
 * have the same ordinary crawling behavior; this type is useful when a host
 * wants the no-retention choice to be visible in its immutable configuration.
 */
public object NoResponseStore : ResponseStore {
    override val id: String = "no-response"
    override val capabilities: Set<ResponseStoreCapability> = emptySet()

    override suspend fun put(response: CrawlResponse): ResponseReference? = null

    override suspend fun find(query: ResponseLookup): StoredResponse? = null
}

/**
 * Bounded process-local response retention for small database-free runs.
 *
 * Bodies are retained only when they are within [maxBodyBytes]. The store
 * evicts the oldest entries when [maxResponses] is reached. It supports
 * lookup, bounded replay, history, and age-based eviction, but it provides no
 * restart or durability guarantee. The mutex owns store state; calls are
 * cancellation-cooperative and no caller resource is retained after return.
 */
public class InMemoryResponseStore(
    override val id: String = "memory-responses",
    public val maxResponses: Int = 1_024,
    public val maxBodyBytes: Int = 1_048_576,
) : ResponseStore,
    ResponseReplayStore,
    ResponseHistoryStore,
    ResponseRetentionStore {

    private val mutex = Mutex()
    private var nextReference: Long = 0
    private val entries = LinkedHashMap<ResponseReference, StoredResponse>()

    override val capabilities: Set<ResponseStoreCapability> = setOf(
        ResponseStoreCapability.LOOKUP,
        ResponseStoreCapability.CACHE,
        ResponseStoreCapability.HISTORY,
        ResponseStoreCapability.REPLAY,
        ResponseStoreCapability.TTL_EVICTION,
    )

    init {
        require(id.isNotBlank()) { "In-memory response store ID must not be blank" }
        require(maxResponses > 0) { "In-memory response capacity must be positive" }
        require(maxBodyBytes > 0) { "In-memory response body limit must be positive" }
    }

    override suspend fun put(response: CrawlResponse): ResponseReference? = mutex.withLock {
        if (response.body.size > maxBodyBytes) return@withLock null
        while (entries.size >= maxResponses) {
            val oldest = entries.entries.firstOrNull()?.key ?: break
            entries.remove(oldest)
        }
        val reference = ResponseReference("$REFERENCE_PREFIX${++nextReference}")
        entries[reference] = StoredResponse(reference, response)
        reference
    }

    override suspend fun find(query: ResponseLookup): StoredResponse? = mutex.withLock {
        when (query) {
            is ResponseLookup.ByReference -> entries[query.reference]
            is ResponseLookup.ByTarget -> entries.values.lastOrNull {
                it.response.request.target == query.target
            }

            is ResponseLookup.ByRequestId -> entries.values.lastOrNull {
                it.response.request.attributes[REQUEST_ID_ATTRIBUTE] == query.requestId
            }
        }
    }

    override suspend fun replay(query: ResponseReplayQuery, limit: Int, pipelineId: String?): List<StoredResponse> = mutex.withLock {
        require(limit > 0) { "Replay limit must be positive" }
        require(pipelineId == null || pipelineId.isNotBlank()) { "Replay pipeline ID must not be blank" }
        val owned = entries.values.asSequence()
            .filter { stored -> (pipelineId == null || stored.pipelineId == pipelineId) && matches(stored, query) }
        when (query) {
            is ResponseReplayQuery.ByPipelineId -> {
                val afterSequence = query.after?.let(::sequenceOf)
                val latestByTarget = if (query.versions == ResponseReplayVersions.LATEST_PER_TARGET) {
                    owned.associate { it.response.request.target to it.reference }
                } else {
                    null
                }
                owned
                    .filter { afterSequence == null || sequenceOf(it.reference) > afterSequence }
                    .filter { latestByTarget == null || latestByTarget[it.response.request.target] == it.reference }
                    .take(limit)
                    .toList()
            }

            else -> owned.take(limit).toList()
        }
    }

    override suspend fun history(requestId: String, limit: Int): List<StoredResponse> = mutex.withLock {
        require(requestId.isNotBlank()) { "Response request ID must not be blank" }
        require(limit > 0) { "History limit must be positive" }
        entries.values.asSequence()
            .filter { it.response.request.attributes[REQUEST_ID_ATTRIBUTE] == requestId }
            .take(limit)
            .toList()
    }

    override suspend fun evictBefore(cutoff: Instant): Long = mutex.withLock {
        val old = entries.values.filter { it.response.receivedAt.isBefore(cutoff) }.map { it.reference }
        old.forEach(entries::remove)
        old.size.toLong()
    }

    override suspend fun prune(request: ResponseRetentionRequest): ResponseRetentionResult = mutex.withLock {
        val candidates = entries.values
            .filter { it.response.receivedAt.isBefore(request.cutoff) }
        val protected = candidates.count { it.reference in request.protectedReferences }
        val removable = candidates
            .asSequence()
            .filterNot { it.reference in request.protectedReferences }
            .take(request.maxEntries)
            .map { it.reference }
            .toList()
        removable.forEach(entries::remove)
        ResponseRetentionResult(
            deleted = removable.size.toLong(),
            protectedEntries = protected.toLong(),
        )
    }

    private fun matches(stored: StoredResponse, query: ResponseReplayQuery): Boolean = when (query) {
        is ResponseReplayQuery.ByReference -> stored.reference == query.reference
        is ResponseReplayQuery.ByRequestId ->
            stored.response.request.attributes[REQUEST_ID_ATTRIBUTE] == query.requestId

        is ResponseReplayQuery.ByPipelineId ->
            stored.response.request.attributes[PIPELINE_ID_ATTRIBUTE] == query.pipelineId
    }

    /** References are issued from a monotonic counter, so their suffix is the store order. */
    private fun sequenceOf(reference: ResponseReference): Long =
        reference.value.removePrefix(REFERENCE_PREFIX)
            .takeIf { reference.value.startsWith(REFERENCE_PREFIX) }
            ?.toLongOrNull()
            ?.takeIf { it > 0 }
            ?: throw IllegalArgumentException("Replay cursor was not issued by in-memory response store '$id'")

    public companion object {
        private const val REFERENCE_PREFIX: String = "memory-response-"

        /** Internal metadata names used only when the engine supplies them. */
        public const val REQUEST_ID_ATTRIBUTE: String = "trawler.request-id"
        public const val PIPELINE_ID_ATTRIBUTE: String = "trawler.pipeline-id"
    }
}
