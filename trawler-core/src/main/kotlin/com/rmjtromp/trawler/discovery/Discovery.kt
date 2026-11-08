package com.rmjtromp.trawler.discovery

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.state.CheckpointResolution
import com.rmjtromp.trawler.state.DiscoveryCheckpoint
import com.rmjtromp.trawler.state.DiscoveryCheckpointCodecDescriptor
import com.rmjtromp.trawler.transport.RequestExecutor
import kotlinx.coroutines.flow.Flow
import com.rmjtromp.trawler.policy.IncrementalBoundaryPolicy

/**
 * Produces crawl work or already-obtained responses for one pipeline.
 *
 * Discovery is an extension-owned policy and does not own a transport client.
 * When it needs I/O it must use the engine-provided [DiscoveryContext.executor]
 * so routing, middleware, safety, pacing, observability, cancellation, and
 * attempt accounting remain visible to the engine. The returned flow is
 * collected in the caller's structured scope; implementations must honor
 * cancellation and must not create unbounded internal buffers.
 */
public interface Discovery {
    /** Stable identifier unique within one pipeline's ordered discovery list. */
    public val id: String

    /** Emits requests or responses in the discovery-defined order. */
    public fun discover(context: DiscoveryContext): Flow<DiscoveryEmission>
}

/** Context and engine-controlled I/O surface supplied to [Discovery]. */
public data class DiscoveryContext(
    public val crawler: CrawlerContext,
    public val executor: RequestExecutor,
    /**
     * The last compatible checkpoint loaded for this discovery, or `null` for
     * a first walk. The payload is opaque to core; a [ResumableDiscovery]'s
     * codec owns interpretation and migration. The context is valid only for
     * this structured discovery call and must not be retained after it ends.
     */
    public val checkpoint: DiscoveryCheckpoint? = null,
    /** The current recurring boundary, when the selected harvest strategy provides one. */
    public val incrementalWindow: IncrementalBoundaryPolicy.Window? = null,
)

/**
 * A discovery that can resume from an engine-owned opaque checkpoint.
 *
 * The discovery owns the codec and therefore owns the meaning of the payload.
 * The engine only validates the descriptor, applies compare-and-set revision
 * rules, and persists a candidate after its emitted work is accepted. A
 * discovery must honor cancellation and must not create a transport client.
 */
public interface ResumableDiscovery : Discovery {
    /** Codec identity used to validate and migrate stored checkpoints. */
    public val checkpointCodec: DiscoveryCheckpointCodec
}

/**
 * Codec boundary for opaque discovery checkpoints.
 *
 * Implementations may return [CheckpointResolution.Migrated] when they can
 * safely convert an older payload. Returning [CheckpointResolution.Reset] or
 * [CheckpointResolution.Quarantine] makes incompatibility explicit; core
 * never guesses at a payload format. Codec calls are local, bounded, and do
 * not own network or database resources.
 */
public interface DiscoveryCheckpointCodec {
    /** Stable codec ID and version persisted beside each payload. */
    public val descriptor: DiscoveryCheckpointCodecDescriptor

    /** Stable discovery/strategy mode used to reject incompatible state. */
    public val strategyId: String

    /** Resolves a loaded checkpoint before discovery starts. */
    public fun resolve(checkpoint: DiscoveryCheckpoint): CheckpointResolution {
        return if (checkpoint.codecId == descriptor.id &&
            checkpoint.codecVersion == descriptor.version &&
            checkpoint.strategyId == strategyId
        ) {
            CheckpointResolution.Use(checkpoint)
        } else {
            CheckpointResolution.Quarantine(
                "Checkpoint descriptor or strategy does not match " +
                    "${descriptor.id}@${descriptor.version}/$strategyId",
            )
        }
    }
}

/**
 * Generic pagination hook for custom discovery implementations.
 *
 * The strategy receives one already-fetched response and returns at most one
 * next request; the owning discovery controls the engine-provided emission
 * and page bounds. It never owns a transport client, persistence, or retry
 * loop, and it must remain cancellation-cooperative.
 */
public interface PaginationStrategy {
    /** Stable identifier used for configuration and checkpoint compatibility. */
    public val id: String

    /** Selects the next page or declares that pagination has ended. */
    public fun next(
        request: CrawlRequest,
        response: CrawlResponse,
        context: DiscoveryContext,
    ): PaginationDecision
}

/** Expected result from a [PaginationStrategy]. */
public sealed interface PaginationDecision {
    /** No further page is available. */
    public data object Complete : PaginationDecision

    /** The caller may fetch this bounded next page through its executor. */
    public data class Next(public val request: CrawlRequest) : PaginationDecision
}

/**
 * A new opaque checkpoint payload emitted alongside one discovery item.
 * Revision and completion are assigned by the engine, never by discovery.
 */
public data class DiscoveryCheckpointCandidate(
    public val descriptor: DiscoveryCheckpointCodecDescriptor,
    public val strategyId: String,
    public val payload: com.rmjtromp.trawler.state.CheckpointPayload,
    public val generationId: String? = null,
) {
    init {
        require(strategyId.isNotBlank()) { "Checkpoint strategy ID must not be blank" }
        require(generationId == null || generationId.isNotBlank()) {
            "Checkpoint generation ID must not be blank when present"
        }
    }
}

/** Emission accepted from discovery before admission/deduplication policy. */
public sealed interface DiscoveryEmission {
    /** Checkpoint candidate to advance after this emission is accepted. */
    public val checkpoint: DiscoveryCheckpointCandidate?

    /** A new request that may become crawl work. */
    public data class Request(
        public val request: CrawlRequest,
        override val checkpoint: DiscoveryCheckpointCandidate? = null,
    ) : DiscoveryEmission

    /** A response already obtained by discovery and therefore not fetched again. */
    public data class ExistingResponse(
        public val request: CrawlRequest,
        public val response: CrawlResponse,
        override val checkpoint: DiscoveryCheckpointCandidate? = null,
    ) : DiscoveryEmission

    /**
     * Advances a [ResumableDiscovery] checkpoint without creating work.
     *
     * Emissions are processed in order, so every earlier emission has already
     * been accepted, suppressed, or rejected when this marker is handled. Use
     * it after a region that produced no new work (for example a page whose
     * items were all filtered or already known), so a resumed walk does not
     * repeat that region. A rejected earlier emission stops the walk before the
     * marker is reached. The marker counts toward the engine's discovery
     * emission bounds but is not reported to budget policies as a work
     * candidate. It is a configuration error for a non-resumable discovery.
     */
    public data class Checkpoint(
        override val checkpoint: DiscoveryCheckpointCandidate,
    ) : DiscoveryEmission
}
