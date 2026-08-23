package com.rmjtromp.trawler.discovery

import com.rmjtromp.trawler.model.CrawlRequest
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take

/**
 * Finite seed discovery backed by a caller-owned immutable list.
 *
 * The list is copied at construction and [maxEmissions] bounds what the
 * discovery can emit. It performs no I/O and owns no coroutine beyond the
 * collector's structured scope.
 */
public class SeedDiscovery(
    override val id: String = "seeds",
    requests: List<CrawlRequest>,
    maxEmissions: Int = requests.size,
) : Discovery {
    private val requests: List<CrawlRequest> = requests.toList()
    private val emissionLimit: Int = maxEmissions

    init {
        require(id.isNotBlank()) { "Seed discovery ID must not be blank" }
        require(maxEmissions >= 0) { "Seed discovery emission limit must not be negative" }
    }

    override fun discover(context: DiscoveryContext): Flow<DiscoveryEmission> = flow {
        requests.asSequence()
            .take(emissionLimit)
            .forEach { emit(DiscoveryEmission.Request(it)) }
    }
}

/**
 * Adapts a caller-provided request flow into engine-controlled discovery.
 *
 * [maxEmissions] is mandatory in effect even when the upstream flow is
 * infinite: collection uses `take` and therefore never creates an unbounded
 * discovery buffer. Upstream cancellation propagates from the engine.
 */
public class FlowDiscovery(
    override val id: String = "flow-discovery",
    private val requests: Flow<CrawlRequest>,
    private val maxEmissions: Int = 1_024,
) : Discovery {
    init {
        require(id.isNotBlank()) { "Flow discovery ID must not be blank" }
        require(maxEmissions > 0) { "Flow discovery emission limit must be positive" }
    }

    override fun discover(context: DiscoveryContext): Flow<DiscoveryEmission> =
        requests.take(maxEmissions).map { DiscoveryEmission.Request(it) }
}
