package com.rmjtromp.trawler.discovery

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.policy.IncrementalBoundaryPolicy
import com.rmjtromp.trawler.state.CheckpointPayload
import com.rmjtromp.trawler.state.CheckpointResolution
import com.rmjtromp.trawler.state.DiscoveryCheckpoint
import com.rmjtromp.trawler.state.DiscoveryCheckpointCodecDescriptor
import com.rmjtromp.trawler.state.HarvestState
import com.rmjtromp.trawler.state.HarvestStateResolution
import com.rmjtromp.trawler.transport.RequestExecutor
import java.nio.ByteBuffer
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class DiscoveryHarvestTest {
    @Test
    fun `seed discovery snapshots requests and respects zero exact and above limits`() = runTest {
        val requests = mutableListOf(
            CrawlRequest("one"),
            CrawlRequest("two"),
            CrawlRequest("three"),
        )
        val discovery = SeedDiscovery(requests = requests, maxEmissions = 2)
        requests.clear()

        val emissions = discovery.discover(discoveryContext()).toList()
        assertEquals(
            listOf("one", "two"),
            emissions.map { (it as DiscoveryEmission.Request).request.target },
        )
        assertEquals(0, SeedDiscovery(requests = listOf(CrawlRequest("one")), maxEmissions = 0)
            .discover(discoveryContext()).toList().size)
        assertEquals(3, SeedDiscovery(requests = listOf(CrawlRequest("one"), CrawlRequest("two"), CrawlRequest("three")), maxEmissions = 5)
            .discover(discoveryContext()).toList().size)
        assertFailsWith<IllegalArgumentException> {
            SeedDiscovery(requests = emptyList(), maxEmissions = -1)
        }
    }

    @Test
    fun `flow discovery applies a finite emission bound`() = runTest {
        val requests = flow {
            emit(CrawlRequest("one"))
            emit(CrawlRequest("two"))
            emit(CrawlRequest("three"))
        }

        val emissions = FlowDiscovery(requests = requests, maxEmissions = 2)
            .discover(discoveryContext())
            .toList()

        assertEquals(2, emissions.size)
        assertEquals("two", (emissions[1] as DiscoveryEmission.Request).request.target)
        assertFailsWith<IllegalArgumentException> {
            FlowDiscovery(requests = flow { emit(CrawlRequest("one")) }, maxEmissions = 0)
        }
    }

    @Test
    fun `flow discovery cancellation reaches the upstream flow without arbitrary timing`() = runTest {
        val upstreamStarted = CompletableDeferred<Unit>()
        val upstreamCancelled = CompletableDeferred<Unit>()
        val emitted = mutableListOf<String>()
        val upstream = flow {
            try {
                emit(CrawlRequest("one"))
                upstreamStarted.complete(Unit)
                awaitCancellation()
            } finally {
                upstreamCancelled.complete(Unit)
            }
        }
        val job = launch(start = CoroutineStart.UNDISPATCHED) {
            FlowDiscovery(requests = upstream, maxEmissions = 10)
                .discover(discoveryContext())
                .collect { emitted += (it as DiscoveryEmission.Request).request.target }
        }

        upstreamStarted.await()
        job.cancelAndJoin()

        assertEquals(listOf("one"), emitted)
        assertTrue(upstreamCancelled.isCompleted)
    }

    @Test
    fun `checkpoint codec compatibility is exact and incompatible state is quarantined`() {
        val descriptor = DiscoveryCheckpointCodecDescriptor("feed", 2)
        val codec = object : DiscoveryCheckpointCodec {
            override val descriptor: DiscoveryCheckpointCodecDescriptor = descriptor
            override val strategyId: String = "cursor"
        }
        val compatible = DiscoveryCheckpoint(
            revision = 0,
            codecId = "feed",
            codecVersion = 2,
            payload = CheckpointPayload.from(byteArrayOf(1)),
            strategyId = "cursor",
        )

        assertIs<CheckpointResolution.Use>(codec.resolve(compatible))
        val wrongVersion = codec.resolve(compatible.copy(codecVersion = 1))
        assertIs<CheckpointResolution.Quarantine>(wrongVersion)
        val wrongStrategy = codec.resolve(compatible.copy(strategyId = "other"))
        assertIs<CheckpointResolution.Quarantine>(wrongStrategy)
        assertTrue((wrongVersion as CheckpointResolution.Quarantine).reason.contains("feed@2"))
    }

    @Test
    fun `harvest completion strategies require every completion condition`() {
        assertEquals(HarvestAdmission.Run, StatelessHarvestStrategy.admit(harvestContext(Instant.EPOCH)))
        assertIs<HarvestCompletion.Commit>(
            StatelessHarvestStrategy.complete(completionContext(outcome = HarvestRunOutcome.ABANDONED)),
        )
        val resumableStrategies = listOf<HarvestStrategy>(
            CursorHarvestStrategy(),
            WatermarkHarvestStrategy(FixedOverlapBoundaryPolicy(Duration.ZERO)),
            HistoricalThenIncrementalHarvestStrategy(FixedOverlapBoundaryPolicy(Duration.ZERO)),
        )
        resumableStrategies.forEach { strategy ->
            assertEquals(HarvestAdmission.Run, strategy.admit(harvestContext(Instant.EPOCH)))
        }
        val failures = listOf<HarvestCompletionContext.() -> HarvestCompletionContext>(
            { copy(discoveryReachedNaturalEnd = false) },
            { copy(checkpointWorkSettled = false) },
            { copy(generationWorkOpen = 1) },
            { copy(runOutcome = HarvestRunOutcome.ABANDONED) },
            { copy(completionPredicatePassed = false) },
        )

        resumableStrategies.forEach { strategy ->
            val complete = strategy.complete(completionContext())
            assertIs<HarvestCompletion.Commit>(complete)
            failures.forEach { change ->
                val incomplete = strategy.complete(change(completionContext()))
                assertIs<HarvestCompletion.Incomplete>(incomplete)
            }
        }
    }

    @Test
    fun `stateless cursor watermark and historical completion carry the right state`() {
        val now = Instant.parse("2026-01-01T00:00:00Z")
        val stateless = StatelessHarvestStrategy.complete(completionContext(now, HarvestRunOutcome.FAILED))
        assertIs<HarvestCompletion.Commit>(stateless)
        assertEquals(null, (stateless as HarvestCompletion.Commit).state)

        val cursor = CursorHarvestStrategy().complete(completionContext(now))
        assertIs<HarvestCompletion.Commit>(cursor)
        assertEquals(null, (cursor as HarvestCompletion.Commit).state)

        val watermark = WatermarkHarvestStrategy(FixedOverlapBoundaryPolicy(Duration.ZERO))
        val watermarkCompletion = watermark.complete(completionContext(now))
        assertIs<HarvestCompletion.Commit>(watermarkCompletion)
        val watermarkState = (watermarkCompletion as HarvestCompletion.Commit).state
        assertEquals(false, watermarkState!!.historicalComplete)
        assertEquals(watermark.id, watermarkState.strategyId)
        assertEquals(now, Instant.ofEpochMilli(ByteBuffer.wrap(watermarkState.payload.toByteArray()).long))

        val historical = HistoricalThenIncrementalHarvestStrategy(FixedOverlapBoundaryPolicy(Duration.ZERO))
        val historicalCompletion = historical.complete(completionContext(now))
        assertIs<HarvestCompletion.Commit>(historicalCompletion)
        assertEquals(
            true,
            (historicalCompletion as HarvestCompletion.Commit).state!!.historicalComplete,
        )
    }

    @Test
    fun `watermark and historical windows honor overlap and historical completion`() {
        val now = Instant.parse("2026-01-01T00:00:20Z")
        val previous = Instant.parse("2026-01-01T00:00:10Z")
        val overlap = FixedOverlapBoundaryPolicy(Duration.ofSeconds(3))
        val watermark = WatermarkHarvestStrategy(overlap)
        val watermarkState = HarvestState(
            revision = 0,
            strategyId = watermark.id,
            codecId = watermark.stateCodec!!.descriptor.id,
            codecVersion = watermark.stateCodec!!.descriptor.version,
            payload = timestampPayload(previous),
            historicalComplete = false,
        )

        assertEquals(
            IncrementalBoundaryPolicy.Window(null, now),
            watermark.window(harvestContext(now)),
        )
        val watermarkWindow = watermark.window(harvestContext(now, watermarkState))
        assertEquals(previous.minusSeconds(3), watermarkWindow.startInclusive)
        assertEquals(now, watermarkWindow.endExclusive)

        val historical = HistoricalThenIncrementalHarvestStrategy(overlap)
        val historicalIncomplete = HarvestState(
            revision = 0,
            strategyId = historical.id,
            codecId = historical.stateCodec!!.descriptor.id,
            codecVersion = historical.stateCodec!!.descriptor.version,
            payload = timestampPayload(previous),
            historicalComplete = false,
        )
        val historicalComplete = historicalIncomplete.copy(historicalComplete = true)
        assertEquals(null, historical.window(harvestContext(now, historicalIncomplete)))
        assertEquals(
            previous.minusSeconds(3),
            historical.window(harvestContext(now, historicalComplete))!!.startInclusive,
        )
    }

    @Test
    fun `harvest codecs require exact strategy and descriptor compatibility`() {
        val strategy = WatermarkHarvestStrategy(FixedOverlapBoundaryPolicy(Duration.ZERO))
        val codec = strategy.stateCodec!!
        val compatible = HarvestState(
            revision = 0,
            strategyId = strategy.id,
            codecId = codec.descriptor.id,
            codecVersion = codec.descriptor.version,
            payload = timestampPayload(Instant.EPOCH),
        )

        assertIs<HarvestStateResolution.Use>(codec.resolve(compatible))
        assertIs<HarvestStateResolution.Quarantine>(codec.resolve(compatible.copy(codecVersion = 0)))
        assertIs<HarvestStateResolution.Quarantine>(codec.resolve(compatible.copy(strategyId = "other")))
        assertFailsWith<IllegalArgumentException> {
            strategy.window(harvestContext(Instant.EPOCH, compatible.copy(payload = CheckpointPayload.from(byteArrayOf(1)))))
        }
        assertFailsWith<IllegalArgumentException> { FixedOverlapBoundaryPolicy(Duration.ofNanos(-1)) }
    }

    @Test
    fun `harvest constructors reject invalid bounded inputs`() {
        assertFailsWith<IllegalArgumentException> { HarvestContext(" ", "discovery", null, null, 0, Instant.EPOCH) }
        assertFailsWith<IllegalArgumentException> { HarvestContext("pipeline", "discovery", null, null, -1, Instant.EPOCH) }
        assertFailsWith<IllegalArgumentException> {
            HarvestCompletionContext(true, true, -1, HarvestRunOutcome.COMPLETED, true, Instant.EPOCH)
        }
        assertFailsWith<IllegalArgumentException> { HarvestAdmission.Defer(" ") }
        assertFailsWith<IllegalArgumentException> { HarvestCompletion.Incomplete(" ") }
    }

    private fun discoveryContext(): DiscoveryContext = DiscoveryContext(
        crawler = CrawlerContext("engine", "pipeline-run", "pipeline"),
        executor = RequestExecutor { _, _ -> error("discovery test must not execute transport") },
    )

    private fun completionContext(
        now: Instant = Instant.EPOCH,
        outcome: HarvestRunOutcome = HarvestRunOutcome.COMPLETED,
    ): HarvestCompletionContext = HarvestCompletionContext(
        discoveryReachedNaturalEnd = true,
        checkpointWorkSettled = true,
        generationWorkOpen = 0,
        runOutcome = outcome,
        completionPredicatePassed = true,
        now = now,
    )

    private fun harvestContext(now: Instant, state: HarvestState? = null): HarvestContext = HarvestContext(
        pipelineId = "pipeline",
        discoveryId = "discovery",
        checkpoint = null,
        state = state,
        openWorkCount = 0,
        now = now,
    )

    private fun timestampPayload(value: Instant): CheckpointPayload = CheckpointPayload.from(
        ByteBuffer.allocate(Long.SIZE_BYTES).putLong(value.toEpochMilli()).array(),
    )
}
