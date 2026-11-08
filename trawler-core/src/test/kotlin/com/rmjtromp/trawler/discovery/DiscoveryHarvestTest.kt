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
import kotlin.test.assertNotNull
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
        assertTrue(assertIs<CheckpointResolution.Quarantine>(wrongVersion).reason.contains("feed@2"))
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
    fun `settled item failures commit only for strategies that explicitly allow them`() {
        val settled = completionContext().copy(
            runOutcome = HarvestRunOutcome.COMPLETED_WITH_SETTLED_ITEM_FAILURES,
            completionPredicatePassed = false,
            settledItemFailures = 2,
        )
        val overlap = FixedOverlapBoundaryPolicy(Duration.ZERO)
        listOf<HarvestStrategy>(
            CursorHarvestStrategy(),
            WatermarkHarvestStrategy(overlap),
            HistoricalThenIncrementalHarvestStrategy(overlap),
        ).forEach { strategy ->
            assertIs<HarvestCompletion.Incomplete>(strategy.complete(settled), "${strategy.id} blocks by default")
        }
        val allow = SettledItemFailurePolicy.ALLOWS_COMPLETION
        listOf<HarvestStrategy>(
            CursorHarvestStrategy(settledItemFailures = allow),
            WatermarkHarvestStrategy(overlap, settledItemFailures = allow),
            HistoricalThenIncrementalHarvestStrategy(overlap, settledItemFailures = allow),
        ).forEach { strategy ->
            assertIs<HarvestCompletion.Commit>(strategy.complete(settled), strategy.id)
            assertIs<HarvestCompletion.Commit>(strategy.complete(completionContext()), strategy.id)
            listOf<HarvestCompletionContext.() -> HarvestCompletionContext>(
                { copy(discoveryReachedNaturalEnd = false) },
                { copy(checkpointWorkSettled = false) },
                { copy(generationWorkOpen = 1) },
                { copy(runOutcome = HarvestRunOutcome.FAILED, settledItemFailures = 0) },
                { copy(runOutcome = HarvestRunOutcome.ABANDONED, settledItemFailures = 0) },
                { copy(runOutcome = HarvestRunOutcome.CANCELLED, settledItemFailures = 0) },
            ).forEach { change ->
                assertIs<HarvestCompletion.Incomplete>(strategy.complete(change(settled)), strategy.id)
            }
        }
    }

    @Test
    fun `watermarks commit the end of the searched window rather than the completion time`() {
        val searchedUntil = Instant.parse("2026-01-01T00:00:00Z")
        val completedAt = searchedUntil.plus(Duration.ofHours(2))
        val searched = completionContext(completedAt).copy(
            incrementalWindow = IncrementalBoundaryPolicy.Window(searchedUntil.minusSeconds(60), searchedUntil),
        )
        fun boundary(completion: HarvestCompletion): Instant = Instant.ofEpochMilli(
            ByteBuffer.wrap(assertNotNull(assertIs<HarvestCompletion.Commit>(completion).state).payload.toByteArray()).long,
        )
        val overlap = FixedOverlapBoundaryPolicy(Duration.ofMinutes(5))
        assertEquals(searchedUntil, boundary(WatermarkHarvestStrategy(overlap).complete(searched)))
        assertEquals(
            searchedUntil,
            boundary(HistoricalThenIncrementalHarvestStrategy(overlap).complete(searched)),
            "The incremental phase commits its searched window end",
        )
        assertEquals(
            completedAt,
            boundary(HistoricalThenIncrementalHarvestStrategy(overlap).complete(completionContext(completedAt))),
            "The historical phase has no window and records its completion time",
        )
    }

    @Test
    fun `stateless cursor watermark and historical completion carry the right state`() {
        val now = Instant.parse("2026-01-01T00:00:00Z")
        val stateless = StatelessHarvestStrategy.complete(completionContext(now, HarvestRunOutcome.FAILED))
        assertIs<HarvestCompletion.Commit>(stateless)
        assertEquals(null, assertIs<HarvestCompletion.Commit>(stateless).state)

        val cursor = CursorHarvestStrategy().complete(completionContext(now))
        assertEquals(null, assertIs<HarvestCompletion.Commit>(cursor).state)

        val watermark = WatermarkHarvestStrategy(FixedOverlapBoundaryPolicy(Duration.ZERO))
        val watermarkCompletion = watermark.complete(completionContext(now))
        assertIs<HarvestCompletion.Commit>(watermarkCompletion)
        val watermarkState = assertNotNull(assertIs<HarvestCompletion.Commit>(watermarkCompletion).state)
        assertEquals(false, watermarkState.historicalComplete)
        assertEquals(watermark.id, watermarkState.strategyId)
        assertEquals(now, Instant.ofEpochMilli(ByteBuffer.wrap(watermarkState.payload.toByteArray()).long))

        val historical = HistoricalThenIncrementalHarvestStrategy(FixedOverlapBoundaryPolicy(Duration.ZERO))
        val historicalCompletion = historical.complete(completionContext(now))
        assertIs<HarvestCompletion.Commit>(historicalCompletion)
        assertEquals(
            true,
            assertNotNull(assertIs<HarvestCompletion.Commit>(historicalCompletion).state).historicalComplete,
        )
    }

    @Test
    fun `watermark and historical windows honor overlap and historical completion`() {
        val now = Instant.parse("2026-01-01T00:00:20Z")
        val previous = Instant.parse("2026-01-01T00:00:10Z")
        val overlap = FixedOverlapBoundaryPolicy(Duration.ofSeconds(3))
        val watermark = WatermarkHarvestStrategy(overlap)
        val watermarkCodec = assertNotNull(watermark.stateCodec)
        val watermarkState = HarvestState(
            revision = 0,
            strategyId = watermark.id,
            codecId = watermarkCodec.descriptor.id,
            codecVersion = watermarkCodec.descriptor.version,
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
        val historicalCodec = assertNotNull(historical.stateCodec)
        val historicalIncomplete = HarvestState(
            revision = 0,
            strategyId = historical.id,
            codecId = historicalCodec.descriptor.id,
            codecVersion = historicalCodec.descriptor.version,
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
        val codec = assertNotNull(strategy.stateCodec)
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
        assertFailsWith<IllegalArgumentException> {
            completionContext().copy(
                runOutcome = HarvestRunOutcome.COMPLETED_WITH_SETTLED_ITEM_FAILURES,
                settledItemFailures = -1,
            )
        }
        assertFailsWith<IllegalArgumentException> {
            completionContext().copy(settledItemFailures = 1)
        }
        assertFailsWith<IllegalArgumentException> { HarvestAdmission.Defer(" ") }
        assertFailsWith<IllegalArgumentException> { HarvestCompletion.Incomplete(" ") }
    }

    @Test
    fun `checkpoint codec quarantines a different codec ID with a descriptive reason`() {
        val codec = object : DiscoveryCheckpointCodec {
            override val descriptor: DiscoveryCheckpointCodecDescriptor = DiscoveryCheckpointCodecDescriptor("feed", 2)
            override val strategyId: String = "cursor"
        }
        val foreign = DiscoveryCheckpoint(
            revision = 3,
            codecId = "other-feed",
            codecVersion = 2,
            payload = CheckpointPayload.from(byteArrayOf(1)),
            strategyId = "cursor",
        )
        val quarantine = assertIs<CheckpointResolution.Quarantine>(codec.resolve(foreign))
        assertEquals("Checkpoint descriptor or strategy does not match feed@2/cursor", quarantine.reason)
        assertIs<CheckpointResolution.Quarantine>(codec.resolve(foreign.copy(codecId = "feed", strategyId = null)))
    }

    @Test
    fun `checkpoint candidates and emissions validate optional identifiers and default checkpoints`() {
        val descriptor = DiscoveryCheckpointCodecDescriptor("feed", 1)
        val payload = CheckpointPayload.from(byteArrayOf(9))
        val candidate = DiscoveryCheckpointCandidate(descriptor, "cursor", payload)
        assertEquals(null, candidate.generationId)
        assertEquals(payload, candidate.payload)
        assertEquals("generation", DiscoveryCheckpointCandidate(descriptor, "cursor", payload, "generation").generationId)
        listOf(
            "Checkpoint strategy ID must not be blank" to { DiscoveryCheckpointCandidate(descriptor, " ", payload) },
            "Checkpoint generation ID must not be blank when present" to {
                DiscoveryCheckpointCandidate(descriptor, "cursor", payload, "")
            },
            "Checkpoint generation ID must not be blank when present" to {
                DiscoveryCheckpointCandidate(descriptor, "cursor", payload, " ")
            },
        ).forEach { (message, construct) ->
            assertEquals(message, assertFailsWith<IllegalArgumentException> { construct() }.message)
        }

        val request = CrawlRequest("page")
        assertEquals(null, DiscoveryEmission.Request(request).checkpoint)
        assertEquals(candidate, DiscoveryEmission.Request(request, candidate).checkpoint)
        val response = com.rmjtromp.trawler.model.CrawlResponse(
            request = request,
            statusCode = 200,
            headers = com.rmjtromp.trawler.model.Headers.empty(),
            body = com.rmjtromp.trawler.model.ResponseBody.empty(),
            receivedAt = Instant.EPOCH,
        )
        val existing = DiscoveryEmission.ExistingResponse(request, response)
        assertEquals(null, existing.checkpoint)
        assertEquals(candidate, DiscoveryEmission.ExistingResponse(request, response, candidate).checkpoint)
        assertEquals(request, PaginationDecision.Next(request).request)

        val context = discoveryContext()
        assertEquals(null, context.checkpoint)
        assertEquals(null, context.incrementalWindow)
    }

    @Test
    fun `built-in discoveries reject blank identifiers`() {
        assertEquals(
            "Seed discovery ID must not be blank",
            assertFailsWith<IllegalArgumentException> { SeedDiscovery(id = " ", requests = emptyList()) }.message,
        )
        assertEquals(
            "Flow discovery ID must not be blank",
            assertFailsWith<IllegalArgumentException> {
                FlowDiscovery(id = "", requests = flow { emit(CrawlRequest("one")) })
            }.message,
        )
        assertEquals("seeds", SeedDiscovery(requests = emptyList()).id)
        assertEquals("flow-discovery", FlowDiscovery(requests = flow { emit(CrawlRequest("one")) }).id)
    }

    @Test
    fun `harvest values validate identifiers and bounded counters`() {
        val payload = CheckpointPayload.from(byteArrayOf())
        val candidate = HarvestStateCandidate("strategy", "codec", 0, payload, historicalComplete = false)
        assertEquals(0, candidate.codecVersion)
        listOf(
            "Harvest strategy ID must not be blank" to { HarvestStateCandidate(" ", "codec", 0, payload, false) },
            "Harvest state codec ID must not be blank" to { HarvestStateCandidate("strategy", "", 0, payload, false) },
            "Harvest state codec version must not be negative" to {
                HarvestStateCandidate("strategy", "codec", -1, payload, false)
            },
            "Discovery ID must not be blank" to { HarvestContext("pipeline", " ", null, null, 0, Instant.EPOCH) },
            "Cursor harvest strategy ID must not be blank" to { CursorHarvestStrategy(" ") },
            "Watermark harvest strategy ID must not be blank" to {
                WatermarkHarvestStrategy(FixedOverlapBoundaryPolicy(Duration.ZERO), id = "")
            },
            "Historical harvest strategy ID must not be blank" to {
                HistoricalThenIncrementalHarvestStrategy(FixedOverlapBoundaryPolicy(Duration.ZERO), id = " ")
            },
            "Incremental boundary policy ID must not be blank" to { FixedOverlapBoundaryPolicy(Duration.ZERO, id = " ") },
        ).forEach { (message, construct) ->
            assertEquals(message, assertFailsWith<IllegalArgumentException> { construct() }.message)
        }
        assertEquals("deferred", HarvestAdmission.Defer("deferred").reason)
        assertEquals("open", HarvestCompletion.Incomplete("open").reason)
        assertEquals("fixed-overlap-2000ms", FixedOverlapBoundaryPolicy(Duration.ofSeconds(2)).id)
        assertEquals(null, StatelessHarvestStrategy.stateCodec)
        assertEquals(null, StatelessHarvestStrategy.window(harvestContext(Instant.EPOCH)))
        assertEquals(null, CursorHarvestStrategy().stateCodec)
        assertEquals(HarvestMode.CURSOR, CursorHarvestStrategy().mode)
        assertEquals(null, harvestContext(Instant.EPOCH).incrementalWindow)
        assertEquals(null, completionContext().previousState)
    }

    @Test
    fun `boundary codecs treat absent or empty state as a first window and reject foreign codec IDs`() {
        val now = Instant.parse("2026-01-01T00:00:20Z")
        val watermark = WatermarkHarvestStrategy(FixedOverlapBoundaryPolicy(Duration.ofSeconds(1)))
        val codec = assertNotNull(watermark.stateCodec)
        val emptyState = HarvestState(
            revision = 0,
            strategyId = watermark.id,
            codecId = codec.descriptor.id,
            codecVersion = codec.descriptor.version,
            payload = CheckpointPayload.from(byteArrayOf()),
        )
        assertEquals(IncrementalBoundaryPolicy.Window(null, now), watermark.window(harvestContext(now, emptyState)))
        val foreign = assertIs<HarvestStateResolution.Quarantine>(codec.resolve(emptyState.copy(codecId = "foreign")))
        assertEquals(
            "Harvest state does not match trawler-core-instant-boundary@1/watermark",
            foreign.reason,
        )

        val historical = HistoricalThenIncrementalHarvestStrategy(FixedOverlapBoundaryPolicy(Duration.ZERO))
        assertEquals(null, historical.window(harvestContext(now)))
        assertEquals(HarvestMode.HISTORICAL_THEN_INCREMENTAL, historical.mode)
        assertEquals(HarvestMode.WATERMARK, watermark.mode)
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
