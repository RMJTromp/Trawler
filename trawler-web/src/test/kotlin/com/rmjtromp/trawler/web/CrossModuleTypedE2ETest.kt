package com.rmjtromp.trawler.web

import com.rmjtromp.trawler.discovery.DiscoveryCheckpointCodec
import com.rmjtromp.trawler.discovery.DiscoveryContext
import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.discovery.DiscoveryCheckpointCandidate
import com.rmjtromp.trawler.discovery.ResumableDiscovery
import com.rmjtromp.trawler.event.CrawlerEvent
import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.event.Observer
import com.rmjtromp.trawler.event.ObserverResult
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.pipeline.EngineConfigurationBuilder
import com.rmjtromp.trawler.pipeline.ExtractionResult
import com.rmjtromp.trawler.pipeline.Extractor
import com.rmjtromp.trawler.pipeline.OutputRequirement
import com.rmjtromp.trawler.pipeline.Pipeline
import com.rmjtromp.trawler.pipeline.PipelineBuilder
import com.rmjtromp.trawler.pipeline.PipelineDefaults
import com.rmjtromp.trawler.pipeline.Produced
import com.rmjtromp.trawler.pipeline.RunStatus
import com.rmjtromp.trawler.pipeline.TrawlerEngine
import com.rmjtromp.trawler.pipeline.replay
import com.rmjtromp.trawler.pipeline.Validator
import com.rmjtromp.trawler.pipeline.ValidationResult
import com.rmjtromp.trawler.pipeline.Transform
import com.rmjtromp.trawler.pipeline.TransformDecision
import com.rmjtromp.trawler.response.InMemoryResponseStore
import com.rmjtromp.trawler.response.ResponseReplayQuery
import com.rmjtromp.trawler.schedule.CoroutineScheduler
import com.rmjtromp.trawler.schedule.OverlapBehavior
import com.rmjtromp.trawler.schedule.ScheduleTrigger
import com.rmjtromp.trawler.state.CheckpointPayload
import com.rmjtromp.trawler.state.DiscoveryCheckpointCodecDescriptor
import com.rmjtromp.trawler.transport.Transport
import com.rmjtromp.trawler.web.policy.AllowAllWebDestinationPolicy
import com.rmjtromp.trawler.web.policy.DefaultHeaderRedactionPolicy
import com.rmjtromp.trawler.web.policy.DefaultHttpFailureClassifier
import com.rmjtromp.trawler.web.policy.DefaultRetryAfterPolicy
import com.rmjtromp.trawler.web.policy.NoRobotsPolicy
import com.rmjtromp.trawler.web.session.NoCookieSession
import com.rmjtromp.trawler.web.transport.KtorTransport
import com.rmjtromp.trawler.web.transport.KtorTransportConfig
import com.rmjtromp.trawler.web.transport.NoOriginConcurrencyPolicy
import com.rmjtromp.trawler.web.transport.NoOriginPacingPolicy
import com.rmjtromp.trawler.web.policy.RedirectHeaderPolicy
import com.rmjtromp.trawler.web.transport.WebTransportPolicies
import com.rmjtromp.trawler.model.ExtractionContext
import com.rmjtromp.trawler.model.TransformContext
import com.rmjtromp.trawler.policy.BuiltInPolicies
import com.rmjtromp.trawler.policy.Clock
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockEngineConfig
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.junit.jupiter.api.Test

/** End-to-end typed composition using core, the Ktor adapter, and no network. */
class CrossModuleTypedE2ETest {
    @Test
    fun `four typed news pipelines recurse capture checkpoint stop schedule and replay without network`() = runBlocking {
        val clock = FixedClock(Instant.parse("2026-01-01T00:00:00Z"))
        val requests = CopyOnWriteArrayList<String>()
        val client = HttpClient(MockEngine(MockEngineConfig().apply {
            reuseHandlers = true
            addHandler { request ->
                requests += request.url.encodedPath
                val text = when (request.url.encodedPath) {
                    "/numbers/root" -> "7"
                    "/numbers/follow" -> "14"
                    "/words/root" -> "headline"
                    "/words/follow" -> "follow-up headline"
                    "/dates/root" -> "2026-01-01"
                    "/dates/follow" -> "2026-01-02"
                    "/scores/root" -> "98.5"
                    "/scores/follow" -> "99.0"
                    else -> error("unexpected controlled target ${request.url.encodedPath}")
                }
                respond(text.toByteArray(), HttpStatusCode.OK, headersOf("Content-Type", "text/plain"))
            }
        }))
        val transport = KtorTransport.fromClient(
            client = client,
            configuration = KtorTransportConfig(id = "e2e-ktor"),
            policies = testPolicies(),
            clock = clock,
            closeClient = true,
        )
        val responseStore = InMemoryResponseStore(maxResponses = 32)
        val state = com.rmjtromp.trawler.state.InMemoryCrawlStateStore(clock = clock)
        val events = CopyOnWriteArrayList<CrawlerEvent>()
        val numbersOutput = RecordingOutput<Int>("numbers-output")
        val wordsOutput = RecordingOutput<String>("words-output")
        val datesOutput = RecordingOutput<String>("dates-output")
        val scoresOutput = RecordingOutput<Double>("scores-output")
        val numbersPipeline = typedPipeline<Int>("numbers", "/numbers/root", "/numbers/follow", transport, clock, numbersOutput) { it.toInt() }
        val wordsPipeline = typedPipeline<String>("words", "/words/root", "/words/follow", transport, clock, wordsOutput) { it }
        val datesPipeline = typedPipeline<String>("dates", "/dates/root", "/dates/follow", transport, clock, datesOutput) { it }
        val scoresPipeline = typedPipeline<Double>("scores", "/scores/root", "/scores/follow", transport, clock, scoresOutput) { it.toDouble() }
        val pipelines: List<Pipeline<*>> = listOf(numbersPipeline, wordsPipeline, datesPipeline, scoresPipeline)
        val engine = EngineConfigurationBuilder()
            .stateStore(state)
            .responseStore(responseStore)
            .extension(transport)
            .observe(EventObserver(events))
            .register(numbersPipeline)
            .register(wordsPipeline)
            .register(datesPipeline)
            .register(scoresPipeline)
            .clock(clock)
            .build()
            .let(::TrawlerEngine)

        try {
            val daemon = engine.startDaemon(
                CoroutineScheduler(),
                com.rmjtromp.trawler.pipeline.DaemonConfig(
                    trigger = ScheduleTrigger.Finite(
                        runs = 2,
                        interval = Duration.ZERO,
                        overlap = OverlapBehavior.QUEUE,
                        maxQueuedRuns = 1,
                        immediateFirstRun = true,
                    ),
                    pipelineIds = pipelines.map { it.id }.toSet(),
                ),
            )
            daemon.completion.await()

            assertEquals(8, requests.size)
            assertEquals(2, numbersOutput.values.size)
            assertEquals(listOf(7, 14), numbersOutput.values)
            assertEquals(listOf("headline", "follow-up headline"), wordsOutput.values)
            assertEquals(listOf("2026-01-01", "2026-01-02"), datesOutput.values)
            assertEquals(listOf(98.5, 99.0), scoresOutput.values)
            assertEquals(2, responseStore.replay(ResponseReplayQuery.ByPipelineId("numbers"), 8).size)
            assertTrue(
                events.count { it.type == CrawlerEventType.SCHEDULER_TRIGGERED } >= 2,
                events.joinToString { it.type.name },
            )
            assertTrue(state.load("numbers", "news-numbers")?.completed == true)
            assertTrue(state.load("words", "news-words")?.completed == true)

            val callsBeforeReplay = requests.size
            val replay = engine.replay(
                numbersPipeline.replay(ResponseReplayQuery.ByPipelineId("numbers"), limit = 8),
            )
            assertEquals(RunStatus.COMPLETED, replay.status)
            assertEquals(2, replay.responsesRead)
            assertEquals(2, replay.producedValues)
            assertEquals(2, replay.deliveredValues)
            assertEquals(callsBeforeReplay, requests.size)
            assertEquals(listOf(7, 14, 7, 14), numbersOutput.values)
        } finally {
            engine.shutdown()
        }
    }

    private fun <T : Any> typedPipeline(
        id: String,
        root: String,
        followUp: String,
        transport: Transport,
        clock: Clock,
        output: RecordingOutput<T>,
        parse: (String) -> T,
    ): Pipeline<T> = PipelineBuilder<T>(id)
        .transport(transport)
        .discover(NewsDiscovery(id, root))
        .extract(NewsExtractor("$id-extractor", followUp, parse))
        .transform(object : Transform<T> {
            override val id: String = "$id-transform"
            override suspend fun apply(value: T, context: TransformContext) =
                TransformDecision.Accept(value)
        })
        .validator(object : Validator<T> {
            override val id: String = "$id-validator"
            override suspend fun validate(value: T, context: com.rmjtromp.trawler.model.ValidationContext) =
                ValidationResult.Accepted
        })
        .output(output, OutputRequirement.REQUIRED)
        .build(PipelineDefaults(policies = BuiltInPolicies.ordinary(clock = clock)))

    private fun testPolicies(): WebTransportPolicies = WebTransportPolicies(
        destination = AllowAllWebDestinationPolicy,
        robots = NoRobotsPolicy,
        concurrency = NoOriginConcurrencyPolicy,
        pacing = NoOriginPacingPolicy,
        cookies = NoCookieSession,
        failureClassifier = DefaultHttpFailureClassifier,
        retryAfter = DefaultRetryAfterPolicy,
        redirectHeaders = object : RedirectHeaderPolicy {
            override val id: String = "e2e-redirect-headers"
            override fun headersForRedirect(
                headers: Headers,
                crossOrigin: Boolean,
                bodyRemoved: Boolean,
            ): Headers = headers
        },
        headerRedaction = DefaultHeaderRedactionPolicy(),
    )

    private class NewsDiscovery(
        private val pipelineId: String,
        private val root: String,
    ) : ResumableDiscovery {
        override val id: String = "news-$pipelineId"
        override val checkpointCodec: DiscoveryCheckpointCodec = NewsCheckpointCodec

        override fun discover(context: DiscoveryContext): Flow<DiscoveryEmission> = flow {
            if (context.checkpoint?.completed == true) return@flow
            emit(
                DiscoveryEmission.Request(
                    request = CrawlRequest("https://news.test$root"),
                    checkpoint = DiscoveryCheckpointCandidate(
                        descriptor = checkpointCodec.descriptor,
                        strategyId = checkpointCodec.strategyId,
                        payload = CheckpointPayload.from(byteArrayOf(1)),
                    ),
                ),
            )
        }
    }

    private class NewsExtractor<T : Any>(
        override val id: String,
        private val followUp: String,
        private val parse: (String) -> T,
    ) : Extractor<T> {
        override suspend fun extract(response: CrawlResponse, context: ExtractionContext): ExtractionResult<T> =
            ExtractionResult.Success(
                values = listOf(parse(String(response.body.toByteArray(), Charsets.UTF_8))),
                followUps = if (response.request.target.endsWith("root")) {
                    listOf(CrawlRequest("https://news.test$followUp"))
                } else {
                    emptyList()
                },
            )
    }

    private class RecordingOutput<T : Any>(override val id: String) : com.rmjtromp.trawler.pipeline.Output<T> {
        val values = CopyOnWriteArrayList<T>()

        override suspend fun write(
            items: List<Produced<T>>,
            context: com.rmjtromp.trawler.model.OutputContext,
        ): com.rmjtromp.trawler.pipeline.OutputResult {
            values += items.map(Produced<T>::value)
            return com.rmjtromp.trawler.pipeline.OutputResult.Accepted(items.size)
        }
    }

    private object NewsCheckpointCodec : DiscoveryCheckpointCodec {
        override val descriptor = DiscoveryCheckpointCodecDescriptor("e2e-news", 1)
        override val strategyId: String = "e2e-news"
    }

    private class EventObserver(
        private val events: MutableList<CrawlerEvent>,
    ) : Observer {
        override val id: String = "e2e-events"

        override suspend fun onEvent(event: CrawlerEvent): ObserverResult {
            events += event
            return ObserverResult.Accepted
        }
    }

    private class FixedClock(private val instant: Instant) : Clock {
        override fun now(): Instant = instant
    }
}
