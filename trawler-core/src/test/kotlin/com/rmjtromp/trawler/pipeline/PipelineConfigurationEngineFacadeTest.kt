package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.SeedDiscovery
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.ExtractionContext
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.schedule.ScheduleSubmission
import com.rmjtromp.trawler.schedule.ScheduleTrigger
import com.rmjtromp.trawler.schedule.Scheduler
import com.rmjtromp.trawler.schedule.SchedulerEvent
import com.rmjtromp.trawler.schedule.SchedulerHandle
import com.rmjtromp.trawler.transport.Transport
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test

/**
 * Public [TrawlerEngine] facade contracts defined in PipelineConfiguration.kt:
 * the DSL companion, default idle bound, and the built-in versus
 * caller-selected scheduler entry points. Runtime internals are owned by
 * EngineRuntime tests.
 *
 * The built-in coroutine scheduler runs on the engine-owned
 * `Dispatchers.Default` scope, so the daemon case uses [runBlocking] with
 * explicit completion signals; every wait is bounded by [withTimeout] and no
 * wall-clock ordering is assumed.
 */
class PipelineConfigurationEngineFacadeTest {
    @Test
    fun `DSL companion builds a validated non-generic engine and propagates configuration issues`() {
        val engine = TrawlerEngine {
            register<Int>("dsl") {
                transport(CountingTransport())
                extract(IntExtractor)
            }
        }
        assertEquals(listOf("dsl"), engine.configuration.pipelines.map { it.id })

        val error = assertFailsWith<InvalidConfigurationException> {
            TrawlerEngine { register<Int>("broken") { extract(IntExtractor) } }
        }
        assertEquals(listOf("pipeline[broken].transport"), error.issues.map { it.path })
    }

    @Test
    fun `awaitIdle uses the configured bound and returns immediately for an idle engine`(): Unit = runBlocking {
        val engine = TrawlerEngine {
            runtime(EngineRuntimeOptions(awaitIdleTimeout = Duration.ofMillis(50)))
        }
        try {
            assertTrue(engine.awaitIdle())
            assertFailsWith<IllegalArgumentException> { engine.awaitIdle(Duration.ZERO) }
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `default startDaemon selects the built-in coroutine scheduler and manual triggers run the pipeline`(): Unit = runBlocking {
        val transport = CountingTransport()
        val outputs = Channel<List<Int>>(capacity = 4)
        val engine = TrawlerEngine {
            register<Int>("daemon") {
                transport(transport)
                discover(SeedDiscovery(requests = listOf(CrawlRequest("daemon"))))
                extract(IntExtractor)
                output(ChannelOutput(outputs))
            }
        }
        try {
            val handle = engine.startDaemon()
            assertEquals(ScheduleSubmission.Started(1), handle.trigger())
            assertEquals(listOf(1), withTimeout(BOUND) { outputs.receive() })
            assertTrue(engine.awaitIdle(Duration.ofMillis(BOUND)))
            assertEquals(listOf("daemon"), transport.targets)

            handle.cancel()
            withTimeout(BOUND) { handle.completion.await() }
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `caller-selected scheduler receives the daemon trigger and pipeline selection`(): Unit = runBlocking {
        val scheduler = RecordingScheduler()
        val engine = TrawlerEngine {
            register<Int>("selected") {
                transport(CountingTransport())
                extract(IntExtractor)
            }
        }
        try {
            val trigger = ScheduleTrigger.Manual(maxQueuedRuns = 2)
            val handle = engine.startDaemon(scheduler, DaemonConfig(trigger, setOf("selected")))

            assertSame(trigger, scheduler.trigger)
            assertEquals(setOf("selected"), scheduler.pipelineIds)
            assertEquals(ScheduleSubmission.Queued(9), handle.trigger())
            handle.cancel()
            assertTrue(scheduler.handle.cancelled)

            // Omitting the daemon config selects a manual trigger over every pipeline.
            val defaultScheduler = RecordingScheduler()
            val defaultHandle = engine.startDaemon(defaultScheduler)
            assertEquals(ScheduleTrigger.Manual(), defaultScheduler.trigger)
            assertEquals(null, defaultScheduler.pipelineIds)
            defaultHandle.cancel()
        } finally {
            engine.shutdown()
        }
        assertFailsWith<IllegalStateException> { engine.runOnce() }
    }

    private object IntExtractor : Extractor<Int> {
        override val id: String = "int-extractor"

        override suspend fun extract(response: CrawlResponse, context: ExtractionContext): ExtractionResult<Int> =
            ExtractionResult.Success(listOf(response.body.toByteArray().decodeToString().toInt()))
    }

    private class CountingTransport : Transport {
        override val id: String = "counting-transport"
        val targets: MutableList<String> = java.util.Collections.synchronizedList(mutableListOf())

        override suspend fun execute(request: CrawlRequest): CrawlResponse {
            targets += request.target
            return CrawlResponse(
                request = request,
                statusCode = 200,
                headers = Headers.empty(),
                body = ResponseBody.from("1".toByteArray()),
                receivedAt = Instant.EPOCH,
            )
        }
    }

    private class ChannelOutput(private val channel: Channel<List<Int>>) : Output<Int> {
        override val id: String = "channel-output"

        override suspend fun write(items: List<Produced<Int>>, context: OutputContext): OutputResult {
            channel.send(items.map { it.value })
            return OutputResult.Accepted(items.size)
        }
    }

    private class RecordingScheduler : Scheduler {
        override val id: String = "recording-scheduler"
        var trigger: ScheduleTrigger? = null
        var pipelineIds: Set<String>? = null
        val handle = RecordingHandle()

        override suspend fun start(
            scope: CoroutineScope,
            trigger: ScheduleTrigger,
            clock: com.rmjtromp.trawler.policy.Clock,
            pipelineIds: Set<String>?,
            run: suspend (RunRequest) -> RunResult,
            observe: suspend (SchedulerEvent) -> Unit,
        ): SchedulerHandle {
            this.trigger = trigger
            this.pipelineIds = pipelineIds
            return handle
        }
    }

    private class RecordingHandle : SchedulerHandle {
        var cancelled: Boolean = false
        private val done = CompletableDeferred<Unit>()
        override val completion: Deferred<Unit> = done

        override suspend fun trigger(): ScheduleSubmission = ScheduleSubmission.Queued(9)

        override suspend fun cancel() {
            cancelled = true
            done.complete(Unit)
        }
    }

    private companion object {
        const val BOUND: Long = 10_000
    }
}
