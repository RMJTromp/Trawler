package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.event.CrawlerEvent
import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.event.Observer
import com.rmjtromp.trawler.event.ObserverResult
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.ExtractionContext
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.schedule.ScheduleSubmission
import com.rmjtromp.trawler.schedule.ScheduleTrigger
import com.rmjtromp.trawler.schedule.Scheduler
import com.rmjtromp.trawler.schedule.SchedulerEvent
import com.rmjtromp.trawler.schedule.SchedulerHandle
import com.rmjtromp.trawler.transport.Transport
import com.rmjtromp.trawler.transport.TransportRouter
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class EngineRuntimeSchedulingTest {
    @Test
    fun `daemon selections are non-overlapping and manual triggers delegate`() = runTest {
        val engine = engine("alpha", "beta")
        val scheduler = RecordingScheduler()
        val alphaIds = linkedSetOf("alpha")
        try {
            val alpha = engine.startDaemon(
                scheduler,
                DaemonConfig(pipelineIds = alphaIds),
            )
            alphaIds += "beta"
            val beta = engine.startDaemon(
                scheduler,
                DaemonConfig(pipelineIds = setOf("beta")),
            )

            assertEquals(listOf(setOf("alpha"), setOf("beta")), scheduler.starts.map { it.pipelineIds })
            assertEquals(ScheduleSubmission.Started(1), alpha.trigger())
            assertEquals(1, scheduler.starts[0].handle.triggerCalls)

            assertFailsWith<IllegalStateException> {
                engine.startDaemon(scheduler, DaemonConfig(pipelineIds = setOf("alpha", "beta")))
            }
            assertFailsWith<IllegalStateException> {
                engine.startDaemon(scheduler, DaemonConfig())
            }

            alpha.cancel()
            beta.cancel()
            assertEquals(1, scheduler.starts[0].handle.cancelCalls)
            assertEquals(1, scheduler.starts[1].handle.cancelCalls)
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `daemon selection forwards scheduler events through the engine observer`() = runTest {
        val events = mutableListOf<CrawlerEvent>()
        val engine = engine(
            "selected",
            observer = object : Observer {
                override val id: String = "test-observer"

                override suspend fun onEvent(event: CrawlerEvent): ObserverResult {
                    events += event
                    return ObserverResult.Accepted
                }
            },
        )
        val scheduler = RecordingScheduler()
        try {
            val handle = engine.startDaemon(
                scheduler,
                DaemonConfig(pipelineIds = setOf("selected")),
            )
            val failure = IllegalStateException("scheduled run failed")
            scheduler.starts.single().observe(
                SchedulerEvent.RunFailed(sequence = 7, error = failure),
            )

            assertEquals(listOf(CrawlerEventType.SCHEDULER_RUN_FAILED), events.map { it.type })
            assertTrue(events.single().detail!!.contains("sequence=7"))
            handle.cancel()
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `daemon cancellation removes only the daemon while shutdown stops the engine`() = runTest {
        val engine = engine("pipeline")
        val scheduler = RecordingScheduler()
        try {
            val first = engine.startDaemon(
                scheduler,
                DaemonConfig(pipelineIds = setOf("pipeline")),
            )
            first.cancel()
            first.cancel()
            assertEquals(1, scheduler.starts[0].handle.cancelCalls)

            val second = engine.startDaemon(
                scheduler,
                DaemonConfig(pipelineIds = setOf("pipeline")),
            )
            second.shutdown()
            assertEquals(1, scheduler.starts[1].handle.cancelCalls)
            assertFailsWith<IllegalStateException> { engine.runOnce() }
            assertFailsWith<IllegalStateException> {
                engine.startDaemon(scheduler, DaemonConfig(pipelineIds = setOf("pipeline")))
            }
        } finally {
            engine.shutdown()
        }
    }

    @Test
    fun `unknown daemon pipeline selection is rejected before scheduler startup`() = runTest {
        val engine = engine("known")
        val scheduler = RecordingScheduler()
        try {
            val error = assertFailsWith<IllegalArgumentException> {
                engine.startDaemon(scheduler, DaemonConfig(pipelineIds = setOf("unknown")))
            }
            assertEquals("Unknown pipeline IDs: unknown", error.message)
            assertTrue(scheduler.starts.isEmpty())
        } finally {
            engine.shutdown()
        }
    }

    private fun engine(
        vararg ids: String,
        observer: Observer? = null,
    ): TrawlerEngine = TrawlerEngine {
        observer?.let { observe(it) }
        ids.forEach { id ->
            register<Int>(id) {
                transport(StubRouter("router-$id"))
                extract(StubExtractor("extractor-$id"))
            }
        }
    }

    private class RecordingScheduler : Scheduler {
        override val id: String = "recording-scheduler"
        val starts = mutableListOf<StartRecord>()

        override suspend fun start(
            scope: CoroutineScope,
            trigger: ScheduleTrigger,
            clock: com.rmjtromp.trawler.policy.Clock,
            pipelineIds: Set<String>?,
            run: suspend (RunRequest) -> RunResult,
            observe: suspend (SchedulerEvent) -> Unit,
        ): SchedulerHandle {
            val handle = RecordingHandle()
            starts += StartRecord(
                pipelineIds = pipelineIds?.toSet(),
                trigger = trigger,
                run = run,
                observe = observe,
                handle = handle,
            )
            return handle
        }
    }

    private data class StartRecord(
        val pipelineIds: Set<String>?,
        val trigger: ScheduleTrigger,
        val run: suspend (RunRequest) -> RunResult,
        val observe: suspend (SchedulerEvent) -> Unit,
        val handle: RecordingHandle,
    )

    private class RecordingHandle : SchedulerHandle {
        override val completion: Deferred<Unit> = CompletableDeferred()
        var triggerCalls: Int = 0
        var cancelCalls: Int = 0

        override suspend fun trigger(): ScheduleSubmission {
            triggerCalls++
            return ScheduleSubmission.Started(triggerCalls.toLong())
        }

        override suspend fun cancel() {
            cancelCalls++
            (completion as CompletableDeferred<Unit>).complete(Unit)
        }
    }

    private class StubRouter(override val id: String) : TransportRouter {
        private val transport = StubTransport("$id-transport")

        override fun select(request: CrawlRequest, context: RequestContext): Transport = transport
    }

    private class StubTransport(override val id: String) : Transport {
        override suspend fun execute(request: CrawlRequest): CrawlResponse = error("daemon test must not execute transport")
    }

    private class StubExtractor(override val id: String) : Extractor<Int> {
        override suspend fun extract(response: CrawlResponse, context: ExtractionContext): ExtractionResult<Int> =
            ExtractionResult.Ignore("daemon test")
    }
}
