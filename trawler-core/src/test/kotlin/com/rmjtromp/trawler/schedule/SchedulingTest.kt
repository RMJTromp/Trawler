package com.rmjtromp.trawler.schedule

import com.rmjtromp.trawler.pipeline.RunFailure
import com.rmjtromp.trawler.pipeline.RunRequest
import com.rmjtromp.trawler.pipeline.RunResult
import com.rmjtromp.trawler.pipeline.RunStatus
import com.rmjtromp.trawler.policy.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.TestScope
import org.junit.jupiter.api.Test

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class SchedulingTest {
    @Test
    fun `trigger and scheduler option validation covers every bound`() {
        assertEquals(1, ScheduleTrigger.Manual(maxQueuedRuns = 1).maxQueuedRuns)
        assertEquals(1, ScheduleTrigger.Finite(1, Duration.ZERO, maxQueuedRuns = 1).runs)
        assertEquals(Duration.ofNanos(1), ScheduleTrigger.FixedDelay(Duration.ofNanos(1)).delay)
        assertEquals(Duration.ofNanos(1), ScheduleTrigger.FixedRate(Duration.ofNanos(1)).period)
        assertEquals(LocalTime.MIDNIGHT, ScheduleTrigger.Daily(LocalTime.MIDNIGHT, ZoneOffset.UTC).localTime)
        assertEquals(Duration.ofNanos(1), CoroutineScheduler(cancellationTimeout = Duration.ofNanos(1)).cancellationTimeout)

        assertInvalid("Queued trigger bound must be positive") {
            ScheduleTrigger.Manual(maxQueuedRuns = 0)
        }
        assertInvalid("Finite schedule run count must be positive") {
            ScheduleTrigger.Finite(runs = 0)
        }
        assertInvalid("Finite schedule interval must not be negative") {
            ScheduleTrigger.Finite(runs = 1, interval = Duration.ofNanos(-1))
        }
        assertInvalid("Queued trigger bound must be positive") {
            ScheduleTrigger.Finite(runs = 1, maxQueuedRuns = 0)
        }
        assertInvalid("Fixed delay must be positive") {
            ScheduleTrigger.FixedDelay(Duration.ZERO)
        }
        assertInvalid("Fixed delay must be positive") {
            ScheduleTrigger.FixedDelay(Duration.ofNanos(-1))
        }
        assertInvalid("Fixed-delay run count must be positive when present") {
            ScheduleTrigger.FixedDelay(Duration.ofNanos(1), maxRuns = 0)
        }
        assertInvalid("Queued trigger bound must be positive") {
            ScheduleTrigger.FixedDelay(Duration.ofNanos(1), maxQueuedRuns = 0)
        }
        assertInvalid("Fixed rate period must be positive") {
            ScheduleTrigger.FixedRate(Duration.ZERO)
        }
        assertInvalid("Fixed rate period must be positive") {
            ScheduleTrigger.FixedRate(Duration.ofNanos(-1))
        }
        assertInvalid("Fixed-rate run count must be positive when present") {
            ScheduleTrigger.FixedRate(Duration.ofNanos(1), maxRuns = 0)
        }
        assertInvalid("Queued trigger bound must be positive") {
            ScheduleTrigger.FixedRate(Duration.ofNanos(1), maxQueuedRuns = 0)
        }
        assertInvalid("Daily run count must be positive when present") {
            ScheduleTrigger.Daily(LocalTime.NOON, ZoneOffset.UTC, maxRuns = 0)
        }
        assertInvalid("Queued trigger bound must be positive") {
            ScheduleTrigger.Daily(LocalTime.NOON, ZoneOffset.UTC, maxQueuedRuns = 0)
        }
        assertInvalid("Scheduler ID must not be blank") {
            CoroutineScheduler(id = " ")
        }
        assertInvalid("Scheduler cancellation timeout must be positive") {
            CoroutineScheduler(cancellationTimeout = Duration.ZERO)
        }
        assertInvalid("Scheduler cancellation timeout must be positive") {
            CoroutineScheduler(cancellationTimeout = Duration.ofNanos(-1))
        }
    }

    @Test
    fun `manual triggers snapshot pipeline selection and map completion events`() = runTest {
        val ids = linkedSetOf("selected")
        val requests = mutableListOf<RunRequest>()
        val events = mutableListOf<SchedulerEvent>()
        val handle = CoroutineScheduler(cancellationTimeout = Duration.ofSeconds(1)).start(
            scope = this,
            trigger = ScheduleTrigger.Manual(),
            clock = MutableClock(Instant.EPOCH),
            pipelineIds = ids,
            run = { request ->
                requests += request
                completedRun()
            },
            observe = { events += it },
        )
        try {
            ids += "mutated-after-start"
            assertEquals(0, requests.size)

            assertEquals(ScheduleSubmission.Started(1), handle.trigger())
            runCurrent()

            assertEquals(listOf(setOf("selected")), requests.map { it.selectedPipelineIds })
            assertEquals(
                listOf(setOf("selected")),
                events.filterIsInstance<SchedulerEvent.Triggered>().map { it.selectedPipelineIds },
            )
            assertEquals(listOf(1L), events.filterIsInstance<SchedulerEvent.RunCompleted>().map { it.sequence })
        } finally {
            handle.cancel()
        }

        assertEquals(
            listOf(
                SchedulerEvent.Triggered::class,
                SchedulerEvent.RunCompleted::class,
                SchedulerEvent.Cancelled::class,
            ),
            events.map { it::class },
        )
        assertTrue(handle.completion.isCompleted)
    }

    @Test
    fun `manual immediate first run starts exactly once`() = runTest {
        var calls = 0
        val handle = CoroutineScheduler().start(
            scope = this,
            trigger = ScheduleTrigger.Manual(immediateFirstRun = true),
            clock = MutableClock(Instant.EPOCH),
            run = {
                calls++
                completedRun()
            },
        )
        try {
            runCurrent()
            assertEquals(1, calls)
        } finally {
            handle.cancel()
        }
    }

    @Test
    fun `finite schedule honors immediate first run interval and exact trigger count`() = runTest {
        val calls = mutableListOf<Long>()
        val events = mutableListOf<SchedulerEvent>()
        val handle = CoroutineScheduler().start(
            scope = this,
            trigger = ScheduleTrigger.Finite(
                runs = 3,
                interval = Duration.ofSeconds(1),
                immediateFirstRun = true,
            ),
            clock = MutableClock(Instant.EPOCH),
            run = {
                calls += calls.size.toLong()
                completedRun()
            },
            observe = { events += it },
        )

        runCurrent()
        assertEquals(listOf(0L), calls)
        advanceTimeBy(999)
        runCurrent()
        assertEquals(listOf(0L), calls)
        advanceTimeBy(1)
        runCurrent()
        assertEquals(listOf(0L, 1L), calls)
        advanceTimeBy(1_000)
        runCurrent()
        assertEquals(listOf(0L, 1L, 2L), calls)
        advanceUntilIdle()

        assertTrue(handle.completion.isCompleted)
        assertEquals(listOf(1L, 2L, 3L), events.filterIsInstance<SchedulerEvent.Triggered>().map { it.sequence })
        assertEquals(listOf(1L, 2L, 3L), events.filterIsInstance<SchedulerEvent.RunCompleted>().map { it.sequence })
    }

    @Test
    fun `finite schedule without immediate first run waits for its first interval`() = runTest {
        val calls = mutableListOf<Int>()
        val handle = CoroutineScheduler().start(
            scope = this,
            trigger = ScheduleTrigger.Finite(
                runs = 2,
                interval = Duration.ofSeconds(1),
                immediateFirstRun = false,
            ),
            clock = MutableClock(Instant.EPOCH),
            run = {
                calls += calls.size + 1
                completedRun()
            },
        )

        runCurrent()
        assertTrue(calls.isEmpty())
        advanceTimeBy(999)
        runCurrent()
        assertTrue(calls.isEmpty())
        advanceTimeBy(1)
        runCurrent()
        assertEquals(listOf(1), calls)
        advanceTimeBy(1_000)
        runCurrent()
        assertEquals(listOf(1, 2), calls)
        advanceUntilIdle()
        assertTrue(handle.completion.isCompleted)
    }

    @Test
    fun `fixed delay waits for completion before starting its next delay`() = runTest {
        val firstStarted = CompletableDeferred<Unit>()
        val firstRelease = CompletableDeferred<Unit>()
        val calls = mutableListOf<Int>()
        val handle = CoroutineScheduler().start(
            scope = this,
            trigger = ScheduleTrigger.FixedDelay(
                delay = Duration.ofSeconds(1),
                maxRuns = 2,
                immediateFirstRun = true,
            ),
            clock = MutableClock(Instant.EPOCH),
            run = {
                val call = calls.size + 1
                calls += call
                if (call == 1) {
                    firstStarted.complete(Unit)
                    firstRelease.await()
                }
                completedRun()
            },
        )
        try {
            runCurrent()
            firstStarted.await()
            advanceTimeBy(10_000)
            runCurrent()
            assertEquals(listOf(1), calls)

            firstRelease.complete(Unit)
            runCurrent()
            assertEquals(listOf(1), calls)
            advanceTimeBy(999)
            runCurrent()
            assertEquals(listOf(1), calls)
            advanceTimeBy(1)
            runCurrent()
            assertEquals(listOf(1, 2), calls)
            advanceUntilIdle()
            assertTrue(handle.completion.isCompleted)
        } finally {
            handle.cancel()
        }
    }

    @Test
    fun `fixed rate follows injected clock cadence while a prior run is active`() = runTest {
        val clock = MutableClock(Instant.EPOCH)
        val firstStarted = CompletableDeferred<Unit>()
        val firstRelease = CompletableDeferred<Unit>()
        val secondStarted = CompletableDeferred<Unit>()
        val calls = mutableListOf<Int>()
        val events = mutableListOf<SchedulerEvent>()
        val handle = CoroutineScheduler().start(
            scope = this,
            trigger = ScheduleTrigger.FixedRate(
                period = Duration.ofSeconds(1),
                maxRuns = 2,
                overlap = OverlapBehavior.QUEUE,
                maxQueuedRuns = 1,
                immediateFirstRun = true,
            ),
            clock = clock,
            run = {
                val call = calls.size + 1
                calls += call
                if (call == 1) {
                    firstStarted.complete(Unit)
                    firstRelease.await()
                } else {
                    secondStarted.complete(Unit)
                }
                completedRun()
            },
            observe = { events += it },
        )
        try {
            runCurrent()
            firstStarted.await()
            clock.current = clock.current.plusSeconds(1)
            advanceTimeBy(1_000)
            runCurrent()
            assertEquals(listOf(1), calls)
            assertEquals(listOf(1L, 2L), events.filterIsInstance<SchedulerEvent.Triggered>().map { it.sequence })

            firstRelease.complete(Unit)
            runCurrent()
            secondStarted.await()
            assertEquals(listOf(1, 2), calls)
            advanceUntilIdle()
            assertTrue(handle.completion.isCompleted)
        } finally {
            handle.cancel()
        }
    }

    @Test
    fun `daily schedule uses the next local target before and after the target time`() = runTest {
        val beforeClock = MutableClock(Instant.parse("2026-01-01T08:00:00Z"))
        fireDaily(
            scope = this,
            clock = beforeClock,
            trigger = ScheduleTrigger.Daily(LocalTime.of(10, 0), ZoneOffset.UTC, maxRuns = 1),
            expectedDelay = Duration.ofHours(2),
        )

        val afterClock = MutableClock(Instant.parse("2026-01-01T11:00:00Z"))
        fireDaily(
            scope = this,
            clock = afterClock,
            trigger = ScheduleTrigger.Daily(LocalTime.of(10, 0), ZoneOffset.UTC, maxRuns = 1),
            expectedDelay = Duration.ofHours(23),
        )
    }

    @Test
    fun `daily schedule honors explicit timezone changes`() = runTest {
        val utcClock = MutableClock(Instant.parse("2026-01-01T14:00:00Z"))
        fireDaily(
            scope = this,
            clock = utcClock,
            trigger = ScheduleTrigger.Daily(LocalTime.of(10, 0), ZoneOffset.UTC, maxRuns = 1),
            expectedDelay = Duration.ofHours(20),
        )

        val newYorkClock = MutableClock(Instant.parse("2026-01-01T14:00:00Z"))
        fireDaily(
            scope = this,
            clock = newYorkClock,
            trigger = ScheduleTrigger.Daily(LocalTime.of(10, 0), ZoneId.of("America/New_York"), maxRuns = 1),
            expectedDelay = Duration.ofHours(1),
        )
    }

    @Test
    fun `daily schedule resolves daylight-saving gaps and overlaps deterministically`() = runTest {
        val gapClock = MutableClock(Instant.parse("2026-03-29T00:00:00Z"))
        fireDaily(
            scope = this,
            clock = gapClock,
            trigger = ScheduleTrigger.Daily(LocalTime.of(2, 30), ZoneId.of("Europe/Amsterdam"), maxRuns = 1),
            expectedDelay = Duration.ofMinutes(90),
        )

        val overlapClock = MutableClock(Instant.parse("2026-10-25T00:00:00Z"))
        fireDaily(
            scope = this,
            clock = overlapClock,
            trigger = ScheduleTrigger.Daily(LocalTime.of(2, 30), ZoneId.of("Europe/Amsterdam"), maxRuns = 1),
            expectedDelay = Duration.ofMinutes(30),
        )
    }

    @Test
    fun `skip overlap discards an active trigger and reports its sequence`() = runTest {
        val active = CompletableDeferred<Unit>()
        val started = CompletableDeferred<Unit>()
        val events = mutableListOf<SchedulerEvent>()
        val handle = CoroutineScheduler().start(
            scope = this,
            trigger = ScheduleTrigger.Manual(overlap = OverlapBehavior.SKIP),
            clock = MutableClock(Instant.EPOCH),
            run = {
                started.complete(Unit)
                active.await()
                completedRun()
            },
            observe = { events += it },
        )
        try {
            assertEquals(ScheduleSubmission.Started(1), handle.trigger())
            runCurrent()
            started.await()
            val skipped = assertIs<ScheduleSubmission.Skipped>(handle.trigger())
            assertEquals(2L, skipped.sequence)
            assertEquals(listOf(2L), events.filterIsInstance<SchedulerEvent.Skipped>().map { it.sequence })
        } finally {
            active.complete(Unit)
            handle.cancel()
        }
    }

    @Test
    fun `reject overlap leaves the active run unchanged`() = runTest {
        val release = CompletableDeferred<Unit>()
        val started = CompletableDeferred<Unit>()
        var calls = 0
        val events = mutableListOf<SchedulerEvent>()
        val handle = CoroutineScheduler().start(
            scope = this,
            trigger = ScheduleTrigger.Manual(overlap = OverlapBehavior.REJECT),
            run = {
                calls++
                started.complete(Unit)
                release.await()
                completedRun()
            },
            observe = { events += it },
        )
        try {
            assertEquals(ScheduleSubmission.Started(1), handle.trigger())
            runCurrent()
            started.await()
            val rejected = assertIs<ScheduleSubmission.Rejected>(handle.trigger())
            assertEquals(2L, rejected.sequence)
            assertEquals(1, calls)
            assertEquals(listOf(2L), events.filterIsInstance<SchedulerEvent.Rejected>().map { it.sequence })
        } finally {
            release.complete(Unit)
            handle.cancel()
        }
    }

    @Test
    fun `queued overlap is bounded and drains in FIFO sequence order`() = runTest {
        val gates = listOf(CompletableDeferred<Unit>(), CompletableDeferred<Unit>(), CompletableDeferred<Unit>())
        val calls = mutableListOf<Int>()
        val started = mutableListOf<Int>()
        val events = mutableListOf<SchedulerEvent>()
        val handle = CoroutineScheduler().start(
            scope = this,
            trigger = ScheduleTrigger.Manual(
                overlap = OverlapBehavior.QUEUE,
                maxQueuedRuns = 2,
            ),
            run = {
                val call = calls.size + 1
                calls += call
                started += call
                gates[call - 1].await()
                completedRun()
            },
            observe = { events += it },
        )
        try {
            assertEquals(ScheduleSubmission.Started(1), handle.trigger())
            runCurrent()
            assertEquals(ScheduleSubmission.Queued(2), handle.trigger())
            assertEquals(ScheduleSubmission.Queued(3), handle.trigger())
            val full = assertIs<ScheduleSubmission.Rejected>(handle.trigger())
            assertEquals(4L, full.sequence)
            assertEquals(listOf(1), started)
            assertEquals(listOf(1L, 2L, 3L), events.filterIsInstance<SchedulerEvent.Triggered>().map { it.sequence })

            gates[0].complete(Unit)
            runCurrent()
            assertEquals(listOf(1, 2), started)
            gates[1].complete(Unit)
            runCurrent()
            assertEquals(listOf(1, 2, 3), started)
            gates[2].complete(Unit)
            advanceUntilIdle()

            assertEquals(listOf(1, 2, 3), calls)
            assertTrue(!handle.completion.isCompleted)
        } finally {
            gates.forEach { it.complete(Unit) }
            handle.cancel()
        }
        assertTrue(handle.completion.isCompleted)
    }

    @Test
    fun `run exceptions and terminal results map to scheduler failure events`() = runTest {
        val thrown = IllegalStateException("run threw")
        val failed = IllegalArgumentException("failed")
        val abandoned = IllegalStateException("abandoned")
        val cancelled = CancellationException("cancelled")
        val results = listOf(
            ResultOrThrow(result = completedRun()),
            ResultOrThrow(result = failedRun(RunStatus.FAILED, failed)),
            ResultOrThrow(result = failedRun(RunStatus.ABANDONED, abandoned)),
            ResultOrThrow(result = failedRun(RunStatus.CANCELLED, cancelled)),
            ResultOrThrow(error = thrown),
        )
        val events = mutableListOf<SchedulerEvent>()
        var index = 0
        val handle = CoroutineScheduler().start(
            scope = this,
            trigger = ScheduleTrigger.Manual(),
            run = {
                val outcome = results[index++]
                outcome.error?.let { throw it }
                outcome.result!!
            },
            observe = { events += it },
        )
        try {
            repeat(results.size) {
                assertIs<ScheduleSubmission.Started>(handle.trigger())
                runCurrent()
            }
        } finally {
            handle.cancel()
        }

        val completed = events.filterIsInstance<SchedulerEvent.RunCompleted>()
        assertEquals(listOf(1L), completed.map { it.sequence })
        val failures = events.filterIsInstance<SchedulerEvent.RunFailed>()
        assertEquals(listOf(2L, 3L, 4L, 5L), failures.map { it.sequence })
        assertSame(failed, failures[0].error)
        assertSame(abandoned, failures[1].error)
        assertSame(cancelled, failures[2].error)
        assertSame(thrown, failures[3].error)
    }

    @Test
    fun `observer failures are best effort but observer cancellation stops the scheduler loop`() = runTest {
        var observerCalls = 0
        var runCalls = 0
        val failedObserver = CoroutineScheduler().start(
            scope = this,
            trigger = ScheduleTrigger.Manual(),
            run = {
                runCalls++
                completedRun()
            },
            observe = {
                observerCalls++
                error("observer failed")
            },
        )
        try {
            assertEquals(ScheduleSubmission.Started(1), failedObserver.trigger())
            runCurrent()
            assertEquals(1, runCalls)
            assertEquals(2, observerCalls)
        } finally {
            failedObserver.cancel()
        }

        val observerCancellation = CancellationException("observer cancelled")
        val manualObserverCancellation = CoroutineScheduler().start(
            scope = this,
            trigger = ScheduleTrigger.Manual(),
            run = { completedRun() },
            observe = { throw observerCancellation },
        )
        try {
            val propagated = assertFailsWith<CancellationException> { manualObserverCancellation.trigger() }
            assertSame(observerCancellation, propagated)
        } finally {
            manualObserverCancellation.cancel()
        }

        var cancelledObserverCalls = 0
        val cancellingObserver = CoroutineScheduler().start(
            scope = this,
            trigger = ScheduleTrigger.Finite(runs = 1, immediateFirstRun = true),
            run = {
                completedRun()
            },
            observe = {
                cancelledObserverCalls++
                throw observerCancellation
            },
        )
        runCurrent()
        assertTrue(cancellingObserver.completion.isCompleted)
        assertTrue(cancelledObserverCalls >= 1)
        cancellingObserver.cancel()
    }

    @Test
    fun `cancellation removes pending triggers cancels the active run and is idempotent`() = runTest {
        val started = CompletableDeferred<Unit>()
        val cancelled = CompletableDeferred<Unit>()
        var calls = 0
        val handle = CoroutineScheduler().start(
            scope = this,
            trigger = ScheduleTrigger.Manual(overlap = OverlapBehavior.QUEUE, maxQueuedRuns = 2),
            run = {
                calls++
                started.complete(Unit)
                try {
                    awaitCancellation()
                } finally {
                    cancelled.complete(Unit)
                }
                error("unreachable")
            },
        )
        assertEquals(ScheduleSubmission.Started(1), handle.trigger())
        runCurrent()
        started.await()
        assertEquals(ScheduleSubmission.Queued(2), handle.trigger())
        assertEquals(ScheduleSubmission.Queued(3), handle.trigger())

        handle.cancel()
        cancelled.await()
        handle.completion.await()
        handle.cancel()

        assertEquals(1, calls)
        assertIs<ScheduleSubmission.Rejected>(handle.trigger())
        assertTrue(handle.completion.isCompleted)
    }

    private suspend fun fireDaily(
        scope: TestScope,
        clock: MutableClock,
        trigger: ScheduleTrigger.Daily,
        expectedDelay: Duration,
    ) {
        var calls = 0
        val handle = CoroutineScheduler().start(
            scope = scope,
            trigger = trigger,
            clock = clock,
            run = {
                calls++
                completedRun()
            },
        )
        try {
            scope.runCurrent()
            assertEquals(0, calls)
            scope.advanceTimeBy(expectedDelay.toMillis() - 1)
            scope.runCurrent()
            assertEquals(0, calls)
            scope.advanceTimeBy(1)
            scope.runCurrent()
            assertEquals(1, calls)
            assertTrue(handle.completion.isCompleted)
        } finally {
            handle.cancel()
        }
    }

    private fun completedRun(): RunResult = result(RunStatus.COMPLETED)

    private fun failedRun(status: RunStatus, error: Throwable): RunResult = result(status, error)

    private fun result(status: RunStatus, error: Throwable? = null): RunResult = RunResult(
        engineRunId = "engine-run",
        status = status,
        pipelineRuns = emptyList(),
        failures = error?.let { listOf(RunFailure("run", it)) } ?: emptyList(),
        startedAt = Instant.EPOCH,
        completedAt = Instant.EPOCH,
    )

    private fun assertInvalid(expectedMessage: String, create: () -> Any) {
        val error = assertFailsWith<IllegalArgumentException> { create() }
        assertEquals(expectedMessage, error.message)
    }

    private class MutableClock(var current: Instant) : Clock {
        override fun now(): Instant = current
    }

    private data class ResultOrThrow(
        val result: RunResult? = null,
        val error: Throwable? = null,
    )
}
