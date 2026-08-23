package com.rmjtromp.trawler.schedule

import com.rmjtromp.trawler.pipeline.RunRequest
import com.rmjtromp.trawler.pipeline.RunResult
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.SystemClock
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import java.time.Duration
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.Collections

/** Trigger overlap behavior when a new trigger arrives during an active run. */
public enum class OverlapBehavior {
    /** Discard the new trigger and report the drop. */
    SKIP,

    /** Keep at most [ScheduleTrigger.maxQueuedRuns] pending triggers. */
    QUEUE,

    /** Reject the new trigger without changing the active run. */
    REJECT,
}

/**
 * Opt-in scheduler trigger definitions. Every repeating trigger has finite
 * fields where appropriate; an absent [maxRuns] means daemon-style operation
 * owned by the caller until cancellation.
 */
public sealed interface ScheduleTrigger {
    /** Defines what a new trigger does while a prior run is active. */
    public val overlap: OverlapBehavior
    /** Maximum pending triggers retained when [overlap] is [OverlapBehavior.QUEUE]. */
    public val maxQueuedRuns: Int
    /** Whether the schedule emits its first trigger as soon as it starts. */
    public val immediateFirstRun: Boolean

    /** Manual trigger source; [SchedulerHandle.trigger] is the only trigger. */
    public data class Manual(
        override val overlap: OverlapBehavior = OverlapBehavior.SKIP,
        override val maxQueuedRuns: Int = 1,
        override val immediateFirstRun: Boolean = false,
    ) : ScheduleTrigger {
        init {
            require(maxQueuedRuns > 0) { "Queued trigger bound must be positive" }
        }
    }

    /** A finite number of triggers separated by [interval]. */
    public data class Finite(
        public val runs: Int,
        public val interval: Duration = Duration.ZERO,
        override val overlap: OverlapBehavior = OverlapBehavior.SKIP,
        override val maxQueuedRuns: Int = 1,
        override val immediateFirstRun: Boolean = true,
    ) : ScheduleTrigger {
        init {
            require(runs > 0) { "Finite schedule run count must be positive" }
            require(!interval.isNegative) { "Finite schedule interval must not be negative" }
            require(maxQueuedRuns > 0) { "Queued trigger bound must be positive" }
        }
    }

    /** Repeats after the prior trigger is submitted, with an optional limit. */
    public data class FixedDelay(
        public val delay: Duration,
        public val maxRuns: Int? = null,
        override val overlap: OverlapBehavior = OverlapBehavior.SKIP,
        override val maxQueuedRuns: Int = 1,
        override val immediateFirstRun: Boolean = false,
    ) : ScheduleTrigger {
        init {
            require(!delay.isNegative && !delay.isZero) { "Fixed delay must be positive" }
            require(maxRuns == null || maxRuns > 0) { "Fixed-delay run count must be positive when present" }
            require(maxQueuedRuns > 0) { "Queued trigger bound must be positive" }
        }
    }

    /** Repeats on a fixed wall-clock cadence computed from the injected clock. */
    public data class FixedRate(
        public val period: Duration,
        public val maxRuns: Int? = null,
        override val overlap: OverlapBehavior = OverlapBehavior.SKIP,
        override val maxQueuedRuns: Int = 1,
        override val immediateFirstRun: Boolean = false,
    ) : ScheduleTrigger {
        init {
            require(!period.isNegative && !period.isZero) { "Fixed rate period must be positive" }
            require(maxRuns == null || maxRuns > 0) { "Fixed-rate run count must be positive when present" }
            require(maxQueuedRuns > 0) { "Queued trigger bound must be positive" }
        }
    }

    /** Runs at a local time in an explicit timezone, with an optional limit. */
    public data class Daily(
        public val localTime: LocalTime,
        public val zoneId: ZoneId,
        public val maxRuns: Int? = null,
        override val overlap: OverlapBehavior = OverlapBehavior.SKIP,
        override val maxQueuedRuns: Int = 1,
        override val immediateFirstRun: Boolean = false,
    ) : ScheduleTrigger {
        init {
            require(maxRuns == null || maxRuns > 0) { "Daily run count must be positive when present" }
            require(maxQueuedRuns > 0) { "Queued trigger bound must be positive" }
        }
    }
}

/** Outcome of a manual or automatic trigger submission. */
public sealed interface ScheduleSubmission {
    /**
     * The trigger started a run immediately. The scheduler owns that run until
     * completion or cancellation; the result is reported through the
     * observation callback.
     */
    public data class Started(public val sequence: Long) : ScheduleSubmission

    /** The trigger was retained within the configured bounded pending queue. */
    public data class Queued(public val sequence: Long) : ScheduleSubmission

    /** The trigger was intentionally skipped because overlap policy said so. */
    public data class Skipped(public val sequence: Long, public val reason: String) : ScheduleSubmission

    /** The trigger was rejected because overlap or lifecycle state disallowed it. */
    public data class Rejected(public val sequence: Long, public val reason: String) : ScheduleSubmission
}

/** Scheduler observations delivered through the engine's generic observer path. */
public sealed interface SchedulerEvent {
    /** A trigger was admitted for immediate or bounded queued execution. */
    public data class Triggered(
        public val sequence: Long,
        public val at: Instant,
        public val selectedPipelineIds: Set<String>?,
    ) : SchedulerEvent

    /** A trigger was intentionally discarded by overlap policy. */
    public data class Skipped(
        public val sequence: Long,
        public val reason: String,
    ) : SchedulerEvent

    /** A trigger was rejected by overlap or lifecycle policy. */
    public data class Rejected(
        public val sequence: Long,
        public val reason: String,
    ) : SchedulerEvent

    /** A scheduled run completed without a failed or abandoned status. */
    public data class RunCompleted(
        public val sequence: Long,
        public val result: RunResult,
    ) : SchedulerEvent

    /** A scheduled run failed, was abandoned, or raised an execution error. */
    public data class RunFailed(
        public val sequence: Long,
        public val error: Throwable,
    ) : SchedulerEvent

    /** The scheduler loop was cooperatively cancelled. */
    public data object Cancelled : SchedulerEvent
}

/**
 * Replaceable scheduling implementation selected by a daemon caller.
 * Implementations must launch only under the supplied structured [scope],
 * keep pending triggers bounded, propagate cancellation to owned runs, and
 * report trigger/run failures through [observe] or the returned handle.
 */
public interface Scheduler {
    /** Stable scheduler identifier used in diagnostics. */
    public val id: String

    /** Starts the scheduler as a child of [scope]. */
    public suspend fun start(
        scope: CoroutineScope,
        trigger: ScheduleTrigger,
        clock: Clock = SystemClock,
        pipelineIds: Set<String>? = null,
        run: suspend (RunRequest) -> RunResult,
        observe: suspend (SchedulerEvent) -> Unit = {},
    ): SchedulerHandle
}

/**
 * Handle for a structured scheduler child and its bounded trigger queue.
 * Cancellation stops future triggers and scheduler-owned runs; it does not
 * claim durable delivery or exactly-once execution.
 */
public interface SchedulerHandle {
    /** Completes when the scheduler loop and all owned runs have stopped. */
    public val completion: Deferred<Unit>

    /** Submits one manual trigger through the configured overlap policy. */
    public suspend fun trigger(): ScheduleSubmission

    /** Requests bounded cancellation of the trigger loop and owned runs. */
    public suspend fun cancel()
}

/**
 * Official dependency-free scheduler implementation.
 *
 * The scheduler owns one structured loop and bounded pending trigger list.
 * Fixed-delay waits for the preceding scheduled work to settle before the
 * next delay; fixed-rate and daily triggers follow their injected-clock
 * cadence and apply overlap behavior. No scheduler is started by engine
 * construction or direct [com.rmjtromp.trawler.pipeline.TrawlerEngine.runOnce]
 * calls. [cancellationTimeout] bounds handle cleanup while still allowing
 * the completion deferred to finish later if caller code ignores
 * cancellation.
 */
public class CoroutineScheduler(
    override val id: String = "coroutine-scheduler",
    /** Maximum time spent waiting for cancellation from [SchedulerHandle.cancel]. */
    public val cancellationTimeout: Duration = Duration.ofSeconds(10),
) : Scheduler {
    init {
        require(id.isNotBlank()) { "Scheduler ID must not be blank" }
        require(!cancellationTimeout.isNegative && !cancellationTimeout.isZero) {
            "Scheduler cancellation timeout must be positive"
        }
    }

    override suspend fun start(
        scope: CoroutineScope,
        trigger: ScheduleTrigger,
        clock: Clock,
        pipelineIds: Set<String>?,
        run: suspend (RunRequest) -> RunResult,
        observe: suspend (SchedulerEvent) -> Unit,
    ): SchedulerHandle {
        val controller = Controller(
            scope,
            trigger,
            clock,
            pipelineIds?.let(::immutablePipelineIds),
            run,
            observe,
            cancellationTimeout,
        )
        controller.start()
        return controller
    }

    private class Controller(
        private val scope: CoroutineScope,
        private val trigger: ScheduleTrigger,
        private val clock: Clock,
        private val pipelineIds: Set<String>?,
        private val run: suspend (RunRequest) -> RunResult,
        private val observe: suspend (SchedulerEvent) -> Unit,
        private val cancellationTimeout: Duration,
    ) : SchedulerHandle {
        private val mutex = Mutex()
        private val completionDeferred = CompletableDeferred<Unit>()
        private val pending = ArrayDeque<Long>()
        private var sequence: Long = 0
        private var activeRun: Job? = null
        private var loopJob: Job? = null
        private var cancelled = false

        override val completion: Deferred<Unit>
            get() = completionDeferred

        fun start() {
            loopJob = scope.launch {
                try {
                    runLoop()
                } catch (cancelled: CancellationException) {
                    observeSafely(SchedulerEvent.Cancelled)
                    throw cancelled
                } finally {
                    completionDeferred.complete(Unit)
                }
            }
        }

        override suspend fun trigger(): ScheduleSubmission = submit()

        override suspend fun cancel() {
            val loop: Job?
            val active: Job?
            mutex.withLock {
                if (cancelled) return
                cancelled = true
                pending.clear()
                loop = loopJob
                active = activeRun
            }
            withContext(NonCancellable) {
                loop?.cancel()
                active?.cancel()
                withTimeoutOrNull(cancellationTimeout.toMillis().coerceAtLeast(1)) {
                    loop?.join()
                    active?.join()
                }
            }
        }

        private suspend fun runLoop() {
            when (val configured = trigger) {
                is ScheduleTrigger.Manual -> {
                    if (configured.immediateFirstRun) submit()
                    awaitCancellation()
                }

                is ScheduleTrigger.Finite -> runFinite(configured)
                is ScheduleTrigger.FixedDelay -> runFixedDelay(configured)
                is ScheduleTrigger.FixedRate -> runFixedRate(configured)
                is ScheduleTrigger.Daily -> runDaily(configured)
            }
        }

        private suspend fun runFinite(configured: ScheduleTrigger.Finite) {
            var remaining = configured.runs
            if (configured.immediateFirstRun) {
                submit()
                remaining--
            }
            repeat(remaining) {
                if (configured.interval > Duration.ZERO) delay(configured.interval.toMillis())
                submit()
            }
            awaitSettled()
        }

        private suspend fun runFixedDelay(configured: ScheduleTrigger.FixedDelay) {
            var remaining = configured.maxRuns
            if (configured.immediateFirstRun) {
                submit()
                awaitSettled()
                remaining = remaining?.minus(1)
            }
            while (remaining == null || remaining > 0) {
                delay(configured.delay.toMillis())
                submit()
                awaitSettled()
                remaining = remaining?.minus(1)
            }
        }

        private suspend fun runFixedRate(configured: ScheduleTrigger.FixedRate) {
            var remaining = configured.maxRuns
            var nextAt = clock.now()
            if (configured.immediateFirstRun) {
                submit()
                remaining = remaining?.minus(1)
                nextAt = nextAt.plus(configured.period)
            } else {
                nextAt = nextAt.plus(configured.period)
            }
            while (remaining == null || remaining > 0) {
                delayUntil(nextAt)
                submit()
                remaining = remaining?.minus(1)
                nextAt = nextAt.plus(configured.period)
            }
            awaitSettled()
        }

        private suspend fun runDaily(configured: ScheduleTrigger.Daily) {
            var remaining = configured.maxRuns
            if (configured.immediateFirstRun) {
                submit()
                remaining = remaining?.minus(1)
            }
            while (remaining == null || remaining > 0) {
                val next = nextDailyInstant(clock.now(), configured.localTime, configured.zoneId)
                delayUntil(next)
                submit()
                remaining = remaining?.minus(1)
            }
            awaitSettled()
        }

        private suspend fun delayUntil(target: Instant) {
            val millis = Duration.between(clock.now(), target).toMillis()
            if (millis > 0) delay(millis) else kotlinx.coroutines.yield()
        }

        private suspend fun submit(): ScheduleSubmission {
            val decision = mutex.withLock {
                val current = ++sequence
                if (cancelled) {
                    Decision(ScheduleSubmission.Rejected(current, "Scheduler is cancelled"), null)
                } else if (activeRun?.isActive == true) {
                    when (trigger.overlap) {
                        OverlapBehavior.SKIP -> Decision(
                            ScheduleSubmission.Skipped(current, "An earlier scheduled run is active"),
                            SchedulerEvent.Skipped(current, "An earlier scheduled run is active"),
                        )

                        OverlapBehavior.REJECT -> Decision(
                            ScheduleSubmission.Rejected(current, "An earlier scheduled run is active"),
                            SchedulerEvent.Rejected(current, "An earlier scheduled run is active"),
                        )

                        OverlapBehavior.QUEUE -> if (pending.size < trigger.maxQueuedRuns) {
                            pending.addLast(current)
                            Decision(
                                ScheduleSubmission.Queued(current),
                                SchedulerEvent.Triggered(current, clock.now(), pipelineIds),
                            )
                        } else {
                            Decision(
                                ScheduleSubmission.Rejected(current, "Scheduler pending trigger capacity is full"),
                                SchedulerEvent.Rejected(current, "Scheduler pending trigger capacity is full"),
                            )
                        }
                    }
                } else {
                    val job = launchRunLocked(current)
                    Decision(
                        ScheduleSubmission.Started(current),
                        SchedulerEvent.Triggered(current, clock.now(), pipelineIds),
                        job,
                    )
                }
            }
            decision.event?.let { event -> observeSafely(event) }
            return decision.submission
        }

        private fun launchRunLocked(current: Long): Job {
            val job = scope.launch {
                try {
                    val result = run(RunRequest(pipelineIds = pipelineIds, reason = "scheduled"))
                    if (result.status == com.rmjtromp.trawler.pipeline.RunStatus.FAILED ||
                        result.status == com.rmjtromp.trawler.pipeline.RunStatus.ABANDONED ||
                        result.status == com.rmjtromp.trawler.pipeline.RunStatus.CANCELLED
                    ) {
                        observeSafely(SchedulerEvent.RunFailed(current, result.failures.firstOrNull()?.error
                            ?: IllegalStateException("Scheduled run ended with ${result.status}")))
                    } else {
                        observeSafely(SchedulerEvent.RunCompleted(current, result))
                    }
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (error: Throwable) {
                    observeSafely(SchedulerEvent.RunFailed(current, error))
                } finally {
                    startPendingIfNeeded(current)
                }
            }
            activeRun = job
            return job
        }

        private suspend fun startPendingIfNeeded(completed: Long) {
            mutex.withLock {
                if (activeRun?.isActive == true && activeRun != currentCoroutineContext()[Job]) return
                activeRun = null
                if (!cancelled && pending.isNotEmpty()) {
                    launchRunLocked(pending.removeFirst())
                }
            }
        }

        private suspend fun awaitSettled() {
            while (true) {
                val job = mutex.withLock { activeRun }
                if (job == null) return
                job.join()
            }
        }

        private suspend fun observeSafely(event: SchedulerEvent) {
            try {
                observe(event)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Throwable) {
                // Scheduler observations are best effort; the engine observer
                // boundary records its own failure without stopping scheduling.
            }
        }

        private data class Decision(
            val submission: ScheduleSubmission,
            val event: SchedulerEvent?,
            val job: Job? = null,
        )
    }
}

private fun nextDailyInstant(now: Instant, localTime: LocalTime, zoneId: ZoneId): Instant {
    val zonedNow = now.atZone(zoneId)
    var candidate = ZonedDateTime.of(zonedNow.toLocalDate(), localTime, zoneId)
    if (!candidate.toInstant().isAfter(now)) candidate = candidate.plusDays(1)
    return candidate.toInstant()
}

private fun immutablePipelineIds(values: Set<String>): Set<String> =
    Collections.unmodifiableSet(values.toSet())
