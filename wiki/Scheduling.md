# Scheduling

Scheduling is opt-in. Constructing a TrawlerEngine and calling runOnce never starts a scheduler. A daemon is a structured child of the engine and is stopped by its EngineHandle or by engine shutdown.

## Triggers

| Trigger | Behavior |
| --- | --- |
| ScheduleTrigger.Manual | Only EngineHandle.trigger submits a run |
| ScheduleTrigger.Finite | Emits a finite number of triggers separated by interval |
| ScheduleTrigger.FixedDelay | Waits after the preceding scheduled work settles |
| ScheduleTrigger.FixedRate | Computes a fixed cadence from the injected clock |
| ScheduleTrigger.Daily | Computes the next local time in an explicit ZoneId |

Every trigger has OverlapBehavior and maxQueuedRuns. OverlapBehavior.SKIP drops a trigger during active work, QUEUE retains a bounded number, and REJECT reports a rejection.

## Starting a daemon

~~~kotlin
val handle = engine.startDaemon(
    CoroutineScheduler(),
    DaemonConfig(
        trigger = ScheduleTrigger.FixedDelay(
            delay = Duration.ofHours(24),
            maxRuns = 7,
            overlap = OverlapBehavior.SKIP,
        ),
        pipelineIds = setOf("daily-source"),
    ),
)

handle.completion.await()
~~~

The example is conceptual. A production host should keep the handle in a structured owner and call cancel or shutdown during application shutdown.

The overload engine.startDaemon(config) creates the default CoroutineScheduler. The overload with an explicit Scheduler is useful when the application owns a scheduler implementation but still wants Trawler's engine lifecycle and selection rules.

## Overlap and queue bounds

One selected pipeline set may have only one active scheduler reservation. Starting another daemon for the same overlapping selection is rejected. The pending trigger queue is bounded by maxQueuedRuns; there is no unbounded backlog.

The run itself is still subject to EngineRuntimeOptions.maxPendingRuns, maxWorkItemsPerRun, runTimeout, and the state store's queue bounds.

## Clock behavior

Fixed-rate and daily calculations use the configured Clock. The scheduler does not use Thread.sleep; it uses cancellable coroutine delay. A delay can be interrupted by scheduler cancellation or engine shutdown.

Daily schedules require an explicit ZoneId because local-time recurrence is not well-defined without a timezone. Daylight-saving transitions are handled by the Java time calculation used by the scheduler; operators should still choose a policy for skipped or repeated local times in their deployment documentation.

## Cancellation

EngineHandle.cancel stops only that daemon and leaves the engine available for a later direct run. EngineHandle.shutdown stops the daemon and then shuts down the engine. If the engine is shut down first, active scheduler handles complete through the same structured cancellation path.

## Scheduling versus harvest

Scheduling answers “when should a run start?” Harvest answers “what source boundary should this run use?” A daily scheduler with a stateless discovery rereads its bounded source daily. A daily scheduler with CursorHarvestStrategy or WatermarkHarvestStrategy resumes source state. Configure both intentionally.

