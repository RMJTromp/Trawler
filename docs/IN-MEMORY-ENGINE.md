# Database-Free End-to-End Engine

> **Status:** Implemented
>
> **Owner:** `trawler-core`
>
> **Since:** 0.1.0-SNAPSHOT, unreleased

## Purpose

Phase 2 turns the Phase 1 typed composition boundary into a bounded,
database-free vertical slice. A non-generic `TrawlerEngine` can start
multiple unrelated typed pipelines, collect bounded discovery, execute caller
transport and middleware, preserve each pipeline's `T`, and deliver
`Produced<T>` to typed outputs.

The runtime is process-local. It is intended for one-process runs, manual
invocation, and small finite or opt-in daemon executions. It does not imply
durable restart recovery, multi-process coordination, HTTP behavior, or
exactly-once delivery.

## Non-goals

This phase does not implement:

- Ktor or any HTTP client;
- PostgreSQL, JDBC, SQL, migrations, or durable state;
- robots, public-network safety, redirects, RSS, sitemap, or anti-bot policy;
- response files, WARC, serialization, parsers, or domain models;
- durable audit history, durable scheduling, or process recovery;
- a universal queue, storage, plugin, or produced-record abstraction;
- adapter-specific integration tests; repository-wide test infrastructure is
  documented separately in [`TESTING.md`](TESTING.md).

## Public type ownership

| Area | Implemented types | Owner and boundary |
|---|---|---|
| Engine lifecycle | `TrawlerEngine`, `EngineHandle`, `RunRequest`, `RunResult` | Core owns lifecycle, cancellation, coordination, and bounded run results. The caller owns the suspending call and decides when to invoke or schedule it. |
| Pipeline execution | `Pipeline<T>`, `PipelineRunResult` | Each internal registration owner retains its own `T`; the engine never accepts a produced value at its heterogeneous boundary. |
| Discovery | `SeedDiscovery`, `FlowDiscovery` | Core adapts finite seeds or a caller `Flow`; discovery does not own a transport client. |
| Crawl state | `InMemoryCrawlStateStore` and focused state capabilities | Core owns the process-local implementation; it stores resource/job/lease/attempt metadata only, never `T` or response bodies. |
| Response retention | `NoResponseStore`, `InMemoryResponseStore` | Response storage is optional and independent from crawl state and output. The store owns retention and eviction. |
| Output | `CallbackOutput<T>`, `FlowOutput<T>`, `NoOutput<T>` | The host owns callback side effects and `Produced<T>`. `FlowOutput` owns only its bounded channel. |
| Policy | `BuiltInPolicies` and individual policy objects | Policies remain replaceable. Built-ins supply ordinary in-memory defaults but are not hidden network or web behavior. |
| Scheduling | `Scheduler`, `CoroutineScheduler`, `ScheduleTrigger` | Scheduling is an opt-in extension above `runOnce`; the scheduler owns trigger timing and its bounded pending queue. |
| Events | `CrawlerEvent`, `Observer`, `SchedulerEvent` | Events contain bounded lifecycle metadata only. Observers do not become an implicit durable audit store. |

The typed path remains:

```text
Discovery -> CrawlRequest -> Transport -> CrawlResponse
  -> Extractor<T> -> Transform<T>* -> Validator<T>*
  -> Produced<T> -> Output<T>*
```

No stage converts `T` to JSON, `Any`, a map, a record envelope, or a database
row. The only internal star projections expose pipeline definitions for
configuration inspection; execution is dispatched through the captured typed
registration owner.

## Configuration and defaults

`EngineConfigurationBuilder` validates the complete immutable graph before
constructing an engine. If no state store is selected, it creates a bounded
`InMemoryCrawlStateStore` using the configured queue, retention, lease, and
clock options. If no policy bundle is selected, it installs the ordinary
replaceable built-ins: digest identity, active-work deduplication, always
recrawl, no retry, immediate pacing, bounded fixed concurrency, allow-all
scope/authorization, FIFO ordering, and an engine-bounded budget.

The default response choice is no response store. A response store is only
used when selected, and required response capabilities are checked before
runtime work. A scheduler is never started by construction or by direct
`runOnce`.

`EngineRuntimeOptions` bounds queue admission, pending direct runs, work per
run, discovery emissions, follow-ups, values per response, request/response
bodies, request/response targets, methods, headers, attributes, produced
values, attempts, workers, claims, output batches, lease duration, run and
shutdown waits, and retained in-memory state. Invalid or non-positive bounds
fail during configuration construction. `RunRequest` and `DaemonConfig` copy
selected pipeline IDs at construction. Metadata bounds are enforced again
after canonicalization, middleware, and response adaptation so extensions
cannot grow the in-memory request/response graph without a configured limit.
Checkpoint and harvest payloads are also rejected when they exceed the
configured in-memory checkpoint bound, including direct compare-and-set
updates.

The state adapter applies its configured retention bounds to terminal jobs,
completed attempts, resources, and recurring checkpoint/harvest/health/override
maps. It removes the oldest eligible history while retaining active work. An
atomic discovery admission also requires every item's context pipeline to
match the operation pipeline; that validation happens before lease reclaim,
capacity checks, enqueue, or checkpoint mutation.

Single-cardinality configuration remains explicit: `stateStore`, response
store, transport, extractor, policy slots, and inherited defaults reject
duplicate registration unless the corresponding `replace...` method is used.
Ordered discovery, middleware, transforms, validators, outputs, observers,
and lifecycle extensions retain registration order; inherited middleware is
resolved before local additions. Scheduler selections are zero or one active
daemon per overlapping pipeline selection; replacing one requires cancelling
the existing handle first.

## Lifecycle and state transitions

The engine lifecycle is:

```text
NEW -> STARTING -> RUNNING -> SHUTTING_DOWN -> STOPPED
        \-> FAILED -> SHUTTING_DOWN -> STOPPED
```

Engine extensions start in registration order. A startup failure stops already
started extensions in reverse order, marks the engine failed, and prevents
runtime work. Shutdown stops scheduler loops and active runs, waits only up to
the configured bound, cancels the engine-owned root scope, stops engine
extensions in reverse order, and closes outputs implementing `OutputLifecycle`.
Non-cooperative caller code cannot extend the shutdown wait indefinitely.

Network-free response replay uses a separate `REPLAYING` lifecycle branch from
`NEW`. It starts only explicitly replay-safe lifecycle resources, never starts
the crawl-state extension, and returns to `NEW` after its bounded response
processing completes. A replay running beside an already `RUNNING` engine does
not restart shared engine extensions.

One `runOnce` call is an engine run. Direct runs are serialized at the engine
boundary while selected pipeline runs execute under `supervisorScope`, so a
failure in one pipeline produces a failed pipeline result without
automatically cancelling unrelated pipelines. Caller cancellation propagates
as coroutine cancellation rather than being misreported as successful work.
Run timeouts return `ABANDONED` and retain any resumable operational work;
pipeline failures return `FAILED`; successful drained runs return
`COMPLETED`.

Pipeline extensions start in declared order for that pipeline run and stop in
reverse order with a completion, failure, abandonment, or cancellation
reason. A pipeline-owned lifecycle object cannot also be registered as an
engine extension; this prevents double start/stop ownership. Discovery is
collected before worker draining, through the
engine-provided `RequestExecutor`. Workers claim only their pipeline's work,
execute bounded processing, and stop when the state store reports no active
work. `awaitIdle` joins active engine runs with its explicit timeout.

The in-memory job state is deliberately separate:

```text
Resource(identity, pipeline)
  ├── Job(work item: one desired visit)
  │     ├── Lease(owner, expiry, fencing token)
  │     └── Attempt(meaningful execution number and outcome)
  └── later Job(work item: a permitted recrawl)
```

Leasing does not consume an attempt. An attempt begins immediately before
substantive response processing. A lease is renewed by a structured child
while work is active; a stale or rejected renewal stops the worker without
allowing a late state transition. All state mutations are fenced by the
leased work token. Expiry requeues work and marks an active attempt abandoned;
expiry itself is not a failed attempt.

`SUCCEEDED`, `RETRY_WAIT`, `FAILED`, `SKIPPED`, `CANCELLED`, and `ABANDONED`
remain distinct operational transitions. Cooperative cancellation transitions
queued work to `CANCELLED` and asks active owners to perform their own fenced
transition. A bounded shutdown can leave work reclaimable if caller code does
not honor cancellation before the wait expires.

## Active deduplication and recrawl

The default digest identity is based on request method, opaque target, and
body. Identity and canonicalization are replaceable. The state store atomically
checks active resource jobs while enqueueing, so equivalent active work is
suppressed without suppressing a later visit after a successful completion.
The default `AlwaysRecrawlPolicy` permits a later discovery emission to create
that later visit. `NeverRecrawlPolicy` and `RecrawlAfter` are available as
replaceable alternatives. A deferred recrawl is not hidden in a second queue:
the policy reports the eligible time and the current run is marked abandoned;
a later discovery/run can admit it when eligible.

`ResourceHistory.nextEligibleAt` is the adapter-neutral cooldown hint used by
`RecrawlAfter`. The in-memory store keeps it process-locally when a retry is
scheduled and clears it after a successful visit; the durable PostgreSQL
adapter persists the same operational hint in its resource row.

The atomic discovery admission capability combines bounded active-work
deduplication with checkpoint CAS. A capacity rejection or revision conflict
leaves both the queue and checkpoint unchanged. Direct checkpoint CAS uses the
same immediate-revision and payload rules and trims retained checkpoint state
after an accepted update.

## Discovery, worker, and output bounds

`SeedDiscovery` copies its request list. `FlowDiscovery` applies `take` to the
caller flow, and the engine applies per-discovery and per-run emission bounds
again. Follow-ups, extractor values, request/response bodies, output batches,
retained jobs/resources/attempts, and response entries are all bounded.

The worker count and concurrency policy are independent: workers claim
bounded state batches, while the policy controls request-stage permits. Every
permit and pacing reservation is released in a `finally` path. Cancellation
of discovery, transport, extraction, transformation, validation, or output
propagates through the structured worker scope.

`CallbackOutput<T>` invokes the host callback in the pipeline scope and
returns an explicit acknowledgement or failure. `FlowOutput<T>` exposes a
typed `Flow<Produced<T>>` backed by a bounded channel; an unready collector
applies backpressure rather than growing an unbounded buffer. `NoOutput<T>`
explicitly drops delivery without retaining values. Output acknowledgements
are not transactional with in-memory state, so a crash or callback failure
may produce duplicates or require caller idempotency. Exactly-once delivery is
not claimed.

## Response stores

`NoResponseStore` is an explicit no-retention choice. `InMemoryResponseStore`
retains only responses within its configured body and entry bounds, evicts the
oldest entries at capacity, and exposes optional lookup, history, replay, and
age eviction capabilities. It stores response metadata and bodies only inside
the response-store implementation; the state store remains body-free. The
memory store provides no restart, multi-process, or durable guarantee.

## Scheduler behavior

`CoroutineScheduler` is selected by `TrawlerEngine.startDaemon` or a caller's
`Scheduler` implementation. It calls the engine's `runOnce` function and does
not duplicate worker logic. Direct `runOnce` does not require it.

Supported triggers are:

- `Manual`, triggered through `EngineHandle.trigger`;
- finite trigger counts with an optional interval;
- fixed delay, measured after the preceding scheduled trigger settles;
- fixed rate, computed from the injected `Clock` cadence;
- daily local time in an explicit `ZoneId`.

Every trigger supports optional immediate first execution, finite limits where
applicable, selected pipeline IDs, and `SKIP`, bounded `QUEUE`, or `REJECT`
overlap behavior. Pending triggers are capped by `maxQueuedRuns`. `QUEUE`
never grows without bound. A scheduler is an opt-in structured child; cancel
stops its loop, pending triggers, and scheduler-owned active run. Engine
shutdown cancels every daemon before closing engine resources.

The injected clock controls observable trigger instant calculations. Coroutine
delays provide the actual waiting and therefore remain subject to the host
dispatcher and process lifecycle. Scheduler trigger skips, rejections, and
run failures are observable through `SchedulerEvent` and the engine's generic
event path. Observer failures are best effort and do not silently become
scheduler retries.

## Events and observer delivery

The engine dispatches each `CrawlerEvent` sequentially to observers in
registration order. An observer may return `Accepted`, `Dropped`, or
`Failed`; a dropped event is not retried, and an observer exception is counted
as an observer failure without failing the crawl. Event detail
is truncated to a bounded length and default events contain no response body,
credentials, headers, or produced value. Live observer delivery is not a
durable audit log and has no implicit replay buffer.

## Failure, retry, and cancellation

Thrown stage failures preserve their original cause in bounded run results.
When required response capture declines, the runtime reports the existing
capture stage failure without wrapping it again; policy and store causes are
also preserved. The failure classifier and retry policy decide whether work is failed,
retried immediately, retried at a later instant, or abandoned. Retries stay
visible to attempt accounting; the transport and middleware layers do not
hide retry loops. The ordinary built-in policy does not retry.

Required output failure fails the work according to that policy; best-effort
output failure is recorded without automatically failing the pipeline.
Cancellation is rethrown to the caller after queued and active work receive
the documented cancellation path. Coroutine permits, pacing reservations,
lease renewal children, output channels, pipeline extensions, engine
extensions, and output lifecycle resources have explicit cleanup ownership.

## Security, privacy, and retention

Core treats request targets, headers, attributes, bodies, and produced values
as opaque. The built-in allow-all scope and authorization policies are ordinary
in-memory conveniences, not a public-web safety preset. Hosts must install
appropriate authorization, scope, and network policies for their environment.
Default events and state records do not retain user values or response bodies;
the selected response store and output own their retention and privacy rules.

## Verification

The Phase 2 implementation was verified with:

- `mvn -Dkotlin.compiler.daemon=false clean compile`;
- `mvn -Dkotlin.compiler.daemon=false clean verify`;
- `mvn -Dkotlin.compiler.daemon=false -pl trawler-core -am clean verify`;
- `mvn -Dkotlin.compiler.daemon=false -pl trawler-core dependency:tree`, confirming only the existing
  Kotlin standard library, coroutines, and expected transitives;
- compiler-backed manual smoke runs for a custom discovery/transport/
  extractor/transform/validator/callback pipeline, direct bounded
  `Flow<Produced<T>>` collection, two unrelated typed pipelines in one engine
  with finite scheduling, recrawl, and cooperative shutdown;
- source scans for adapter imports, serialization shortcuts, unchecked casts,
  `GlobalScope`, `Thread.sleep`, and TODO/FIXME markers;
- final diff, status, and documentation-link inspection.

The core contract phase adds deterministic tests for in-memory state
transitions, checkpoint/harvest revisions and payload bounds, response
retention, configuration, discovery, codecs, built-in policies, coroutine
scheduling, daemon selection/cancellation, and controlled database-free
engine execution. `EngineRuntimeTest.kt` covers typed pipeline isolation,
middleware/routing/cache/capture, discovery and stage decisions, retries,
lease loss, cancellation and timeout cleanup, runtime bounds and budgets,
event privacy, lifecycle rollback, and network-free replay. Manual smoke
sources were temporary verification inputs outside the repository and are
not test fixtures. The plain `mvn clean compile` invocation was also attempted, but
the container denied the Kotlin daemon's user-level temporary directory;
disabling that daemon allowed the same reactor build to complete
successfully.

## Known limitations

- State, response retention, scheduler progress, and output channels are
  process-local and disappear on process loss.
- No durable checkpoint, run history, lease recovery, or multi-process fencing
  guarantee is provided by the in-memory implementation.
- A non-cooperative caller transport, extractor, or output can delay
  cancellation until the bounded shutdown wait expires; core cannot force
  arbitrary caller code to stop.
- Fixed-rate and daily scheduling computes instants from the injected clock,
  but the actual suspension uses coroutine delay rather than a virtual-time
  clock.
- The runtime intentionally leaves HTTP safety, redirects, robots, parsers,
  serialization, and domain-specific output to later adapters or hosts.
