# Resumable and Recurring Discovery

> **Status:** Implemented Phase 5; unreleased
>
> **Owner:** `trawler-core`, with RSS/Atom/sitemap implementations in `trawler-web` and durable state in `trawler-postgres`
>
> **Since:** 0.1.0-SNAPSHOT

## Purpose

Phase 5 adds optional resumable discovery to the existing finite engine. A
discovery can emit an opaque, codec-versioned checkpoint candidate beside a
request. Core advances that checkpoint only after the request has been
accepted by the configured state adapter. A later run receives the compatible
checkpoint and can resume the walk without turning discovery state into a
user-data record.

Recurring harvest is an explicit strategy. A stateless pipeline does not load
or write harvest state. Cursor, watermark, and historical-then-incremental
strategies are replaceable policy objects; they do not change the generic
`T` pipeline path.

## Non-goals

This phase does not implement response replay, produced-data persistence,
outbox delivery, browser or anti-bot transport, site-specific pagination,
external scheduling, or a universal XML/JSON serializer. The XML discovery
implementations do not infer a site's feed or sitemap conventions beyond the
standard RSS, Atom, and sitemap location elements.

## Public contracts and ownership

Core owns the contracts and operational coordination:

- `ResumableDiscovery` and `DiscoveryCheckpointCodec` own checkpoint meaning,
  codec ID/version, and migration decisions;
- `DiscoveryEmission` carries an optional `DiscoveryCheckpointCandidate`;
- `DiscoveryCheckpointStore` owns revisioned compare-and-set storage;
- `AtomicEnqueueAndCheckpointStore` declares an adapter transaction boundary;
- `CheckpointQuarantineStore` retains incompatible checkpoint bytes;
- `HarvestStrategy` owns recurring mode, admission, completion, and optional
  `HarvestStateCodec` interpretation;
- `DiscoveryAdmissionPolicy` owns backlog admission;
- `IncrementalBoundaryPolicy` owns deterministic half-open overlap windows;
- `HarvestStateStore` and `HarvestStateQuarantineStore` own optional opaque
  recurring state;
- `PipelineEligibilityPolicy`, `PipelineHealthPolicy`, and
  `CircuitBreakerPolicy` own optional pipeline admission and health decisions.

`trawler-web` owns `RssDiscovery`, `AtomDiscovery`, `SitemapDiscovery`,
`XmlDiscoveryBounds`, and `XmlCursorCheckpointCodec`. These implementations
call the engine-provided `RequestExecutor`; they do not create a Ktor client.
`trawler-postgres` owns the V2 tables and transaction implementation. The
in-memory store implements the same capabilities with process-local bounds.

The checkpoint and harvest payloads are `CheckpointPayload` values with
defensive copying. Core never interprets their bytes, converts them to JSON,
or stores `T` in them.

## Configuration and built-ins

`PipelineBuilder.harvest(...)` installs one explicit harvest strategy;
`replaceHarvest(...)` is required for replacement. The default is
`StatelessHarvestStrategy`. Harvest strategy is pipeline-local and is not
inherited from engine defaults; the ordinary policy preset installs:

| Policy | Default | Behavior |
|---|---|---|
| Discovery admission | `Continuous` | Runs every selected discovery when the pipeline is eligible. |
| Backlog-first | `BacklogFirst` | Defer fresh discovery while open work exists, but resume an unfinished checkpoint. |
| Queue watermark | `QueueWatermark(low, high)` | Hysteresis gate: close at `high`, reopen at `low`; unfinished checkpoints resume. |
| Eligibility | `AlwaysEligiblePipelinePolicy` | Honors a non-expired operator disable override. |
| Health | `NoOpPipelineHealthPolicy` | Records bounded counters without opening a circuit. |
| Circuit breaker | `NeverOpenCircuitBreakerPolicy` | Never suppresses a pipeline unless replaced. |

`CursorHarvestStrategy` uses resumable discovery checkpoints and does not
require a separate harvest-state codec. `WatermarkHarvestStrategy` and
`HistoricalThenIncrementalHarvestStrategy` use an extension-owned, bounded
instant boundary codec. A watermark window is `(lastBoundary - overlap, now)`;
historical-then-incremental uses no window until historical completion is
committed. `FixedOverlapBoundaryPolicy` is deterministic for an injected clock.

Non-stateless strategies that provide a harvest-state codec require a
`HarvestStateStore`. A resumable discovery always requires a checkpoint-capable
state store. Configuration validation checks these capabilities and advertised
interface implementations before runtime work or scheduler children start.

## Checkpoint lifecycle and atomicity

For each emitted item:

```text
discover -> policy admission -> identity/scope/dedup -> accept work
                                      └──────────────> advance checkpoint
```

The engine assigns the next revision. A candidate cannot choose its own
revision or completion flag. An adapter implementing
`AtomicEnqueueAndCheckpointStore` performs bounded enqueue, active
deduplication, and checkpoint CAS in one transaction. The PostgreSQL adapter
uses this path.

Without that capability, core enqueues first and then performs checkpoint
CAS. A CAS conflict leaves accepted work in the queue and marks the run
abandoned; the next walk may replay the emission, but active identity
deduplication suppresses a still-open duplicate. This is a replay-safe,
at-least-once discovery window, not an exactly-once claim.

Codec resolution is explicit. A codec may use a compatible payload, migrate
it, reset it, or require quarantine. Quarantine-capable stores retain the
opaque bytes and reason before core installs an empty current-codec state.
Compare-and-set revision conflicts fail deterministically rather than allowing
a stale writer to overwrite progress.

Checkpoint `completed` is set only after the relevant discovery reaches its
natural end, open generation work is settled, the run is completed, and the
selected harvest strategy returns `Commit`. Budget exhaustion, cancellation,
failure, admission deferral, an open backlog, or a completion predicate that
fails leaves the checkpoint resumable and historical coverage incomplete.

## Harvest and pipeline state

Harvest state is separate from per-emission discovery checkpoints. A strategy
codec resolves its own `strategyId`, codec ID/version, and payload. State CAS
uses the same immediate revision rule as checkpoints. The engine marks all
relevant checkpoints complete only after the conservative completion conditions
pass. It then commits an optional harvest-state candidate; this ordering leaves
an incomplete harvest state, rather than a false historical-complete state, if
the process stops between the two adapter calls. A state or checkpoint CAS
conflict leaves the run abandoned and does not silently claim completion.

`HistoricalThenIncrementalHarvestStrategy` can commit
`historicalComplete=true` only when all of these are true:

- every resumable discovery reached its natural end;
- no generated work remains open;
- the run outcome is `COMPLETED`;
- the strategy completion predicate passes.

The built-in health and eligibility state is operational metadata only. It
contains no response body, produced value, request payload, or domain record.
Expired operator overrides are treated as absent. A circuit decision affects
only its selected pipeline; it never fails unrelated pipelines.

## XML discovery bounds and transport ownership

`RssDiscovery`, `AtomDiscovery`, and `SitemapDiscovery` all fetch through the
normal engine request path, so scope, authorization, concurrency, pacing,
middleware, transport safety, and response limits are applied consistently.
The implementations use JDK StAX with external entities and DTDs disabled.

`XmlDiscoveryBounds` defaults are:

| Bound | Default |
|---|---:|
| Compressed/input document bytes | 2 MiB |
| Locations/items per document | 10,000 |
| Location length | 16 KiB |
| Sitemap recursion depth | 4 |
| Sitemap documents per walk | 256 |

Gzip is decoded only after the input bound is checked and the decompressed
output is bounded by the same document limit. Sitemap traversal is breadth
first and its pending document collection is bounded by the document and item
limits. Feed and sitemap links resolve against the document target and must
remain absolute HTTP(S) locations within the configured length bound.

`PaginationStrategy` is a small core hook for custom discoveries: it receives
one response and returns either completion or one next request. The owning
discovery remains responsible for bounded page count, checkpoint candidates,
and engine-controlled execution.

## PostgreSQL schema and capabilities

`trawler-postgres` V2 adds only operational state:

| Table | Purpose |
|---|---|
| `discovery_checkpoints` | One revisioned checkpoint per pipeline/discovery. |
| `discovery_checkpoint_quarantine` | Incompatible checkpoint bytes and reason. |
| `harvest_state` | One revisioned strategy state per pipeline/strategy. |
| `harvest_state_quarantine` | Incompatible harvest bytes and reason. |
| `pipeline_health` | Compare-and-set health counters and circuit time. |
| `pipeline_overrides` | Explicit operator eligibility override and expiry. |

The adapter advertises `DISCOVERY_CHECKPOINTS`,
`ATOMIC_ENQUEUE_AND_CHECKPOINT`, `CHECKPOINT_QUARANTINE`, `HARVEST_STATE`,
`HARVEST_STATE_QUARANTINE`, `QUEUE_WATERMARKS`, `PIPELINE_HEALTH`, and
`PIPELINE_ELIGIBILITY_OVERRIDES` in addition to its Phase 4 capabilities.
Payloads, reasons, identifiers, batches, and database operations remain
bounded by `PostgresStateConfiguration`. The V2 migration is adapter-owned;
core has no JDBC, Flyway, SQL, or PostgreSQL type.

## Lifecycle, cancellation, and failure

Discovery and harvest calls run inside the existing structured pipeline scope.
Cancellation propagates through the engine-provided executor, discovery
flows, workers, and extension cleanup. A cancelled or timed-out run is not a
successful harvest completion. Queued work is cancelled according to the
existing in-memory or PostgreSQL state-store capability; leased work remains
subject to its owner/fencing rules.

Discovery network failures are reported as discovery failures and do not make
checkpoint advancement appear successful. One pipeline's admission, health,
codec, or harvest failure is isolated from unrelated pipeline registrations.

## Verification and limitations

Verified during this phase with focused Maven compilation of core, web, and
PostgreSQL, plus full reactor clean compile/verify and dependency-tree review.
The controlled manual demonstrations cover checkpoint CAS ordering, in-memory
resume and recrawl behavior, XML bounds/gzip/relative-link handling, and
PostgreSQL Flyway migration visibility. No feature tests are included in this
infrastructure phase; checkpoint ordering, XML bounds, and durable recovery
coverage are planned in [`TEST-MATRIX.md`](TEST-MATRIX.md).

The in-memory implementation is process-local and its recurring state and
quarantine history are finite. `QueueWatermark` hysteresis is held by the
policy instance and is not durable. The fallback enqueue-then-CAS path has an
at-least-once duplicate window. No implementation claims exactly-once
discovery, output, or external delivery.
