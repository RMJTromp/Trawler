# Architecture Contract

> **Status:** Normative summary
>
> **Full plan:** [`PLAN.md`](../PLAN.md)

## Governing rule

> **Core owns coordination. Extensions own policy. The host owns data.**

Use this question for every proposed core type:

> Would this type still make sense for an arbitrary output type, no SQL database, no response archive, no scheduler, and a custom/non-HTTP transport?

If not, it belongs in an adapter, integration, example, or host application.

## Fixed core invariants

Core may fix only architectural semantics:

- structured coroutine ownership;
- cancellation propagation;
- lifecycle state and cleanup order;
- type-safe pipeline composition;
- immutable request/response/context contracts;
- deterministic extension registration and validation;
- explicit error propagation;
- bounded resource ownership;
- capability negotiation.

Core must not fix scraping policy:

- database or serialization format;
- output domain model;
- transport implementation;
- scheduling;
- concurrency numbers or pacing algorithm;
- retry or failure classification algorithm;
- URL identity/canonicalization/deduplication;
- recrawl behavior;
- robots or private-network behavior;
- response archival;
- discovery mechanism;
- anti-bot provider;
- output destination.

## Type ownership

```text
TrawlerEngine                         non-generic coordinator
  ├── Pipeline<A>                     typed binding
  │     Discovery* -> Request -> Transport -> Response
  │     -> Extractor<A> -> Transform/Validator<A>*
  │     -> Produced<A> -> Output<A>*
  └── Pipeline<B>                     unrelated typed binding
        Discovery* -> Request -> Transport -> Response
        -> Extractor<B> -> Transform/Validator<B>*
        -> Produced<B> -> Output<B>*
```

`A` and `B` must never be converted to a universal JSON or entity representation for engine convenience. The heterogeneous registration boundary is isolated internally and constructed through typed registration.

Phase 1 names this boundary `Pipeline<T>`, `PipelineBuilder<T>`,
`EngineConfigurationBuilder`, `EngineConfiguration`, and the non-generic
`TrawlerEngine`. The builder resolves engine defaults and pipeline-local
overrides into immutable pipelines before registration. Its internal
heterogeneous registry stores typed registration owners and exposes only
star-projected definitions for inspection; execution is dispatched through the
captured typed owner and has no value-processing cast. Phase 2 adds the
structured runtime behind this boundary without changing the type ownership
model.

## Ownership matrix

| Concern | Owner |
|---|---|
| Lifecycle, cancellation, extension ordering | Core |
| Typed extraction flow and provenance context | Core |
| Request/response contracts | Core |
| What requests mean and how identity is computed | Pipeline policy |
| Network or other I/O execution | Transport adapter |
| HTTP execution, redirect policy, robots, and public-network safety | `trawler-web`; replaceable web policies |
| Crawl operational state | State-store adapter; `trawler-core` supplies a bounded in-memory implementation and `trawler-postgres` supplies durable state |
| Raw response retention/cache/replay | Response-store adapter; `trawler-core` supplies no-response, in-memory, and bounded JDK file implementations |
| Produced value `T` | Host application |
| Encoding or persistence of `T` | Output adapter/host; caller-owned codecs remain outside core's data model |
| Scheduling triggers | Host or selected scheduling extension; a replaceable coroutine implementation ships in core |
| Robots/public-network safety | Optional web policy adapter |
| Anti-bot/browser behavior | Optional adapter; `trawler-flaresolverr` supplies FlareSolverr transport and adapter-local challenge clearance |
| Logging/metrics/tracing export | Observer adapter |

## Dependency rule

```text
examples/integrations -> adapters -> core
```

Core must have no reverse dependency on an adapter. Shared code is not automatically core: it belongs in core only when it expresses generic engine semantics rather than adapter convenience.

Logical ownership does not require one artifact per extension. Lightweight dependency-free official implementations may ship in `trawler-core`; cohesive external technology stacks use separate artifacts. [`MODULES.md`](MODULES.md) defines the physical packaging boundary.

## Independent service boundaries

### Crawl state store

Owns engine-operational state such as work, claims, retries, optional checkpoints, and optional run history. The in-memory implementation is valid for one process. Durable or multi-process features require declared capabilities.

The implemented `trawler-postgres` adapter supplies durable resources, visit
jobs, row-lock claims, sequence-backed fencing tokens, meaningful attempts,
cooldown/next-eligibility state, engine/pipeline run summaries, resumable
checkpoints, harvest state, pipeline health, and eligibility overrides. Its
`RunHistoryStore` hook remains lifecycle metadata only; stale summary recovery
does not release job leases or bypass the lease predicates. The adapter does
not merge response, output, or user-data concerns into operational state. See
[`POSTGRES-STATE.md`](POSTGRES-STATE.md) and
[`RECURRING-DISCOVERY.md`](RECURRING-DISCOVERY.md).

### Response store

Owns optional response lookup, caching, history, replay, and retention.
No-store is valid. Enabling replay requires a replay-capable implementation;
the engine then uses the normal typed extraction/output path without jobs or
network calls. Response codecs and output codecs are caller-owned and exact
ID/version selection prevents silent historical reinterpretation.

### Output

Consumes `Produced<T>`. It may call back into the host, write files, map
values to an adapter-owned representation, publish messages, or use a durable
outbox decorator. Output-specific codecs and tables do not belong to state
storage.

Retention is independently scoped to operational state, responses, output
history, and outbox history. PostgreSQL output/outbox lineages protect local
response rows until the owning history is pruned. No retention operation
claims exactly-once delivery or durable live-event history.

## Required durable model

Durable recurring implementations separate:

```text
Resource        stable logical request identity
  └── Job       one desired visit
       ├── Lease        fenced temporary ownership
       └── Attempt      meaningful execution start/outcome
```

Rules:

- active jobs may be deduplicated without preventing future visits;
- a lease acquisition does not consume retry budget;
- every lease-owned mutation includes its token;
- checkpoints advance after accepted work, never before;
- checkpoint codecs are extension-owned and versioned;
- checkpoint and harvest revisions use compare-and-set, with explicit
  migration, reset, or quarantine outcomes;
- atomic enqueue-plus-checkpoint is an adapter capability; the fallback is
  replay-safe and may emit an at-least-once duplicate;
- response references are optional and opaque;
- user output never enters state tables.

## Optional recurring harvest model

Historical/incremental behavior is an installed strategy, not universal engine state. A historical completion claim is allowed only after its discovery end condition, relevant checkpoints, dependent jobs, run outcome, and configured completion predicate are all settled. Permanently failed items count as settled only for strategies that explicitly opt in; infrastructure, store, output, and exhausted transient failures always block completion (ADR-0010).

Stateless and one-shot pipelines must not pay for harvest state they do not use.

## Phase 5 resumable discovery

The implemented recurring-discovery layer loads compatible state before a
discovery walk, supplies the checkpoint and optional incremental window through
`DiscoveryContext`, and advances checkpoint state only after accepted work.
`BacklogFirst`, `Continuous`, and queue-watermark admission are replaceable
policies. A harvest strategy can commit state only after all relevant
resumable discoveries reach natural end, generated work settles, the run is
completed, and its predicate passes. Budget exhaustion and cancellation leave
state resumable and do not claim historical coverage.

RSS, Atom, and sitemap discovery live in `trawler-web` and use the same
engine-controlled request path as ordinary work. Their JDK StAX/gzip parser is
bounded and does not introduce a core parser dependency. See
[`RECURRING-DISCOVERY.md`](RECURRING-DISCOVERY.md).

## Delivery semantics

Completion means all required outputs have acknowledged according to their declared contract. Generic exactly-once delivery is not promised.

For non-transactional outputs, a process may fail after external acceptance but before job completion. Such outputs need idempotency, value deduplication, or an optional durable output/outbox adapter. Each adapter documents its duplicate window.

The implemented file output forces accepted frames to its configured file
channel. PostgreSQL typed output acknowledges after commit. PostgreSQL outbox
delivery is explicit and fenced; a crash after delegate acceptance but before
the fenced acknowledgement can produce a duplicate.

## Phase 2 database-free runtime

The implemented core vertical slice uses one structured engine scope rooted in a
`SupervisorJob`. `runOnce` starts the engine when needed, serializes finite
engine runs, and executes selected pipeline runs under supervisor isolation.
Engine extensions start in order and stop in reverse order; pipeline-owned
extensions have the same ordering within each pipeline run. `awaitIdle` and
shutdown waits are explicitly bounded. Caller cancellation propagates through
discovery, transport, extraction, typed stages, outputs, scheduler children,
permits, pacing reservations, and lease renewal.

The in-memory state implementation keeps stable resource identity, visit jobs,
lease ownership, and meaningful attempts as separate records. Active work is
deduplicated atomically while completed resources remain eligible for a later
recrawl policy decision. State capacity and retained history are finite. A
stale or expired lease is reclaimed and cannot accept a late fenced mutation.

Discovery is collected through an engine-controlled `RequestExecutor` with
bounded `Flow` emissions. Workers use pipeline-scoped claims and bounded
execution. Response retention is optional and independent from both state and
typed output. Callback and bounded `Flow<Produced<T>>` outputs acknowledge
typed values without introducing a core serialization or record model.

Scheduling is an opt-in layer above `runOnce`. The bundled coroutine
scheduler supports manual/finite triggers, fixed delay, fixed rate, daily local
time with an explicit timezone, immediate first execution, selected pipeline
sets, bounded skip/queue/reject overlap behavior, cancellation, and observable
trigger/run failures. No scheduler is active by default. One active scheduler
may own a given pipeline selection; replacement is explicit cancellation then
registration.

Generic live events are delivered to observers in registration order. Observer
drops and failures do not become hidden retries, and event metadata is bounded
and does not contain `T`, response bodies, or secrets. Live events are not
durable audit history.

## Runtime profiles

### Minimal profile

```text
in-memory state
  + caller discovery/transport/extractor
  + callback, no-output, or bounded Flow output
  + runOnce
```

No database, serializer, response archive, scheduler, or web policy is
required. A scheduler and an in-memory response store can be selected when
their process-local behavior is wanted.

### Durable web profile

```text
PostgreSQL state adapter
  + optional response store
  + Ktor transport adapter
  + public-web policy preset
  + typed output adapter
  + host/scheduler-triggered runOnce
```

Both profiles use the same core contracts.

## Phase 4/5/6 PostgreSQL state, response, and output adapters

`trawler-postgres` is an optional adapter profile that can replace the
in-memory state store without changing a typed pipeline. State-adapter startup
runs only its state-owned Flyway migrations; an ordinary engine run explicitly
performs bounded stale run-summary recovery, while replay does neither. Runtime
claims use PostgreSQL `FOR UPDATE SKIP LOCKED`, active work uses a partial
unique index, and every lease-owned mutation checks the current owner and
fencing token. Lease expiry is reclaimed at the beginning of a bounded claim
transaction. A lease is not an attempt: `attempt_count` changes only when a
worker successfully begins meaningful execution. V2 adds revisioned
checkpoints, harvest state, quarantine records, health, and eligibility state.
Phase 6 adds independently migrated response history, typed output history,
lineage, and optional outbox tables; none is a universal extracted-record
table or a replacement for operational state.

The adapter owns a Hikari pool and a fixed dispatcher whose parallelism cannot
exceed the pool size. Its queue, claim/cancellation/recovery batches, request
payloads, identifiers, and JDBC waits are bounded. Terminal jobs do not block
later jobs for the same stable resource, and resource cooldown is separate
from active deduplication. Request payloads carry explicit codec IDs and
versions. Produced `T` and response/output payloads enter only the separately
owned adapter tables when those capabilities are selected. PostgreSQL is
optional, and the core dependency tree remains Kotlin stdlib plus coroutines.

## Phase 3 web adapter

The implemented `trawler-web` module is an optional adapter profile. It owns
one Ktor CIO client per configured transport, adapts HTTP bodies and headers
to the core models, and keeps redirect hops inside a bounded transport-owned
loop. Ktor automatic redirects are disabled so every hop reruns destination
scheme/scope checks, DNS/address safety, robots, origin concurrency, pacing,
cookie selection, and credential-forwarding policy before the next request.
Previous-hop permits are released before the next origin is acquired.

The public-web preset is opt-in and fail-closed for loopback, private,
link-local, multicast, metadata, carrier-grade NAT, and benchmark addresses.
An internal-network or custom destination policy remains possible through the
same replaceable `WebTransportPolicies` bundle. DNS is revalidated for every
hop, but the adapter does not claim CIO socket-address pinning against a
DNS-to-connect race.

Response bodies, decoded bodies, headers, redirects, DNS answers, robots
documents, cookie entries, origin maps, and pacing waits are bounded. Non-2xx
responses remain inspectable `CrawlResponse` values. Web classification and
the optional `DefaultHttpRetryPolicy` are separate from transport execution;
no retry plugin or hidden client loop changes core attempt accounting. The
web preset's diagnostic redaction policy creates safe header copies without
mutating the headers used for HTTP execution.

Core's ordered request/response middleware remains the only pipeline
middleware composition path: inherited middleware precedes local additions,
request middleware wraps the transport, and response middleware sees the
final adapted response. The web module provides only opt-in helpers and does
not add an unobservable Ktor retry/interceptor chain. See
[`WEB-ADAPTER.md`](WEB-ADAPTER.md) for lifecycle, robots, security, and
dependency details.

## Change test

Before accepting an architectural change, answer all of these:

1. Does core remain useful without the proposed adapter?
2. Does `T` remain typed and host-owned?
3. Is every new policy replaceable?
4. Are optional capability requirements validated?
5. Are lifecycle, cancellation, and cleanup explicit?
6. Are queues, bodies, buffers, attempts, and waits bounded?
7. Are delivery and recovery guarantees honest?
8. Is the owning dependency/module documented?
9. Does documentation describe actual rather than planned behavior?

Any “no” blocks completion unless the architecture plan is intentionally amended first.
