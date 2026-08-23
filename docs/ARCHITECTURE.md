# Architecture Contract

> **Status:** Normative summary
>
> **Full plan:** [`plans/FINAL-PLAN.md`](../plans/FINAL-PLAN.md)

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
| Crawl operational state | State-store adapter; `trawler-core` supplies a bounded in-memory implementation |
| Raw response retention/cache | Response-store adapter; `trawler-core` supplies no-response and bounded in-memory implementations |
| Produced value `T` | Host application |
| Encoding or persistence of `T` | Output adapter/host |
| Scheduling triggers | Host or selected scheduling extension; a replaceable coroutine implementation ships in core |
| Robots/public-network safety | Optional web policy adapter |
| Anti-bot/browser behavior | Optional adapter |
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

### Response store

Owns optional response lookup, caching, history, and replay. No-store is valid. Enabling replay requires a replay-capable implementation.

### Output

Consumes `Produced<T>`. It may call back into the host, write files, map values to JDBC, publish messages, or use a durable outbox decorator. Output-specific serialization and tables do not belong to state storage.

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
- response references are optional and opaque;
- user output never enters state tables.

## Optional recurring harvest model

Historical/incremental behavior is an installed strategy, not universal engine state. A historical completion claim is allowed only after its discovery end condition, relevant checkpoints, dependent jobs, run outcome, and configured completion predicate are all settled.

Stateless and one-shot pipelines must not pay for harvest state they do not use.

## Delivery semantics

Completion means all required outputs have acknowledged according to their declared contract. Generic exactly-once delivery is not promised.

For non-transactional outputs, a process may fail after external acceptance but before job completion. Such outputs need idempotency, value deduplication, or an optional durable output/outbox adapter. Each adapter documents its duplicate window.

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
