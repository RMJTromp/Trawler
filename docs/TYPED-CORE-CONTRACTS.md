# Typed Core Contracts and Composition

> **Status:** Implemented
>
> **Owner:** `trawler-core`
>
> **Since:** 0.1.0-SNAPSHOT, unreleased

## Purpose

Phase 1 establishes the type-safe composition boundary for Trawler. It lets a
non-generic `TrawlerEngine` configuration register unrelated `Pipeline<A>` and
`Pipeline<B>` definitions while preserving each value type from extraction
through transformation, validation, provenance, and output.

This document records the Phase 1 public boundary. Phase 2 now consumes that
boundary through the database-free runtime described in
[`IN-MEMORY-ENGINE.md`](IN-MEMORY-ENGINE.md); the contracts still remain
adapter-neutral and no Phase 1 type owns a transport, database, serializer, or
domain model.

## Non-goals

The Phase 1 contract layer deliberately did not implement:

- the worker loop, `runOnce`, daemon execution, or scheduling;
- an in-memory or durable state-store implementation;
- HTTP/Ktor, PostgreSQL, file, browser, anti-bot, parser, or web behavior;
- response retention, replay, serialization, or output destinations;
- type-changing mappers, domain models, schemas, record keys, or entity tables;
- adapter-specific integration tests; repository-wide test infrastructure is
  documented separately in [`TESTING.md`](TESTING.md).

Durable persistence, technology-specific transports, web policy, parsers, and
domain models remain later phases or adapter responsibilities. The Phase 2
runtime invokes only the lightweight implementations listed in its feature
record.

## Public contracts

The public types are grouped by ownership:

| Area | Core types | Ownership |
|---|---|---|
| Request/response/context | `CrawlRequest`, `CrawlResponse`, `CrawlerContext`, stage contexts | Core owns immutable coordination metadata; the host/transport owns meaning and resources. |
| Typed pipeline values | `Produced<T>`, `ProductionContext`, `Extractor<T>`, `ExtractionResult<T>`, `Transform<T>`, `TransformDecision<T>`, `Validator<T>`, `ValidationResult` | The host owns `T`; core owns only typed stage coordination and provenance. |
| Output | `Output<T>`, `OutputBinding<T>`, `OutputResult` | The selected output owns encoding, persistence, acknowledgement, retention, and idempotency. |
| Discovery | `Discovery`, `DiscoveryContext`, `DiscoveryEmission` | The discovery extension owns admission semantics and uses the engine-provided `RequestExecutor` for I/O. |
| Transport | `Transport`, `TransportRouter`, request/response middleware | The transport adapter owns clients and I/O; core owns the controlled boundary. |
| Operational state | `CrawlStateStore` and focused `WorkQueueStore`, `LeaseStore`, `AttemptStore`, `ResourceHistoryStore`, `RunHistoryStore`, `DiscoveryCheckpointStore` capabilities | The state adapter owns operational state only, never `T` or response bodies. |
| Response retention | `ResponseStore`, `ResponseLookup`, response capabilities | The response adapter owns optional retention, replay, and privacy policy. |
| Observation/lifecycle | `CrawlerEvent`, `Observer`, `ExtensionLifecycle` | Observers receive bounded metadata; lifecycle resources remain under the engine scope. |
| Time/policy | `Clock`, `RandomSource`, and the focused policy interfaces | The host or extension selects policy; core does not hardcode web, retry, or scheduling algorithms. |

`CrawlRequest.target` is an opaque string and is not required to be a URL.
`CrawlResponse.statusCode` is optional, so custom, file, cached, API, browser,
and non-HTTP transports remain possible. `ContextAttributes` is a
caller-bounded string metadata facility, not `Map<String, Any?>`, JSON, or a
user-data record envelope. Request and response bodies defensively copy byte
arrays.

The typed value path is:

```text
Extractor<T>
  -> Transform<T>*
  -> Validator<T>*
  -> Produced<T>
  -> Output<T>*
```

`TransformDecision<T>` distinguishes `Accept`, `Mutate`, and `Drop` without
changing `T`. `ValidationResult.Rejected` is an expected data decision;
`ValidationResult.Failed` preserves an exceptional cause. `OutputResult`
separates acknowledgement, intentional skip, and failure. No core event or
state contract contains a produced value.

## Configuration

`Pipeline<T>` and `EngineConfiguration` are immutable snapshots. The DSL
builders are configuration-thread-confined and produce those snapshots before
runtime work. `TrawlerEngine` is non-generic and exposes only its immutable
configuration; runtime starts only through explicit `runOnce` or
`startDaemon` calls.

An engine configuration has exactly one effective `CrawlStateStore`; the
builder supplies the bounded process-local `InMemoryCrawlStateStore` by
default, and a caller can explicitly replace it with a capable adapter.
`ResponseStore` is optional and defaults to no retention. `Clock` and
`RandomSource` have replaceable JDK-backed defaults. Required state and response capabilities
are declared with `requireStateCapability` and `requireResponseCapability` and
are checked before configuration construction completes.

Phase 4 adds adapter-neutral `RunHistoryStore` and `DurableRunStatus` hooks for
operational engine/pipeline summaries, plus `ResourceHistory.nextEligibleAt`
for adapter-owned cooldown state. Core does not define their persistence
format; `trawler-postgres` implements them without changing the typed value
path. The in-memory store intentionally does not advertise durable run history
or recovery.

Configuration failures are reported as a sorted
`InvalidConfigurationException.issues` list. Diagnostics include a stable
path, `ConfigurationIssueCode`, and redacted message; they never include
request bodies, response bodies, credentials, or `T`.

## Extension cardinality and composition

The effective cardinality is:

| Extension | Cardinality and composition |
|---|---|
| State store | Exactly one engine-level selection; repeated registration is invalid unless `replaceStateStore` is used. |
| Response store | Zero or one; repeated registration is invalid unless `replaceResponseStore` is used. |
| Transport router | Exactly one per pipeline after inheritance; `transport`/`transportRouter` sets it and `replaceTransport` explicitly replaces it. |
| Extractor | Exactly one typed extractor per pipeline; `extract` sets it and `replaceExtractor` explicitly replaces it. |
| Discovery | Zero or more, appended in registration order. |
| Transform/validator | Zero or more, appended and applied in declared order. Both preserve `T`. |
| Output | Zero or more ordered `OutputBinding<T>` values. Each declares `REQUIRED` or `BEST_EFFORT`; no exactly-once claim is implied. |
| Request/response middleware | Ordered chains. Engine defaults precede pipeline additions. `replaceRequestMiddleware`/`replaceResponseMiddleware` discard inherited chains before local additions. |
| Observer | Zero or more, invoked in registration order. Live observation is not durable audit history. |
| Lifecycle extension | Engine extensions precede pipeline extensions at startup; shutdown is reverse order. An object cannot be registered in both ownership lists because that would make start/stop cardinality ambiguous. |
| Identity, deduplication, recrawl, failure classification, retry, pacing, concurrency, scope, authorization, ordering, and budget | Exactly one effective policy each. A pipeline-local policy method overrides the inherited bundle once; repeated local registration is invalid unless its `replace...` method is used. |

Calling an `add...`, `discover`, `transform`, `validator`, `output`,
`observe`, or `extension` method appends. Calling a single-cardinality setter
twice produces a deterministic conflict. Explicit `replace...` methods are the
only replacement path. This distinction prevents accidental policy stacking
while keeping ordered fan-out and middleware composition intentional.

## Heterogeneous registration safety

`EngineConfigurationBuilder.register` is generic at the public boundary:

**Conceptual API sketch (not compiled as a repository example):**

```kotlin
engineConfiguration.register<Alpha>("alpha") { /* typed Alpha stages */ }
engineConfiguration.register<Beta>("beta") { /* typed Beta stages */ }
```

Each call builds a `Pipeline<T>` and captures it in an internal
`TypedPipelineRegistration<T>`. The internal
`HeterogeneousPipelineRegistry` stores registration owners, not produced
values. Its inspection view is `List<Pipeline<*>>`; it has no operation that
accepts, converts, or casts a value. The direct immutable constructor creates
the same typed registration owner from each already-built definition. There
are no unchecked casts in core source, and the normal pipeline path never
erases `T` to `Any`, JSON, a map, or a library-owned record envelope.

This is safe because type-sensitive composition finishes before registration:
the extractor, same-type transforms, validators, and outputs are all held by
one `Pipeline<T>`. The non-generic engine may enumerate definitions and
validate metadata, while the Phase 2 runtime invokes the typed owner operation
without erasing the value type. This preserves the proof during execution.

## Lifecycle and state

Phase 1 defines the contracts that Phase 2 executes:

- `WorkQueueStore` bounds claim batches and returns lease-owned work;
- `LeaseStore` requires the current fencing token on validation, renewal, and
  transitions; validation does not extend the lease and is used before a
  response reference can be attached to produced data;
- `DiscoveryCheckpointStore` uses opaque codec/version metadata and
  compare-and-set revisions;
- response storage is independent of operational state;
- output acknowledgement is independent of both.

`ExtensionLifecycle.start` receives an engine-owned `CoroutineScope` and
`stop` receives an explicit reason. The Phase 2 runtime starts and stops
extensions in those orders, transitions in-memory jobs through bounded
lease-owned states, and closes selected output lifecycle resources. Phase 5
adds optional resumable checkpoint/harvest coordination; durable persistence,
migration, and quarantine remain adapter capabilities. Response replay remains
outside this phase.

## Capabilities and dependencies

The only `trawler-core` runtime dependencies remain Kotlin stdlib and
`kotlinx-coroutines-core`. No serializer, SQL/JDBC, Ktor, parser, logging,
telemetry, browser, anti-bot, or domain dependency is present or exposed.

State capabilities cover work queues, durability, transactions, multi-process
coordination, lease fencing, checkpoints, atomic enqueue/checkpoint behavior,
and queryable history. Response capabilities cover lookup, cache, history,
replay, bounded eviction, and range queries. A configuration may require a
capability, but the adapter remains responsible for proving its actual
guarantee.

## Concurrency and resource bounds

All suspending contracts are designed for structured ownership: cancellation
propagates from the engine scope, and adapters release permits/resources in
`finally` paths. Pacing and concurrency return explicit releaseable
reservations/permits. Discovery uses a `Flow` and engine-controlled executor;
extensions must not create hidden clients or unbounded buffers. Batch and
claim limits are explicit contract parameters; extraction lists, output
batches, headers, attributes, and response/request bodies are intentionally
caller/adapter-bounded inputs. The Phase 2 runtime adds explicit bounds for
queues, workers, follow-ups, values, output batches, retained state, and
lifecycle waits; it creates no unstructured global scope.

## Failure and delivery semantics

Expected extraction, validation, output, policy, state, and observer outcomes
are represented by sealed results where appropriate. Failure values retain the
original `Throwable`; retry is selected only by `FailureClassifier` plus
`RetryPolicy`. A lease acquisition is distinct from a meaningful attempt, and
stale lease mutations are expected to be rejected by capable stores.

Outputs declare required versus best-effort delivery, but no output or engine
configuration claims exactly-once delivery. Non-transactional output adapters
must document their duplicate/loss window and idempotency requirements. The
Phase 2 in-memory runtime reports worker completion and supports optional
in-memory response replay. Phase 5 adds resumable discovery and recurring
harvest contracts; the optional PostgreSQL adapter supplies their durable
revisioned state without changing the typed value boundary.

## Security, privacy, and retention

Core contracts do not log or archive request/response bodies, headers, or
produced values. Headers and bodies may contain secrets and are therefore
opaque to core; observer/event producers must redact sensitive metadata before
emission. Response retention, encryption, deletion, and replay authorization
belong to the selected response adapter. State stores must not place user data
or response bodies in operational state. Hosts remain responsible for target
authorization, privacy, terms, and retention.

## Verification

The implementation was checked with:

- `mvn clean compile`;
- `mvn clean verify`;
- `mvn -pl trawler-core -am clean verify`;
- `mvn -pl trawler-core -am dependency:tree -Dscope=runtime -Dverbose`, confirming only Kotlin stdlib,
  coroutines, and their expected transitives;
- `javap` inspection of the compiled engine and typed-stage signatures;
- source inspection for adapter/serializer imports, unchecked casts, `Any?`
  contract shortcuts, `GlobalScope`, `Thread.sleep`, and TODO/FIXME markers;
- Markdown target checks for the changed documentation links.

The core contract phase adds deterministic tests for typed registration and
pipeline values, configuration conflicts, value ownership, discovery/harvest
helpers, codecs, response retention, and built-in policies. The database-free
runtime phase adds controlled-transport tests for full typed engine execution,
concurrent pipeline isolation, lease loss, bounded admission,
replay network isolation, output delivery, and lifecycle cleanup. The
repository test lifecycle and remaining planned owners are recorded in
[`TESTING.md`](TESTING.md) and [`TEST-MATRIX.md`](TEST-MATRIX.md); adapter
transport behavior, durable persistence, and the explicitly listed
cancellation gaps remain untested.

## Limitations

- This Phase 1 contract record does not define durable recovery or a web
  adapter; those are documented by later feature/module records.
- The Phase 2 in-memory state and response implementations are process-local;
  they do not provide restart recovery or multi-process guarantees.
- The configuration DSL does not solve arbitrary type-changing composition;
  that requires a separately designed mapper boundary.
- Capability declarations are validated, but adapter implementations and
  their persistence/delivery guarantees are outside this module.
- Examples in this document are conceptual API sketches, not compiled example
  sources.
