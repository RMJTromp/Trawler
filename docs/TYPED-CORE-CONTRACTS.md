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

The implementation is a configuration and contract layer. It does not start a
worker, execute a transport, persist state, archive responses, or deliver a
produced value.

## Non-goals

Phase 1 deliberately does not implement:

- the worker loop, `runOnce`, daemon execution, or scheduling;
- an in-memory or durable state-store implementation;
- HTTP/Ktor, PostgreSQL, file, browser, anti-bot, parser, or web behavior;
- response retention, replay, serialization, or output destinations;
- type-changing mappers, domain models, schemas, record keys, or entity tables;
- automated tests or test dependencies.

Those are later phases or adapter responsibilities. Their contracts may be
selected in a Phase 1 configuration, but no runtime code invokes them yet.

## Public contracts

The public types are grouped by ownership:

| Area | Core types | Ownership |
|---|---|---|
| Request/response/context | `CrawlRequest`, `CrawlResponse`, `CrawlerContext`, stage contexts | Core owns immutable coordination metadata; the host/transport owns meaning and resources. |
| Typed pipeline values | `Produced<T>`, `ProductionContext`, `Extractor<T>`, `ExtractionResult<T>`, `Transform<T>`, `TransformDecision<T>`, `Validator<T>`, `ValidationResult` | The host owns `T`; core owns only typed stage coordination and provenance. |
| Output | `Output<T>`, `OutputBinding<T>`, `OutputResult` | The selected output owns encoding, persistence, acknowledgement, retention, and idempotency. |
| Discovery | `Discovery`, `DiscoveryContext`, `DiscoveryEmission` | The discovery extension owns admission semantics and uses the engine-provided `RequestExecutor` for I/O. |
| Transport | `Transport`, `TransportRouter`, request/response middleware | The transport adapter owns clients and I/O; core owns the controlled boundary. |
| Operational state | `CrawlStateStore` and focused `WorkQueueStore`, `LeaseStore`, `DiscoveryCheckpointStore` capabilities | The state adapter owns operational state only, never `T` or response bodies. |
| Response retention | `ResponseStore`, `ResponseLookup`, response capabilities | The response adapter owns optional retention, replay, and privacy policy. |
| Observation/lifecycle | `CrawlerEvent`, `Observer`, `ExtensionLifecycle` | Observers receive bounded metadata; lifecycle resources remain under the engine scope. |
| Time/policy | `Clock`, `RandomSource`, and the focused policy interfaces | The host or extension selects policy; core does not hardcode web, retry, or scheduling algorithms. |

`CrawlRequest.target` is an opaque string and is not required to be a URL.
`CrawlResponse.statusCode` is optional, so custom, file, cached, API, browser,
and non-HTTP transports remain possible. `ContextAttributes` is a
caller-bounded string metadata facility, not `Map<String, Any?>`, JSON, or a user-data record
envelope. Request and response bodies defensively copy byte arrays.

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
configuration; it does not start a worker in this phase.

An engine configuration requires exactly one caller-selected
`CrawlStateStore`. Phase 1 has no built-in state implementation, so requiring
the contract makes the absence explicit rather than introducing a fake or
placeholder store. `ResponseStore` is optional. `Clock` and `RandomSource`
have replaceable JDK-backed defaults. Required state and response capabilities
are declared with `requireStateCapability` and `requireResponseCapability` and
are checked before configuration construction completes.

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
| Lifecycle extension | Engine extensions precede pipeline extensions at startup; shutdown is reverse order. |
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
accepts, converts, or casts a value. The direct immutable constructor uses a
star-projection wrapper only for already-built definitions. There are no
unchecked casts in core source, and the normal pipeline path never erases `T`
to `Any`, JSON, a map, or a library-owned record envelope.

This is safe because type-sensitive composition finishes before registration:
the extractor, same-type transforms, validators, and outputs are all held by
one `Pipeline<T>`. The non-generic engine may enumerate definitions and
validate metadata, but it cannot process a value without a future typed owner
operation. That future runtime boundary must preserve this proof.

## Lifecycle and state

Phase 1 has no runtime state transitions. It defines the contracts needed by
later execution:

- `WorkQueueStore` bounds claim batches and returns lease-owned work;
- `LeaseStore` requires the current fencing token on renewal and transitions;
- `DiscoveryCheckpointStore` uses opaque codec/version metadata and
  compare-and-set revisions;
- response storage is independent of operational state;
- output acknowledgement is independent of both.

`ExtensionLifecycle.start` receives an engine-owned `CoroutineScope` and
`stop` receives an explicit reason. Later runtime code must start in declared
order, stop in reverse order, propagate cancellation, and close resources
deterministically. Phase 1 itself performs neither call.

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
caller/adapter-bounded inputs rather than unbounded core queues. Phase 1
creates no coroutine, queue, worker, scheduler, or background scope.

## Failure and delivery semantics

Expected extraction, validation, output, policy, state, and observer outcomes
are represented by sealed results where appropriate. Failure values retain the
original `Throwable`; retry is selected only by `FailureClassifier` plus
`RetryPolicy`. A lease acquisition is distinct from a meaningful attempt, and
stale lease mutations are expected to be rejected by capable stores.

Outputs declare required versus best-effort delivery, but no output or engine
configuration claims exactly-once delivery. Non-transactional output adapters
must document their duplicate/loss window and idempotency requirements.
Response replay, durable checkpointing, and worker completion are not
implemented in this phase.

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
- `mvn -pl trawler-core -am dependency:tree`, confirming only Kotlin stdlib,
  coroutines, and their expected transitives;
- `javap` inspection of the compiled engine and typed-stage signatures;
- source inspection for adapter/serializer imports, unchecked casts, `Any?`
  contract shortcuts, `GlobalScope`, `Thread.sleep`, and TODO/FIXME markers;
- Markdown target checks for the changed documentation links.

Automated tests were not authorized, added, or run. A later runtime phase must
add tests only after explicit authorization, with typed heterogeneity,
configuration conflicts, cancellation, lease fencing, checkpoint ordering,
replay isolation, and output duplicate windows as high-risk scenarios.

## Limitations

- No worker runtime can execute these contracts yet.
- Phase 1 requires a caller-supplied state-store contract; the in-memory store
  is a later phase.
- The configuration DSL does not solve arbitrary type-changing composition;
  that requires a separately designed mapper boundary.
- Capability declarations are validated, but adapter implementations and
  their persistence/delivery guarantees are outside this module.
- Examples in this document are conceptual API sketches, not compiled example
  sources.
