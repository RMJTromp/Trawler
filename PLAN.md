# Trawler — Final Implementation Plan

> **Status:** Authoritative implementation plan
>
> **Repository baseline:** Maven/Kotlin skeleton; no production source files currently exist
>
> **Inputs reconciled:** `PLAN-4.md`, `TRAWLER_GAP_REVIEW.md`, and `TRAWLER_GENERIC_ARCHITECTURE_REVIEW.md`
>
> **Precedence:** `TRAWLER_GENERIC_ARCHITECTURE_REVIEW.md` controls every conflict. This document supersedes the other plans for implementation decisions.

---

## 1. Final product definition

Trawler is a generic Kotlin library for composing and running typed scraping pipelines.

Its governing rule is:

> **Core owns coordination. Extensions own policy. The host owns data.**

Trawler coordinates lifecycle, structured concurrency, cancellation, work execution, extension composition, typed extraction flow, and capability validation. It must not decide what the caller scrapes, what the resulting values mean, how those values are serialized, whether SQL is used, whether responses are retained, or where results are delivered.

The architecture must support both of these without changing core:

1. a small, one-shot in-memory pipeline with a custom transport and callback output;
2. a durable, recurring, multi-process crawler with PostgreSQL state, resumable discovery, response replay, scheduled triggers, and external delivery.

The second configuration is a supplied set of adapters and policies, not the definition of Trawler.

---

## 2. Non-negotiable architecture rules

1. **The engine is not generic; registered pipelines are.** `TrawlerEngine` may coordinate unrelated `Pipeline<A>` and `Pipeline<B>` instances in the same process.

2. **A produced value remains `T`.** Core must not convert it to `JsonNode`, `Map`, an entity envelope, or a database row.

3. **Core has no data-model ownership.** Record keys, schema names, entity types, content hashes, and user-data tables belong to host code or output adapters.

4. **Core imports no adapter technology.** Core must not import PostgreSQL, JDBC, HikariCP, Flyway, Ktor, Jackson, Jsoup, FlareSolverr, browser, or application-domain types.

5. **Adapter dependencies point toward core only.** The required direction is `adapter -> core`; core never depends on an adapter.

6. **State, responses, and produced data are separate concerns.** They use separate contracts and can be configured independently.

7. **Policies are replaceable.** Retry, failure classification, pacing, concurrency, request identity, canonicalization, deduplication, recrawl, work ordering, authorization, scope, safety, cache behavior, and scheduling are not fixed algorithms in core.

8. **Built-ins are defaults, not hidden engine behavior.** A convenient preset may install several built-ins, but each remains independently replaceable.

9. **Serialization is local to the component that needs it.** Persistent state, checkpoints, outputs, and outboxes receive explicit codecs. Core has no universal serializer.

10. **Structured concurrency and cleanup are core invariants.** Engine-owned scopes, cancellation propagation, bounded resources, deterministic startup/shutdown, and no `GlobalScope` are fixed.

11. **Capabilities are validated, not assumed.** Enabling replay, multi-process leasing, atomic checkpoint advancement, or durable scheduling must fail fast when the installed adapter cannot support it.

12. **Incomplete is distinct from failed.** Budget exhaustion, long cooldowns, circuit breaking, and cooperative interruption may leave resumable work and finish a pipeline run as `ABANDONED` or `CANCELLED` rather than `FAILED`.

13. **Resource identity is distinct from work identity.** A logical resource may be visited repeatedly; active duplicate work can be suppressed without suppressing future recrawls forever.

14. **Live observability may be lossy; durable truth is explicit.** An event flow is not an audit log. Durable run or attempt history exists only when a corresponding observer/store is installed.

---

## 3. Scope and release profiles

### 3.1 Core release

The first core release must provide:

- non-generic engine lifecycle;
- type-safe pipeline registration;
- typed extraction, same-type transforms, validation, and typed outputs;
- direct `runOnce` execution;
- a replaceable built-in coroutine scheduler for common fixed-delay, fixed-rate, and daily timezone-aware triggers;
- an in-memory state implementation suitable for one process;
- replaceable transport, discovery, identity, deduplication, retry, concurrency, pacing, scope, budget, and authorization contracts;
- generic lifecycle events and observers;
- deterministic configuration and capability validation;
- cooperative cancellation and ordered resource cleanup.

It must be usable without a database, archive, scheduler, web-specific policy, or serializer.

### 3.2 Web convenience profile

An optional public-web preset may assemble:

- Ktor transport;
- HTTP/HTTPS URL handling;
- bounded redirects and response bodies;
- robots policy;
- public-network/SSRF protection;
- common HTTP failure classification and retry behavior;
- per-origin concurrency and pacing;
- safe cross-origin credential handling.

Every installed part must remain replaceable. Crawling localhost, private networks, authenticated services, or non-web resources must remain possible with an explicit alternative policy.

### 3.3 Durable recurring profile

Optional adapters may add:

- durable resources, jobs, attempts, leases, and run summaries;
- PostgreSQL state and fenced multi-process claiming;
- resumable discovery checkpoints;
- optional historical/incremental harvest strategies;
- resource cooldown and recrawl history;
- response retention and replay;
- persistent operational overrides and health state;
- durable outbox-backed outputs;
- external scheduling integrations beyond the bundled coroutine scheduler.

### 3.4 Explicitly not required for the first core release

- PostgreSQL or any SQL database;
- response archival or offline replay;
- FlareSolverr or any anti-bot provider;
- a browser transport;
- RSS, Atom, sitemap, pagination, or Wayback discovery;
- historical/incremental harvesting;
- source auto-detection;
- persistent cookies or sessions;
- external scheduling;
- domain-specific examples as acceptance criteria;
- exactly-once external delivery.

---

## 4. Canonical terminology

The implementation and documentation must use these terms consistently:

| Term | Meaning |
|---|---|
| **Engine** | Non-generic coordinator of registered typed pipelines and shared services. |
| **Pipeline `<T>`** | Type-safe binding of discovery, request processing, extractor, transforms, validators, and outputs that produces `T`. |
| **Engine run** | One finite or daemon execution session across one or more pipelines. |
| **Pipeline run** | One pipeline's lifecycle within an engine run. It replaces the application-specific assumption that every pipeline is a “source.” |
| **Request** | Immutable description of work to execute through a transport. |
| **Resource** | Stable logical identity selected by `RequestIdentityStrategy`; optional durable adapters may retain it across visits. |
| **Job** | One desired visit or processing operation for a resource. |
| **Lease** | Time-bounded, fenced ownership of a job. |
| **Attempt** | A meaningful execution attempt, beginning only when processing actually starts—not when a lease is merely acquired. |
| **Response** | Transport result consumed by extraction and optionally stored by a response adapter. |
| **Produced `<T>`** | Caller-owned value plus crawler-owned provenance metadata. |
| **Output `<T>`** | Typed consumer that accepts produced values. |
| **Checkpoint** | Opaque, codec-versioned discovery state owned by a discovery extension. |
| **Harvest strategy** | Optional recurring-coverage policy such as stateless, cursor-based, watermark, or historical-then-incremental. |

`SourcePlugin<T>`, `ExtractedRecord`, `RecordSink`, and `crawl_requests` are not final core abstractions.

---

## 5. Architecture and dependency boundary

```text
                         TrawlerEngine
                 lifecycle + orchestration
                             │
                  registered typed bindings
                             │
           ┌─────────────────┴─────────────────┐
           │                                   │
      Pipeline<A>                         Pipeline<B>
           │                                   │
      Discovery*                          Discovery*
           │                                   │
      CrawlRequest                        CrawlRequest
           │                                   │
   policies + middleware              policies + middleware
           │                                   │
       Transport                           Transport
           │                                   │
      CrawlResponse                       CrawlResponse
           │                                   │
      Extractor<A>                        Extractor<B>
           │                                   │
 Transform/Validator<A>*             Transform/Validator<B>*
           │                                   │
      Produced<A>                        Produced<B>
           │                                   │
       Output<A>*                         Output<B>*
```

Shared optional services sit beside the pipelines:

```text
CrawlStateStore      ResponseStore       Observer(s)
Clock                RandomSource        Scheduler integration
ConcurrencyPolicy    RequestPacingPolicy Extension lifecycle
```

No shared service may inspect or serialize `A` or `B`.

---

## 6. Core typed contracts

Signatures below express ownership and type flow. Exact names may change during implementation only if the same constraints remain true.

### 6.1 Produced values

```kotlin
data class Produced<T : Any>(
    val value: T,
    val context: ProductionContext,
)

data class ProductionContext(
    val pipelineId: String,
    val engineRunId: String,
    val pipelineRunId: String,
    val requestId: String?,
    val responseReference: String?,
    val originalTarget: String?,
    val finalTarget: String?,
    val producedAt: Instant,
    val attributes: AttributeMap,
)
```

`AttributeMap` is a typed/opaque core attribute facility, not JSON. Attributes intended for persistence require an explicit codec at the persistence boundary.

### 6.2 Discovery

```kotlin
interface Discovery {
    val id: String
    fun discover(context: DiscoveryContext): Flow<DiscoveryEmission>
}

sealed interface DiscoveryEmission {
    data class Request(val request: CrawlRequest) : DiscoveryEmission
    data class ExistingResponse(
        val request: CrawlRequest,
        val response: CrawlResponse,
    ) : DiscoveryEmission
}
```

The existing-response form prevents duplicate network calls for feeds, APIs, solvers, or list endpoints that already contain/fetched extractable content. If response persistence is enabled, it occurs before extraction according to the configured response policy.

Discovery implementations must use engine-provided request execution when they need I/O. They must not create hidden HTTP clients that bypass policy and observability.

### 6.3 Extraction

```kotlin
interface Extractor<T : Any> {
    suspend fun extract(
        response: CrawlResponse,
        context: ExtractionContext,
    ): ExtractionResult<T>
}

sealed interface ExtractionResult<out T> {
    data class Success<T>(
        val values: List<T>,
        val followUps: List<CrawlRequest> = emptyList(),
    ) : ExtractionResult<T>

    data class Ignore(val reason: String? = null) : ExtractionResult<Nothing>

    data class Failure(
        val error: Throwable,
        val retryHint: RetryHint? = null,
    ) : ExtractionResult<Nothing>
}
```

Core does not require record keys, entity types, schema versions, content hashes, or serialization.

### 6.4 Transforms and validation

The initial transform contract preserves type:

```kotlin
interface Transform<T : Any> {
    suspend fun apply(value: T, context: TransformContext): TransformDecision<T>
}

interface Validator<T : Any> {
    suspend fun validate(value: T, context: ValidationContext): ValidationDecision
}
```

Filtering or validation drops are successful processing decisions, not infrastructure failures. The design must leave room for a later `Mapper<I, O>` without forcing the first release to solve arbitrary type-changing DSL composition.

### 6.5 Outputs

```kotlin
interface Output<T : Any> {
    suspend fun write(items: List<Produced<T>>): OutputResult
}
```

Supported composition:

- no output, exposing `Flow<Produced<T>>` to the host;
- one output;
- ordered or parallel fan-out, configured explicitly;
- buffered/batched decorators;
- retrying decorators;
- durable/outbox decorators;
- transactional adapters where available.

Each output binding declares whether it is required or best-effort and documents its delivery semantics. If a required non-transactional output succeeds and the process crashes before job completion, retry may deliver the same value again. Such outputs must be idempotent or install deduplication. Trawler must not claim generic exactly-once delivery.

### 6.6 Pipeline binding and registration

```kotlin
data class PipelineBinding<T : Any>(
    val definition: PipelineDefinition,
    val discoveries: List<Discovery>,
    val extractor: Extractor<T>,
    val transforms: List<Transform<T>>,
    val validators: List<Validator<T>>,
    val outputs: List<OutputBinding<T>>,
)
```

Heterogeneous storage inside the engine is isolated behind a registration object created by the typed builder. Normal processing must not cast `T` to `Any`, JSON, or a library-owned record envelope.

---

## 7. Extension registry and configuration rules

Cardinality must be explicit:

| Extension | Cardinality |
|---|---|
| State store | Exactly one effective store; in-memory default |
| Response store | Zero or one; no-store default |
| Transport router | Exactly one per pipeline after inheritance |
| Discovery | Zero to many per pipeline |
| Extractor `<T>` | Exactly one per pipeline |
| Transform `<T>` | Ordered zero to many |
| Validator `<T>` | Ordered zero to many |
| Output `<T>` | Zero to many |
| Request middleware | Ordered zero to many |
| Response middleware | Ordered zero to many |
| Observer | Zero to many |
| Scheduler | Zero or one per pipeline; absent by default |
| Identity, dedup, retry, pacing, concurrency, scope, recrawl | Exactly one effective policy per applicable scope, with explicit inheritance |

The registry must distinguish replacement from addition. Calling `retry(...)` replaces the inherited retry policy; calling `addRequestMiddleware(...)` appends to an ordered chain. Silent stacking of single-cardinality policies is a configuration error.

Configuration ownership follows the extension:

```text
core config        lifecycle and shared engine bounds
pipeline config    bindings and policy selection
adapter config     adapter-specific settings
host config        secrets and application integration
```

Precedence, where supported, is:

```text
library preset defaults
  < extension defaults
  < host/pipeline configuration
  < persistent operator override
  < one-run override
  < mandatory constraints imposed by the selected safety/server policy
```

Core must not grow one universal configuration object containing every adapter's options.

---

## 8. Independent state, response, and output services

### 8.1 Crawl state

`CrawlStateStore` owns only engine-operational data such as queued work, claims, retries, checkpoints, and optional run history.

The initial interface should be decomposed into focused capabilities rather than one permanent giant store interface. Expected capability areas include:

- work enqueue/claim/transition;
- fenced leases;
- request/resource history;
- discovery checkpoints;
- run summaries;
- optional harvest state;
- optional operational overrides/health.

One adapter may implement several capability interfaces, but a user-written adapter need implement only the features the selected configuration requires.

### 8.2 State capabilities

Capabilities to validate include:

- `DURABLE`;
- `TRANSACTIONAL`;
- `MULTI_PROCESS`;
- `LEASE_FENCING`;
- `DISCOVERY_CHECKPOINTS`;
- `ATOMIC_ENQUEUE_AND_CHECKPOINT`;
- `QUERYABLE_HISTORY`.

Examples:

- daemon restart recovery requires `DURABLE`;
- multiple worker processes require `MULTI_PROCESS` and `LEASE_FENCING`;
- exactly-once cursor advancement relative to enqueue requires `ATOMIC_ENQUEUE_AND_CHECKPOINT`, or the discovery implementation must use a documented replay-safe fallback;
- the in-memory store is valid for a one-process run and must not pretend to provide restart recovery.

### 8.3 Response storage

```kotlin
interface ResponseStore {
    suspend fun put(response: CrawlResponse): ResponseReference?
    suspend fun find(query: ResponseLookup): StoredResponse?
}
```

Optional response capabilities include lookup/cache, history, replay enumeration, TTL eviction, and range query. `NoResponseStore` is valid and is the default. Append-only PostgreSQL, filesystem, object storage, WARC, and in-memory caches are adapters.

Replay exists only when the selected store supports enumeration/replay. Removing stored bodies must obey adapter-specific lineage and retention rules; core imposes no global retention model.

### 8.4 Produced-data output

User data never enters the crawl-state schema. A JDBC output may define tables or accept row mappers; a JSON output may accept a serializer; a callback output may persist nothing. Output-specific outbox tables and codecs belong to that output adapter.

---

## 9. Correct durable recurring model

The gap review correctly identifies that durable recurring crawls cannot treat identity, work, and attempts as one row. This model belongs in capable state adapters and must not make SQL mandatory.

### 9.1 Resource, job, and attempt

```text
Resource
  stable identity chosen by RequestIdentityStrategy
      │
      └── Job 1 (one desired visit)
      │      ├── lease 1
      │      └── attempt 1..N
      └── Job 2 (a later recrawl)
             └── attempt 1..N
```

Required semantics:

- only active work is deduplicated by default;
- a completed resource may receive a later job;
- leasing does not increment meaningful attempt count;
- an attempt begins immediately before transport or other substantive execution;
- all mutations owned by a lease include its fencing token;
- a stale worker cannot attach a response, reschedule, fail, or complete a reclaimed job;
- process loss before attempt start does not consume retry budget;
- outcome history may include policy rejection, cooldown suppression, cancellation, or extraction failure even when no HTTP response exists.

### 9.2 Logical PostgreSQL adapter schema

The PostgreSQL state adapter should use the following logical tables. Exact DDL is designed inside that adapter after core contracts stabilize:

```text
trawler_engine_runs
trawler_pipeline_runs
trawler_resources
trawler_jobs
trawler_attempts
trawler_discovery_checkpoints        optional capability
trawler_harvest_state                optional capability
trawler_pipeline_health              optional capability
trawler_pipeline_overrides           optional capability
trawler_operational_activity         optional observer
```

Important corrections from the earlier proposed schema:

- no `entity_type` or scraped payload column;
- no `extracted_records` table in the state adapter;
- no mandatory response-body table in the state adapter;
- encoded requests/checkpoints use explicit codec ID and version, not universal JSON;
- resources have a stable identity key, while jobs have separate visit IDs;
- the uniqueness rule covers active jobs, not all historical visits;
- attempts are append-only and distinct from leases;
- response references are opaque and optional;
- an outbox belongs to an output adapter, not the state schema.

### 9.3 Pipeline-run lifecycle

Pipeline-run statuses:

```text
RUNNING
COMPLETED
ABANDONED
FAILED
CANCELLED
```

Stable phases may include `STARTING`, `BACKLOG`, `DISCOVERY`, `PROCESSING`, and `FINALIZING`. Extensions may add detail through attributes/events without extending a closed core phase enum.

On recovery, expired leases are reclaimed according to store semantics. Stale engine/pipeline runs may be finalized as `ABANDONED(PROCESS_LOST)` or `FAILED(PROCESS_LOST)` according to a configured recovery policy. Recovery must never bypass lease fencing by blindly rewriting active work.

### 9.4 Discovery checkpoints

Checkpoints are opaque bytes plus plugin-owned codec metadata:

```kotlin
data class DiscoveryCheckpoint(
    val revision: Long,
    val codecId: String,
    val codecVersion: Int,
    val payload: ByteArray,
    val generationId: String?,
)
```

Rules:

- advance a checkpoint only after emitted work is durably accepted;
- use compare-and-set revisions to prevent stale writers;
- validate codec and mode compatibility on load;
- allow an extension-supplied migration;
- otherwise quarantine/reset explicitly and emit an event;
- a store without atomic enqueue-and-checkpoint must use replay-safe identity and document at-least-once discovery emission.

### 9.5 Optional harvest strategies

Historical/incremental behavior is valuable but not universal. It is installed through a `HarvestStrategy`.

Built-ins may include:

- stateless;
- cursor-only;
- watermark;
- historical-then-incremental;
- ETag/Last-Modified;
- custom.

For historical-then-incremental, completion is conservative. The strategy may commit historical completion only when discovery reached its declared end, relevant checkpoints are settled, generation jobs are settled, the run was not abandoned/cancelled, and the configured completion predicate passes. Incremental overlap is provided by a replaceable boundary policy.

### 9.6 Discovery admission and backlog control

Backpressure on a `Flow` prevents memory exhaustion but does not prevent persistent backlog growth. `DiscoveryAdmissionPolicy` therefore receives open-work counts, oldest age, checkpoint state, budgets, and optional health data.

Built-ins:

- `BacklogFirst`;
- `Continuous`;
- `QueueWatermark`;
- custom.

Queue high/low watermarks are adapter-independent configuration. Exceeding them defers discovery and emits an operational event; it must not silently grow work without bound.

---

## 10. Replaceable policy model

### 10.1 Request identity, canonicalization, deduplication, and recrawl

These are four independent decisions:

1. `RequestCanonicalizer` optionally normalizes a request;
2. `RequestIdentityStrategy` produces its logical identity, including any URL, method, body, headers, credentials, locale, or variant data the host considers significant;
3. `DeduplicationPolicy` decides whether equivalent active work may coexist;
4. `RecrawlPolicy` decides whether/when a completed resource may be scheduled again.

The PLAN-4 SHA-256 URL formula is a useful web preset, not a core invariant. A permanent unique fingerprint that makes successful URLs impossible to revisit is forbidden as the default durable behavior.

### 10.2 Failure classification and retry

`FailureClassifier` answers what happened. `RetryPolicy` decides what to do given that classification and attempt history.

The classifier must permit extension-defined categories rather than require every transport to fit a closed enum. Core can define broad lifecycle categories and an extensible category identifier.

Retry decisions may include:

- retry now;
- retry at/after a delay;
- do not retry;
- abandon the pipeline run while retaining work;
- reroute to another transport;
- invoke challenge resolution;
- delegate to a host error handler.

Transport middleware must not hide independent retry loops from the engine, because that corrupts attempt accounting and observability.

### 10.3 Cooldown and eligibility

Job retry timing and stable-resource eligibility are separate. `CooldownPolicy` can persist a next eligible time based on outcomes; `RecrawlPolicy` consults resource history. Exact durations are preset defaults, never core truth.

### 10.4 Concurrency and pacing

`ConcurrencyPolicy` returns a releasable permit for a request context. `RequestPacingPolicy` runs before a request and observes responses afterward.

Built-ins may implement global, pipeline, source-tag, origin, credential, proxy, or hierarchical limits. For the public-web preset, safe merging across shared origins is:

```text
effective spacing = max(configured spacing,
                        robots delay,
                        persisted adaptive delay,
                        active server cooldown)

effective concurrency = minimum applicable active limit
```

Never hold an unrelated origin permit through a long cooldown or while waiting for another origin if avoidable.

### 10.5 Work ordering

`WorkOrderingPolicy` supports FIFO, LIFO, breadth-first, depth-first, priority, freshness, or fairness. Persistent adapters may optimize known policies but must reject unsupported custom ordering rather than silently substitute SQL order.

### 10.6 Authorization, safety, scope, and robots

Request admission is an ordered, composable policy chain. Built-ins may include:

- crawl scope;
- robots authorization;
- public-network URL safety;
- scheme allowlist;
- depth and content bounds;
- host/application authorization.

Robots is a policy extension. Its configuration defines unavailable behavior, cache behavior, crawler identity, and handling of impractically long crawl delays. A configured cap must not silently shorten a robots delay and continue; it may instead abandon/defer the run.

### 10.7 Budgets and stop conditions

`CrawlBudget` may limit requests, produced values, newly accepted values, bytes, failures, discovery emissions, depth, cost, or wall time. Budgets may apply at engine, pipeline, discovery, or host-defined scope.

Budget exhaustion preserves jobs/checkpoints and records `ABANDONED(BUDGET_EXHAUSTED)` when work remains. Unchanged/duplicate output can still count as successful processing even if it does not consume a “new value” budget.

### 10.8 Optional health and adaptive behavior

Health tracking, circuit breaking, and adaptive pacing consume generic events/statistics. Thresholds and persistence are extension-owned. A pipeline circuit breaker abandons only that pipeline run, never unrelated pipelines. Deploying code must not silently overwrite persisted operator/adaptive state.

---

## 11. Transport, middleware, redirects, and sessions

### 11.1 Transport boundary

```kotlin
interface Transport {
    suspend fun execute(request: CrawlRequest): CrawlResponse
}

interface TransportRouter {
    fun select(request: CrawlRequest, context: RequestContext): Transport
}
```

Possible transports include Ktor, Java HTTP client, OkHttp, browser, file, cached/offline, mock, API-specific, or composite/fallback implementations.

### 11.2 Middleware

Ordered request and response middleware support headers, authentication, signing, cookies, proxies, tracing, decoding, challenge detection, and response classification. Attempt ownership remains visible to the engine.

### 11.3 Redirects

Every redirect hop is a first-class request transition:

1. resolve the new target;
2. rerun scope/safety validation and DNS/address checks where applicable;
3. determine the new policy key/origin;
4. apply destination authorization/robots policy;
5. acquire destination concurrency and pacing;
6. execute and record the hop;
7. release permits deterministically.

On cross-origin redirects, strip authorization and sensitive source headers by default and consult the destination session/cookie provider. Redirect limits and history are transport-adapter configuration surfaced in response metadata.

### 11.4 Authentication and session extensions

Use focused surfaces such as `RequestEnricher`, `CredentialProvider`, `CookieJar`, `SessionProvider`, and `SessionStore`. Durable sensitive state is opt-in and requires host-selected encryption/retention. Run-scoped external session handles close in `finally`; reusable material may outlive a run according to provider policy.

### 11.5 Anti-bot extensions

`ChallengeDetector` and `ChallengeResolver` are optional extension families. Challenge identifiers are extensible values, not a vendor-shaped closed enum. FlareSolverr is one adapter and is not a core milestone or acceptance requirement. Origin/session single-flight can be a reusable decorator when installed.

---

## 12. Runtime semantics

### 12.1 Engine lifecycle

The public lifecycle starts with direct primitives:

```kotlin
suspend fun runOnce(request: RunRequest = RunRequest()): RunResult
suspend fun startDaemon(config: DaemonConfig = DaemonConfig()): EngineHandle
suspend fun awaitIdle()
suspend fun shutdown()
```

Scheduling sits above `runOnce`. `trawler-core` ships a replaceable coroutine scheduler for common trigger patterns, but no scheduler activates unless selected. Hosts may instead use cron, Quartz, Spring, Kubernetes, Airflow, or manual calls.

### 12.2 Minimal in-memory run

```text
validate typed bindings and capabilities
  -> create engine/pipeline run context
  -> collect bounded discovery emissions
  -> enqueue in memory
  -> claim work
  -> apply policies/middleware
  -> execute transport
  -> optionally pass response to ResponseStore
  -> extract T
  -> transform/validate T
  -> emit Produced<T>
  -> required outputs acknowledge
  -> complete work
  -> drain and finalize
```

### 12.3 Durable recurring run

```text
host calls runOnce
  -> create durable engine run
  -> select eligible pipelines
  -> create pipeline run
  -> load optional checkpoint/harvest/health state
  -> drain due backlog according to admission policy
  -> decide whether discovery may run
  -> restore discovery checkpoint
  -> execute discovery through engine-controlled transport
  -> durably enqueue emissions
  -> checkpoint only after durable acceptance
  -> drain newly eligible work
  -> evaluate optional harvest completion
  -> evaluate optional health/adaptive state
  -> finalize pipeline run
  -> finalize engine run
```

### 12.4 Work state and fenced writes

A durable adapter may use:

```text
QUEUED -> LEASED -> SUCCEEDED
              ├──> RETRY_WAIT -> QUEUED/LEASED
              ├──> FAILED
              ├──> SKIPPED
              └──> CANCELLED/ABANDONED as adapter policy requires
```

All lease-owned writes include job ID and lease token. Lease expiry only releases ownership; it does not by itself define a failed attempt.

### 12.5 Transaction and delivery boundaries

Core defines acceptance semantics, not a universal transaction:

- in-memory execution has process-local guarantees only;
- a transactional state adapter may atomically enqueue and checkpoint;
- a transactional output colocated with state may atomically accept produced data and complete work;
- an external output can use idempotency or an optional outbox decorator;
- a response may be processed without ever being archived;
- when archival is required by configuration, extraction begins only after the response store acknowledges it;
- replay reads stored responses directly and must not create ordinary crawl jobs or make network calls.

Every adapter documents its failure window and delivery guarantee.

### 12.6 Cancellation and cleanup

The engine owns one structured scope rooted in a `SupervisorJob`. Shutdown stops new run/discovery intake, stops claiming work, cancels suspending I/O, allows a bounded finalization window, closes outputs and extensions in reverse dependency order, then closes transports and stores. No resource cleanup depends solely on process exit.

Blocking adapter calls run only on adapter-owned bounded dispatchers sized to the relevant resource pool.

---

## 13. Observability and failure reporting

Core events are generic:

- engine/pipeline run started, completed, abandoned, failed, cancelled;
- discovery started, checkpointed, completed, failed;
- request queued, claimed, started, redirected, completed, failed;
- retry/cooldown scheduled;
- extraction started, completed, ignored, failed;
- value produced, transformed, dropped, accepted, delivery failed;
- response stored or store failed;
- configuration/capability warning.

Observers adapt these events to callbacks, SLF4J, Micrometer, OpenTelemetry, Prometheus, or durable operational storage. A bounded live flow may drop low-priority events and must expose/drop-count telemetry. Audit-required events must use a durable observer selected by the host.

Diagnostics are bounded and redactable. Secrets, credentials, cookie values, sensitive headers, and configured target data must not enter logs or default event payloads.

Failure reports preserve the original throwable at runtime while exposing stable, extensible category identifiers and stage metadata. Retryability is a policy decision, not a permanent property baked into the failure category.

---

## 14. Safe public-web preset

Safety is a recommended preset, not an unchangeable core restriction.

The public-web preset should install:

- HTTP/HTTPS-only scheme policy;
- private, loopback, link-local, multicast, and metadata-address blocking;
- DNS rebinding/redirect revalidation;
- bounded redirects;
- compressed and decompressed body limits;
- header/cookie redaction;
- cross-origin credential stripping;
- robots support with explicit unavailable behavior;
- `Retry-After` aware pacing;
- bounded discovery documents and follow-ups;
- host-configured user agent.

An internal-network preset or custom policy can intentionally permit localhost/private services. Documentation must state operator responsibility for authorization, terms, privacy, and retention.

---

## 15. Target caller experience

Minimal usage:

```kotlin
val engine = TrawlerEngine {
    register(
        pipeline<MyType>("example") {
            discover(myDiscovery)
            transport(myTransport)
            extract(myExtractor)
            output(CallbackOutput { produced -> consume(produced.value) })
        }
    )
}

val result = engine.runOnce()
```

Durable composition:

```kotlin
val engine = TrawlerEngine {
    stateStore(postgresStateStore)
    responseStore(fileResponseStore)
    observe(openTelemetryObserver)

    register(
        pipeline<MyType>("durable-example") {
            discover(myResumableDiscovery)
            transport(ktorTransport)
            identity(myIdentityStrategy)
            deduplicate(activeWorkOnly())
            recrawl(after(24.hours))
            retry(myRetryPolicy)
            pacing(myPacingPolicy)
            extract(myExtractor)
            transform(myTransform)
            output(buffered(myOutput))
        }
    )
}

engine.runOnce()
```

These configurations use the same core. The DSL is a thin builder over immutable validated configuration; direct constructors remain usable.

---

## 16. Built-ins and module roadmap

Logical extension boundaries do not map one-to-one to Maven artifacts. Trawler uses a small number of cohesive physical modules:

```text
trawler-parent
    build/version/dependency/plugin management only

trawler-core
    engine + contracts
    in-memory state
    no-response and memory response stores
    seed / caller-Flow discovery
    callback / no-output collection
    bounded JDK file response/output implementations with caller codecs
    lightweight generic policy implementations
    built-in coroutine scheduler

trawler-web                         added when Phase 3 implementation begins
    Ktor transport
    HTTP middleware and redirects
    public-web safety and robots
    HTTP retry/pacing/cookie behavior
    RSS/Atom and sitemap discovery

trawler-postgres                    added when Phase 4 implementation begins
    durable resources/jobs/attempts/leases
    run/checkpoint/optional harvest state
    optional PostgreSQL response store
    optional PostgreSQL output and outbox

specialized artifacts              created only on implementation demand
    trawler-antibot-flaresolverr
    trawler-transport-browser
    trawler-observability-otel
    trawler-warc
```

The built-in scheduler is part of `trawler-core`, not a separate tiny artifact. It must support manual/finite triggering, fixed delay, fixed rate, daily local time with timezone, optional immediate first run, explicit overlap behavior, cancellation, and pipeline selection. It remains opt-in and replaceable; a host may instead call `runOnce` from Quartz, Spring, Kubernetes, cron, or custom code.

A separate artifact is justified by an external dependency stack, heavy operational/security footprint, independent ecosystem compatibility, or meaningful ability for most users to omit it. Small size, replaceability, or a focused interface alone is insufficient.

`trawler-web` groups cohesive HTTP behavior rather than splitting Ktor, robots, RSS, sitemap, and web policies into tiny modules. `trawler-postgres` groups capabilities that share the PostgreSQL/JDBC/pool/migration stack while keeping each capability independently activated. Depending on it must not automatically enable response archival, output persistence, health state, or outbox delivery.

Create `trawler-bom` when at least two runtime artifacts are ready for publication. All artifacts initially use one coordinated project version. Do not create a depends-on-everything artifact or placeholder module.

Jackson or another codec, if used, belongs only to the adapter selecting it. PostgreSQL migrations live only in `trawler-postgres`. Reference content models and language-specific logic live only in non-runtime examples.

---

## 17. Repository layout target

```text
pom.xml                                  trawler-parent reactor

trawler-core/
  pom.xml
  src/main/kotlin/.../
    engine/                              lifecycle and run coordination
    model/                               generic request/response/contexts
    pipeline/                            typed binding and DSL
    discovery/                           contracts and generic built-ins
    extraction/                          extractor/result contracts
    transform/                           transform/validator contracts
    output/                              contracts and lightweight built-ins
    policy/                              contracts and generic built-ins
    transport/                           transport/router/middleware contracts
    state/                               capability contracts and memory store
    response/                            contracts and lightweight stores
    schedule/                            contract and coroutine implementation
    event/                               generic events and observer contract
    extension/                           lifecycle and registry support

trawler-web/                             created with Phase 3 code
trawler-postgres/                        created with Phase 4 code
trawler-<specialized>/                   created only with requested implementation

examples/                                not a runtime dependency
```

Package names may be consolidated when a layer is small. Do not create one class, package, or module per diagram box merely to reproduce this tree. Physical packaging rules are maintained in `docs/MODULES.md`.

---

## 18. Delivery phases

### Phase 0 — Baseline and decisions

**Goal:** Make the repository accurately describe an unimplemented generic library and freeze vocabulary before code contracts spread.

Work:

- record the architecture rules in concise ADRs;
- align README claims with actual implementation status and this plan;
- confirm JDK/Kotlin/Maven baseline from the working `pom.xml` rather than copying stale version tables;
- remove or relocate dependencies that imply core serialization/storage requirements before core implementation;
- establish `trawler-parent` and the implementation-ready `trawler-core` module;
- record the cohesive module strategy in `docs/MODULES.md` and ADR-0004;
- define resource, job, lease, attempt, response, produced value, engine run, and pipeline run precisely.

Exit gate:

- the Maven reactor builds `trawler-parent` and `trawler-core` on the selected JDK;
- documentation no longer claims unimplemented adapters are current mandatory features;
- core's dependency boundary is enforceable and only Kotlin/coroutines are on its runtime dependency tree;
- terminology is consistent.

### Phase 1 — Typed core contracts and composition

**Goal:** Prove that unrelated typed pipelines can coexist without serialization or database coupling.

Work:

- implement request/response/context models;
- implement `Produced<T>`, `Extractor<T>`, `ExtractionResult<T>`, `Transform<T>`, `Validator<T>`, and `Output<T>`;
- implement type-safe pipeline builder/registration and the isolated heterogeneous engine registry;
- implement extension cardinality and configuration validation;
- define transport, middleware, state, response, observer, clock, and policy contracts;
- implement direct immutable configuration in addition to the DSL facade.

Exit gate:

- one engine registers at least two pipelines with unrelated output types;
- no core type contains `JsonNode` or application record fields;
- no core package imports an adapter dependency;
- conflicting extension registrations fail with useful diagnostics.

### Phase 2 — In-memory end-to-end engine

**Goal:** Deliver a useful database-free vertical slice.

Work:

- implement structured engine/run/pipeline lifecycle;
- implement in-memory state and queue;
- implement bounded discovery collection and worker execution;
- implement active-work dedup and repeatable resource visits;
- implement attempt ownership distinct from claiming;
- implement callback/no-output collection modes;
- implement generic events, observers, run results, cancellation, and cleanup;
- implement `runOnce`, `awaitIdle`, and shutdown semantics;
- implement the opt-in coroutine scheduler with fixed delay, fixed rate, daily local time/timezone, immediate-first-run, overlap, cancellation, and pipeline-selection behavior.

Exit gate:

- a custom discovery, transport, extractor, transform, and callback output run end to end;
- a second run may revisit a completed resource according to recrawl policy;
- cancellation releases permits and closes extensions;
- a caller can schedule `runOnce` with the bundled scheduler or invoke it directly without scheduling;
- the engine works with no database, response store, scheduler, JSON library, or web policy.

### Phase 3 — Policy set, Ktor adapter, and public-web preset

**Goal:** Provide a practical, replaceable HTTP crawling configuration in `trawler-web`.

Work:

- create `trawler-web` and implement Ktor transport behind core contracts;
- implement request/response middleware chains;
- implement replaceable identity, canonicalization, active dedup, retry, failure classification, work ordering, concurrency, pacing, cooldown, scope, and budget defaults;
- implement redirect hop policy re-entry and safe credential handling;
- implement public-web safety and robots adapters;
- use the seed discovery bundled in core;
- keep robots, web safety, RSS/sitemap foundations, and HTTP defaults cohesive inside `trawler-web` rather than splitting tiny artifacts;
- keep retry ownership outside the transport.

Exit gate:

- a local/mock HTTP target crawls through the public-web preset;
- redirect hops reacquire destination policy and permits;
- body/redirect limits and policy failures are observable;
- every default policy can be replaced independently.

### Phase 4 — Durable state adapter

**Goal:** Add optional restart and multi-process guarantees in `trawler-postgres` without changing core API ownership.

Work:

- create `trawler-postgres` and implement the corrected resource/job/attempt model;
- implement durable engine/pipeline runs;
- implement atomic active-work dedup;
- implement fenced leases, renewal, reclaim, and stale-writer rejection;
- implement explicit attempt-start accounting and resource cooldown state;
- implement codecs for request/state persistence;
- implement capability declaration and startup validation;
- keep PostgreSQL migrations and dependencies inside the adapter.

Exit gate:

- two processes cannot own the same lease;
- stale tokens cannot mutate reclaimed work;
- process death before attempt start does not consume retry budget;
- completed resources can be legitimately scheduled again;
- the same core pipeline still runs with the in-memory adapter.

### Phase 5 — Resumable and recurring discovery

**Goal:** Add robust recurring harvesting as optional composition.

Work:

- implement discovery checkpoint capability with explicit codecs/revisions;
- implement atomic enqueue/checkpoint where the adapter supports it and a replay-safe fallback contract otherwise;
- implement discovery admission (`BacklogFirst`, `Continuous`, watermarks);
- implement optional harvest strategies and conservative completion;
- implement RSS/Atom and sitemap components inside `trawler-web` through engine-controlled transport;
- add paginated/custom strategy hooks;
- implement budgets that preserve resumable state;
- add optional persistent pipeline eligibility, overrides, health, and circuit breaker adapters.

Exit gate:

- interrupted discovery resumes from a compatible checkpoint;
- checkpoint advancement never precedes durable acceptance;
- budget abandonment does not falsely mark historical coverage complete;
- unresolved backlog can defer new discovery;
- stateless pipelines incur none of this state model.

### Phase 6 — Response stores, replay, and output adapters

**Goal:** Add optional retention/reprocessing and production delivery modes.

Work:

- implement memory and bounded JDK file response/output components in `trawler-core` using caller-supplied codecs;
- implement PostgreSQL response/output/outbox components inside `trawler-postgres`, independently activated;
- implement cache policies outside core;
- implement replay only for replay-capable stores;
- ensure replay creates no normal crawl jobs and performs no network requests;
- implement file/PostgreSQL outputs using caller-supplied serializers or mappers;
- implement optional buffered, retrying, idempotent, and outbox decorators;
- define independent retention policies inside relevant adapters.

Exit gate:

- disabling the response store causes no loss of ordinary crawling functionality;
- multiple historical responses can coexist when an append-only store is selected;
- replay uses the same typed extraction/transform/output path;
- output guarantees and duplicate windows are documented accurately.

### Phase 7 — Optional integrations and examples

**Goal:** Expand the ecosystem without changing core semantics.

Candidates, prioritized by actual demand:

- FlareSolverr challenge resolver and session single-flight;
- browser transport;
- cookie/session/credential stores;
- external scheduler integrations beyond the bundled coroutine scheduler;
- plugin/source probing and persistent resolution;
- proxy and client-identity providers;
- OpenTelemetry/Micrometer observers;
- WARC/object storage;
- value deduplication and change detection;
- reference pipelines using only public contracts.

No domain-specific example is a prerequisite for core correctness. An adapter may be developed earlier for a real acceptance target without moving its assumptions into core.

---

## 19. Verification policy for implementation

Each phase's exit gate is mandatory. Verification should be proportionate to risk and include compilation, static inspection, and focused runtime demonstrations.

This plan does **not** authorize adding or refactoring automated tests. The repository instruction requires explicit user authorization before tests are added. If tests are later authorized, the highest-risk scenarios are typed pipeline isolation, lease fencing, recrawl identity, attempt/crash accounting, atomic checkpoint advancement, redirect safety, cancellation cleanup, replay network isolation, and output duplicate windows.

Dependency versions must be selected from the actual project baseline and compatibility requirements at implementation time; the stale version table in PLAN-4 is not normative.

---

## 20. Definition of done

The generic core and selected adapters are complete only when all applicable statements are true:

- [ ] `TrawlerEngine` coordinates multiple unrelated typed pipelines without becoming generic itself.
- [ ] `T` remains typed through extraction, transforms, validation, and outputs.
- [ ] Core has no universal JSON/serialization dependency and no scraped-record schema.
- [ ] Core can run with in-memory state and no response store.
- [ ] PostgreSQL, Ktor, Jackson, Jsoup, FlareSolverr, and domain code exist only outside core.
- [ ] State, response retention, and produced-data output are independently selectable.
- [ ] Optional features fail fast when required adapter capabilities are absent.
- [ ] Request identity, active deduplication, and recrawl are independent policies.
- [ ] A completed resource may be revisited without allowing duplicate active work by default.
- [ ] Leasing and meaningful attempt accounting are distinct; stale workers are fenced.
- [ ] Discovery implementations use engine-controlled I/O and bounded emission.
- [ ] Checkpoints are opaque, codec-versioned, and advance only after accepted work.
- [ ] Historical/incremental harvesting is optional and never claims completion after abandonment or unsettled work.
- [ ] Scheduling is optional; finite `runOnce` works without a scheduler.
- [ ] A built-in replaceable coroutine scheduler covers fixed delay, fixed rate, daily timezone-aware triggers, immediate-first-run, overlap, cancellation, and pipeline selection.
- [ ] Redirect hops rerun destination safety, authorization, pacing, and credential policy.
- [ ] Robots and public-network safety are replaceable policies offered in a safe preset.
- [ ] Anti-bot support is optional and vendor-neutral in core.
- [ ] Output acknowledgement and duplicate windows are documented honestly; core claims no generic exactly-once guarantee.
- [ ] Replay is available only with a capable response store and performs no network calls or normal job creation.
- [ ] Live events are not presented as durable audit history.
- [ ] Configuration ownership, extension cardinality, replacement, inheritance, and cleanup order are deterministic.
- [ ] README and examples describe implemented behavior rather than aspirational features as current facts.

---

## 21. Resolved contradictions and corrections

This final plan deliberately corrects the following issues in its inputs:

1. **PostgreSQL is no longer mandatory.** It is a capable state adapter, not Trawler's source of truth by definition.
2. **Response archival is no longer mandatory.** Append-only history is an optional response-store behavior.
3. **`ExtractedRecord` and `JsonNode` are removed from core.** Values remain `T`; adapters own encoding.
4. **`extracted_records` is removed from the core/state schema.** User data belongs to outputs.
5. **The delivery outbox is optional and output-owned.** It is not a universal engine table or transaction.
6. **`SourcePlugin<T>` is split into composable discovery and extraction responsibilities.** Serialization is removed; “source” becomes optional host/pipeline metadata rather than the engine's universal unit.
7. **The engine is non-generic and pipelines are generic.** This permits unrelated result types in one engine.
8. **Fixed algorithms are replaced by policies.** PLAN-4's URL fingerprint, HTTP retry table, token bucket, priority SQL, robots behavior, safety rules, and cache semantics become built-ins/presets.
9. **Recrawl identity is repaired.** Stable resources, visit jobs, leases, and attempts are distinct; active dedup no longer suppresses later visits forever.
10. **Attempt accounting is repaired.** Claiming a lease is not a completed retry attempt.
11. **Gap-review durability features are retained without becoming universal.** Runs, checkpoints, harvest state, cooldowns, health, registry overrides, and history are optional store/policy capabilities.
12. **Historical/incremental crawling is optional.** It is a harvest strategy, not a state machine every pipeline must use.
13. **Scheduling is optional.** Direct finite execution is the primary API.
14. **Ktor, FlareSolverr, RSS, sitemap, PostgreSQL migrations, Jackson, and content-domain code move entirely to adapters/examples.**
15. **Redirect semantics are made explicit.** Each hop re-enters safety, origin, pacing, and credential policy.
16. **Flow backpressure is not mistaken for durable backlog control.** Discovery admission and watermarks address cross-run growth.
17. **Live events are not treated as durable operational truth.** Durable summaries/history require explicit adapters.
18. **Retention is not one global TTL.** Each state, response, output, session, or observer adapter owns its retention and lineage rules.
19. **Anti-bot is not a first-release core priority.** Its generic hooks are allowed early; provider adapters are demand-driven.
20. **The repository baseline is corrected.** There is currently no production implementation, and the README's claims of mandatory PostgreSQL/archival/anti-bot behavior are aspirational and conflict with this authoritative architecture.

This is the plan to implement. Lower-priority plan documents remain historical design input only.
