# Response Storage, Replay, Outputs, and Retention

> **Status:** Implemented Phase 6; unreleased
>
> **Owners:** `trawler-core` for generic contracts and JDK/in-memory
> implementations; `trawler-postgres` for PostgreSQL response, output, and
> outbox adapters.
>
> **Since:** 1.0.0

## Purpose

This phase adds optional response retention, replay, typed output persistence,
and independently operated retention capabilities. Crawl state remains an
operational concern. A response store may be absent, and produced values stay
as `Produced<T>` until the selected output owns their encoding or delivery.

The normal path is:

```text
CrawlResponse -> optional ResponseStore
              -> Extractor<T> -> Transform<T>* -> Validator<T>*
              -> Produced<T> -> Output<T>*
```

Replay enters the same typed extraction, transformation, validation, and
output path from a selected response store. It performs no network call and
creates no ordinary crawl job.

## Non-goals

This phase does not implement WARC, object storage, browser transport,
FlareSolverr, a generic extracted-record table, a mandatory response archive,
or generic exactly-once delivery. Core does not choose JSON, a serializer,
schema name, entity type, database row, or application record key.

## Public contracts and ownership

### Core response contracts

`ResponseStore` is the small optional root contract. Its focused capabilities
are advertised through `ResponseStoreCapability` and implemented separately:

| Contract | Owner and behavior |
|---|---|
| `ResponseStore` / `NoResponseStore` | Optional lookup and capture; no store is valid for ordinary crawling. |
| `ResponseReplayStore` | Bounded selection of stored responses for replay. |
| `ResponseHistoryStore` | Bounded request history. |
| `ResponseRetentionStore` | Age/batch pruning with caller-protected references. |
| `ResponseStoreLifecycle` | Explicit close for stores that own files, pools, or dispatchers. |
| `ResponseCodec` / `ResponseCodecRegistry` | Caller-owned response representation with exact codec ID/version selection. |
| `ResponseCapturePolicy` | Decides whether a selected store is offered a response; `RequireResponseCapture` makes refusal a pipeline failure. |
| `ResponseCachePolicy` | Optional pre-transport lookup; it never creates work or a client. |

The codec must encode the complete response information needed by its caller,
including request metadata if replay selection depends on it. Encoding is
bounded and local. Core copies byte arrays but does not inspect or persist a
user serialization format.

Configuration validation checks that advertised history, replay, and eviction
capabilities have their corresponding focused interfaces. A store cannot
advertise one of those capabilities and defer the mismatch until a runtime
call.

### Core output contracts

`Output<T>` remains the typed acknowledgement boundary. `OutputCodec<T>` owns
the representation of `Produced<T>` for file and PostgreSQL outputs. The
following decorators are generic and retain `T`:

- `BatchedOutput` splits one bounded engine batch into bounded acknowledged
  delegate batches;
- `RetryingOutput` retries a complete failed delegate batch under its hard
  `maxAttempts` bound and an explicit policy, documenting the duplicate window;
- `IdempotentOutput` applies a caller-owned typed idempotency hook;
- `FileOutput` writes caller-encoded values to bounded rotating JDK files.

`OutputRetentionStore` owns only output-owned categories. Required output
acknowledgement is explicit through `OutputResult.Accepted`, `Skipped`, or
`Failed`; a required binding turns a non-acceptance into pipeline failure.

## Core implementations

### In-memory response store

`InMemoryResponseStore` retains a finite insertion-ordered ring of responses,
supports lookup, bounded history/replay, and age pruning, and provides no
restart or multi-process guarantee. `NoResponseStore` and a null response
store both keep ordinary crawling independent of response retention.

### JDK file response store

`FileResponseStore` uses a host-selected normalized directory and a fixed ring
of numeric slot files. It writes a bounded record to a temporary file and
atomically replaces the selected slot; temporary files are removed on load and
after each write. Targets and references never become path components.

`maxResponses`, `maxBodyBytes`, `maxEncodedResponseBytes`, and the fixed
dispatcher bound the retained data and work. A file store can retain multiple
historical responses until ring eviction or explicit pruning. It is durable
only as far as the configured filesystem and its permissions/backups; it is
not a transaction with crawl state or output.

### JDK file output

`FileOutput` uses caller-encoded frames in a fixed ring of segment files.
Segments rotate by `maxItemsPerSegment`, `maxSegmentBytes`, or codec
descriptor. Each append forces the file channel before returning
`Accepted`. `maxSegments`, item size, item count, and the adapter dispatcher
are finite. A failed frame append is truncated back to its prior segment
boundary when that repair is possible; an unrecoverable repair failure is
returned with the original failure. A process crash after a caller-visible filesystem acceptance can
still create a duplicate at the surrounding crawl boundary; this is not an
exactly-once external delivery claim.

## Replay

`Pipeline<T>.replay(query, limit)` creates a typed `ReplaySelection<T>` bound
to that exact pipeline object. `TrawlerEngine.replay` verifies that the
selection is registered, the configured response store implements
`ResponseReplayStore`, and the store advertises `REPLAY` before reading.
The store receives the selected pipeline ID as an ownership predicate and must
apply it before the query limit; a post-read filter is not sufficient. A
reference, request ID, or target query therefore cannot cross a pipeline
boundary. Configuration and runtime limits bound the number of responses.

Replay uses a separate engine/pipeline run context for event and provenance
diagnostics, but it does not start a durable run, claim a lease, begin an
attempt, enqueue follow-ups, or mutate crawl state. The replay context does
not expose crawl-state capabilities. Only extensions implementing
`ReplayLifecycle` are activated for replay; a crawl-state extension is never
started by replay. Pipeline/response/output lifecycle resources must be
explicitly replay-safe and registered before use. Extractor follow-ups are
reported as suppressed. A missing historical codec is a visible failure; the
store must not decode bytes with a different descriptor. A response store
without replay capability fails the replay operation before processing. A
replay lifecycle releases its activation resources after each replay and must
remain restartable for a later replay or ordinary engine start; ordinary
engine shutdown remains the terminal close.

The replay result distinguishes `COMPLETED`, `FAILED`, and `ABANDONED` and
contains bounded failure summaries. Cancellation propagates through the
typed pipeline and stops extension/output work under the owning structured
scope.

### Paged and latest-version replay

`ResponseReplayQuery.ByPipelineId(pipelineId, after, versions)` reprocesses
a pipeline's history larger than one bounded replay:

- results are in ascending store order (the order the store accepted them);
- `after` is an exclusive keyset cursor holding a reference issued by the same
  store. A store rejects a reference it cannot order with
  `IllegalArgumentException`. The built-in stores encode their sequence in the
  reference (`memory-response-N`, `file-response-N`, `postgres-response-N`),
  so a cursor whose own response was pruned still orders correctly;
- `versions = ALL` (default) replays every retained response;
  `LATEST_PER_TARGET` replays only the newest retained response for each
  request target within the pipeline. Superseded responses do not consume the
  limit. Selection is evaluated when each page is read, so a target that gains
  a newer response between pages is replayed again on a later page;
- ownership, cursor, and version selection are applied before the limit.

`ReplayResult.lastReference` is the last response, in store order, that
finished processing successfully or with a recorded failure. A response
interrupted by a budget stop is excluded, so the next page replays it again.
To walk a whole history, replay with `after = lastReference` until a page
returns fewer responses than its limit. Per-response failures do not stop a
page; the caller decides whether to continue past a `FAILED` page. Each page
is a separate replay operation, serialized with ordinary runs, and outputs see
at-least-once delivery for a page interrupted between delivery and the next
cursor. Custom `ResponseReplayStore` implementations must honor the ordering,
cursor, and version contract for `ByPipelineId`.

## PostgreSQL implementation

`trawler-postgres` keeps response and output data outside the operational state
tables while reusing the module's existing pinned PostgreSQL, Hikari, Flyway,
Kotlin, and coroutines dependencies. `PostgresResponseStore`,
`PostgresOutput<T>`, and `PostgresOutboxOutput<T>` each own a bounded pool and
blocking dispatcher and must be registered as lifecycle extensions. Each
adapter owns an independent Flyway location and history table: state uses
`db/migration/state` (`flyway_schema_history`), response uses `response`
(`flyway_response_schema_history`), output uses `output`
(`flyway_output_schema_history`), and outbox uses `outbox`
(`flyway_outbox_schema_history`). Selecting one does not require the other
schemas or their database privileges.

### Logical schema

| Table | Ownership and important behavior |
|---|---|
| `response_records` | Append-only encoded response history with opaque reference, sequence, request/pipeline/target metadata, received/stored times, body size, and codec ID/version. |
| `response_lineage` | Output-owned protection link from a response reference to an output or outbox owner. It has a foreign key to PostgreSQL response rows and prevents deletion while a local lineage exists. |
| `output_entries` | Typed output history encoded by the caller's `OutputCodec<T>`. It is not a state table and has no entity/schema/application record field. |
| `outbox_entries` | Output-owned pending/leased/delivered/failed queue with typed codec metadata, lease owner/expiry, fencing token, attempt count, and bounded failure text. |
| `response_sequence`, `output_sequence`, `outbox_sequence`, `outbox_fence_sequence` | Adapter-owned bounded-ID and unique-fencing-token sources; sequence gaps are not delivery or attempt claims. |

Response and output writes are separate transactions. PostgreSQL output
acknowledgement means the adapter transaction committed. It does not mean an
external consumer accepted the value.

During ordinary leased work, core validates the fencing token immediately
before response capture and again before response processing. A stale worker
cannot attach the captured response reference to produced data or complete
the work item. Response storage remains an independent adapter transaction,
so a store write that races lease reclamation can leave an unreferenced raw
response for the adapter's retention policy; core does not claim an atomic
state/response transaction.

The relevant acknowledgement/duplicate windows are explicit:

| Output path | `Accepted` means | Uncertain or duplicate window |
|---|---|---|
| Callback / `FlowOutput` | The callback completed, or the value was sent to the bounded channel. | A caller-side effect can have happened before a thrown/cancelled call; a channel consumer owns its own redelivery policy. |
| `FileOutput` | The bounded frame was written and the file channel was forced. | A process/filesystem failure around the acknowledgement can make the host retry a frame; the file output does not fence callers. |
| `PostgresOutput` | The output transaction committed. | A caller that cannot determine commit outcome may retry a batch and create duplicates; the table is history, not an idempotency claim. |
| `PostgresOutboxOutput.write` | The outbox enqueue transaction committed. | `deliverPending` may deliver a value again when the delegate side effect precedes a lost fenced completion; exhausted failures remain `FAILED`. |
| `IdempotentOutput` | The hook suppressed the item or the wrapped output acknowledged it. | Durable duplicate suppression and key retention belong to the caller-supplied hook. |

Required bindings turn `Skipped`, `Failed`, or an invalid acknowledgement count
into a pipeline failure; best-effort bindings record the failure/drop and let
the run continue. None of these rows or decorators makes a generic exactly-once
delivery claim.

### Outbox lifecycle and fencing

The explicit `deliverPending(ownerId, limit, now)` operation claims at most a
bounded number of `PENDING` or expired `LEASED` rows with
`FOR UPDATE SKIP LOCKED`. Each claim receives a new value from
`outbox_fence_sequence`. The database transaction ends before the delegate is
called:

```text
PENDING -> LEASED -> DELIVERED
             |  \-> PENDING (bounded retry)
             \----> FAILED (attempt bound reached)
```

Every completion, failure, or cancellation release includes output ID, row
ID, owner, and fencing token. A stale worker cannot mutate a reclaimed row.
Delegate calls are visible to the caller and are not hidden in a coroutine
background loop. If a delegate accepts a value and the process fails before
the fenced `DELIVERED` update, the value may be delivered again. If a process
fails before the delegate call, the expired lease is reclaimable. Cancellation
releases the current claim under a bounded `NonCancellable` cleanup path.

An outbox does not implicitly start a lifecycle-owning delegate. A delegate
that implements `ExtensionLifecycle` must be registered as a preceding engine
extension, making startup and shutdown ownership explicit. Outbox `close` is
idempotent and closes such a delegate at most once.

`Skipped` is treated as a delivery failure, not as silent loss. A failed
delegate is requeued until `maxOutboxAttempts`, then becomes `FAILED`.

## Retention and lineage

Retention is independently activated by category:

| Category | Owner | Protected behavior |
|---|---|---|
| `COMPLETED_JOBS`, `ATTEMPTS`, `RUN_HISTORY` | `CrawlStateRetentionStore` / PostgreSQL state | Only terminal, age-eligible rows are considered; foreign-key order is preserved. |
| `RESPONSES` | `ResponseRetentionStore` | Caller-protected references and PostgreSQL output/outbox lineage prevent deletion. |
| `OUTPUT_HISTORY` | `OutputRetentionStore` / file or PostgreSQL output | Output rows are removed in bounded batches; local response lineage is removed in the same transaction. |
| `OUTBOX_HISTORY` | `OutputRetentionStore` / PostgreSQL outbox | Only `DELIVERED` and terminal `FAILED` rows are eligible; pending or leased work is never pruned. |
| `OPERATIONAL_EVENTS` | Not implemented | Live events remain best-effort observations, not a durable audit history. |

PostgreSQL output/outbox lineages are inserted only when the referenced row is
present in the same PostgreSQL response schema. A response reference from a
different store cannot be protected by this adapter's foreign key; callers
must pass it in `ResponseRetentionRequest.protectedReferences` to that store.
Deleting output/outbox ownership removes its lineage before the owning row,
so response retention cannot silently delete a response still referenced by a
local retained output. Retention results report bounded deleted/protected
counts, not a claim that every external lineage was discovered.

## Configuration and resource bounds

Core file stores and outputs validate positive finite capacities at
construction. PostgreSQL adds response body, encoded response, output
payload, query, outbox-claim, retention-row, and outbox-attempt bounds to
`PostgresStateConfiguration`. Its pool and blocking dispatcher remain aligned;
JDBC acquisition and statements have configured timeouts. No operation reads an
unbounded response history, output batch, file body, database claim, or
retention batch.

The selected PostgreSQL response/output/outbox object must be started by the
engine's ordered extension lifecycle. `stop` rejects new adapter operations,
closes the pool and dispatcher, and is idempotent. A failed migration closes
resources. File stores and outputs close their dispatcher on shutdown; file
roots, permissions, encryption, and backup remain host responsibilities.

## Security, privacy, and retention

Raw response bodies, cookies, authorization headers, and produced values are
not emitted in default core events. Response codecs and outputs are caller
owned, so hosts must decide whether to encrypt, redact, or avoid sensitive
content before retention. File roots must be authorized and permissioned by
the host. PostgreSQL credentials remain in adapter configuration and are not
included in adapter `toString` output. Retention is a deletion policy, not an
encryption or legal-retention guarantee.

## Dependencies

No dependency or POM change was needed in Phase 6. Core remains limited to
Kotlin stdlib, coroutines, and JDK APIs. PostgreSQL response/output/outbox
behavior uses the already-owned `trawler-postgres` driver, Hikari, Flyway,
Kotlin, and coroutines graph. See [`docs/DEPENDENCIES.md`](DEPENDENCIES.md).

## Verification

The core contract phase adds deterministic tests for response capture, cache
age and ownership, in-memory/file response retention, replay selection, and
core response/output codec registries. `OutputsTest` covers callback, flow,
batching, retry, and idempotency decorators. `FileResponseStoreIT` and
`FileOutputIT` cover bounded temporary-directory persistence, restart and
ring behavior, exact codec versions, replay/retention ordering, malformed
frames, cancellation, forced acceptance, and append repair. PostgreSQL
`PostgresResponseStoreIT`, `PostgresOutputIT`, and
`PostgresOutboxOutputIT` add direct-SQL coverage for independent migrations,
exact codec metadata, ownership-before-limit replay, transactional writes,
lineage-protected retention, bounded FIFO claims, cancellation release,
fresh fencing tokens, stale completion, and the documented duplicate window.
Their source compiles and Failsafe discovers it; execution remains dependent
on a Docker-compatible Testcontainers runtime.

Paged and latest-version replay is covered for the in-memory, file
(including a reopened store), and PostgreSQL stores: exclusive cursors,
superseded rows not consuming the limit, cursor composition with version
selection, pipeline scoping of latest versions, pruned cursors, and rejection
of foreign cursors (`InMemoryResponseStoreTest`, `FileResponseStoreIT`,
`PostgresResponseStoreIT`, the latter run against PostgreSQL 16 via
Testcontainers). `EngineRuntimeReplayTest` walks a recrawled history in
bounded pages through the engine with no network calls and checks
`lastReference` after stage failures and budget stops.

## Limitations

- File stores do not provide multi-process fencing or a cross-store lineage
  transaction.
- PostgreSQL outputs and outbox delivery do not claim exactly-once external
  delivery; idempotency remains caller/destination-owned.
- Replay requires the selected response store to retain a compatible caller
  codec and does not replay extractor follow-up jobs.
- Latest-version replay compares exact request targets; two targets that
  canonicalize to the same resource are distinct unless the caller stores
  them under one target.
- There is no durable operational-event history in this phase.
