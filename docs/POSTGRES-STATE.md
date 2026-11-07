# Durable PostgreSQL State

> **Status:** Implemented Phase 4/5/6; unreleased
>
> **Owner:** `trawler-postgres`
>
> **Since:** 0.1.0-SNAPSHOT

## Purpose

`trawler-postgres` is the optional durable operational-state, response, and
typed-output adapter for Trawler. It gives the core engine restart-surviving
resources, visit jobs, leases, meaningful attempts, run summaries,
request-codec payloads, resumable discovery checkpoints, optional harvest
state, pipeline health, eligibility overrides, append-only response history,
typed output history, fenced outbox delivery, and independent retention.

The module is selected explicitly. Configure one `PostgresCrawlStateStore` as
the engine state store and register the same instance as an engine lifecycle
extension so its pool, dispatcher, and Flyway migrations have one clear owner.
Response/output adapters remain separate objects and tables; selecting the
state store does not activate response retention or output delivery. None of
these adapters creates a transport or hidden network connection.

## Non-goals

The PostgreSQL module still does not implement:

- extracted-record, entity, schema, or user-data tables;
- a mandatory response store or produced-data output;
- exactly-once delivery, WARC, or object storage;
- HTTP/Ktor, robots, RSS/Atom, sitemap, browser, or anti-bot behavior;
- a generic JDBC abstraction for another database;
- a serializer for `T` or a universal JSON representation of requests.

The request payload stored for a job is an operational request snapshot
encoded by an explicit caller/adapter codec. It is not a produced-data
record.

## Ownership and public surface

The core owns the adapter-neutral capability contracts:

- `CrawlStateStore` and `CrawlStateCapability`;
- `PipelineScopedWorkQueueStore`;
- `LeaseStore` and `LeaseMutationResult`;
- `AttemptStore` and `ExecutionAttempt`;
- `ResourceHistoryStore` and `ResourceHistory`;
- `WorkCancellationStore`, `WorkOrderingStore`, and `WorkQueueStatus`;
- `RunHistoryStore` and `DurableRunStatus`;
- `DiscoveryCheckpointStore`, atomic checkpoint admission, and checkpoint
  quarantine;
- `HarvestStateStore`, harvest-state quarantine, queue metrics, pipeline health,
  and eligibility override capabilities.

Phase 6 also uses the core `ResponseStore`, `ResponseReplayStore`,
`ResponseRetentionStore`, `Output<T>`, `OutputCodec<T>`, and
`OutputRetentionStore` contracts. The PostgreSQL module owns
`PostgresResponseStore`, `PostgresOutput<T>`, `PostgresOutboxOutput<T>`, and
`OutboxDeliverySummary`.

The PostgreSQL module owns:

- `PostgresStateConfiguration`;
- `PostgresCrawlStateStore`;
- `PostgresRequestCodec`, `PostgresRequestCodecRegistry`, and codec metadata;
- the adapter-owned Flyway migration.

No public core or PostgreSQL state contract contains `T`, `JsonNode`, a
serializer annotation, an entity type, schema name, application record key,
database row type, Ktor type, or JDBC type outside the adapter's own
implementation.

## Configuration and bounds

`PostgresStateConfiguration` is immutable and validates before the adapter can
start. Its defaults are deliberately finite:

| Setting | Default | Meaning |
|---|---:|---|
| `maximumPoolSize` | `8` | Hikari connections owned by this adapter; minimum 2, because Flyway holds a second connection during startup migration. |
| `databaseDispatcherParallelism` | pool size | Fixed blocking-dispatcher threads; it cannot exceed the pool. |
| `connectionTimeout` | 30 seconds | Hikari pool acquisition bound. |
| `statementTimeout` | 30 seconds | JDBC statement timeout, rounded up to seconds. |
| `leaseDuration` | 5 minutes | Initial claim lease duration. |
| `staleRunAfter` | 30 minutes | Adapter-configured lower bound retained for stale-summary recovery policy; the engine invokes recovery explicitly at run start. |
| `maxActiveJobs` | 100,000 | Active queued/leased/retry-wait capacity for one schema. |
| `maxClaimBatch` | 64 | Maximum rows claimed or reclaimed in one transaction. |
| `maxCancellationRows` | 4,096 | Maximum rows selected by one bounded cancellation batch. |
| `maxRecoveryRows` | 1,024 | Maximum stale rows recovered per run-summary table per call. |
| `maxCheckpointBatch` | 64 | Maximum work items in one enqueue-plus-checkpoint transaction. |
| `maxCheckpointPayloadBytes` | 2 MiB | Maximum checkpoint or harvest payload. |
| `maxEncodedRequestBytes` | 8 MiB | Maximum stored request-codec payload. |
| `maxRequestBodyBytes` | 8 MiB | Maximum decoded request body. |
| `maxResponseBodyBytes` | 8 MiB | Maximum decoded response body accepted by the PostgreSQL response store. |
| `maxEncodedResponseBytes` | 8 MiB | Maximum caller-encoded response payload. |
| `maxOutputPayloadBytes` | 1 MiB | Maximum caller-encoded output or outbox payload. |
| `maxOutputBatchItems` | 256 | Maximum items acknowledged in one PostgreSQL output transaction. |
| `maxResponseQueryLimit` | 4,096 | Maximum response/replay rows read by one query. |
| `maxOutboxClaimBatch` | 64 | Maximum rows claimed by one explicit outbox delivery pass. |
| `maxRetentionRows` | 4,096 | Maximum rows scanned/deleted by one adapter retention request. |
| `outboxLeaseDuration` | 5 minutes | Lease interval before an uncompleted outbox row is reclaimable. |
| `maxOutboxAttempts` | 10 | Maximum delegate attempts before a row becomes `FAILED`. |
| `maxHeaderCount` / `maxAttributeCount` | 256 / 256 | Maximum entries in one request. |
| `maxIdentityBytes` | 4 KiB | UTF-8 bound for stable resource identity. |
| `maxReasonBytes` | 4 KiB | UTF-8 bound for operational reasons. |

The binary codec also bounds target, method, header, attribute, and body
strings. A custom codec remains responsible for its own format, but the store
revalidates decoded core request bounds before returning claimed work.

The database queue is bounded by `maxActiveJobs`; terminal history is durable
and remains subject to the host's database retention/partitioning policy.
The adapter never loads an unbounded result set, request body, claim batch,
cancellation batch, or recovery batch into memory.

## Lifecycle, cancellation, and cleanup

Construction creates no pool threads or database handles. `start` lazily
creates the adapter-owned Hikari pool and fixed dispatcher, then performs, in
order:

1. Flyway migration in the configured schema;
2. publication of the started state.

The engine invokes `RunHistoryStore.recoverStaleRuns` explicitly at the start
of an ordinary engine run, using that run's bounded timeout plus shutdown
window. Replay does not start this state adapter and therefore does not recover
or mutate durable run summaries.

Core starts extensions in configuration order, so the state store must be
started before runtime work. Every database operation checks that lifecycle
state. `stop` first rejects new calls, then closes the pool, interrupts the
adapter-owned dispatcher, and waits at most five seconds for its threads to
terminate. A failed startup closes the resources it created and leaves the
adapter reusable for a later explicit start; a normal `stop` is terminal.

JDBC work runs on the adapter-owned bounded dispatcher. Coroutine cancellation
stops awaiting the operation; Hikari acquisition and every JDBC statement are
also bounded by configured timeouts. Transactions roll back on failure or
cancellation and connections are returned through `use` blocks. A caller
cancelled before commit is rolled back (the transaction checks for
cancellation immediately before `commit`); cancellation that arrives during
or after the commit can still commit while the caller observes
`CancellationException`, the same at-least-once window as a crash after
commit. Codec encode
and decode are local operations and do not create clients or acquire unrelated
resources.

## Logical and physical schema

The state adapter applies `db/migration/state/V1__create_trawler_state.sql`
and `V2__add_discovery_and_pipeline_state.sql` with
`flyway_schema_history`. Response, output, and outbox adapters apply their own
`V1` migrations under separate locations and history tables, so selecting a
response or output adapter does not require operational-state tables. Several
adapters may share one schema: each Flyway run uses `baselineOnMigrate` with
baseline version `0`, so a schema that already contains another adapter's (or
the host's) tables receives this adapter's history table and all of its `V1`+
migrations instead of being rejected as non-empty. A table-name collision with
host-owned objects still fails the migration loudly:

| Object | Important columns and purpose |
|---|---|
| `lease_token_sequence` | Monotonic source for unique fencing-token values within a state schema. |
| `attempt_sequence` | Monotonic source for meaningful attempt IDs; gaps do not represent attempts. |
| `engine_runs` | Engine run ID, lifecycle status, start/completion/observation times, reason, recovery reason, and bounded failure count. |
| `pipeline_runs` | Pipeline run ID, engine FK, pipeline ID, lifecycle status, timestamps, recovery reason, and failure count. |
| `resources` | Stable `(pipeline_id, identity_value)`, last completion, `next_eligible_at`, visit count, and maintenance timestamps. |
| `jobs` | One desired visit: job ID, resource FK, versioned request codec ID/version/payload, status, queue sequence, retry time, lease fields, attempt count, active attempt, completion, reason, and update time. |
| `attempts` | One append-only meaningful attempt row with job/pipeline IDs, attempt number, lease token/owner, start/completion, outcome, and reason. |
| `discovery_checkpoints` | One revisioned opaque checkpoint per `(pipeline_id, discovery_id)`, with codec ID/version, strategy, generation, and completion flag. |
| `discovery_checkpoint_quarantine` | Retained incompatible checkpoint payloads and reasons. |
| `harvest_state` | One revisioned opaque strategy state per `(pipeline_id, strategy_id)`. |
| `harvest_state_quarantine` | Retained incompatible harvest-state payloads and reasons. |
| `pipeline_health` | Compare-and-set health counters and optional circuit-open time. |
| `pipeline_overrides` | Explicit operator eligibility state and optional expiry. |
| `flyway_schema_history` | State adapter migration bookkeeping. |
| `flyway_response_schema_history` | Response adapter migration bookkeeping. |
| `flyway_output_schema_history` | PostgreSQL typed-output migration bookkeeping. |
| `flyway_outbox_schema_history` | PostgreSQL outbox migration bookkeeping. |
| `response_records` | Append-only encoded response history with opaque reference, target/request/pipeline metadata, body size, and codec ID/version. |
| `response_lineage` | Foreign-keyed local response protection for retained PostgreSQL output/outbox owners. |
| `output_entries` | Caller-encoded `Produced<T>` output history; not operational state and not a generic record table. |
| `outbox_entries` | Output-owned pending/leased/delivered/failed rows with lease owner, expiry, attempts, and fencing token. |
| `response_sequence`, `output_sequence`, `outbox_sequence`, `outbox_fence_sequence` | Bounded identifiers and unique outbox fencing tokens. |

The `jobs_one_active_resource_idx` partial unique index enforces at most one
`QUEUED`, `LEASED`, or `RETRY_WAIT` job for a resource. It does not include
terminal statuses, so a completed, failed, skipped, cancelled, or abandoned
resource can receive a later job. The resource uniqueness constraint is only
for stable identity; it is not a permanent “seen” suppression rule.

There is no `extracted_records` table, universal user-data envelope, or
mandatory response/output activation. Checkpoint, harvest, response, and
output payloads carry explicit caller/extension-owned codec metadata. Response,
output, and outbox schema objects are created only by their selected adapters.
Output and outbox response lineage is recorded only when the response adapter's
schema is present; without it, those adapters retain their own rows but cannot
make a cross-store response-reference protection claim.

## State transitions

```text
Resource(identity)
       │
       └── Job(visit)
             ├── QUEUED ──claim──> LEASED ──success──> SUCCEEDED
             │                      │  ├──retry──> RETRY_WAIT ──claim──┐
             │                      │  ├──failure/cancel/skip──> terminal
             │                      │  └──expiry──> QUEUED <───────────┘
             │                      └──active Attempt (only after beginAttempt)
             └──terminal job does not block a later job for the Resource
```

`claim` uses `FOR UPDATE SKIP LOCKED`, assigns a fresh sequence-backed lease
token, and does not increment `attempt_count`. Expired leases are reclaimed
at the beginning of a bounded claim transaction. If an active attempt exists,
the adapter records `ABANDONED`; if no attempt exists, no retry budget is
consumed. A later claim can then receive the same job.

Engine and pipeline summaries follow:

```text
RUNNING ──normal completion──> COMPLETED
       ├──run failure─────────> FAILED
       ├──cooperative stop────> CANCELLED
       ├──run timeout/recovery> ABANDONED
```

Stale-summary recovery changes only summaries. It never releases or rewrites
a job lease; job reclaim remains governed by the lease predicates in `claim`.
Completions use `WHERE status = 'RUNNING'`, so a late completion from a
recovered worker cannot overwrite an already terminal summary.

## Transactions and fencing

| Operation | Transaction boundary and guarantee |
|---|---|
| Enqueue | Resource upsert, active-capacity check, and job insert commit together. The partial unique index is the final active-deduplication authority. |
| Claim | Expired-lease reclaim and selected job claims commit together. `FOR UPDATE SKIP LOCKED` prevents two claimers from selecting the same row. |
| Lease validation / renew | A non-extending check and each renewal require job ID, pipeline ID, `LEASED` status, token, owner, and an unexpired lease. |
| Begin attempt | One fenced job update increments `attempt_count` and sets `active_attempt_id`; the attempt row insert is in the same transaction. |
| Finish attempt | One fenced job update clears the active attempt, followed by its outcome update in the same transaction. |
| Transition | A fenced row lock captures the current attempt, then the status/lease transition, attempt cleanup, and resource cooldown/completion update commit together. |
| Cancellation | Bounded `QUEUED`/`RETRY_WAIT` batches transition without pretending to own active leases. |
| Run history | Each run-summary mutation is its own bounded transaction and never changes job leases. |
| Checkpoint admission | V2's atomic path locks capacity, applies active deduplication, and advances the checkpoint in one transaction; rollback leaves both unchanged. |
| Checkpoint fallback | Core's non-atomic adapter fallback enqueues before checkpoint CAS and documents its replay window. |
| Harvest/health/override state | Each revisioned mutation uses a row lock and compare-and-set; incompatible payloads are quarantined before reset. |

All lease-owned mutations and non-extending lease checks reject stale tokens
with `LeaseMutationResult.Stale`. Run-history retention shares one deletion
budget across pipeline and engine summaries, deleting eligible pipeline
children before an unreferenced engine parent.
The database sequence makes each newly issued token distinct within the
configured state schema. A worker that loses its lease cannot finish an
attempt, renew, or transition the reclaimed job.

## Request codec and schema migration

Every job stores `request_codec_id` and `request_codec_version` beside the
opaque payload. `PostgresRequestCodecRegistry` selects an exact descriptor;
missing historical codecs fail claim/decode rather than silently reading old
bytes with a new interpretation. The built-in `trawler-binary-request@1`
codec uses bounded JDK `DataInputStream`/`DataOutputStream` and carries only
core request fields.

To change a request encoding, register a new descriptor, keep the old codec
available until all rows using it are drained or explicitly migrated, and add
an adapter-owned Flyway/data operation if the payload requires a controlled
conversion. There is no universal JSON fallback and no automatic reinterpretation.

The schema is versioned by Flyway in four adapter-owned locations. State uses
`state/V1__create_trawler_state.sql` and
`state/V2__add_discovery_and_pipeline_state.sql`; response, output, and outbox
each use their own `V1` migration and history table. They are applied during
explicit adapter extension startup. Checkpoint and harvest codec migration is
strategy/extension-owned; incompatible bytes are retained in the corresponding
quarantine table before an explicit reset.

## Capability declaration and validation

The adapter advertises:

`WORK_QUEUE`, `DURABLE`, `TRANSACTIONAL`, `MULTI_PROCESS`, `LEASE_FENCING`,
`QUERYABLE_HISTORY`, `RESOURCE_HISTORY`, `ATTEMPT_HISTORY`,
`PIPELINE_SCOPED_WORK`, `CANCELLATION`, `WORK_ORDERING`, `WORK_STATUS`,
`RUN_HISTORY`, and `RUN_RECOVERY`.

It additionally advertises `DISCOVERY_CHECKPOINTS`,
`ATOMIC_ENQUEUE_AND_CHECKPOINT`, `CHECKPOINT_QUARANTINE`, `HARVEST_STATE`,
`HARVEST_STATE_QUARANTINE`, `QUEUE_WATERMARKS`, `PIPELINE_HEALTH`, and
`PIPELINE_ELIGIBILITY_OVERRIDES`, and `STATE_RETENTION` for terminal jobs,
attempts, and run history. Core validates advertised runtime
capabilities against the actual focused interfaces before background work
starts. The adapter accepts only the `fifo` ordering policy and reports a
deterministic configuration issue for another ordering instead of silently
substituting it.

## Recovery, retention, security, and delivery

The adapter makes durable state and fenced multi-process coordination
available; it does not make output delivery exactly once. A process may fail
after an external output accepts a value and before the job transition, so
outputs still need their own idempotency/duplicate policy.

Request payloads may contain caller-supplied headers, attributes, credentials,
or bodies. The adapter does not log them, but database access, encryption,
retention, deletion, backups, and access auditing remain host/operator
responsibilities. Response bodies and produced values are retained only by
the separately selected Phase 6 adapters and their caller codecs. State,
response, output, and outbox retention are independent; pending/leased outbox
rows and response rows with local lineage are not silently deleted.

## Verification and limitations

The PostgreSQL configuration and binary request-codec unit tests run without a
database. `PostgresCrawlStateStoreIT.kt` is the Testcontainers-owned database
suite; it uses the pinned `postgres:16.4-alpine` image, a fresh schema per
test, and two store instances only where a multi-process race is the contract.
It covers state migrations, lifecycle retry/cleanup, enqueue/claim/capacity,
leases and fencing, attempts, every work transition, resource history,
checkpoints/harvest/health/overrides, run recovery, and state retention.
`PostgresResponseStoreIT.kt`, `PostgresOutputIT.kt`, and
`PostgresOutboxOutputIT.kt` cover the independent response/output/outbox
migrations and lifecycle, exact codec persistence, bounded replay and
retention, transactional output, lineage release, FIFO outbox claims,
reclaim/fencing, retry exhaustion, cancellation release, and duplicate-window
delivery semantics with direct SQL assertions.

`PostgresCrawlStateStoreContractIT.kt` and `PostgresAdapterContractIT.kt` add
every configuration bound, lock-loss and cancellation rollback, connection
termination with pool recovery, and outbox lease expiry during delivery. The
whole suite (20 unit, 69 integration tests) passes against
`postgres:16.4-alpine` with Testcontainers 1.21.4 on Docker Engine 29, at
99.2% line / 95.1% branch coverage (2026-11-07). A focused run is:

```bash
mvn -pl trawler-postgres -am -Dit.test='Postgres*IT' \
  -Dfailsafe.failIfNoSpecifiedTests=false verify
```

The required full command is `mvn clean verify` in an environment with a
Testcontainers-compatible Docker runtime. Test
ownership, Docker requirements, and the full contract matrix are recorded in [`TESTING.md`](TESTING.md) and
[`TEST-MATRIX.md`](TEST-MATRIX.md).
