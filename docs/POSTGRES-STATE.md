# Durable PostgreSQL State

> **Status:** Implemented Phase 4; unreleased
>
> **Owner:** `trawler-postgres`
>
> **Since:** 0.1.0-SNAPSHOT

## Purpose

`trawler-postgres` is the optional durable operational-state adapter for
Trawler. It gives the core engine restart-surviving resources, visit jobs,
leases, meaningful attempts, run summaries, and request-codec payloads while
keeping response retention and produced-data output independent.

The module is selected explicitly. Configure one `PostgresCrawlStateStore` as
the engine state store and register the same instance as an engine lifecycle
extension so its pool, dispatcher, and Flyway migrations have one clear owner.
The adapter does not create a transport, response archive, output client, or
hidden network connection.

## Non-goals

This phase does not implement:

- extracted-record, entity, schema, or user-data tables;
- a mandatory response-body archive or replay store;
- produced-data output, an outbox, or exactly-once delivery;
- discovery checkpoints or checkpoint/enqueue atomicity;
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
- `RunHistoryStore` and `DurableRunStatus`.

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
| `maximumPoolSize` | `8` | Hikari connections owned by this adapter. |
| `databaseDispatcherParallelism` | pool size | Fixed blocking-dispatcher threads; it cannot exceed the pool. |
| `connectionTimeout` | 30 seconds | Hikari pool acquisition bound. |
| `statementTimeout` | 30 seconds | JDBC statement timeout, rounded up to seconds. |
| `leaseDuration` | 5 minutes | Initial claim lease duration. |
| `staleRunAfter` | 30 minutes | Startup stale-summary recovery threshold. |
| `maxActiveJobs` | 100,000 | Active queued/leased/retry-wait capacity for one schema. |
| `maxClaimBatch` | 64 | Maximum rows claimed or reclaimed in one transaction. |
| `maxCancellationRows` | 4,096 | Maximum rows selected by one bounded cancellation batch. |
| `maxRecoveryRows` | 1,024 | Maximum stale rows recovered per run-summary table per call. |
| `maxEncodedRequestBytes` | 8 MiB | Maximum stored request-codec payload. |
| `maxRequestBodyBytes` | 8 MiB | Maximum decoded request body. |
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

Construction creates the adapter-owned Hikari pool and fixed dispatcher.
`start` performs, in order:

1. Flyway migration in the configured schema;
2. bounded recovery of stale engine and pipeline run summaries;
3. publication of the started state.

Core starts extensions in configuration order, so the state store must be
started before runtime work. Every database operation checks that lifecycle
state. `stop` first rejects new calls, then closes the pool, interrupts the
adapter-owned dispatcher, and waits at most five seconds for its threads to
terminate. A failed startup closes the resources it created; the instance is
not a restartable lifecycle object after that failure.

JDBC work runs on the adapter-owned bounded dispatcher. Coroutine cancellation
stops awaiting the operation; Hikari acquisition and every JDBC statement are
also bounded by configured timeouts. Transactions roll back on failure or
cancellation and connections are returned through `use` blocks. Codec encode
and decode are local operations and do not create clients or acquire unrelated
resources.

## Logical and physical schema

The migration creates only operational tables and sequences:

| Object | Important columns and purpose |
|---|---|
| `lease_token_sequence` | Monotonic source for unique fencing-token values within a state schema. |
| `attempt_sequence` | Monotonic source for meaningful attempt IDs; gaps do not represent attempts. |
| `engine_runs` | Engine run ID, lifecycle status, start/completion/observation times, reason, recovery reason, and bounded failure count. |
| `pipeline_runs` | Pipeline run ID, engine FK, pipeline ID, lifecycle status, timestamps, recovery reason, and failure count. |
| `resources` | Stable `(pipeline_id, identity_value)`, last completion, `next_eligible_at`, visit count, and maintenance timestamps. |
| `jobs` | One desired visit: job ID, resource FK, versioned request codec ID/version/payload, status, queue sequence, retry time, lease fields, attempt count, active attempt, completion, reason, and update time. |
| `attempts` | One append-only meaningful attempt row with job/pipeline IDs, attempt number, lease token/owner, start/completion, outcome, and reason. |
| `flyway_schema_history` | Flyway's adapter-owned migration bookkeeping. |

The `jobs_one_active_resource_idx` partial unique index enforces at most one
`QUEUED`, `LEASED`, or `RETRY_WAIT` job for a resource. It does not include
terminal statuses, so a completed, failed, skipped, cancelled, or abandoned
resource can receive a later job. The resource uniqueness constraint is only
for stable identity; it is not a permanent “seen” suppression rule.

There is no `extracted_records`, user-payload, response-body, output, or
outbox table in this migration.

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
| Renew | One fenced update requires job ID, `LEASED` status, token, owner, and an unexpired lease. |
| Begin attempt | One fenced job update increments `attempt_count` and sets `active_attempt_id`; the attempt row insert is in the same transaction. |
| Finish attempt | One fenced job update clears the active attempt, followed by its outcome update in the same transaction. |
| Transition | A fenced row lock captures the current attempt, then the status/lease transition, attempt cleanup, and resource cooldown/completion update commit together. |
| Cancellation | Bounded `QUEUED`/`RETRY_WAIT` batches transition without pretending to own active leases. |
| Run history | Each run-summary mutation is its own bounded transaction and never changes job leases. |

All lease-owned mutations reject stale tokens with `LeaseMutationResult.Stale`.
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

The schema itself is versioned by Flyway. `V1__create_trawler_state.sql` is
owned by this module and is applied only during explicit extension startup.

## Capability declaration and validation

The adapter advertises:

`WORK_QUEUE`, `DURABLE`, `TRANSACTIONAL`, `MULTI_PROCESS`, `LEASE_FENCING`,
`QUERYABLE_HISTORY`, `RESOURCE_HISTORY`, `ATTEMPT_HISTORY`,
`PIPELINE_SCOPED_WORK`, `CANCELLATION`, `WORK_ORDERING`, `WORK_STATUS`,
`RUN_HISTORY`, and `RUN_RECOVERY`.

It intentionally does not advertise `DISCOVERY_CHECKPOINTS` or
`ATOMIC_ENQUEUE_AND_CHECKPOINT`. Core validates advertised runtime capabilities
against the actual focused interfaces before background work starts. The
adapter accepts only the `fifo` ordering policy and reports a deterministic
configuration issue for another ordering instead of silently substituting it.

## Recovery, retention, security, and delivery

The adapter makes durable state and fenced multi-process coordination
available; it does not make output delivery exactly once. A process may fail
after an external output accepts a value and before the job transition, so
outputs still need their own idempotency/duplicate policy.

Request payloads may contain caller-supplied headers, attributes, credentials,
or bodies. The adapter does not log them, but database access, encryption,
retention, deletion, backups, and access auditing remain host/operator
responsibilities. The core never stores `T` in these tables. Run and job
history are operational records and can grow until the host applies an
explicit retention strategy; the migration does not silently delete history.

## Verification and limitations

Verified for this phase with:

- focused reactor compilation of `trawler-postgres` and `trawler-core`;
- a real local PostgreSQL 17.9 smoke run using three independently pooled
  adapter instances against one database schema;
- inspection of the smoke schema showing only migration, run, resource, job,
  and attempt tables;
- Maven runtime dependency-tree inspection.

The smoke run is a manual demonstration, not an automated test, and it does
not claim to fork separate operating-system processes. PostgreSQL row locks,
partial uniqueness, and fencing predicates are database-level guarantees that
apply across processes using the same schema. Automated tests were not added
or run.
