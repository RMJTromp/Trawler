# PostgreSQL Adapter

The PostgreSQL module supplies durable state and history adapters. It does not replace TrawlerEngine, and it does not define application-domain tables or serializers.

## Components

| Component | Owns | Registration |
| --- | --- | --- |
| PostgresCrawlStateStore | Durable work, leases, attempts, recovery, checkpoints, harvest state, health, overrides, state retention | Engine stateStore and ExtensionLifecycle |
| PostgresResponseStore | Response lookup, cache/history, replay, response retention | Engine responseStore and ExtensionLifecycle |
| PostgresOutput<T> | Typed output history | Pipeline output and ExtensionLifecycle |
| PostgresOutboxOutput<T> | Pending typed output delivery | Pipeline output and ExtensionLifecycle |

Each adapter owns a Hikari pool and bounded blocking dispatcher. Dispatcher parallelism must not exceed the connection pool size.

## Configuration

PostgresStateConfiguration requires a JDBC URL beginning with jdbc:postgresql:. The password is copied defensively and excluded from toString. Secret sourcing, TLS settings in the JDBC URL, database authorization, and backup policy remain host responsibilities.

Important defaults:

| Option | Default |
| --- | ---: |
| schema | trawler |
| maximumPoolSize | 8 |
| minimumIdle | 0 |
| databaseDispatcherParallelism | pool size |
| connectionTimeout | 30 seconds |
| statementTimeout | 30 seconds |
| leaseDuration | 5 minutes |
| staleRunAfter | 30 minutes |
| maxActiveJobs | 100,000 |
| maxClaimBatch | 64 |
| maxRecoveryRows | 1,024 |
| maxCheckpointPayloadBytes | 2 MiB |
| maxEncodedRequestBytes | 8 MiB |
| maxResponseBodyBytes | 8 MiB |
| maxEncodedResponseBytes | 8 MiB |
| maxOutputPayloadBytes | 1 MiB |
| maxOutputBatchItems | 256 |
| maxResponseQueryLimit | 4,096 |
| maxOutboxClaimBatch | 64 |
| outboxLeaseDuration | 5 minutes |
| maxOutboxAttempts | 10 |
| maxRetentionRows | 4,096 |

The adapter also bounds headers, attributes, targets, identities, reasons, identifiers, request codec payloads, and output batches. Invalid pool, timeout, schema, or bound values are rejected before the pool starts.

## Registration shape

Conceptual example:

~~~kotlin
val state = PostgresCrawlStateStore(
    configuration = postgresConfiguration,
)

val engine = EngineConfigurationBuilder()
    .stateStore(state)
    .extension(state)
    .register(pipeline)
    .requireStateCapability(CrawlStateCapability.DURABLE)
    .requireStateCapability(CrawlStateCapability.LEASE_FENCING)
    .build()
    .let(::TrawlerEngine)
~~~

When response history is needed, create a PostgresResponseStore with an explicit ResponseCodecRegistry, register it as responseStore and extension, and require the capabilities the application actually needs. Output adapters and outbox decorators are registered through the pipeline's output list and extension list.

The store must use a codec registry. PostgreSQL stores encoded request/response/output payloads with codec IDs and versions; it does not make JSON mandatory.

## Migrations

The adapter keeps migration histories separate:

| Migration path | Main structures |
| --- | --- |
| state/V1 and state/V2 | engine_runs, pipeline_runs, resources, jobs, attempts, checkpoints, health, overrides, and state history |
| response/V1 | response_records and lineage |
| output/V1 | output_entries |
| outbox/V1 | outbox_entries |

State, response, output, and outbox migrations must be applied by their owning lifecycle. Do not manually merge the schemas into one application migration without preserving version ownership.

## Durable state semantics

PostgresCrawlStateStore advertises durable, transactional, multi-process, lease-fencing, queryable-history, attempt-history, resource-history, pipeline-scoped work, cancellation, checkpoint, harvest, health, override, recovery, and retention capabilities.

The durable distinction matters:

- resource identity is not job identity;
- a lease is not an attempt;
- an attempt is substantive work;
- all lease-owned mutations carry the fencing token;
- a stale worker cannot attach a response or transition reclaimed work;
- abandoned, cancelled, and failed remain distinct;
- recovery is bounded by maxRecoveryRows and staleRunAfter.

The adapter currently accepts FIFO ordering. A custom WorkOrderingPolicy that requires another ordering mode must be rejected by configuration rather than silently approximated.

## Response history

PostgresResponseStore implements lookup, cache, history, replay, TTL eviction, and range query capabilities. Rows retain codec descriptors and lineage. Replay reads the database and never creates ordinary crawl jobs or makes network calls.

The database does not automatically delete history. Use the adapter's bounded retention operations and host-owned backup/retention policy. See [Response Storage and Replay](Response-Storage-and-Replay) and [Retention](Retention).

## Outbox delivery

PostgresOutboxOutput.write commits PENDING rows before acknowledging the output. Delivery is a separate, explicit operation:

~~~kotlin
val summary = outbox.deliverPending(
    ownerId = "operator-pass",
    limit = configuration.maxOutboxClaimBatch,
)
~~~

The delivery pass:

1. claims a bounded batch with row locks and a fencing token;
2. decodes typed values;
3. calls the delegate outside the database transaction;
4. marks each item DELIVERED, retries it, or marks it FAILED using the fencing token.

The delegate lifecycle is not started implicitly. If it implements ExtensionLifecycle, register it separately before the outbox lifecycle.

If the delegate side effect succeeds and the process dies before the fenced DELIVERED mutation, the next pass can deliver a duplicate. This is an intentional at-least-once duplicate window, not exactly-once delivery. The delegate must be idempotent or deduplicate using an application-owned key.

## PostgreSQL operational checklist

- Use TLS and secret management appropriate to the deployment.
- Keep databaseDispatcherParallelism at or below maximumPoolSize.
- Monitor pool exhaustion, statement timeouts, stale runs, claim age, checkpoint quarantine, outbox attempts, and retention backlog.
- Run bounded recovery and retention passes; do not create unbounded maintenance jobs.
- Grant only the schema permissions the adapters need.
- Back up state and response data according to their separate retention requirements.
- Treat output history and outbox rows as operational data that may contain host-produced values.

