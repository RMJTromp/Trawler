# Trawler PostgreSQL

> **Status:** Phase 6 implemented; unreleased.

`trawler-postgres` is the optional durable state/response/output adapter for
Trawler. It depends on `trawler-core` and owns PostgreSQL connections, bounded
database dispatchers, Flyway migrations, request-state codecs, durable run
summaries, resources, visit jobs, attempts, leases, retry eligibility,
stale-run recovery, discovery checkpoints, harvest state, pipeline health,
eligibility overrides, append-only response history, caller-typed output
history, fenced outbox delivery, and independent retention capabilities.

Response and output data are separate from crawl state. State, response,
output, and outbox adapters each own a separate Flyway location and migration
history table; selecting one does not require the tables or privileges of the
others. There is no extracted-record table or
universal JSON representation of `T`. The same pipeline can select
`InMemoryCrawlStateStore`, omit response storage/output persistence, and omit
this artifact entirely.

The adapter owns its Hikari pool and must be registered both as the configured
state store and as an engine lifecycle extension:

```kotlin
// Conceptual usage sketch; not a compiled repository example.
val state = PostgresCrawlStateStore(
    PostgresStateConfiguration(
        jdbcUrl = "jdbc:postgresql://localhost:5432/trawler",
        username = "trawler",
        password = charArrayOf(),
    ),
)
val engine = TrawlerEngine {
    stateStore(state)
    extension(state)
    requireStateCapability(CrawlStateCapability.DURABLE)
    register(myPipeline)
}
try {
    engine.runOnce()
} finally {
    engine.shutdown()
}
```

The database must be authorized by the host. Startup applies only the
adapter-owned migrations. Configuration and request-codec unit tests run in
Surefire; `PostgresCrawlStateStoreIT.kt` owns the real PostgreSQL
Testcontainers coverage and runs in Failsafe. The IT suite requires Docker;
see [`docs/TESTING.md`](../docs/TESTING.md) and
[`docs/TEST-MATRIX.md`](../docs/TEST-MATRIX.md).

See [`docs/RESPONSE-OUTPUT.md`](../docs/RESPONSE-OUTPUT.md) for response
codec ownership, replay isolation, PostgreSQL response/output/outbox schemas, typed output
acknowledgement, outbox fencing, duplicate windows, lineage, and retention.

`PostgresResponseStore`, `PostgresOutput<T>`, and
`PostgresOutboxOutput<T>` each use caller-supplied versioned codecs and must
also be registered as lifecycle extensions when selected. Outbox delivery is
explicitly invoked with `deliverPending`; the adapter does not start a hidden
delivery coroutine or create a network client.

## Build

```bash
mvn -pl trawler-postgres -am clean verify
```

See [`docs/POSTGRES-STATE.md`](../docs/POSTGRES-STATE.md) for the schema,
transactions, fencing, recovery, retention, dependency, and verification
details.
