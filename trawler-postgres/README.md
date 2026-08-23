# Trawler PostgreSQL

> **Status:** Phase 4 implemented; unreleased.

`trawler-postgres` is the optional durable operational-state adapter for
Trawler. It depends on `trawler-core` and owns PostgreSQL connections, a
bounded database dispatcher, Flyway migrations, request-state codecs, durable
run summaries, resources, visit jobs, attempts, leases, retry eligibility,
and stale-run recovery.

It does not store produced values, extracted records, response bodies, or
outbox data. Checkpoint persistence remains a later discovery capability. The
same pipeline can select `InMemoryCrawlStateStore` and omit this artifact.

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
adapter-owned migrations. No automated tests or test dependencies are
included; tests require explicit project-owner authorization.

## Build

```bash
mvn -pl trawler-postgres -am clean verify
```

See [`docs/POSTGRES-STATE.md`](../docs/POSTGRES-STATE.md) for the schema,
transactions, fencing, recovery, retention, dependency, and verification
details.
