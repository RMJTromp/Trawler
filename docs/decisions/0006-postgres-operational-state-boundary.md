# ADR-0006: Keep durable PostgreSQL operational state behind focused core capabilities

- **Status:** Accepted
- **Date:** 2026-08-23
- **Decision owners:** Project owner
- **Related plan:** `PLAN.md`, Phase 4; [`POSTGRES-STATE.md`](../POSTGRES-STATE.md)

## Context

Phase 4 needs durable engine and pipeline run summaries, stable resources,
visit jobs, fenced leases, meaningful attempts, cooldown state, and versioned
request payloads. Those concerns must coordinate with the existing core engine,
but SQL, JDBC, HikariCP, Flyway, and PostgreSQL schema choices must not enter
`trawler-core` or become a universal storage contract.

The core engine also needs an adapter-neutral way to record run lifecycle
metadata. Without that boundary, the PostgreSQL adapter would either remain
invisible to engine lifecycle or force SQL-specific hooks into the coordinator.

## Decision drivers

- Preserve the dependency direction `adapter -> core`.
- Keep crawl state, response storage, and produced-data output independent.
- Preserve `T` and avoid a serializer or universal record envelope.
- Make lease fencing, attempt accounting, recrawl, and recovery explicit.
- Allow the same pipelines to run with the existing in-memory state store.
- Make capability validation fail before runtime work.

## Considered options

### Put PostgreSQL operations in `trawler-core`

This would simplify direct calls but violate the core dependency boundary,
force SQL/JDBC dependencies on every consumer, and make PostgreSQL semantics
part of generic engine contracts.

### Add one universal durable-storage interface

This would make run history, job leasing, response archival, and produced-data
output appear to share one owner. It would encourage unsupported no-op methods,
erase capability differences, and blur retention and delivery guarantees.

### Add focused core capabilities with a PostgreSQL implementation

`RunHistoryStore` records only operational lifecycle metadata; existing focused
queue, lease, attempt, resource-history, cancellation, ordering, and status
contracts describe the remaining runtime needs. `trawler-postgres` implements
those capabilities with adapter-owned SQL and migrations.

## Decision

Use focused adapter-neutral core hooks and a separate `trawler-postgres`
artifact:

- core adds `RunHistoryStore`/`DurableRunStatus`, the `RUN_HISTORY` and
  `RUN_RECOVERY` capabilities, and `ResourceHistory.nextEligibleAt` for
  adapter-owned cooldown state;
- core's ordering capability can reject an unsupported policy before runtime;
- PostgreSQL owns the Flyway schema, Hikari pool, bounded JDBC dispatcher,
  request codecs, row-lock claim protocol, partial active-job uniqueness,
  sequence-backed fencing tokens, and stale-summary recovery;
- the PostgreSQL migration contains no extracted records, user-data table,
  response archive, mandatory outbox, or checkpoint table;
- run-summary recovery changes summaries only and never bypasses job lease
  fencing;
- the engine advertises and validates capabilities through the focused core
  contracts, while PostgreSQL remains optional and explicitly lifecycle-owned.

## Consequences

### Positive

- Core remains usable with no database and retains only Kotlin/coroutines
  runtime dependencies.
- PostgreSQL can provide multi-process lease fencing and durable recrawl state
  without changing the typed `Pipeline<T>` path.
- Request encoding has explicit codec identity/version and can evolve without
  silently decoding old bytes with a new format.
- State, responses, and outputs retain separate ownership and guarantees.

### Negative or accepted trade-offs

- The module has a PostgreSQL-specific schema and cannot claim generic JDBC
  portability.
- Run history is operational metadata only; it is not a durable event audit
  stream and does not recover job leases by itself.
- Request payloads can contain sensitive caller data, so database encryption,
  access control, backup, and retention are host/operator responsibilities.
- Durable attempts and job history require an explicit retention strategy;
  this phase does not silently delete historical rows.
