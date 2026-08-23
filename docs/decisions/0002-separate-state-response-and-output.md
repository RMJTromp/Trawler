# ADR-0002: Separate crawl state, response storage, and output

- **Status:** Accepted
- **Date:** 2026-08-23
- **Decision owners:** Project owner
- **Related plan:** `plans/FINAL-PLAN.md`, section 8

## Context

Earlier plans treated PostgreSQL as the universal source of truth for work, archived responses, extracted records, and output delivery. These responsibilities have different owners, retention requirements, security concerns, and capability needs.

## Decision drivers

- Keep Trawler usable without SQL or response retention.
- Keep user data outside engine state.
- Allow response storage choices ranging from none to WARC/object storage.
- Avoid a giant storage interface that every adapter must implement.
- Make persistence and delivery guarantees explicit per adapter.

## Considered options

### Mandatory PostgreSQL model

Provides strong durability but turns one deployment profile into the library definition and imposes SQL/JSON/user-data schemas on all callers.

### One universal storage interface with optional methods

Appears pluggable but couples unrelated responsibilities and encourages unsupported no-op behavior.

### Independent focused contracts

Separates operational state, raw responses, and typed outputs while allowing capability negotiation.

## Decision

Trawler defines independent contracts for:

1. crawl/engine operational state;
2. optional response storage/cache/history/replay;
3. typed produced-data output.

An in-memory state store is valid. No response store is valid. Output may be absent when the host collects produced values directly. PostgreSQL, filesystem, JDBC output, and outbox behavior are adapters. User values and output payloads must never enter the core state schema.

## Consequences

### Positive

- Minimal users avoid unused infrastructure and sensitive retention.
- Adapters can state accurate capabilities and guarantees.
- Retention can differ between work history, raw bodies, and user output.
- Replay and outbox behavior remain optional.

### Negative or accepted trade-offs

- Some deployments configure multiple cooperating adapters.
- Cross-component atomicity is available only for deliberately integrated adapters.
- Documentation must describe each adapter's failure and duplicate windows.

### Follow-up work

- Phase 1 defined focused state and response capability interfaces; their
  implementations remain later-phase/adaptor work.
- Validate feature requirements at engine startup.
- Introduce physical modules before adapter dependencies can leak into core.

## Validation

The in-memory core profile must run without SQL, JSON, or response-storage dependencies. Durable adapters must be replaceable without changing pipeline output types.

## Supersession

Any consolidation requires a superseding ADR proving that independent configuration, host data ownership, and no-storage/no-response-store profiles remain intact.
