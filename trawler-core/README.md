# Trawler Core

> **Status:** Phase 6 core support surface implemented; unreleased.

`trawler-core` is the primary planned published artifact. It contains the
typed models and extension contracts, immutable composition configuration,
deterministic validation, non-generic engine registration boundary, bounded
in-memory execution, callback/flow outputs, optional response retention and
replay, bounded JDK file response/output stores using caller codecs, and the
opt-in coroutine scheduler. It remains unreleased.

Read the root [`AGENTS.md`](../AGENTS.md), [`PLAN.md`](../PLAN.md), and [`MODULES.md`](../docs/MODULES.md) before implementing here.

## Package namespace

Production packages use:

```text
com.rmjtromp.trawler
```

Create packages only when their first real type is implemented. Do not scaffold the complete planned tree with empty files or placeholders.

## Implemented scope

The implemented scope includes typed contracts and composition:

- generic request, response, and context models;
- `Produced<T>`;
- typed extractor, transform, validator, and output contracts;
- type-safe pipeline definition and registration;
- isolated heterogeneous engine registration boundary;
- focused transport, state, response-store, policy, observer, clock, and extension contracts;
- immutable configuration and validation;
- explicit extension cardinality, replacement, inheritance, and ordering;
- structured engine and pipeline lifecycle with `runOnce`, `awaitIdle`, and
  cooperative shutdown;
- bounded in-memory resource/job/lease/attempt state with active deduplication
  and repeatable recrawl;
- seed and caller-provided `Flow` discovery;
- callback, no-output, and bounded `Flow<Produced<T>>` output;
- no-response and bounded in-memory response stores;
- bounded JDK file response store and typed file output using caller-supplied
  codecs;
- response capture/cache policies, replay selection, typed output codecs,
  batching/retry/idempotency decorators, and independent retention contracts;
- generic lifecycle events and ordered observers;
- opt-in manual, finite, fixed-delay, fixed-rate, and daily timezone-aware
  coroutine scheduling with bounded overlap handling.
- adapter-neutral durable run-history and resource-cooldown capabilities used
  by the optional PostgreSQL state artifact.
- resumable discovery checkpoints with codec resolution, CAS, and replay-safe
  fallback admission;
- optional cursor, watermark, and historical-then-incremental harvest
  strategies with deterministic overlap boundaries;
- backlog admission, pipeline eligibility, health, and circuit-breaker
  contracts with bounded in-memory state.

See the focused [typed core contracts feature record](../docs/TYPED-CORE-CONTRACTS.md),
[in-memory engine feature record](../docs/IN-MEMORY-ENGINE.md), and
[resumable discovery feature record](../docs/RECURRING-DISCOVERY.md), and
[response/output feature record](../docs/RESPONSE-OUTPUT.md) for
ownership, capability, lifecycle, registration, bound, and delivery details.

Core does not implement database adapters, Ktor, web policies, site parsing, or
domain models. The PostgreSQL implementation lives in `trawler-postgres` and
owns its response/output/outbox tables. File stores use host-selected paths,
caller codecs, bounded rotation, and explicit lifecycle cleanup.

The engine runtime also bounds UTF-8 target, method, header, and attribute
metadata in addition to request and response bodies. Replay requires the
response store to enforce ownership for the selected pipeline before applying
the bounded response query limit.

## Later bundled implementations

Technology-specific transport, web, and durable modules remain separate
artifacts; the implemented PostgreSQL state/response/output adapter is
documented in [`POSTGRES-STATE.md`](../docs/POSTGRES-STATE.md) and
[`RESPONSE-OUTPUT.md`](../docs/RESPONSE-OUTPUT.md).

Bundled does not mean automatically active. Every implementation remains explicitly selected and replaceable.

## Forbidden dependencies

This module must not depend on HTTP clients, SQL/database libraries, serializers, HTML/XML parsers, logging/telemetry vendors, browser/anti-bot libraries, or application models.

The only approved direct runtime dependencies are documented in [`docs/DEPENDENCIES.md`](../docs/DEPENDENCIES.md).

## Build

From the repository root:

```bash
mvn -pl trawler-core -am clean compile
```

Core has 318 unit and 27 integration tests (`mvn -pl trawler-core verify`),
gated at 95% line / 90% branch coverage. The shared test-scoped dependencies and lifecycle are documented in
[`docs/TESTING.md`](../docs/TESTING.md); the contract coverage baseline is in
[`docs/TEST-MATRIX.md`](../docs/TEST-MATRIX.md).
