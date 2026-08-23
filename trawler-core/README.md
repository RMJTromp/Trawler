# Trawler Core

> **Status:** Phase 4 core support surface implemented; unreleased.

`trawler-core` is the primary planned published artifact. It contains the
typed models and extension contracts, immutable composition configuration,
deterministic validation, non-generic engine registration boundary, bounded
in-memory execution, callback/flow outputs, optional response retention, and
the opt-in coroutine scheduler. It remains unreleased.

Read the root [`AGENTS.md`](../AGENTS.md), [`FINAL-PLAN.md`](../plans/FINAL-PLAN.md), and [`MODULES.md`](../docs/MODULES.md) before implementing here.

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
- generic lifecycle events and ordered observers;
- opt-in manual, finite, fixed-delay, fixed-rate, and daily timezone-aware
  coroutine scheduling with bounded overlap handling.
- adapter-neutral durable run-history and resource-cooldown capabilities used
  by the optional PostgreSQL state artifact.

See the focused [typed core contracts feature record](../docs/TYPED-CORE-CONTRACTS.md)
and [in-memory engine feature record](../docs/IN-MEMORY-ENGINE.md) for
ownership, capability, lifecycle, registration, bound, and delivery details.

Core does not implement database adapters, Ktor, web policies, site parsing, or
domain models. The Phase 4 durable hooks remain adapter-neutral; the PostgreSQL
implementation lives in `trawler-postgres`. A file response/output adapter is
also not implemented.

## Later bundled implementations

Subsequent core work may add the bounded JDK file response/output
implementations using caller codecs. Technology-specific transport, web, and
durable modules remain separate artifacts; the implemented PostgreSQL state
adapter is documented in [`POSTGRES-STATE.md`](../docs/POSTGRES-STATE.md).

Bundled does not mean automatically active. Every implementation remains explicitly selected and replaceable.

## Forbidden dependencies

This module must not depend on HTTP clients, SQL/database libraries, serializers, HTML/XML parsers, logging/telemetry vendors, browser/anti-bot libraries, or application models.

The only approved direct runtime dependencies are documented in [`docs/DEPENDENCIES.md`](../docs/DEPENDENCIES.md).

## Build

From the repository root:

```bash
mvn -pl trawler-core -am clean compile
```

No automated tests may be added unless explicitly requested by the project owner.
