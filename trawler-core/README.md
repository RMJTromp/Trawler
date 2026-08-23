# Trawler Core

> **Status:** Phase 1 implemented; Phase 2 runtime not started.

`trawler-core` is the primary planned published artifact. It now contains the
Phase 1 generic models, typed extension contracts, immutable composition
configuration, deterministic validation, and non-generic engine registration
boundary. It remains unreleased and does not execute crawl work.

Read the root [`AGENTS.md`](../AGENTS.md), [`FINAL-PLAN.md`](../plans/FINAL-PLAN.md), and [`MODULES.md`](../docs/MODULES.md) before implementing here.

## Package namespace

Production packages use:

```text
com.rmjtromp.trawler
```

Create packages only when their first real type is implemented. Do not scaffold the complete planned tree with empty files or placeholders.

## Implemented Phase 1 scope

The first implementation phase is limited to typed contracts and composition:

- generic request, response, and context models;
- `Produced<T>`;
- typed extractor, transform, validator, and output contracts;
- type-safe pipeline definition and registration;
- isolated heterogeneous engine registration boundary;
- focused transport, state, response-store, policy, observer, clock, and extension contracts;
- immutable configuration and validation;
- explicit extension cardinality, replacement, inheritance, and ordering.

See the focused [typed core contracts feature record](../docs/TYPED-CORE-CONTRACTS.md)
for ownership, capability, lifecycle, and registration-safety details.

Phase 1 does not implement the runtime worker loop, database adapters, Ktor, web policies, persistence, response archives, site parsing, or domain models.

## Later bundled implementations

Subsequent core phases are planned to add:

- structured engine lifecycle and `runOnce`;
- in-memory state;
- seed and caller-`Flow` discovery;
- callback/no-output collection;
- generic policy defaults;
- the replaceable coroutine scheduler;
- no-response, memory, and bounded JDK file response/output implementations using caller codecs.

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
