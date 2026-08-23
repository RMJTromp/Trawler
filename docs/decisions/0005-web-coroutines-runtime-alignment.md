# ADR-0005: Isolate the Ktor coroutines runtime alignment in the web adapter

- **Status:** Accepted
- **Date:** 2026-08-23
- **Decision owners:** Project owner
- **Related plan:** `plans/FINAL-PLAN.md`, sections 3.2, 11, and Phase 3

## Context

Phase 3 adds a real Ktor CIO transport in `trawler-web`. Ktor 3.5.1 is
compiled against `kotlinx-coroutines` 1.11.0, while the implemented core
runtime is intentionally pinned to 1.10.2. Resolving the Ktor graph through
core's dependency path would select 1.10.2 for the web artifact and produces
runtime linkage failures in Ktor timeout/lifecycle code.

## Decision drivers

- Keep Ktor and its binary compatibility requirements out of `trawler-core`.
- Preserve the Phase 2 core dependency tree and its minimal Kotlin/coroutines
  boundary.
- Make the compatibility choice visible in Maven and dependency documentation.
- Avoid a hidden runtime override selected by dependency declaration order.

## Considered options

### Upgrade the shared core coroutines version

This would align Ktor and core, but it would change the already-delivered core
runtime dependency without a core implementation need and would make the web
adapter's technology choice affect all core consumers.

### Rely on Maven mediation or declaration order

This keeps POM text shorter but makes the selected runtime version implicit and
can select a Ktor-incompatible version when another dependency is added.

### Keep a web-owned compatibility version

This gives the adapter the version Ktor requires, leaves core unchanged, and
makes the boundary reviewable in the web POM and dependency inventory.

## Decision

`trawler-web` declares `kotlinx-coroutines-core:1.11.0` through the
`ktor.coroutines.version` parent property. `trawler-core` continues to declare
and resolve `kotlinx-coroutines-core:1.10.2` through `coroutines.version`.
The web dependency is an adapter-owned runtime alignment, not a Ktor type or
dependency exposed by any core public API. The split is revisited when the
core baseline or Ktor version changes as a coordinated compatibility decision.

## Consequences

### Positive

- Core consumers do not inherit Ktor or its coroutines runtime alignment.
- Ktor timeout, cancellation, and client lifecycle code runs against its
  compatible coroutines API.
- The dependency tree makes the selected versions and ownership explicit.

### Negative or accepted trade-offs

- A process using both artifacts resolves the web adapter's newer coroutines
  runtime for the combined classpath; the compatible newer runtime must remain
  binary-compatible with core's compiled calls.
- Two version properties require coordinated upgrade review and documentation.

## Validation

The core-only dependency tree remains on Kotlin stdlib 2.3.0,
`kotlinx-coroutines-core:1.10.2`, and its existing transitives. The web module
resolves coroutines 1.11.0 alongside Ktor 3.5.1, and the controlled local HTTP
smoke exercises Ktor request timeout/lifecycle execution without linkage
errors.

## Supersession

Supersede this ADR if the project adopts a single compatible coroutines
baseline or changes the Ktor compatibility line. Update the core and web
dependency records together.
