# ADR-0004: Cohesive Maven module strategy

- **Status:** Accepted
- **Date:** 2026-08-23
- **Decision owners:** Project owner
- **Related plan:** `plans/FINAL-PLAN.md`, sections 16–18

## Context

Trawler needs replaceable extensions and strong dependency isolation. Mapping every extension contract or small built-in implementation to a separate Maven artifact would create excessive POMs, publishing overhead, documentation burden, and consumer confusion. Bundling every integration into one JAR would instead force unused database, HTTP, telemetry, browser, and anti-bot dependencies onto all users.

## Decision drivers

- Keep the standard library pleasant to consume.
- Prevent heavy or technology-specific dependencies from leaking into core.
- Ship useful official implementations rather than requiring users to implement common policies.
- Avoid one-artifact-per-interface fragmentation.
- Keep one coordinated version and repository.

## Considered options

### One monolithic artifact

Simple coordinates, but all users inherit every dependency and technology compatibility/security footprint.

### One artifact per extension

Maximum isolation, but excessive fragmentation for small dependency-free implementations such as scheduling, seed discovery, or callback output.

### A few cohesive artifacts

Bundle lightweight dependency-free built-ins in core and group integrations by shared technology stack.

## Decision

Trawler uses:

- `trawler-parent` for build and version management;
- `trawler-core` for engine contracts plus lightweight official implementations requiring only Kotlin, coroutines, and the JDK;
- `trawler-web` for Ktor and cohesive public-web behavior;
- `trawler-postgres` for PostgreSQL-backed state, response, output, and outbox implementations that share one database stack;
- separate artifacts only for specialized/heavy integrations such as FlareSolverr, browser transport, OpenTelemetry, or WARC.

The built-in coroutine scheduler, in-memory state, seed/Flow discovery, callback output, bounded JDK file response/output implementations, and generic policies ship in `trawler-core`. They remain explicitly selected and replaceable and introduce no technology dependency beyond Kotlin, coroutines, and the JDK.

Modules use one coordinated project version. A BOM is added when multiple runtime artifacts are published. Placeholder modules are forbidden.

## Consequences

### Positive

- Common usage requires one dependency.
- Users can schedule ordinary jobs without implementing or installing a tiny scheduler module.
- Heavy dependencies remain optional.
- The number of modules stays understandable.
- Logical extension boundaries remain independent of physical packaging.

### Negative or accepted trade-offs

- `trawler-core` contains some official extension implementations in addition to contracts.
- Package and dependency discipline inside core must prevent built-ins from becoming hidden engine behavior.
- Users of one PostgreSQL capability receive the code for other PostgreSQL capabilities, though none activates automatically.

### Follow-up work

- Keep `docs/MODULES.md` authoritative for module ownership.
- Add `trawler-web` and `trawler-postgres` only when implementation starts.
- Add a BOM at the first multi-artifact publication.

## Validation

The Maven reactor must build `trawler-core` independently with only Kotlin/coroutines runtime dependencies. No separate artifact may exist solely for a small dependency-free policy or scheduler.

## Supersession

Revisit if measured artifact size, dependency conflicts, release cadence, or consumer usage demonstrates that a bundled capability needs independent packaging. Any split requires an updated module matrix and migration guidance.
