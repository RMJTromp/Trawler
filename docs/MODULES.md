# Maven Modules and Published Artifacts

> **Status:** Normative packaging plan

Trawler uses a small number of cohesive Maven modules. Extension contracts remain focused and replaceable, but a small extension does not automatically receive its own artifact.

## Packaging principle

> **Logical modularity is mandatory. Physical modularity is selective.**

Three ideas must remain distinct:

- A capability can be replaceable while shipping in `trawler-core`.
- A capability can be opt-in at runtime without being a separate artifact.
- A focused interface or package does not require a dedicated POM.

Physical modules exist to isolate dependencies, technology stacks, heavy operational footprints, or meaningful independent replacement—not to mirror every interface.

> **Current repository status:** The reactor contains the build-only `trawler-parent` and `trawler-coverage` projects, the version-alignment `trawler-bom`, plus `trawler-core` and the implemented optional `trawler-web`, `trawler-postgres`, and `trawler-flaresolverr` adapters. Core typed contracts, composition, configuration validation, the bounded database-free runtime, lightweight built-ins, resumable/recurring discovery coordination, Ktor/web behavior, bounded XML discovery, durable PostgreSQL operational state, optional response storage/replay, typed file output, PostgreSQL output, fenced outbox delivery, and the opt-in FlareSolverr transport and challenge-clearance reuse are implemented. Other specialized modules remain planned.

## Module admission test

A separate published artifact is justified when at least one of these is materially true:

1. It introduces external dependencies that core users should not inherit.
2. It integrates one cohesive technology stack such as Ktor or PostgreSQL.
3. It has a substantial binary, security, native, or operational footprint.
4. Most users can reasonably omit it.
5. It needs independent compatibility management against an external ecosystem.
6. Keeping it separate enforces an important dependency direction that packages alone cannot protect.

Small size alone is not a reason to split. Replaceability alone is not a reason to split.

## Approved module map

### `trawler-parent`

**Packaging:** Build-only Maven parent; not a user runtime dependency.

Owns:

- reactor modules;
- project version;
- Java/Kotlin baseline;
- dependency management;
- plugin management;
- shared build settings.

It must contain no runtime code or runtime dependencies.

### `trawler-bom`

**Packaging:** Published Maven `pom` module containing only `dependencyManagement`.

Aligns the versions of the published Trawler runtime artifacts. It deliberately
does not inherit `trawler-parent`, because an imported BOM contributes its whole
effective dependency management; inheriting the parent would leak Ktor,
PostgreSQL, JUnit, and Testcontainers versions into consumers. Its version must
be kept equal to the reactor version, and every new runtime artifact must be
added to it in the same change that creates the artifact.

### `trawler-coverage`

**Packaging:** Non-published build-only Maven `pom` module, last in the reactor.
Deployment is disabled with `maven.deploy.skip`.

Owns only the JaCoCo `report-aggregate` execution. It has provided-scoped
references to each implementation module so the aggregate report is generated
after all module tests and reports have run. It is not a runtime artifact, does
not add an adapter dependency, and must not contain production or feature test
sources.

### `trawler-core`

**Packaging:** Published JAR and primary user dependency.

Owns generic engine contracts and lightweight official implementations that require no technology dependency beyond Kotlin/coroutines/JDK.

Core contracts:

- engine and pipeline lifecycle;
- typed pipeline registration;
- request/response/context models;
- extraction, transformation, validation, and output contracts;
- state and response-store capability contracts;
- policy and middleware contracts;
- generic events and observers;
- extension lifecycle and configuration validation.

Bundled official implementations:

- in-memory crawl state (implemented in Phase 2);
- no-response and in-memory response stores (implemented in Phase 2);
- seed and caller-supplied `Flow` discovery (implemented in Phase 2);
- callback, no-output, and bounded `Flow<Produced<T>>` collection (implemented in Phase 2);
- generic retry, identity, active-deduplication, recrawl, ordering, concurrency, pacing, and budget policies (implemented in Phase 2);
- coroutine scheduler supporting finite/manual triggers, fixed delay, fixed rate, daily local time with timezone, immediate-first-run option, and explicit overlap behavior (implemented in Phase 2);
- bounded JDK file response/output implementations using caller-supplied codecs (implemented in Phase 6).

Bundling these does not add an external technology requirement. The ordinary
builder selects the bounded in-memory state and generic policy preset; the
scheduler, response retention, and output implementations remain explicit
choices, and every built-in may be replaced.

Forbidden in `trawler-core`:

- HTTP-client implementations;
- SQL/database code;
- universal JSON serialization;
- HTML/XML parser dependencies;
- logging/telemetry vendor dependencies;
- browser or anti-bot implementations;
- application/domain models.

### `trawler-web`

**Creation:** Phase 3; created with the first real web implementation.

**Packaging:** Published optional JAR depending on `trawler-core`.

Bundles the common public-web technology surface:

- Ktor transport;
- HTTP request/response adapters and middleware;
- redirect processing;
- cookie/session integration appropriate to HTTP;
- robots support;
- public-network/SSRF safety preset;
- HTTP failure classification and retry defaults;
- origin concurrency, pacing, and `Retry-After` behavior;
- bounded RSS/Atom and sitemap discovery through engine-controlled transport;
- web cache/conditional-request policies that do not require persistence.

These belong together because they share HTTP semantics and the Ktor/web dependency stack. They remain separate extension contracts internally. A user may use `trawler-core` with another transport and omit this artifact entirely.

Do not add Jsoup, Jackson, or another parser/codec automatically. Add one only when implemented behavior proves it is needed and record it in `docs/DEPENDENCIES.md`.

### `trawler-postgres`

**Creation:** Phase 4; created with the real durable PostgreSQL state implementation.

**Packaging:** Published optional JAR depending on `trawler-core`.

Owns the implemented PostgreSQL operational-state surface that shares one
JDBC/pool/migration stack:

- durable engine and pipeline run summaries with bounded stale recovery;
- stable resources, visit jobs, active-work deduplication, cooldown state,
  row-lock claims, lease fencing, and meaningful attempts;
- adapter-owned Flyway migrations and versioned request codecs;
- bounded Hikari pool and adapter-owned blocking dispatcher;
- capability declarations for durability, transactions, multi-process use,
  fencing, history, cancellation, ordering, and run recovery.

These capabilities are independently activated. Depending on
`trawler-postgres` does not force response archival, user-data output, or
outbox delivery. Phase 5 adds optional checkpoints, harvest state, health, and
eligibility state; Phase 6 adds separate PostgreSQL response history, typed
output history, optional outbox delivery, local response lineage, and
independent retention without adding a universal produced-data table.

A generic multi-database JDBC abstraction is not part of this initial artifact. Add `trawler-jdbc` later only if real non-PostgreSQL implementations demonstrate a shared contract and implementation worth extracting.

### `trawler-flaresolverr`

**Creation:** Phase 7; created with the first real FlareSolverr implementation.

**Packaging:** Published optional JAR depending on `trawler-core` and
`trawler-web`.

Owns the opt-in integration with an operator-run FlareSolverr v3 service:

- `FlareSolverrTransport`, a core `Transport` that executes `GET` and
  form-urlencoded `POST` through FlareSolverr's `/v1` API, with an optional
  lifecycle-owned session pool;
- adapter-local challenge contracts (`ChallengeDetector`, `ChallengeResolver`,
  `ChallengeKind`) and the bundled `CloudflareChallengeDetector`;
- `FlareSolverrChallengeResolver`, the bounded process-local
  `ClearanceStore`, and `ChallengeClearance`, which coordinates per-origin
  single-flight resolution and reuses clearance through `trawler-web`'s
  `CookieSession` and request middleware;
- `FlareSolverrFailureClassifier` and `ChallengeRetryPolicy`, keeping retry
  visible to engine attempt accounting.

It is separate because it integrates a specialized external service with its
own session, credential, and network-safety footprint that most users omit,
and it adds the module-owned `kotlinx-serialization-json-jvm` dependency. The
`trawler-web` dependency is inherent: clearance is reused by `KtorTransport`
and target admission uses `WebDestinationPolicy`. Challenge contracts stay in
this artifact rather than core; see
[ADR-0009](decisions/0009-flaresolverr-adapter-boundary.md) and the
[feature record](FLARESOLVERR.md).

### Specialized artifacts

Create these only when their implementations are requested:

| Artifact | Why separate |
|---|---|
| `trawler-transport-browser` | Heavy browser/native/runtime footprint and distinct lifecycle. |
| `trawler-observability-otel` | Substantial telemetry dependency graph and ecosystem compatibility. |
| `trawler-warc` | Specialized archive format and conformance requirements. |

Do not create placeholder POMs for these modules before implementation begins.

### Build and consumer support artifacts

`trawler-bom` exists because multiple runtime artifacts are published. It aligns all Trawler runtime artifact versions and contains no code or third-party versions.

Do not create a “depends on everything” artifact initially. Such an artifact would pull PostgreSQL, Ktor, telemetry, browser, or anti-bot dependencies into users that do not need them. A convenience starter may be added later only for a proven common deployment profile and must list exactly what it brings in.

Examples remain in the repository but are not published as production runtime dependencies.

## What is deliberately bundled

| Capability | Physical location | Reason |
|---|---|---|
| Coroutine scheduler | `trawler-core` | Implemented; small, uses the existing coroutine/clock lifecycle, and introduces no new dependency. |
| In-memory state | `trawler-core` | Implemented bounded default for database-free use; no external dependency. |
| Seed/Flow discovery | `trawler-core` | Implemented, generic, and dependency-free. |
| Callback/no-output | `trawler-core` | Implemented fundamental host handoff with no dependency. |
| In-memory response store | `trawler-core` | Implemented optional bounded retention/replay without a storage dependency. |
| Generic policies | `trawler-core` | Implemented reusable defaults with no technology coupling. |
| Bounded file response/output | `trawler-core` | Implemented JDK-only, codec-driven standard capability; no separate tiny artifact. WARC/object storage remains separate. |
| Ktor + web policies and bounded RSS/Atom/sitemap discovery | `trawler-web` | Cohesive HTTP semantics and shared external stack; XML behavior uses JDK APIs without a parser dependency. |
| PostgreSQL state, run history, leases, attempts, request codecs, checkpoints, recurring state, response history, typed output, outbox, and retention | `trawler-postgres` | Cohesive database stack; Phase 4/5/6 capabilities remain separately activated. |
| FlareSolverr transport, challenge detection/resolution contracts, clearance store, and challenge retry/classification | `trawler-flaresolverr` | One specialized external service and its JSON protocol; transport and clearance reuse remain separately selected. |

## What must not become a module

Do not create separate artifacts solely for:

- one policy interface;
- one small built-in policy;
- one state model;
- one event type;
- seed discovery;
- callback output;
- the coroutine scheduler;
- package organization;
- internal implementation layers.

These remain focused packages/types inside their owning artifact.

## Dependency graph

```text
trawler-parent                         build management only

trawler-core                           Kotlin + coroutines
  ↑
  ├── trawler-web                      Ktor and justified web dependencies
  ├── trawler-postgres                 JDBC/PostgreSQL/pool/migrations
  ├── trawler-flaresolverr             FlareSolverr client (Ktor CIO + kotlinx-serialization JSON); also depends on trawler-web
  ├── trawler-coverage                 provided-scoped report aggregation only
  ├── trawler-observability-otel       OpenTelemetry stack
  └── specialized adapters             their technology dependencies

trawler-bom                            version alignment only; no parent, no code
```

No sibling adapter may become a transitive requirement of another unless the relationship is inherent and documented. For example, `trawler-flaresolverr` depends on `trawler-web`; PostgreSQL must never become a dependency of web transport.

## Repository creation policy

Add a runtime module in the same change that begins its real implementation.
The Phase 3 change created `trawler-web`, the Phase 4 change created
`trawler-postgres`, and the Phase 7 FlareSolverr change created
`trawler-flaresolverr` with:

- the module POM;
- dependency inventory update;
- module documentation;
- its public capability and ownership description;
- lifecycle/verification setup;
- no empty placeholder packages.

The non-published `trawler-coverage` module is the build exception: it contains
only its aggregate-report POM and provided-scoped internal module references, so
the report executes after all implementation modules in the reactor.

## Publication policy

All Trawler artifacts use group ID `com.rmjtromp.trawler` and one coordinated project version initially. Modules are separately selectable artifacts, not independently versioned products.

The first publication containing multiple artifacts must provide compatibility guidance or a BOM. Do not publish internal example or build-only modules as runtime libraries.
