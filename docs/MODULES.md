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

> **Current repository status:** The reactor contains the build-only `trawler-parent`, `trawler-core`, and the implemented optional `trawler-web` and `trawler-postgres` adapters. Core typed contracts, composition, configuration validation, the bounded database-free runtime, lightweight built-ins, Ktor/web behavior, and durable PostgreSQL operational state are implemented. Response archival, produced-data output, checkpoints, and specialized modules remain planned.

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
- bounded JDK file response/output implementations using caller-supplied codecs (planned; not part of Phase 2).

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
- bounded transport/robots foundations for later RSS/Atom and sitemap discovery;
- web cache/conditional-request policies that do not require persistence.

These belong together because they share HTTP semantics and the Ktor/web dependency stack. They remain separate extension contracts internally. RSS/Atom and sitemap parsers/discovery are still Phase 5 work. A user may use `trawler-core` with another transport and omit this artifact entirely.

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
`trawler-postgres` does not force response archival, user-data output,
checkpoints, or outbox delivery. Response storage, produced-data output,
checkpoints, harvest state, and outbox behavior remain later implementations
inside this cohesive artifact or another explicitly justified adapter.

A generic multi-database JDBC abstraction is not part of this initial artifact. Add `trawler-jdbc` later only if real non-PostgreSQL implementations demonstrate a shared contract and implementation worth extracting.

### Specialized artifacts

Create these only when their implementations are requested:

| Artifact | Why separate |
|---|---|
| `trawler-antibot-flaresolverr` | Specialized external service, session/security behavior, and limited audience. Depends on core and normally web. |
| `trawler-transport-browser` | Heavy browser/native/runtime footprint and distinct lifecycle. |
| `trawler-observability-otel` | Substantial telemetry dependency graph and ecosystem compatibility. |
| `trawler-warc` | Specialized archive format and conformance requirements. |

Do not create placeholder POMs for these modules before implementation begins.

### Build and consumer support artifacts

Create `trawler-bom` when at least two runtime artifacts are ready to be published. It aligns all Trawler artifact versions and contains no code.

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
| Bounded file response/output | `trawler-core` | Planned JDK-only, codec-driven standard capability; no separate tiny artifact. WARC/object storage remains separate. |
| Ktor + web policies and bounded discovery foundations | `trawler-web` | Cohesive HTTP semantics and shared external stack; RSS/sitemap implementations remain Phase 5. |
| PostgreSQL state, run history, leases, attempts, and request codecs | `trawler-postgres` | Cohesive database stack; implemented Phase 4 capabilities remain separately activated. |

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
  ├── trawler-observability-otel       OpenTelemetry stack
  └── specialized adapters             their technology dependencies

trawler-bom                            version alignment only, added later
```

No sibling adapter may become a transitive requirement of another unless the relationship is inherent and documented. For example, a FlareSolverr integration may depend on `trawler-web`; PostgreSQL must never become a dependency of web transport.

## Repository creation policy

Add a module in the same change that begins its real implementation. The Phase 3 change created `trawler-web` and the Phase 4 change created `trawler-postgres` with:

- the module POM;
- dependency inventory update;
- module documentation;
- its public capability and ownership description;
- lifecycle/verification setup;
- no empty placeholder packages.

## Publication policy

All Trawler artifacts use group ID `com.rmjtromp.trawler` and one coordinated project version initially. Modules are separately selectable artifacts, not independently versioned products.

The first publication containing multiple artifacts must provide compatibility guidance or a BOM. Do not publish internal example or build-only modules as runtime libraries.
