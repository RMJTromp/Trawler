# Trawler

Trawler is a generic Kotlin library for composing typed scraping pipelines.

> **Project status:** The current `1.0.0` release includes the `trawler-core`, `trawler-web`, `trawler-postgres`, and `trawler-flaresolverr` artifacts. Core provides typed pipeline composition, bounded execution, scheduling, discovery coordination, response storage and replay, typed outputs, and replaceable policies. The optional web artifact provides Ktor/CIO transport, web policies, and bounded XML discovery. The optional PostgreSQL artifact provides durable operational state, fenced leases, recurring discovery state, response/output history, outbox delivery, and retention. The optional FlareSolverr artifact provides an opt-in FlareSolverr transport and solve-then-reuse challenge clearance for web pipelines. The artifacts are implemented but unreleased; specialized integrations such as browser, telemetry, WARC/object storage, and external delivery/scheduling are not included in the current distribution.

The governing product rule is:

> **Core owns coordination. Extensions own policy. The host owns data.**

This rule is not aspirational wording. It is the boundary against which every implementation change is reviewed.

## Source of truth

Read these documents before changing the project:

1. [AGENTS.md](AGENTS.md) — mandatory repository rules for humans and coding agents.
2. [PLAN.md](PLAN.md) — authoritative architecture and implementation plan.
3. [Architecture](docs/ARCHITECTURE.md) — concise architectural invariants and dependency boundaries.
4. [Modules and artifacts](docs/MODULES.md) — concrete Maven packaging and bundling decisions.
5. [Dependencies](docs/DEPENDENCIES.md) — approved dependencies, ownership, and admission rules.
6. [Development and quality](docs/DEVELOPMENT.md) — workflow, verification, and test policy.
7. [Documentation rules](docs/DOCUMENTATION.md) — required documentation for every feature or decision.

If documentation conflicts, use this precedence:

```text
explicit user instruction
  > AGENTS.md
  > PLAN.md
  > focused documents under docs/
  > README.md
  > historical discussion or examples
```

`PLAN.md` is the current architecture plan. Older design drafts are historical
context and are not implementation authority.

## Product definition

Trawler coordinates:

- engine and pipeline lifecycle;
- structured concurrency and cancellation;
- typed pipeline composition;
- bounded work execution;
- extension startup and shutdown;
- immutable request, response, and context contracts;
- capability and configuration validation;
- generic lifecycle events.

The host application or installed extensions decide:

- what to crawl;
- when to run;
- how to discover requests;
- how requests are transported;
- how responses are interpreted;
- which type `T` is produced;
- how `T` is transformed and validated;
- whether responses or operational state are persisted;
- where `T` is delivered or stored.

## Non-negotiable boundaries

The following rules apply to every implementation:

1. `TrawlerEngine` is non-generic; each registered `Pipeline<T>` is typed.
2. A produced value remains `T` through extraction, transformation, validation, and output.
3. Core never converts user data to `JsonNode`, a generic entity envelope, or a database row.
4. Core never owns user record keys, schemas, entity tables, or serialization choices.
5. Crawl state, response storage, and produced-data output are independent contracts.
6. PostgreSQL, Ktor, Jackson, Jsoup, FlareSolverr, browser libraries, and domain models are adapter/example concerns.
7. Adapter dependencies point toward core. Core must never depend on an adapter.
8. Retry, pacing, concurrency, identity, canonicalization, deduplication, recrawl, scope, safety, robots behavior, scheduling, and ordering are replaceable policies.
9. Built-ins are replaceable conveniences, never hidden mandatory behavior.
10. Optional features validate the capabilities of installed adapters and fail fast when unsupported.
11. Structured concurrency, cancellation propagation, deterministic cleanup, and bounded resources are core invariants.
12. Trawler makes no generic exactly-once delivery claim.

See [AGENTS.md](AGENTS.md) for the enforceable version of these rules.

## Architecture at a glance

```text
                         TrawlerEngine
                  lifecycle + orchestration
                              │
                    typed registrations
                              │
            ┌─────────────────┴─────────────────┐
            │                                   │
       Pipeline<A>                         Pipeline<B>
            │                                   │
       Discovery*                          Discovery*
            ↓                                   ↓
       CrawlRequest                        CrawlRequest
            ↓                                   ↓
   policies + middleware              policies + middleware
            ↓                                   ↓
        Transport                           Transport
            ↓                                   ↓
       CrawlResponse                       CrawlResponse
            ↓                                   ↓
       Extractor<A>                        Extractor<B>
            ↓                                   ↓
 Transform/Validator<A>*             Transform/Validator<B>*
            ↓                                   ↓
       Produced<A>                        Produced<B>
            ↓                                   ↓
        Output<A>*                         Output<B>*
```

Optional shared services—state store, response store, observers, clock, scheduler integration, and concurrency coordination—must not inspect or serialize `A` or `B`.

## Core versus adapters

The current core is usable with:

- Kotlin/JVM;
- Kotlin coroutines;
- in-memory state;
- a caller-provided transport;
- a caller-provided extractor;
- callback output or direct `Flow<Produced<T>>` collection;
- no SQL database;
- no response archive when no response store is selected;
- no scheduler, by calling `runOnce` directly;
- no JSON library;
- no web-specific policy.

Implemented lightweight core built-ins are replaceable. The ordinary builder
installs bounded in-memory state and generic policies; scheduler, response
retention, and output choices remain explicit:

- in-memory state;
- seed and caller-provided `Flow` discovery, with checkpoint-only progress
  emissions for resumable discoveries;
- cursor, watermark, and historical-then-incremental harvest strategies with
  an explicit opt-in to progress past permanently failed items;
- callback/no-output collection;
- direct bounded `Flow<Produced<T>>` output collection;
- no-response and bounded in-memory response stores;
- bounded JDK file response/output stores using caller-supplied codecs;
- response capture/cache policies, replay through the normal typed path
  (paged by store cursor, optionally only the latest version per target), and
  output batching/retry/idempotency decorators;
- common generic policies;
- a coroutine scheduler with fixed-delay, fixed-rate, daily timezone-aware, immediate-first-run, and overlap behavior.

The implemented optional `trawler-web` adapter provides:

- Ktor HTTP transport and response adaptation;
- bounded redirects, public-web safety, robots, cookies, origin pacing, and HTTP classification;
- bounded RSS, Atom, sitemap, gzip, and XML discovery through the normal engine transport path.

The implemented optional `trawler-postgres` adapter provides:

- durable engine and pipeline run summaries with bounded stale recovery;
- stable resources, visit jobs, active-work deduplication, cooldown state,
  PostgreSQL row-lock claims, lease fencing, and meaningful attempt history;
- adapter-owned Flyway migrations and versioned request codecs;
- durable checkpoint, harvest, health, and eligibility state;
- optional append-only response history, typed output history, fenced outbox
  delivery, and independent retention; these are not part of operational state
  and do not define a universal extracted-record table.

The implemented optional `trawler-flaresolverr` adapter provides:

- a FlareSolverr v3 transport for `GET` and form-urlencoded `POST`, with an
  optional lifecycle-owned session pool and initial/final URL destination checks;
- adapter-local challenge detection (Cloudflare detector included), per-origin
  single-flight resolution through FlareSolverr, and a bounded in-memory
  clearance store whose cookies and User-Agent are reused by `KtorTransport`;
- failure classification and a retry decorator that keep challenge retries
  visible to engine attempt accounting.

Not included in the current distribution:

- object-store or WARC response storage;
- queue-backed outputs and external delivery integrations;
- specialized discovery beyond the bounded RSS/Atom/sitemap and pagination hooks;
- browser transport or browser-based challenge resolution beyond the FlareSolverr adapter;
- SLF4J, Micrometer, or OpenTelemetry observers;
- specialized external scheduling integrations when the built-in coroutine scheduler is insufficient.

The presence of an adapter in the repository never makes it mandatory for core users.

## Maven packaging strategy

Trawler uses logical extension boundaries everywhere, but only a few cohesive physical artifacts:

```text
trawler-parent                 build/version management only
├── trawler-bom                version alignment for Trawler artifacts
├── trawler-core               engine + lightweight dependency-free built-ins
├── trawler-web                Ktor + cohesive public-web behavior
├── trawler-postgres            PostgreSQL durable operational state
├── trawler-flaresolverr       FlareSolverr transport + challenge clearance (uses trawler-web)
└── trawler-coverage            non-published JaCoCo reactor aggregate

not in the current reactor:
└── specialized artifacts      browser, OpenTelemetry, WARC
```

A small extension does not receive its own Maven artifact merely because it is replaceable. The built-in scheduler, in-memory state, seed discovery, callback output, response stores, and generic policies are bundled in `trawler-core`.

A separate artifact is used when a capability introduces an external technology stack, heavy operational/security footprint, or meaningful independent compatibility boundary. See [docs/MODULES.md](docs/MODULES.md) for the complete decision matrix.

## Scope

### In scope

- A reusable library for constructing different scraping systems.
- Multiple unrelated typed pipelines in one engine.
- One-shot, finite, and daemon-style execution primitives.
- Replaceable policies and focused extension contracts.
- Optional durable crawling with resources, jobs, attempts, cooldown state, and fenced leases.
- Optional response caching, history, and replay.
- Optional public-web safety defaults.
- Honest, explicit delivery and recovery guarantees.

### Out of scope for core

- Website-specific selectors or content models.
- A mandatory database or serialization format.
- A mandatory scheduler or execution time.
- A universal scraped-record schema.
- Mandatory response retention.
- Mandatory robots, anti-bot, proxy, browser, or public-network behavior.
- A built-in administration UI or hosted control plane.
- Claims of exactly-once external delivery.
- Hidden network calls from discovery or extraction plugins.

## Current repository state

The repository contains the implemented core contracts/runtime, optional Ktor
web adapter, optional durable PostgreSQL state/response/output adapter, and
optional FlareSolverr adapter. Other specialized integrations are
intentionally outside the current reactor.

```text
Trawler/
├── AGENTS.md                  mandatory contributor and agent rules
├── CLAUDE.md                  symlink to AGENTS.md
├── README.md                  project overview and status
├── pom.xml                    Maven parent and reactor
├── trawler-core/
│   ├── pom.xml                core artifact
│   ├── src/main/kotlin/       typed contracts, runtime, stores, outputs, and core built-ins
│   └── README.md              core-specific scope and starting point
├── trawler-web/
│   ├── pom.xml                optional web adapter artifact
│   ├── src/main/kotlin/       Ktor transport, web policies, and XML discovery
│   └── README.md              web-specific scope and build entry point
├── trawler-postgres/
│   ├── pom.xml                optional durable state adapter artifact
│   ├── src/main/kotlin/       PostgreSQL state, response/output history, leases, attempts, codecs, and recurring state
│   ├── src/main/resources/    adapter-owned Flyway migrations
│   └── README.md              PostgreSQL-specific scope and build entry point
├── trawler-flaresolverr/
│   ├── pom.xml                optional FlareSolverr adapter artifact
│   ├── src/main/kotlin/       FlareSolverr transport, challenge detection/resolution, clearance reuse
│   └── README.md              FlareSolverr-specific scope and build entry point
├── trawler-bom/
│   └── pom.xml                published version-alignment BOM
├── trawler-coverage/
│   └── pom.xml                non-published JaCoCo aggregate module
├── docs/
│   ├── ARCHITECTURE.md        concise architecture contract
│   ├── MODULES.md             Maven artifact ownership and split rules
│   ├── DEPENDENCIES.md        dependency inventory and admission policy
│   ├── DEVELOPMENT.md         workflow and quality gates
│   ├── DOCUMENTATION.md       documentation requirements
│   ├── IN-MEMORY-ENGINE.md    database-free runtime feature record
│   ├── WEB-ADAPTER.md         web adapter feature record
│   ├── POSTGRES-STATE.md      durable state feature record
│   ├── RECURRING-DISCOVERY.md resumable/recurring discovery record
│   ├── RESPONSE-OUTPUT.md     response/output/replay/retention record
│   ├── FLARESOLVERR.md        FlareSolverr adapter feature record
│   └── decisions/             architecture decision records
└── PLAN.md                    authoritative architecture plan
```

Do not add empty production packages or placeholder classes merely to resemble
a future layout. Structure should appear when a capability has a real
implementation.

## Build baseline

Requirements:

- JDK 17 or newer capable of targeting JVM 17;
- Maven 3.9 or newer;
- UTF-8 source and reporting output.

Compile the current project:

```bash
mvn clean compile
```

Package and run all configured Maven lifecycle checks:

```bash
mvn clean verify
```

The reactor contains the core JAR, optional `trawler-web` JAR, optional
`trawler-postgres` JAR, optional `trawler-flaresolverr` JAR, the `trawler-bom`
version-alignment POM, and the
non-published `trawler-coverage` reporting module. Core contains typed contracts, immutable
configuration, bounded runtime/scheduling, generic resumable-discovery
coordination, optional response stores/replay, and typed file output; the web
artifact owns Ktor/HTTP and bounded XML discovery; the PostgreSQL artifact owns
JDBC, Hikari, Flyway, durable operational state, response/output history,
fenced outbox delivery, leases, attempts, request codecs, checkpoints, harvest
state, eligibility state, and independent retention; the FlareSolverr
artifact owns the FlareSolverr client, transport, and challenge-clearance
reuse. Do not add placeholder
classes or resources for capabilities that are not implemented.

There is currently no runnable main class; the engine and adapters are library
APIs. See the [typed core contracts feature record](docs/TYPED-CORE-CONTRACTS.md),
the [in-memory engine feature record](docs/IN-MEMORY-ENGINE.md), the [web
adapter feature record](docs/WEB-ADAPTER.md), the [PostgreSQL state feature
record](docs/POSTGRES-STATE.md), the [recurring discovery feature record](docs/RECURRING-DISCOVERY.md),
the [response/output feature record](docs/RESPONSE-OUTPUT.md), and the
[FlareSolverr adapter feature record](docs/FLARESOLVERR.md) for the current
module boundaries and implemented capabilities.

## Using Trawler with Maven

The current published version is `1.0.0`. Add the RMJTromp Maven
repository to your project:

```xml
<repositories>
    <repository>
        <id>rmjtromp</id>
        <url>https://repo.rmjtromp.com/releases</url>
    </repository>
</repositories>
```

Optionally import the BOM so every Trawler artifact uses one version:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.rmjtromp.trawler</groupId>
            <artifactId>trawler-bom</artifactId>
            <version>1.0.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

The BOM manages only Trawler artifacts; it does not pin Ktor, PostgreSQL, or
any other third-party version. With it imported, the `<version>` elements
below may be omitted.

Add the core library:

```xml
<dependency>
    <groupId>com.rmjtromp.trawler</groupId>
    <artifactId>trawler-core</artifactId>
    <version>1.0.0</version>
</dependency>
```

Add these optional adapters only when needed:

```xml
<dependency>
    <groupId>com.rmjtromp.trawler</groupId>
    <artifactId>trawler-web</artifactId>
    <version>1.0.0</version>
</dependency>

<dependency>
    <groupId>com.rmjtromp.trawler</groupId>
    <artifactId>trawler-postgres</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Requires a separately operated FlareSolverr v3 service; brings in trawler-web. -->
<dependency>
    <groupId>com.rmjtromp.trawler</groupId>
    <artifactId>trawler-flaresolverr</artifactId>
    <version>1.0.0</version>
</dependency>
```

For a released version, use the same coordinates with the release version and
`https://repo.rmjtromp.com/releases` instead of the snapshots repository.

## Dependencies

The `trawler-core` runtime dependency set is deliberately minimal:

| Dependency | Scope | Approved use |
|---|---|---|
| Kotlin standard library | Compile/runtime | Kotlin language runtime. |
| Kotlin coroutines core | Compile/runtime | Structured concurrency, suspending lifecycle, `Flow`, channels, and cancellation. |

Core has no logging, JSON, SQL, HTTP, HTML parsing, or anti-bot runtime
dependencies. Its test-only JUnit/Kotlin/coroutines support is not present on
the published runtime classpath. The optional `trawler-web` artifact adds only
the Ktor CIO transport and its adapter-scoped transitive graph; MockEngine is
owned by that module's tests. The optional
`trawler-postgres` artifact owns the PostgreSQL driver, HikariCP, Flyway, and
their adapter-scoped transitive graph; Testcontainers is owned by that
module's tests and none of those dependencies enter core. The optional
`trawler-flaresolverr` artifact depends on `trawler-web`, reuses its Ktor CIO
client library for the FlareSolverr endpoint, and adds only
`kotlinx-serialization-json-jvm` for the FlareSolverr JSON protocol.
See
[docs/DEPENDENCIES.md](docs/DEPENDENCIES.md) for the complete direct and
resolved inventory and [docs/TESTING.md](docs/TESTING.md) for test commands and
ownership.

Every future dependency requires:

- an implemented, in-scope need;
- the narrowest owning module or adapter;
- an entry in [docs/DEPENDENCIES.md](docs/DEPENDENCIES.md);
- license and maintenance review;
- confirmation that an existing dependency or standard library cannot reasonably provide the capability;
- verification that it does not leak adapter types into core public APIs.

Dependencies may not be added “for later,” for convenience, or because a draft plan mentioned them.

## Quality policy

High quality is a release property, not a claim. Every change must preserve:

- architectural boundaries;
- type safety;
- deterministic lifecycle and cleanup;
- bounded memory, queues, concurrency, and I/O;
- explicit failure and delivery semantics;
- compatibility or an intentional documented break;
- accurate documentation;
- reproducible verification.

No change is complete merely because it compiles.

### Tests

Every change that adds or alters behavior must add or update automated tests
in the same change; bug fixes include a test that fails without the fix. Tests
must:

- test externally observable contracts, not private implementation trivia;
- cover failure, cancellation, boundary, and recovery behavior—not only happy paths;
- use deterministic clocks/randomness and controlled transports;
- do not access live third-party sites in the default suite;
- do not weaken assertions or delete coverage to make a change pass;
- document intentionally untested risk.

Behavior that genuinely cannot be tested automatically is recorded in the
untested-risk register in [docs/TEST-MATRIX.md](docs/TEST-MATRIX.md).

Full rules are in [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).

## Documentation policy

Documentation is part of the feature. Every implemented capability must update the relevant documents in the same change.

At minimum, a feature must document:

- purpose and non-goals;
- public contracts and ownership;
- configuration and defaults;
- lifecycle and state transitions;
- capability requirements;
- concurrency and cancellation behavior;
- failure and retry semantics;
- persistence, delivery, and duplicate guarantees;
- security, privacy, and retention implications;
- operational verification;
- known limitations.

Architectural decisions that affect multiple components require an ADR under `docs/decisions/`. See [docs/DOCUMENTATION.md](docs/DOCUMENTATION.md).

## Contribution workflow

Before changing anything:

1. Read `AGENTS.md` and the relevant architecture documents.
2. Inspect the current repository and preserve unrelated work.
3. Identify the owning layer: core, policy, adapter, output, example, or documentation.
4. State required capabilities and dependency impact.
5. Confirm that the requested work belongs to the current architecture and
   repository scope.

Before handing off:

1. Verify only the changed scope.
2. Run the applicable Maven lifecycle checks.
3. Check formatting, warnings, and documentation accuracy.
4. Confirm core has not gained adapter or domain dependencies.
5. Report exactly what changed, what was verified, and what remains unverified.

Agents must work on the currently checked-out branch. They must not create branches, commits, tags, or pull requests unless explicitly requested.

## Security and legal posture

Trawler provides optional safe public-web policies, but the host remains
responsible for authorization, site terms, robots policy selection, privacy,
credential handling, and data retention.

Secrets must never be committed, logged, archived by default, placed in examples, or included in diagnostic events. Security behavior that appears safe but can be bypassed by redirects, DNS changes, plugin-owned clients, or middleware ordering is not acceptable.

## License

No project license has been selected yet. Until a license file is added, do not describe Trawler as open source or assume redistribution rights.
