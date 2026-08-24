# Trawler

Trawler is a generic Kotlin library for composing typed scraping pipelines.

> **Project status: Phase 6 implemented.** `trawler-core` provides the typed composition boundary, bounded database-free engine, opt-in scheduling, resumable checkpoint contracts, recurring harvest policies, backlog admission, optional pipeline health state, bounded in-memory/file response stores, replay, typed file output, and output decorators. The optional `trawler-web` artifact provides the Ktor/CIO transport, replaceable web policies, and bounded RSS/Atom/sitemap discovery. The optional `trawler-postgres` artifact provides durable operational state, fenced leases, versioned request codecs, durable checkpoints, harvest state, health, eligibility overrides, response history, typed output history, fenced outbox delivery, and independent retention. Later specialized adapters remain planned. Artifacts are unreleased.

The governing product rule is:

> **Core owns coordination. Extensions own policy. The host owns data.**

This rule is not aspirational wording. It is the boundary against which every implementation change is reviewed.

## Source of truth

Read these documents before changing the project:

1. [AGENTS.md](AGENTS.md) — mandatory repository rules for humans and coding agents.
2. [FINAL-PLAN.md](plans/FINAL-PLAN.md) — authoritative architecture and phased implementation plan.
3. [Architecture](docs/ARCHITECTURE.md) — concise architectural invariants and dependency boundaries.
4. [Modules and artifacts](docs/MODULES.md) — concrete Maven packaging and bundling decisions.
5. [Dependencies](docs/DEPENDENCIES.md) — approved dependencies, ownership, and admission rules.
6. [Development and quality](docs/DEVELOPMENT.md) — workflow, verification, and test policy.
7. [Documentation rules](docs/DOCUMENTATION.md) — required documentation for every feature or decision.

If documentation conflicts, use this precedence:

```text
explicit user instruction
  > AGENTS.md
  > plans/FINAL-PLAN.md
  > focused documents under docs/
  > README.md
  > historical discussion or examples
```

The old design drafts were consolidated into `plans/FINAL-PLAN.md` and are not implementation authority.

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
- seed and caller-provided `Flow` discovery;
- callback/no-output collection;
- direct bounded `Flow<Produced<T>>` output collection;
- no-response and bounded in-memory response stores;
- bounded JDK file response/output stores using caller-supplied codecs;
- response capture/cache policies, replay through the normal typed path, and
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

Future adapters may provide:

- object-store or WARC response storage;
- queue-backed outputs and external delivery integrations;
- specialized discovery beyond the bounded RSS/Atom/sitemap and pagination hooks;
- FlareSolverr or browser challenge resolution;
- SLF4J, Micrometer, or OpenTelemetry observers;
- specialized external scheduling integrations when the built-in coroutine scheduler is insufficient.

The presence of an adapter in the repository never makes it mandatory for core users.

## Maven packaging strategy

Trawler uses logical extension boundaries everywhere, but only a few cohesive physical artifacts:

```text
trawler-parent                 build/version management only
├── trawler-core               engine + lightweight dependency-free built-ins
├── trawler-web                Ktor + cohesive public-web behavior
├── trawler-postgres            PostgreSQL durable operational state
└── trawler-coverage            non-published JaCoCo reactor aggregate

planned when implemented:
└── specialized artifacts      browser, FlareSolverr, OpenTelemetry, WARC
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

The repository contains the Phase 6 core contracts/runtime, optional Ktor web
adapter, and optional durable PostgreSQL state/response/output adapter. Later
specialized adapter phases remain unimplemented.

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
├── trawler-coverage/
│   └── pom.xml                non-published JaCoCo aggregate module
├── docs/
│   ├── ARCHITECTURE.md        concise architecture contract
│   ├── MODULES.md             Maven artifact ownership and split rules
│   ├── DEPENDENCIES.md        dependency inventory and admission policy
│   ├── DEVELOPMENT.md         workflow and quality gates
│   ├── DOCUMENTATION.md       documentation requirements
│   ├── IN-MEMORY-ENGINE.md    Phase 2 runtime feature record
│   ├── WEB-ADAPTER.md         Phase 3 web adapter feature record
│   ├── POSTGRES-STATE.md      Phase 4 durable state feature record
│   ├── RECURRING-DISCOVERY.md Phase 5 resumable/recurring discovery record
│   ├── RESPONSE-OUTPUT.md     Phase 6 response/output/replay/retention record
│   └── decisions/             architecture decision records
└── plans/
    └── FINAL-PLAN.md          authoritative implementation plan
```

Do not add empty production packages or placeholder classes merely to resemble the future layout. Structure should appear when a phase genuinely implements it.

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

The Phase 6 reactor contains the core JAR, optional `trawler-web` JAR, optional
`trawler-postgres` JAR, and the non-published `trawler-coverage` reporting
module. Core contains typed contracts, immutable
configuration, bounded runtime/scheduling, generic resumable-discovery
coordination, optional response stores/replay, and typed file output; the web
artifact owns Ktor/HTTP and bounded XML discovery; the PostgreSQL artifact owns
JDBC, Hikari, Flyway, durable operational state, response/output history,
fenced outbox delivery, leases, attempts, request codecs, checkpoints, harvest
state, eligibility state, and independent retention. Do not add placeholder
classes or resources to represent later phases.

There is currently no runnable main class; the engine and adapters are library
APIs. See the [typed core contracts feature record](docs/TYPED-CORE-CONTRACTS.md),
the [in-memory engine feature record](docs/IN-MEMORY-ENGINE.md), the [web
adapter feature record](docs/WEB-ADAPTER.md), the [PostgreSQL state feature
record](docs/POSTGRES-STATE.md), the [recurring discovery feature record](docs/RECURRING-DISCOVERY.md),
and the [response/output feature record](docs/RESPONSE-OUTPUT.md) for the
implemented Phase 1 through Phase 6 boundaries.

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
module's tests and none of those dependencies enter core.
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

Repository policy requires explicit owner authorization before an agent adds or refactors automated tests. Agents must not infer permission from an implementation request.

When tests are authorized:

- test externally observable contracts, not private implementation trivia;
- cover failure, cancellation, boundary, and recovery behavior—not only happy paths;
- use deterministic clocks/randomness and controlled transports;
- do not access live third-party sites in the default suite;
- do not weaken assertions or delete coverage to make a change pass;
- document intentionally untested risk.

When tests are not authorized, compilation, static inspection, and focused manual verification remain mandatory, and the handoff must state that automated tests were not added or run.

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
5. Confirm that the requested work belongs to the current delivery phase.

Before handing off:

1. Verify only the changed scope.
2. Run the applicable Maven lifecycle checks.
3. Check formatting, warnings, and documentation accuracy.
4. Confirm core has not gained adapter or domain dependencies.
5. Report exactly what changed, what was verified, and what remains unverified.

Agents must work on the currently checked-out branch. They must not create branches, commits, tags, or pull requests unless explicitly requested.

## Security and legal posture

Trawler will provide optional safe public-web policies, but the host remains responsible for authorization, site terms, robots policy selection, privacy, credential handling, and data retention.

Secrets must never be committed, logged, archived by default, placed in examples, or included in diagnostic events. Security behavior that appears safe but can be bypassed by redirects, DNS changes, plugin-owned clients, or middleware ordering is not acceptable.

## License

No project license has been selected yet. Until a license file is added, do not describe Trawler as open source or assume redistribution rights.
