# Trawler

Trawler is a generic Kotlin library for composing typed scraping pipelines.

> **Project status: Phase 1 implementation-ready.** The parent/core Maven boundary and project rules are established; no production crawler implementation exists yet. APIs shown in planning documents are design targets, not released code.

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

The initial core must be usable with:

- Kotlin/JVM;
- Kotlin coroutines;
- in-memory state;
- a caller-provided transport;
- a caller-provided extractor;
- callback output or direct `Flow<Produced<T>>` collection;
- no SQL database;
- no response archive;
- no scheduler;
- no JSON library;
- no web-specific policy.

Later core phases are planned to ship lightweight official implementations that require no additional technology dependency. They will be opt-in and replaceable when implemented:

- in-memory state;
- seed and caller-provided `Flow` discovery;
- callback/no-output collection;
- no-response, in-memory, and bounded JDK file response/output implementations using caller codecs;
- common generic policies;
- a coroutine scheduler with fixed-delay, fixed-rate, daily timezone-aware, immediate-first-run, and overlap behavior.

Optional adapters may later provide:

- Ktor HTTP transport;
- public-web safety and robots policies;
- PostgreSQL durable state and fenced leases;
- PostgreSQL, object-store, or WARC response storage;
- PostgreSQL, queue, or outbox-backed outputs;
- RSS/Atom, sitemap, pagination, or specialized discovery;
- FlareSolverr or browser challenge resolution;
- SLF4J, Micrometer, or OpenTelemetry observers;
- specialized external scheduling integrations when the built-in coroutine scheduler is insufficient.

The presence of an adapter in the repository never makes it mandatory for core users.

## Maven packaging strategy

Trawler uses logical extension boundaries everywhere, but only a few cohesive physical artifacts:

```text
trawler-parent                 build/version management only
└── trawler-core               engine + planned lightweight dependency-free built-ins

planned when implemented:
├── trawler-web                Ktor + cohesive public-web behavior
├── trawler-postgres           PostgreSQL-backed state/response/output
└── specialized artifacts      browser, FlareSolverr, OpenTelemetry, WARC
```

A small extension does not receive its own Maven artifact merely because it is replaceable. When implemented, the built-in scheduler, in-memory state, seed discovery, callback output, and generic policies will be bundled in `trawler-core`.

A separate artifact is used when a capability introduces an external technology stack, heavy operational/security footprint, or meaningful independent compatibility boundary. See [docs/MODULES.md](docs/MODULES.md) for the complete decision matrix.

## Scope

### In scope

- A reusable library for constructing different scraping systems.
- Multiple unrelated typed pipelines in one engine.
- One-shot, finite, and daemon-style execution primitives.
- Replaceable policies and focused extension contracts.
- Optional durable recurring crawling with resources, jobs, attempts, checkpoints, and fenced leases.
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

The repository intentionally contains project setup and architecture documentation only.

```text
Trawler/
├── AGENTS.md                  mandatory contributor and agent rules
├── CLAUDE.md                  symlink to AGENTS.md
├── README.md                  project overview and status
├── pom.xml                    Maven parent and reactor
├── trawler-core/
│   ├── pom.xml                implementation-ready core artifact
│   └── README.md              core-specific scope and starting point
├── docs/
│   ├── ARCHITECTURE.md        concise architecture contract
│   ├── MODULES.md             Maven artifact ownership and split rules
│   ├── DEPENDENCIES.md        dependency inventory and admission policy
│   ├── DEVELOPMENT.md         workflow and quality gates
│   ├── DOCUMENTATION.md       documentation requirements
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

Until the first production source is implemented, Maven reports that the `trawler-core` JAR is empty. This is expected and must not be hidden with placeholder classes or resources; it disappears when Phase 1 adds real contracts.

There is currently no runnable main class and no production implementation. Do not document a run command until one exists.

## Dependencies

The `trawler-core` runtime dependency set is deliberately minimal:

| Dependency | Scope | Approved use |
|---|---|---|
| Kotlin standard library | Compile/runtime | Kotlin language runtime. |
| Kotlin coroutines core | Compile/runtime | Structured concurrency, suspending lifecycle, `Flow`, channels, and cancellation. |

There are currently no logging, JSON, SQL, HTTP, HTML parsing, anti-bot, or test dependencies. They were removed because no implemented component justified them and several would violate core isolation if treated as universal.

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
