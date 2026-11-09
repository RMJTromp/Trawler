# Trawler Repository Instructions

These instructions apply to every file and directory in this repository. They are mandatory for humans and automated coding agents.

The words **must**, **must not**, **required**, and **forbidden** are normative. If a user instruction conflicts with this file, the explicit user instruction wins for that task. Otherwise, do not improvise around these rules.

## 1. Required reading and authority

Before making changes, read:

1. this file in full;
2. `PLAN.md` for architecture and delivery phases;
3. `docs/MODULES.md` before adding/moving a Maven module or deciding where an implementation belongs;
4. the focused documents under `docs/` relevant to the task;
5. the files currently being changed and their direct collaborators.

Authority order:

```text
explicit user instruction
  > AGENTS.md
  > PLAN.md
  > focused docs under docs/
  > README.md
  > examples, comments, old discussion, or inferred convention
```

If two authoritative rules still conflict, stop and ask. Do not silently select the more convenient rule.

## 2. Current project status

Phases 0–6 of `PLAN.md` are implemented and released as `1.3.1`. The Maven reactor contains the build-only `trawler-parent`, the version-alignment `trawler-bom`, the runtime artifacts `trawler-core`, `trawler-web`, `trawler-postgres`, and the Phase 7 `trawler-flaresolverr` adapter, and the non-published `trawler-coverage` aggregate. `README.md` and the feature records under `docs/` describe exactly what each artifact implements; other Phase 7 integrations exist only when their module is present in the reactor.

Do not:

- claim that planned features are implemented;
- create placeholder production classes, empty packages, fake APIs, or speculative scaffolding unless explicitly requested;
- copy design snippets from the plan as though they are finalized compiling APIs;
- introduce an adapter dependency before implementing the adapter that owns it;
- implement production code when the task is documentation or setup only.

## 3. Product rule

Every implementation must satisfy:

> **Core owns coordination. Extensions own policy. The host owns data.**

A proposed type does not belong in core if it would stop making sense for a caller that:

- scrapes a completely different domain;
- produces an arbitrary Kotlin type;
- uses no SQL database;
- retains no responses;
- uses no scheduler;
- uses a custom or non-HTTP transport.

## 4. Architectural invariants

### 4.1 Typed pipeline ownership

- `TrawlerEngine` must remain non-generic.
- Each pipeline must retain its own `T` from extraction through output.
- Core must not erase `T` into `Any`, JSON, maps, or a library-owned record envelope during normal processing.
- Any unavoidable heterogeneous engine registry boundary must be isolated behind type-safe registration built once during configuration.
- Core must not define application record keys, entity types, schemas, payload columns, or persistence models for produced data.

### 4.2 Dependency direction

The only valid high-level dependency direction is:

```text
examples/integrations -> adapters -> core
```

Forbidden:

```text
core -> adapter
core -> example
adapter A -> unrelated adapter B, unless an explicit integration module owns the coupling
```

Core must not import or expose types from:

- Ktor;
- JDBC, PostgreSQL, HikariCP, or Flyway;
- Jackson, Gson, kotlinx.serialization, protobuf, or another serializer;
- Jsoup or another content parser;
- SLF4J, Logback, Micrometer, or OpenTelemetry;
- FlareSolverr or browser automation;
- application/domain models.

Physical packaging follows `docs/MODULES.md`. Do not create one artifact per extension. Lightweight official implementations that require only Kotlin/coroutines/JDK—such as the in-memory store, seed discovery, generic policies, callback output, and coroutine scheduler—belong in `trawler-core` but remain logically isolated, explicitly selected, and replaceable.

Create a separate module before introducing an external technology stack that core consumers should not inherit. Currently approved future groupings are `trawler-web`, `trawler-postgres`, and demand-driven specialized artifacts. Do not create their POMs or empty packages before implementation begins.

### 4.3 Separate responsibilities

Never merge these into one universal storage abstraction:

1. crawl/engine operational state;
2. raw response storage or cache;
3. produced-data output.

User-produced values must never be stored in a core state table. Response storage must remain optional. Output serialization belongs to the output that needs it.

### 4.4 Replaceable policies

The following are extension or policy decisions, not hardcoded engine algorithms:

- request identity;
- canonicalization;
- active-work deduplication;
- recrawl behavior;
- failure classification;
- retry;
- cooldown;
- concurrency;
- pacing/rate limiting;
- work ordering;
- crawl scope;
- authorization;
- robots behavior;
- URL/network safety;
- cache behavior;
- discovery admission;
- harvest semantics;
- health/circuit breaking;
- budgets and stop conditions;
- scheduling.

Built-ins may supply defaults. Every default must have a clear replacement path and must not be activated invisibly.

### 4.5 Durable state semantics

When durable state is implemented:

- resource identity, job identity, lease ownership, and execution attempts must be distinct concepts;
- active duplicate work may be suppressed without permanently suppressing recrawls;
- acquiring a lease must not count as a meaningful attempt;
- every lease-owned mutation must include the fencing token;
- stale workers must be unable to attach responses or transition reclaimed work;
- discovery checkpoints must advance only after emitted work is accepted durably;
- checkpoint payloads must use extension-owned codec IDs and versions, not mandatory JSON;
- historical/incremental harvesting must be optional;
- abandonment, cancellation, and failure must remain distinct;
- live event streams must not be presented as durable audit history.

### 4.6 Concurrency and lifecycle

- Use structured coroutine ownership only.
- `GlobalScope` is forbidden.
- `Thread.sleep()` is forbidden in suspending/runtime paths; use suspending delay or an injected clock.
- Cancellation must propagate and resources must close deterministically.
- Engine and extension startup/shutdown order must be explicit.
- Queues, buffers, bodies, redirects, recursion, batches, concurrency, and waits must be bounded or intentionally documented as caller-bounded.
- Blocking adapter work must run on an adapter-owned bounded dispatcher sized to its blocking resource pool.
- Do not hold permits or database transactions across unrelated network waits.
- Inject clock and randomness wherever time or jitter affects observable behavior.

### 4.7 Transport and network safety

- Discovery, probing, redirects, challenge resolution, and follow-up fetching must use engine-controlled request execution.
- Plugins must not create hidden network clients.
- Retry loops must remain visible to engine attempt accounting; do not hide retries inside transport middleware.
- Every redirect hop must rerun destination safety, authorization, concurrency, pacing, and credential-forwarding rules.
- Sensitive credentials must be stripped on cross-origin redirects by default.
- Public-network blocking and robots support are replaceable safe presets, not unavoidable core restrictions.
- Secrets, cookies, authorization values, proxy credentials, and configured sensitive data must never enter default logs, events, archives, fixtures, or examples.

### 4.8 Delivery guarantees

- Do not claim exactly-once delivery unless a specific adapter proves a narrowly documented transactional guarantee.
- Required versus best-effort outputs must be explicit.
- External outputs must document duplicate windows and idempotency requirements.
- Outbox behavior is an optional output/decorator concern, not a mandatory engine table.
- Replay requires a capable response store, must make zero network calls, and must not create ordinary crawl jobs.

## 5. Scope discipline

Implement only the requested scope and necessary supporting work.

Do not:

- build future phases speculatively;
- introduce abstractions with only hypothetical consumers unless required by the final architecture boundary;
- refactor unrelated code for style;
- rename or move unrelated files;
- modify generated files manually;
- add compatibility shims without a stated compatibility requirement;
- broaden a public API while fixing an internal issue;
- add examples that become de facto core dependencies.

If a required change crosses a phase or architecture boundary, explain the impact before proceeding.

## 6. Dependency policy

Read `docs/DEPENDENCIES.md` before modifying `pom.xml` or adding a module.

Read `docs/MODULES.md` before creating, splitting, merging, or moving an artifact. A separate artifact must satisfy its module admission test; replaceability or small package boundaries alone are insufficient.

Every new dependency requires all of the following:

1. a current implemented use—not anticipated future use;
2. a single owning module/adapter;
3. the narrowest possible Maven scope;
4. an entry in `docs/DEPENDENCIES.md` explaining purpose and ownership;
5. license, maintenance, security, and transitive-dependency consideration;
6. confirmation that JDK/Kotlin/current dependencies cannot reasonably cover the need;
7. confirmation that its types do not leak into core public contracts;
8. a removal plan if the owning feature is removed.

Forbidden dependency practices:

- dependency bundles “just in case”;
- two libraries for the same responsibility without a documented decision;
- serializer, database, HTTP, logging, parser, browser, or telemetry dependencies in core;
- unpinned dynamic versions, version ranges, `LATEST`, snapshots, or local unpublished artifacts;
- duplicating versions inline instead of using managed properties/module management;
- adding a repository declaration when Maven Central or the approved repository configuration already resolves the artifact;
- suppressing convergence or vulnerability issues without documenting the reason.

Changes to dependencies must update both the POM and dependency documentation in the same change.

## 7. Testing policy

### 7.1 Tests are required

Every change that adds or alters behavior must add or update automated tests in the same change. Untested production code is incomplete work, not a finished change. Bug fixes must include a test that fails without the fix.

Fixing a test that encodes a wrong expectation is allowed, but the handoff must say which assertion changed and why; never edit a test merely to make a build green.

If a behavior genuinely cannot be tested automatically, document it in the untested-risk register in `docs/TEST-MATRIX.md` and state it in the handoff.

### 7.2 Test standards

All tests must follow these standards:

- test public behavior and contracts rather than private implementation details;
- include negative, boundary, cancellation, failure, and recovery cases;
- make time and randomness deterministic;
- use controlled fake transports and fixed response fixtures;
- never depend on live third-party sites in the default suite;
- isolate integration tests that require external infrastructure;
- make concurrency tests bounded and deterministic enough to avoid timing roulette;
- assert delivery, retry, checkpoint, and duplicate semantics explicitly;
- do not reduce coverage, weaken assertions, add sleeps, or swallow failures to make tests pass;
- document any remaining untested risk.

High-risk areas that require explicit coverage include:

- type isolation across heterogeneous pipelines;
- cancellation and cleanup;
- lease fencing and stale writers;
- recrawl versus active deduplication;
- attempt accounting across crashes;
- atomic enqueue/checkpoint behavior;
- redirect safety and credential stripping;
- response replay network isolation;
- output duplicate windows and idempotency.

## 8. Documentation requirements

Documentation is part of the deliverable, not follow-up work.

Every implemented feature must update the appropriate documentation in the same change. Read `docs/DOCUMENTATION.md` for the required feature record.

At minimum, document:

- purpose and non-goals;
- owning layer/module;
- public contracts;
- configuration, defaults, and validation;
- lifecycle/state transitions;
- capability requirements;
- concurrency and cancellation behavior;
- failure/retry behavior;
- persistence/delivery/duplicate guarantees;
- security, privacy, and retention implications;
- verification performed;
- known limitations.

Additional requirements:

- Any dependency change updates `docs/DEPENDENCIES.md`.
- Any cross-cutting architecture decision adds or updates an ADR under `docs/decisions/`.
- Any change to project status or supported capabilities updates `README.md`.
- Any changed public terminology updates the glossary/architecture documentation consistently.
- Examples must be labeled conceptual if they are not compiled and verified.
- Never document an aspirational feature as implemented.
- Remove or correct stale documentation in the same change that makes it stale.

## 9. Code standards for future implementation

- Prefer small, focused interfaces based on actual capabilities over one universal plugin interface.
- Prefer immutable data and explicit state transitions.
- Prefer sealed results for expected outcomes and exceptions for truly exceptional/unrecoverable programmer or infrastructure failures where appropriate.
- Preserve original causes; do not reduce failures to strings internally.
- Redact only at persistence/observability boundaries while retaining safe structured diagnostics.
- Public APIs require KDoc describing ownership, cancellation, thread/coroutine safety, and failure behavior.
- Avoid nullable booleans, stringly typed states, public mutable collections, global singletons, and ambient configuration.
- Avoid default values that silently select durable, network, security, or retention behavior.
- Extension cardinality and replacement/ordering semantics must be explicit.
- Validate configuration before starting background work.
- Public contracts must not expose implementation-specific exceptions or types unless the module explicitly is that adapter.
- Do not use reflection or unchecked casts to bypass typed pipeline design. Any unavoidable internal cast requires an isolated proof of safety and documentation.
- No unbounded `Flow` collection, channel, queue, cache, archive query, or response read.
- No empty catch blocks, broad exception swallowing, or logging-and-continuing without a defined state transition.
- No TODO/FIXME in completed work unless the user explicitly accepts a tracked limitation and documentation records it.
- No dead code, commented-out implementation, placeholder return values, or unused dependencies.

## 10. Repository and Git rules

- Work on the currently checked-out branch.
- Do not create or switch branches unless explicitly requested.
- Do not create commits, tags, pull requests, or releases unless explicitly requested.
- Preserve all unrelated user changes, including uncommitted changes.
- Never use destructive Git commands to clean up work you did not create.
- Inspect `git status` before and after changes.
- Keep changes focused; do not reformat unrelated files.
- Do not edit IDE workspace state or commit machine-specific files.
- Do not commit secrets, credentials, local databases, build output, captured sensitive responses, or private fixtures.
- Prefer deterministic, reviewable changes over generated bulk output.

## 11. Required workflow

### Before implementation

1. Read the required documents.
2. Inspect repository state and relevant files.
3. Restate the requested outcome and identify the owning layer.
4. Identify architecture, dependency, security, persistence, and documentation impact.
5. Confirm whether the request authorizes production code, dependency changes, and external actions; do not infer broader authorization. Tests for the changed behavior are always in scope.
6. Make the smallest complete plan appropriate to the task.

### During implementation

1. Keep the build in a recoverable state.
2. Make focused edits.
3. Update documentation alongside behavior.
4. Validate assumptions against actual code rather than plans or filenames.
5. Do not conceal uncertainty with speculative APIs or fake implementations.

### Before handoff

1. Inspect the final diff and `git status`.
2. Run formatting/check commands only on the relevant changed scope when possible.
3. Run `mvn clean compile` for production/build changes.
4. Run `mvn clean verify` when the configured lifecycle and task scope justify it.
5. Add or update tests for the changed behavior and run them; `mvn clean verify` must pass.
6. Confirm documentation, dependency inventory, and project-status claims are accurate.
7. Confirm core dependency boundaries are intact.
8. Report files changed, important decisions, verification performed, tests not run, and remaining risk.

Do not report success when required verification failed. Do not hide skipped verification.

## 12. Review priorities

Review changes in this order:

1. correctness and data loss;
2. stale lease, retry, checkpoint, or delivery semantics;
3. security and credential leakage;
4. cancellation, cleanup, deadlock, and resource bounds;
5. type safety and architecture boundaries;
6. public API compatibility and configuration validation;
7. observability and diagnosability;
8. documentation accuracy;
9. performance based on evidence;
10. style.

A stylistically clean change that violates an earlier priority must not be accepted.

## 13. Definition of a complete change

A change is complete only when:

- it fulfills the exact requested outcome;
- it respects `PLAN.md` and these rules;
- it introduces no unjustified dependency or adapter leakage;
- lifecycle, cancellation, failure, persistence, and delivery behavior are explicit;
- resources are bounded and cleaned up;
- documentation matches reality;
- applicable verification succeeds;
- the changed behavior is covered by automated tests that pass;
- the handoff accurately reports what was and was not verified;
- no unrelated user work was altered.

If one of these cannot be satisfied, report the blocker instead of redefining “done.”
