# Development and Quality Rules

> **Status:** Normative
>
> Root rules in [`AGENTS.md`](../AGENTS.md) also apply.

## Supported baseline

- JDK 17 or newer, targeting JVM 17.
- Maven 3.9 or newer.
- Kotlin/core dependency versions are defined once in `pom.xml`; the web
  adapter's separately documented Ktor-compatible coroutines alignment is
  also managed there.
- UTF-8 source and reporting output.
- Official Kotlin code style.

Do not raise the JVM target, change the build system, or introduce a wrapper/toolchain manager without an explicit project decision.

## Standard commands

Compile production sources:

```bash
mvn clean compile
```

Compile only core and its required reactor dependencies:

```bash
mvn -pl trawler-core -am clean compile
```

Compile the implemented web adapter and its core dependency:

```bash
mvn -pl trawler-web -am clean compile
```

Run the configured verification lifecycle:

```bash
mvn clean verify
```

Inspect dependency resolution when dependencies change:

```bash
mvn dependency:tree
```

The root POM is the reactor/build parent. Production implementation is in
`trawler-core` and the optional `trawler-web` adapter; future modules are
added only with their real implementation according to `docs/MODULES.md`.
There is no runnable main class and no authorized automated test suite, but
the Phase 2 core library has a bounded `runOnce` runtime and opt-in scheduler,
and Phase 3 has a bounded Ktor transport.

Phase 1 contracts and the Phase 2 database-free runtime provide real core
sources. Later phases must not add placeholder content or suppress build
warnings merely to make an unimplemented adapter appear available.

## Change workflow

### 1. Establish scope

- Read `AGENTS.md`, `plans/FINAL-PLAN.md`, and relevant docs.
- Inspect `git status` before editing.
- Identify the requested outcome and owning layer.
- Identify whether the task authorizes production code, tests, dependency changes, migrations, external systems, or destructive actions.
- Preserve unrelated work.

### 2. Design against boundaries

For every new public contract, state:

- why it belongs in core or a specific adapter;
- ownership of its data;
- extension cardinality and composition order;
- lifecycle and cleanup responsibility;
- cancellation behavior;
- failure model;
- capability requirements;
- resource bounds;
- compatibility impact.

Do not start with concrete PostgreSQL, Ktor, JSON, or site-specific types and attempt to abstract them afterward.

### 3. Implement narrowly

- Make the smallest complete change.
- Keep public APIs minimal.
- Validate configuration before background work starts.
- Keep runtime state transitions explicit.
- Preserve original error causes.
- Add no dependency until the implementing code needs it.
- Update feature and dependency documentation in the same change.

### 4. Verify

At minimum for build or production changes:

- inspect the diff;
- run `mvn clean compile`;
- run static inspections available in the working environment when the change is substantial;
- exercise the changed behavior with a focused deterministic demonstration when practical;
- run `mvn clean verify` before final handoff when the configured lifecycle is meaningful;
- confirm documentation and dependency inventory remain accurate.

Documentation-only changes require Markdown/link/structure review and a final diff inspection. POM changes require at least Maven model/build verification.

### 5. Handoff

Report:

- outcome first;
- files changed;
- architecture or dependency decisions;
- commands and inspections run;
- whether automated tests were authorized, added, and run;
- limitations, risks, or follow-up work.

Never say “all checks passed” without listing the checks.

## Testing authorization

Automated tests are controlled by the project owner.

Agents must not add or refactor tests unless explicitly requested for the current task. This includes fixtures, test utilities, test-only dependencies, test plugins, snapshots, and generated test scaffolding.

If tests are not authorized, production quality still requires:

- compiler validation;
- static inspection;
- deterministic manual or local demonstrations where appropriate;
- explicit reasoning about failure and boundary cases;
- disclosure that automated tests were not added or run.

If tests are authorized, use the following standards.

### Test levels

| Level | Purpose | Restrictions |
|---|---|---|
| Unit | Pure policies, state transitions, configuration, typed composition. | No real network, sleeps, database, or system clock. |
| Contract | Verify every adapter against its declared core capability contract. | Shared contract suites must not expose adapter implementation details. |
| Integration | Database, filesystem, transport, migration, or multi-process behavior. | Isolated resources, deterministic cleanup, explicit environment requirements. |
| End-to-end | Small representative compositions across modules. | Local controlled targets only in default CI; never third-party sites. |

### Test design rules

- Test observable behavior.
- Name the scenario and expected result.
- Cover success, rejection, retry, cancellation, abandonment, and recovery as applicable.
- Inject clocks and randomness.
- Use virtual time where semantics allow it.
- Never add real sleeps to “stabilize” concurrency tests.
- Bound all waits and joins.
- Assert that stale leases cannot mutate state.
- Assert checkpoint ordering relative to durable enqueue.
- Assert replay cannot reach a live transport.
- Assert output duplicate semantics rather than assuming exactly-once behavior.
- Treat flaky tests as defects; do not rerun until green and ignore the cause.
- Do not mock the unit under test or assert internal call choreography unless the contract is explicitly orchestration order.

## Static quality expectations

Until dedicated formatter/linter tooling is intentionally selected:

- use official Kotlin style;
- use IDE/compiler inspections on substantial changed code;
- keep warnings at zero where under project control;
- avoid wildcard suppressions;
- explain any localized suppression in code and documentation when non-obvious;
- do not mass-format unrelated files.

A future formatter or linter must be documented in `docs/DEPENDENCIES.md` and applied consistently through Maven before being treated as mandatory.

## API quality

Public APIs must:

- use domain-neutral names in core;
- preserve typed values;
- expose immutable inputs/results where practical;
- document coroutine context, cancellation, ownership, and cleanup;
- avoid adapter types;
- distinguish expected outcomes from programmer errors;
- fail configuration deterministically before runtime;
- state default policy inheritance and extension ordering;
- evolve deliberately with migration notes for breaking changes.

Avoid:

- giant universal plugin/store/configuration interfaces;
- boolean parameters whose meaning is unclear at call sites;
- stringly typed lifecycle state;
- public mutable collections;
- ambient global configuration;
- callbacks with undocumented threading/coroutine behavior;
- generic `Map<String, Any?>` as a substitute for a real public contract;
- serialization annotations on core models.

## Concurrency quality

Every concurrent component must document:

- owning scope;
- child failure behavior;
- cancellation behavior;
- permit acquisition order;
- bounded queue/buffer size;
- lease/timeout relationship where applicable;
- shutdown behavior;
- blocking operations and dispatcher ownership.

Deadlock avoidance and stale-worker fencing take precedence over throughput. Performance changes require measurements representative of the claimed improvement.

## Security quality

- Never commit secrets or live authentication material.
- Default logs/events must redact sensitive values.
- Treat stored raw responses as potentially sensitive.
- Revalidate redirects and resolved addresses in public-web policies.
- Keep plugin I/O behind engine-controlled paths.
- Document persistence, encryption, and retention expectations for credentials/cookies.
- Do not weaken a safe preset silently; require explicit replacement/configuration.

## Compatibility and migrations

- Public breaking changes require an ADR or explicit design record.
- Persistent schema/codec changes require versioning and a forward migration plan.
- Checkpoint incompatibility must be migrated, quarantined, or reset explicitly—never silently misread.
- Avoid destructive migrations unless the user explicitly authorizes them and recovery is documented.
- An adapter migration must not become a core lifecycle requirement.

## Completion checklist

- [ ] Requested scope is complete.
- [ ] Architecture ownership is correct.
- [ ] `T` remains host-owned and typed.
- [ ] Dependencies are justified and documented.
- [ ] Configuration validates before work begins.
- [ ] Lifecycle, cancellation, cleanup, and bounds are explicit.
- [ ] Failure, retry, recovery, and delivery semantics are documented.
- [ ] Security and retention impacts are addressed.
- [ ] Relevant documentation is updated.
- [ ] Applicable build/inspection checks passed.
- [ ] Test authorization was followed.
- [ ] Final diff contains no unrelated edits.
- [ ] Handoff reports verification and residual risk honestly.
