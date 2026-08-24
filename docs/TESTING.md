# Testing

> **Status:** Infrastructure implemented; core contract, scheduler/daemon, JDK file-adapter, database-free runtime, web policy/session/discovery, Ktor transport, and PostgreSQL state tests added; PostgreSQL execution requires Docker
>
> **Owner:** `trawler-parent` build configuration, with test dependencies owned by each implementation module

## Purpose

This document defines the repository-wide test lifecycle and makes unit-test
discovery, integration-test discovery, dependency ownership, deterministic
concurrency, fixtures, database requirements, coverage reporting, and failure
handling consistent across the reactor.

The core contract phases add deterministic unit tests for the value models,
configuration DSL, codecs, discovery and harvest helpers, built-in policies,
response stores, outputs, in-memory state, the coroutine scheduler, and the
database-free engine runtime. Runtime tests use controlled transports,
policies, stores, extensions, observers, barriers, virtual coroutine time,
and mutable injected clocks to prove typed pipeline isolation, middleware and
stage decisions, response capture/cache behavior, retries, lease loss,
cancellation, bounds, budgets, event privacy, lifecycle ownership, and
network-free replay. The scheduler tests use virtual coroutine time, mutable
injected clocks, and barriers for active and queued runs; daemon registration
uses a handwritten scheduler fake. In-memory state coverage includes terminal
lifecycle transitions, lease fencing/reclaim, bounded retention, atomic
checkpoint admission, and deterministic enqueue/claim/CAS races. Core's JDK
file response/output adapters use per-test temporary directories and Failsafe
`*IT` coverage for restart, corruption, retention, and filesystem repair
paths. The contract-by-contract status is recorded in
[`TEST-MATRIX.md`](TEST-MATRIX.md). PostgreSQL state tests are owned by
`trawler-postgres`; response/output/outbox database tests remain future work.

## Commands

Run all unit tests:

```bash
mvn clean test
```

Run the complete configured lifecycle, including integration tests and
JaCoCo reports:

```bash
mvn clean verify
```

Run a focused module and its reactor dependencies:

```bash
mvn -pl trawler-core -am clean verify
mvn -pl trawler-web -am clean verify
mvn -pl trawler-postgres -am clean verify
```

Run one unit class or one integration class:

```bash
mvn -pl trawler-core -am -Dtest=TypedPipelineCompositionTest test
mvn -pl trawler-postgres -am -Dit.test=PostgresCrawlStateStoreIT verify
```

`-DskipITs=true` skips the Failsafe integration-test executions when the
environment cannot provide Docker. It is an explicit local choice and must
not be used to hide a failing required verification run. Unit tests can be
skipped with Maven's standard `-DskipTests=true` option. Neither option is a
default.

## Source layout and naming

All Kotlin tests live under `src/test/kotlin` in their owning module. The
Kotlin Maven plugin compiles both classes into the test output directory; the
Maven lifecycle separates them by class name:

| Test kind | Source name | Maven phase | Runner |
|---|---|---|---|
| Unit, policy, contract, and deterministic component tests | `*Test.kt` | `test` | Surefire on the JUnit Platform |
| Database, filesystem, transport, migration, or multi-process tests | `*IT.kt` | `integration-test` and `verify` | Failsafe on the JUnit Platform |

The explicit Surefire and Failsafe include patterns match the compiled
`*Test.class` and `*IT.class` names. A class must not be renamed to make a
slow or externally dependent test run in the unit phase.

## Test dependency ownership

The parent POM pins versions but does not put test libraries on a runtime
classpath. Each module declares its test dependencies with Maven `test` scope:

- `trawler-core`: JUnit 5, Kotlin test/JUnit integration, and
  `kotlinx-coroutines-test`.
- `trawler-web`: the shared test support plus Ktor `MockEngine`; MockEngine is
  not a core or PostgreSQL dependency. CIO integration tests use a small
  process-local loopback `ServerSocket` fixture and add no runtime dependency.
- `trawler-postgres`: the shared test support plus Testcontainers PostgreSQL
  and its JUnit 5 integration; Testcontainers is not a core or web dependency.

Tests use handwritten fakes by default. No mocking framework is selected.
The full version, license, transitive-dependency, and removal inventory is in
[`DEPENDENCIES.md`](DEPENDENCIES.md).

## Deterministic concurrency

Concurrency tests must prove observable behavior without timing roulette.

- Use `kotlinx-coroutines-test` with `runTest`, a
  `StandardTestDispatcher`, `TestCoroutineScheduler`, `advanceUntilIdle`, and
  explicit virtual-time advancement where the contract is time based.
- Use `CompletableDeferred`, bounded `Channel`s, barriers, and controlled fake
  transports to establish exact happens-before relationships.
- Inject `Clock` and `RandomSource`; use fixed values or a seeded test source
  when randomness is part of the contract.
- Bound every join, collection, queue, and fixture. A test must fail with a
  useful assertion if a barrier is never released.
- Assert permit release, child cancellation, close order, stale-writer
  rejection, and forbidden calls directly.

`Thread.sleep`, blocking waits, unseeded randomness, arbitrary timeouts used
to make a race likely, test retries, shared mutable fixtures, and swallowed
exceptions are forbidden. A virtual-time test may use the scheduler's
deterministic clock; it must not use wall-clock timing as synchronization.

## Fakes, fixtures, and resources

Use the smallest fixture that proves the contract:

- handwritten transports record requests and return fixed responses;
- discovery fakes emit bounded requests or responses through a controlled
  `Flow`;
- output fakes record acknowledged batches and can return explicit failures;
- clocks, randomness, state stores, and lifecycle extensions are deterministic
  test doubles;
- temporary directories are created per test for file stores and cleaned in a
  `finally`/resource scope;
- PostgreSQL integration tests create isolated Testcontainers databases and
  close them deterministically after each class or fixture scope.

Fixtures must not contain credentials, cookies, authorization values, private
responses, or live third-party content. Tests must never call a live website
or an externally managed database. A test that needs a database uses the
PostgreSQL container owned by `trawler-postgres` tests.

## Docker and PostgreSQL integration tests

Unit tests, Ktor MockEngine tests, and the web module's loopback CIO `*IT`
tests do not require Docker. PostgreSQL `*IT` tests require a reachable
Docker-compatible runtime supported by Testcontainers
and permission to start the pinned PostgreSQL image. No host PostgreSQL
installation or shared database is used.

When Docker is unavailable, the focused PostgreSQL integration command fails
as an environment failure and must be reported; tests must not silently skip,
disable themselves, or retry until green. The suite uses the pinned
`postgres:16.4-alpine` image and creates a fresh schema per test, while two
store instances intentionally share a schema only in multi-process race
scenarios. The full reactor's unit-only verification can be run with
`-DskipITs=true` for local diagnosis, but the required handoff command remains
`mvn clean verify` in an environment that provides Docker.

## Coverage policy

JaCoCo is configured in the parent POM for every module:

- `jacoco:prepare-agent` instruments unit and integration test JVMs;
- `jacoco:report` writes per-module HTML and XML reports under
  `<module>/target/site/jacoco/`;
- `jacoco:report-aggregate` writes the reactor report under
  `target/site/jacoco-aggregate/` when reactor execution data is available.

Coverage is report-only in this phase. No `jacoco:check` goal or threshold can
fail the build yet. The final audit phase will choose and enable line and
branch hard gates after the matrix has real feature tests. A report with no
executed feature tests is not evidence of behavioral coverage.

## Failure handling

Surefire and Failsafe fail the build on test failures; they do not ignore
failures or retry tests. The temporary discovery check for this phase uses a
deliberately failing `*Test.kt`, confirms Maven reports the failure, and then
removes that file before the passing verification run. No sentinel test is
part of the repository.

Tests must preserve original exception causes and assert failure categories,
state transitions, cleanup, and side effects. Container startup or migration
errors are reported as infrastructure failures with their original cause;
they are not converted into passing assumptions. A failing feature regression
test remains failing until the smallest production correction is made.

## Current limitations

The core module contains deterministic `*Test.kt` coverage for contracts,
the database-free engine runtime, scheduling, and daemon behavior, plus JDK
file-store `*IT.kt` coverage. Web policy, session, XML-discovery, Ktor
MockEngine, and loopback CIO tests cover the bounded web-adapter contracts
without public-network calls. The PostgreSQL module has configuration/codec
unit coverage and real-database `PostgresCrawlStateStoreIT.kt` source coverage
for migrations, queue, leases, attempts, transitions, CAS state, recovery,
and retention. Its execution remains dependent on a working
Docker/Testcontainers connection in the verification environment.
The actual CIO timeout behavior of the underlying socket engine is also not
directly exercised with wall-clock timing; deterministic timeout exceptions,
outer cancellation, and reservation cleanup are covered. Coverage is
report-only, so the matrix and the generated JaCoCo reports must be read
together; a report is not a substitute for a contract assertion. Feature
phases must add tests to the owning module and update the corresponding matrix
row in the same change.
