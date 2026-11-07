# Testing

> **Status:** Infrastructure implemented; core contract, scheduler/daemon, JDK file-adapter, database-free runtime, web policy/session/discovery, Ktor transport, cross-module typed composition, architecture/API guards, and PostgreSQL state/response/output/outbox tests added; FlareSolverr transport/challenge-clearance unit, engine end-to-end, and loopback CIO tests added; all suites pass with JaCoCo gates of 95% line / 90% branch per module; PostgreSQL execution requires Docker
>
> **Owner:** `trawler-parent` build configuration and the non-published
> `trawler-coverage` aggregate module, with test dependencies owned by each
> implementation module

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
[`TEST-MATRIX.md`](TEST-MATRIX.md). PostgreSQL state, response, output, and
outbox tests are owned by `trawler-postgres` and use direct SQL assertions.
The cross-module test composes four typed pipelines with Ktor MockEngine,
response capture, scheduled overlap, checkpoint stopping, and zero-network
replay. `PostgresDurableE2EIT` composes fresh state, response, output, and
outbox adapter instances across a restart and verifies fencing, checkpoint
resume, replay, and lineage retention with direct SQL.

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
mvn -pl trawler-flaresolverr -am clean verify
```

`trawler-flaresolverr` can also be verified alone against installed core/web
snapshots with `mvn -B -pl trawler-flaresolverr verify`.

Run one unit class or one integration class:

```bash
mvn -pl trawler-core -am -Dtest=EngineRuntimeTest \
  -Dsurefire.failIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false test
mvn -pl trawler-postgres -am -Dit.test=PostgresCrawlStateStoreIT \
  -Dfailsafe.failIfNoSpecifiedTests=false verify
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
- `trawler-flaresolverr`: the shared test support plus Ktor `MockEngine`
  (the same test-scoped artifact `trawler-web` uses). A module-internal
  `HttpClientEngine` seam lets unit tests drive the FlareSolverr client through
  MockEngine; `FlareSolverrCioIT` exercises the real CIO client against a
  process-local loopback JDK `HttpServer`. No Docker, live FlareSolverr, or
  public network is used. The parent's strict `surefire.failIfNoTests`
  default applies.

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

For a focused unit class in the reactor, set both
`-Dsurefire.failIfNoTests=false` and
`-Dsurefire.failIfNoSpecifiedTests=false` so the parent and unrelated modules
do not fail merely because they contain no matching class. These are explicit
focused-run overrides; the default reactor settings remain strict. For a
focused Failsafe class, set
`-Dfailsafe.failIfNoSpecifiedTests=false` for the same reason:

```bash
mvn -pl trawler-postgres -am -Dit.test=PostgresResponseStoreIT \
  -Dfailsafe.failIfNoSpecifiedTests=false verify
```

## Coverage policy

JaCoCo is configured in the parent POM for every implementation module, with
the final aggregate owned by `trawler-coverage`:

- `jacoco:prepare-agent` instruments unit and integration test JVMs;
- `jacoco:report` writes per-module HTML and XML reports under
  `<module>/target/site/jacoco/`;
- `jacoco:check` fails `verify` for any implementation module below 95% line
  or 90% branch coverage (`jacoco.line.minimum` / `jacoco.branch.minimum` in
  the parent POM). Build-only `pom` modules have no execution data and are
  skipped;
- `trawler-coverage` runs `jacoco:report-aggregate` after every
  implementation module, writing the reactor report under
  `target/site/jacoco-aggregate/`.

Coverage measured by the 2026-11-07 `mvn clean verify` run (Docker Engine 29,
Testcontainers 1.21.4, `postgres:16.4-alpine`):

| Module | Line | Branch | Tests (unit + IT) |
|---|---:|---:|---:|
| `trawler-core` | 98.9% | 95.9% | 318 + 27 |
| `trawler-web` | 99.7% | 96.3% | 146 + 3 |
| `trawler-postgres` | 99.2% | 95.1% | 20 + 69 |
| `trawler-flaresolverr` | 99.5% | 96.1% | 105 + 6 |

The remaining uncovered code is defensive, race-only, or compiler-generated;
each module's residue is listed in [`TEST-MATRIX.md`](TEST-MATRIX.md). The
gate is a floor, not a goal: a coverage number never replaces a contract
assertion, and thresholds must not be lowered or code excluded to pass it.

The PostgreSQL integration suite first executed on 2026-11-07; earlier runs
could not reach Docker because Testcontainers 1.20.x negotiates Docker API 1.32,
which Docker Engine 29 rejects. PostgreSQL fault injection uses triggers, an
external transaction holding row locks (observed through `pg_stat_activity`
rather than sleeps), and `pg_terminate_backend`. Exception-identity
assertions use `assertOriginalFailure`, because kotlinx.coroutines stack-trace
recovery (enabled with JVM assertions) may surface a copy whose cause is the
original exception.

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

`trawler-flaresolverr` tests run against a controlled fake FlareSolverr
(MockEngine for unit tests, a loopback JDK `HttpServer` for `*IT`), so they
prove adapter behavior against the documented v3 protocol shape, not against
a real FlareSolverr release; compatibility with FlareSolverr 3.5.2 rests on
the manual smoke check in [`FLARESOLVERR.md`](FLARESOLVERR.md#verification).
The single-flight and leader-cancellation tests use virtual time; the
engine end-to-end test synchronizes its first round with a barrier and is
bounded by a timeout guard. The CIO call-timeout IT intentionally waits for
the minimum one-second FlareSolverr `maxTimeout`. Not covered: real browser
behavior, FlareSolverr session TTL rotation, and the CIO engine's own socket
timeout tuning beyond the asserted call timeout.

The core module contains deterministic `*Test.kt` coverage for contracts,
the database-free engine runtime, scheduling, and daemon behavior, plus JDK
file-store `*IT.kt` coverage. Web policy, session, XML-discovery, Ktor
MockEngine, and loopback CIO tests cover the bounded web-adapter contracts
without public-network calls. The PostgreSQL module has configuration/codec
unit coverage and real-database `*IT.kt` source coverage for independent
state/response/output/outbox migrations, lifecycle, codecs, queue/lease
fencing, output transactions, response ownership/retention, outbox
retries/reclaim/cancellation, local lineage, and the durable composition
restart/replay scenario, plus contract ITs for every configuration bound,
lock-loss and cancellation rollback, connection termination, and outbox lease
expiry during delivery. Its execution requires a working Docker runtime.
The actual CIO timeout behavior of the underlying socket engine is also not
directly exercised with wall-clock timing; deterministic timeout exceptions,
outer cancellation, and reservation cleanup are covered. The coverage
gate and the matrix must be read together; a report is not a substitute for a
contract assertion. Feature work
must add tests to the owning module and update the corresponding matrix row in
the same change.
