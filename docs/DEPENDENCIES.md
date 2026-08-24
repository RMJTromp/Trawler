# Dependency Policy and Inventory

> **Status:** Normative
>
> **Rule:** A dependency must be justified by implemented code in its owning module. Planned features do not justify dependencies.

## Current Maven modules

| Module | Packaging | Published runtime artifact | Purpose |
|---|---|---|---|
| `trawler-parent` | `pom` | No | Reactor, version, dependency, and plugin management. It has no runtime dependencies. |
| `trawler-core` | `jar` | Yes, once releases begin | Generic contracts/engine and lightweight official implementations. |
| `trawler-web` | `jar` | Yes, once releases begin | Optional Ktor/CIO HTTP transport, replaceable web policies, and bounded XML discovery. |
| `trawler-postgres` | `jar` | Yes, once releases begin | Optional PostgreSQL operational state, response history, typed output, fenced outbox, leases, attempts, run history, request codecs, checkpoints, and recurring state. |

Future module ownership is defined in [`MODULES.md`](MODULES.md). Modules are not created before their implementation begins.

The ownership and purpose descriptions below include approved future core
capabilities. Phases 1 through 6 are implemented in the current core/web/
PostgreSQL sources. Later specialized adapters remain planned and do not
justify additional dependencies.

All project modules use the group ID `com.rmjtromp.trawler` and inherit the coordinated version from `trawler-parent`.

## `trawler-core` direct runtime dependencies

| Coordinate | Version source | Scope | Owner | Purpose | Why retained |
|---|---|---|---|---|---|
| `org.jetbrains.kotlin:kotlin-stdlib` | `kotlin.version` in parent `pom.xml` | Compile | `trawler-core` | Kotlin runtime and standard APIs. | Required for all Kotlin production code. |
| `org.jetbrains.kotlinx:kotlinx-coroutines-core` | `coroutines.version` in parent `pom.xml` | Compile | `trawler-core` | Structured coroutine scopes, cancellation, suspending lifecycle, `Flow`, channels, synchronization primitives, bounded in-memory coordination, and the bundled scheduler. | Structured concurrency and the Phase 2 database-free runtime are current core needs. |

`trawler-core` still has only Kotlin standard library and coroutines as direct
runtime dependencies. The current adapter dependency inventory is separate so
core consumers do not inherit Ktor.

## `trawler-web` direct runtime dependencies

| Coordinate | Version source | Scope | Owner | Purpose | Why retained |
|---|---|---|---|---|---|
| `com.rmjtromp.trawler:trawler-core` | `${project.version}` in parent `pom.xml` | Compile | `trawler-web` | Core transport, model, lifecycle, policy, and response contracts. | The adapter must implement the existing core boundary and cannot duplicate it. |
| `org.jetbrains.kotlinx:kotlinx-coroutines-core` | `ktor.coroutines.version` in parent `pom.xml` (`1.11.0`) | Compile | `trawler-web` | Suspending bounded DNS dispatch, policy synchronization, pacing, and cancellation. | Ktor 3.5.1 is compiled against coroutines 1.11.0; the adapter keeps that binary-compatible runtime isolated from core's 1.10.2 dependency tree. |
| `io.ktor:ktor-client-cio-jvm` | `ktor.version` in parent `pom.xml` (`3.5.1`) | Compile | `trawler-web` | One owned CIO HTTP client, request/response adaptation, timeout plugins, and byte-channel access. | A real HTTP transport is the current Phase 3 implementation need; JDK URL APIs do not provide the required replaceable Ktor client boundary and timeout/body behavior. |

The Phase 3/5 direct external technology dependency remains Ktor CIO. Ktor is Apache
License 2.0, is pinned through one parent property, and is maintained in its
upstream repository. The web adapter's direct coroutines version is an explicit
compatibility alignment with Ktor; `trawler-core` continues to resolve its
existing `coroutines.version` (`1.10.2`) independently. The resolved graphs are
reviewed with the dependency-tree command below; no retry,
content-negotiation, Jackson, parser, database, browser, telemetry, or test
artifact is declared directly. Removing `trawler-web` removes the Ktor graph
without changing the core dependency tree.

## `trawler-postgres` direct runtime dependencies

| Coordinate | Version source | Scope | Owner | Purpose | Why retained |
|---|---|---|---|---|---|
| `com.rmjtromp.trawler:trawler-core` | `${project.version}` in parent `pom.xml` | Compile | `trawler-postgres` | Core state capabilities, request models, lifecycle, clocks, and coroutine contracts. | The adapter implements the existing focused core boundary. |
| `org.jetbrains.kotlin:kotlin-stdlib` | `kotlin.version` in parent `pom.xml` | Compile | `trawler-postgres` | Kotlin runtime and standard APIs for adapter implementation. | Required by the Kotlin production source. |
| `org.jetbrains.kotlinx:kotlinx-coroutines-core` | `coroutines.version` in parent `pom.xml` | Compile | `trawler-postgres` | Bounded adapter dispatcher, lifecycle synchronization, cancellation, and suspending JDBC ownership. | The adapter owns blocking work under structured coroutine calls. |
| `org.postgresql:postgresql` | `postgresql.version` in parent `pom.xml` (`42.7.10`) | Compile | `trawler-postgres` | PostgreSQL JDBC driver used by the state adapter. | Current implemented database target; no generic JDBC abstraction is added. |
| `com.zaxxer:HikariCP` | `hikari.version` in parent `pom.xml` (`6.3.3`) | Compile | `trawler-postgres` | Adapter-owned bounded connection pool and connection acquisition timeout. | The adapter must align blocking dispatcher capacity with a bounded pool. |
| `org.flywaydb:flyway-core` | `flyway.version` in parent `pom.xml` (`13.2.0`) | Compile | `trawler-postgres` | Adapter-owned migration lifecycle and schema history. | Durable schema changes require versioned migrations owned by this artifact. |
| `org.flywaydb:flyway-database-postgresql` | `flyway.version` in parent `pom.xml` (`13.2.0`) | Compile | `trawler-postgres` | Flyway's PostgreSQL database support separated from the core migration API. | Required by the current Flyway API for PostgreSQL database support. |

PostgreSQL JDBC is BSD-2-Clause, HikariCP is Apache License 2.0, and Flyway
Community artifacts are distributed under their published upstream terms. The
versions are pinned, the upstream projects are maintained, and the adapter's
release process must review current security advisories and Flyway licensing
terms before publication. None of these types appears in core. Removing the
PostgreSQL state feature removes this whole direct graph and its migration
resource.

The adapter deliberately does not declare Jackson, a request serializer, or a
JSON codec. Its default request codec uses JDK binary streams and callers may
register a versioned codec. Flyway 13.2.0 itself brings the Jackson runtime
listed below as a required migration-tool transitive; it is not used to encode
requests, `T`, responses, or state payloads by Trawler. Replacing Flyway would
remove that transitive graph only if another migration implementation supplied
the same validated lifecycle.

Phases 2, 5, and 6 added no runtime dependency or Maven-module change. The
in-memory state and response stores, callback/flow outputs, generic policies,
coroutine scheduler, checkpoint/harvest codecs, and pipeline health/admission
policies use only the already-approved Kotlin standard library, coroutines, and
JDK APIs. Phase 5 XML discovery likewise uses JDK StAX and gzip inside
`trawler-web`; Phase 6 file response/output uses JDK NIO and caller codecs; no
parser, compression, serializer, or output dependency was added. No
serializer, database, HTTP, logging, telemetry, browser, or anti-bot type
enters core.

## Test-only dependencies

Test libraries are never placed on a production dependency path. The parent
POM manages one pinned version for each test responsibility; module POMs opt in
with Maven `test` scope only where the test runtime owns the dependency.

| Coordinate | Version source | Scope | Owner | Purpose | Boundary and removal plan |
|---|---|---|---|---|---|
| `org.jetbrains.kotlin:kotlin-test-junit5` | `kotlin.version` (`2.3.0`) | Test | All implementation modules | Kotlin assertions and JUnit 5 test integration. | Test source support only; remove with the repository test suite. Apache-2.0, maintained with Kotlin, no production API leakage. |
| `org.junit.jupiter:junit-jupiter` | `junit.jupiter.version` (`5.11.4`) | Test | All implementation modules | JUnit 5 API, engine, and parameterized-test support. | Test runner only; remove with JUnit tests. EPL-2.0, pinned and isolated from runtime artifacts. |
| `org.jetbrains.kotlinx:kotlinx-coroutines-test` | `coroutines.version` (`1.10.2`) | Test | All implementation modules | Virtual coroutine time, deterministic dispatchers, and structured test scopes. | Test-only coroutine scheduler; it does not replace production coroutines. Apache-2.0, removed if coroutine tests are removed. |
| `io.ktor:ktor-client-mock-jvm` | `ktor.version` (`3.5.1`) | Test | `trawler-web` tests only | Ktor `MockEngine` for controlled, in-process transport tests. | Web test dependency; it cannot be used by core or PostgreSQL tests and is removed with the Ktor adapter tests. Apache-2.0; Ktor transitives remain test-scoped. |
| `org.testcontainers:postgresql` | `testcontainers.version` (`1.20.6`) | Test | `trawler-postgres` tests only | Isolated PostgreSQL containers for database integration tests. | PostgreSQL test infrastructure only; Docker/Testcontainers transitives never enter production. MIT; remove with PostgreSQL integration tests. |
| `org.testcontainers:junit-jupiter` | `testcontainers.version` (`1.20.6`) | Test | `trawler-postgres` tests only | JUnit 5 lifecycle integration for PostgreSQL containers. | PostgreSQL IT support only; remove with container-based tests. MIT; no core or web exposure. |

Kotlin and kotlinx libraries use Apache-2.0 licensing. JUnit Jupiter uses the
EPL-2.0 license, Ktor uses Apache-2.0, and Testcontainers uses MIT. Testcontainers
adds Docker client/container-management transitives, while MockEngine adds the
Ktor test engine graph; both are deliberately test-only. Security advisories,
license changes, and container-image updates must be reviewed before release.
No mocking framework is selected; tests should use small handwritten fakes.

## Resolved transitive runtime dependencies

The following are not declared directly. They are present because Maven resolves them from the approved direct dependencies:

| Coordinate | Introduced by | Purpose | Policy |
|---|---|---|---|
| `org.jetbrains:annotations:13.0` | Kotlin standard library | JVM annotation metadata used by Kotlin/JVM interoperability. | Do not declare directly unless a future implementation uses its API explicitly and documents that need. |
| `org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.10.2` | Kotlin coroutines core | JVM implementation selected by the multiplatform coroutines artifact. | Version follows `coroutines.version`; do not declare a second independent version. |

The implemented `trawler-web` graph additionally resolves these unique runtime
coordinates from `ktor-client-cio-jvm:3.5.1`. Core resolves coroutines 1.10.2;
the web module explicitly resolves coroutines 1.11.0 so Ktor's runtime API is
not down-selected by the core dependency path:

| Coordinate | Introduced by | Purpose | Policy |
|---|---|---|---|
| `org.slf4j:slf4j-api:2.0.18` | Ktor client | Logging API used by Ktor internals. | Adapter transitive only; Trawler does not add a logging backend or expose SLF4J from core. |
| `org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.11.0` | Web module direct coroutines dependency and Ktor | JVM coroutines implementation aligned with Ktor's compiled API. | Web-only runtime; it does not change `trawler-core`'s 1.10.2 graph. |
| `io.ktor:ktor-client-core-jvm:3.5.1` | Ktor CIO | Ktor client request/response abstractions used by the CIO engine. | Keep transitive; only the CIO artifact is declared directly. |
| `io.ktor:ktor-http-jvm:3.5.1` | Ktor client core | Ktor HTTP method, header, and content primitives. | Keep transitive and adapt into core models at the web boundary. |
| `io.ktor:ktor-utils-jvm:3.5.1` | Ktor client core | Ktor shared utilities. | Keep transitive; no public core leakage. |
| `org.jetbrains.kotlinx:kotlinx-serialization-core-jvm:1.11.0` | Ktor HTTP | Ktor's internal serialization plumbing. | Not used to encode `T`; do not add a serializer module. |
| `io.ktor:ktor-http-cio-jvm:3.5.1` | Ktor CIO | CIO HTTP engine integration. | Keep transitive; owned by `trawler-web`. |
| `io.ktor:ktor-events-jvm:3.5.1` | Ktor client core | Ktor internal event support. | Keep transitive; it is not Trawler's observer/event contract. |
| `io.ktor:ktor-websocket-serialization-jvm:3.5.1` | Ktor client core | Ktor client distribution's websocket serialization support. | Not used by this phase; remains transitively supplied by Ktor and is not exposed. |
| `io.ktor:ktor-serialization-jvm:3.5.1` | Ktor websocket serialization | Ktor serialization integration plumbing. | Not used to serialize produced values; do not promote to a direct dependency. |
| `io.ktor:ktor-sse-jvm:3.5.1` | Ktor client core | Ktor client SSE support transitively included by the selected client artifact. | Not used in Phase 3; keep transitive until a later implementation needs it. |
| `org.jetbrains.kotlinx:kotlinx-coroutines-slf4j:1.11.0` | Ktor client core | Ktor coroutine/SLF4J context integration. | Adapter transitive only; it follows the web module's explicitly selected 1.11.0 runtime. |
| `io.ktor:ktor-network-jvm:3.5.1` | Ktor HTTP CIO | Ktor socket/network primitives. | Keep transitive; the web adapter owns network execution. |
| `io.ktor:ktor-io-jvm:3.5.1` | Ktor HTTP CIO | Ktor byte-channel I/O. | Keep transitive; response bytes are bounded before conversion to core `ResponseBody`. |
| `org.jetbrains.kotlinx:kotlinx-io-core-jvm:0.9.0` | Ktor I/O | Kotlinx I/O implementation used by Ktor channels. | Keep transitive; no public core API leakage. |
| `org.jetbrains.kotlinx:kotlinx-io-bytestring-jvm:0.9.0` | Kotlinx I/O core | Kotlinx I/O byte-string support. | Keep transitive; no direct use in core. |
| `io.ktor:ktor-websockets-jvm:3.5.1` | Ktor websocket serialization | Ktor websocket primitives. | Not used by Phase 3; do not add a websocket feature or direct dependency. |
| `io.ktor:ktor-network-tls-jvm:3.5.1` | Ktor network | Ktor TLS support for HTTPS. | Keep transitive; no TLS type leaks into core. |

The implemented `trawler-postgres` graph additionally resolves these unique
runtime coordinates:

| Coordinate | Introduced by | Purpose | Policy |
|---|---|---|---|
| `org.checkerframework:checker-qual:3.52.0` | PostgreSQL JDBC | Nullness/type-use annotations consumed by the driver. | Keep transitive; do not expose it from core. |
| `org.slf4j:slf4j-api:2.0.17` | HikariCP | Logging API used by the pool. | Adapter transitive only; no logging backend is selected. |
| `tools.jackson.core:jackson-databind:3.1.5` | Flyway Core | Flyway's migration-tool configuration/runtime dependency. | Not a Trawler request/data codec; do not declare directly or expose it from core. |
| `com.fasterxml.jackson.core:jackson-annotations:2.21` | Flyway's Jackson databind | Jackson annotation metadata required by that Flyway graph. | Keep only as Flyway transitive; no core API or produced-value use. |
| `tools.jackson.core:jackson-core:3.1.5` | Flyway's Jackson databind | Flyway's Jackson runtime support. | Keep only as Flyway transitive; remove with Flyway if the migration owner changes. |
| `org.flywaydb:flyway-database-cockroachdb:13.2.0` | Flyway PostgreSQL database support | Flyway's shared database-support graph. | Transitive Flyway implementation artifact; Trawler does not expose or configure CockroachDB. |

The PostgreSQL module also receives the core transitive
`org.jetbrains:annotations:13.0` and
`org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.10.2` entries listed
above. The resolved dependency tree is intentionally retained in this file
so the unavoidable Flyway Jackson transitive is visible rather than mistaken
for an adapter-owned request serializer.

The exact resolved versions and mediation are checked with:

```bash
mvn -pl trawler-web -am dependency:tree -Dscope=runtime -Dverbose
mvn -pl trawler-postgres -am dependency:tree -Dscope=runtime -Dverbose
```

The resolved tree was verified with `mvn dependency:tree`. Maven plugin implementation dependencies are build-tool internals and are not placed on Trawler's application runtime classpath; they are reviewed when an explicitly configured plugin is added or upgraded.

## Current explicitly configured Maven build plugins

| Coordinate | Version | Purpose |
|---|---|---|
| `org.jetbrains.kotlin:kotlin-maven-plugin` | `${kotlin.version}` | Managed by `trawler-parent` and activated by the Kotlin modules; compiles Kotlin main and test sources. |
| `org.apache.maven.plugins:maven-compiler-plugin` | `${maven.compiler.version}` | Managed by `trawler-parent` and activated by the Kotlin modules; enforces JVM release 17 for Java or mixed Kotlin/Java builds. |
| `org.apache.maven.plugins:maven-surefire-plugin` | `${maven.surefire.version}` (`3.5.4`) | Runs unit classes matching `*Test` during `test`; JUnit Platform is explicit and empty test phases remain valid while feature tests are being added. |
| `org.apache.maven.plugins:maven-failsafe-plugin` | `${maven.surefire.version}` (`3.5.4`) | Runs classes matching `*IT` during `integration-test` and verifies them during `verify`; `skipITs` is explicit and defaults to false. |
| `org.jacoco:jacoco-maven-plugin` | `${jacoco.version}` (`0.8.13`) | Attaches the test agent, writes per-module HTML/XML reports at `verify`, and configures the reactor aggregate report. No coverage check gate is enabled yet. |

Maven Central is used through Maven's default repository configuration; the project does not declare a redundant repository entry.

The test plugins are explicitly pinned because test discovery, the `test` versus
`verify` split, and report generation are repository contracts. The core module
now owns deterministic contract tests and JDK file-store `*IT` sources; the
configured empty phases in web and PostgreSQL therefore pass, while a
discovered failing test still fails the build. JaCoCo is report-only until the
final audit phase enables hard thresholds.

## Removed from the unexplained baseline

The following were removed because no implemented component owns them:

| Removed dependency/plugin | Reason |
|---|---|
| `slf4j-api` | Observability exporters belong in adapters; core logging API has not been implemented. |
| `logback-classic` | Application logging backend must not be a mandatory library runtime dependency. |
| `jackson-module-kotlin` | Core must not mandate JSON or serialize `T`. |
| `jackson-datatype-jsr310` | Same ownership violation; no current adapter requires it. |
| `mockk` | No mocking framework is selected; the authorized suite uses small handwritten fakes. |
| `io.ktor:ktor-client-mock-jvm` in core or PostgreSQL | MockEngine belongs only to `trawler-web` tests. |
| `org.testcontainers:postgresql` or `org.testcontainers:junit-jupiter` in core or web | PostgreSQL containers belong only to `trawler-postgres` tests. |
| `exec-maven-plugin` | It referenced a non-existent example main class. Trawler is a library and currently has no runnable application. |

The test dependencies and lifecycle plugins above are now authorized and
owned. Removal still requires deleting the corresponding test sources and
updating [`docs/TESTING.md`](TESTING.md) and [`docs/TEST-MATRIX.md`](TEST-MATRIX.md).

## Dependency admission checklist

Every dependency change must answer these questions in the change description and update this file:

1. Which implemented capability requires it?
2. Which module or adapter owns it?
3. Can the JDK, Kotlin, coroutines, or an existing dependency satisfy the need adequately?
4. What public types, if any, could leak across the owning boundary?
5. What transitive dependencies are introduced?
6. Is the license compatible with the project's eventual distribution model?
7. Is the project maintained and suitable for production use?
8. Are there known security or supply-chain concerns?
9. Is the scope as narrow as possible?
10. How is the dependency removed if its owning feature is removed?

A missing answer blocks addition.

## Version rules

- Pin exact versions through a single property or dependency-management location.
- Do not use version ranges, dynamic versions, `LATEST`, `RELEASE`, snapshots, or local filesystem artifacts.
- Do not scatter the same version across multiple POM files.
- Upgrade one responsibility at a time unless a compatibility matrix requires a coordinated change.
- Read release/migration notes for non-patch upgrades.
- Record behavioral or compatibility changes in the relevant feature documentation or ADR.
- Do not call a version “latest” without verifying it at the time of the change.

## Scope and module rules

- Core dependencies are restricted to dependencies that implement generic engine invariants.
- HTTP clients belong in transport adapters.
- SQL drivers, pools, and migration tools belong in state/response/output adapters that use them.
- Serializers belong only in adapters that encode data.
- Parsers belong in discovery/extraction adapters or examples.
- Logging and telemetry libraries belong in observer adapters.
- Browser and anti-bot libraries belong in their provider adapters.
- Test libraries may be added only after explicit user authorization for tests.
- A dependency used by one example must not become a transitive core dependency.

## Expected future dependency ownership

These are architectural placements for capabilities that are not implemented
in the current delivery. They are not approval to add dependencies now:

| Capability | Expected owner |
|---|---|
| Built-in coroutine scheduling | `trawler-core`; no additional dependency |
| RSS/Atom and sitemap parsing/discovery | `trawler-web`; implemented with JDK StAX/gzip and no new direct dependency |
| PostgreSQL JDBC, HikariCP, Flyway | `trawler-postgres` |
| Jackson or another codec | Only the adapter selecting that codec |
| Jsoup | `trawler-web` or an example only if its actual parser requires it |
| SLF4J | A dedicated observer adapter only if demand justifies the artifact; otherwise host event consumption |
| Micrometer/OpenTelemetry | `trawler-observability-otel` or another dedicated observability artifact |
| FlareSolverr client | `trawler-antibot-flaresolverr` |
| Browser automation | `trawler-transport-browser` |
| JUnit/coroutines-test/MockK | Authorized test scope only |

Do not add any item in this table until its owning implementation is part of the requested task.
