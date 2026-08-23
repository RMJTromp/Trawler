# Dependency Policy and Inventory

> **Status:** Normative
>
> **Rule:** A dependency must be justified by implemented code in its owning module. Planned features do not justify dependencies.

## Current Maven modules

| Module | Packaging | Published runtime artifact | Purpose |
|---|---|---|---|
| `trawler-parent` | `pom` | No | Reactor, version, dependency, and plugin management. It has no runtime dependencies. |
| `trawler-core` | `jar` | Yes, once releases begin | Generic contracts/engine and lightweight official implementations. |
| `trawler-web` | `jar` | Yes, once releases begin | Optional Ktor/CIO HTTP transport and replaceable web policies. |

Future module ownership is defined in [`MODULES.md`](MODULES.md). Modules are not created before their implementation begins.

The ownership and purpose descriptions below include approved future core
capabilities. Phase 1 contracts and the Phase 2 database-free runtime are
implemented in `trawler-core`; Phase 3 adds the implemented optional
`trawler-web` adapter. Durable persistence and later specialized adapters
remain planned and do not justify additional dependencies.

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

The only new direct external technology dependency is Ktor CIO. Ktor is Apache
License 2.0, is pinned through one parent property, and is maintained in its
upstream repository. The web adapter's direct coroutines version is an explicit
compatibility alignment with Ktor; `trawler-core` continues to resolve its
existing `coroutines.version` (`1.10.2`) independently. The resolved graphs are
reviewed with the dependency-tree command below; no retry,
content-negotiation, Jackson, parser, database, browser, telemetry, or test
artifact is declared directly. Removing `trawler-web` removes the Ktor graph
without changing the core dependency tree.

Phase 2 added no dependency or Maven-module change. The in-memory state and
response stores, callback/flow outputs, generic policies, and coroutine
scheduler use only the already-approved Kotlin standard library, coroutines,
and JDK APIs. No serializer, database, HTTP, parser, logging, telemetry,
browser, or anti-bot type enters core.

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

The exact resolved versions and mediation are checked with:

```bash
mvn -pl trawler-web -am dependency:tree -Dscope=runtime -Dverbose
```

The resolved tree was verified with `mvn dependency:tree`. Maven plugin implementation dependencies are build-tool internals and are not placed on Trawler's application runtime classpath; they are reviewed when an explicitly configured plugin is added or upgraded.

## Current explicitly configured Maven build plugins

| Coordinate | Version | Purpose |
|---|---|---|
| `org.jetbrains.kotlin:kotlin-maven-plugin` | `${kotlin.version}` | Managed by `trawler-parent` and activated by `trawler-core`; compiles Kotlin main and, if later authorized, test sources. |
| `org.apache.maven.plugins:maven-compiler-plugin` | `${maven.compiler.version}` | Managed by `trawler-parent` and activated by `trawler-core`; enforces JVM release 17 for Java or mixed Kotlin/Java builds. |

Maven Central is used through Maven's default repository configuration; the project does not declare a redundant repository entry.

Maven may still invoke lifecycle-default plugins (for example resources, Surefire, JAR, and clean plugins) even when they are not explicitly declared in this POM. With no authorized test sources or test dependencies, the default test phase discovers no project tests. Pin a lifecycle plugin explicitly only when the project needs configuration or reproducibility beyond Maven's effective defaults, and document that decision here.

## Removed from the unexplained baseline

The following were removed because no implemented component owns them:

| Removed dependency/plugin | Reason |
|---|---|
| `slf4j-api` | Observability exporters belong in adapters; core logging API has not been implemented. |
| `logback-classic` | Application logging backend must not be a mandatory library runtime dependency. |
| `jackson-module-kotlin` | Core must not mandate JSON or serialize `T`. |
| `jackson-datatype-jsr310` | Same ownership violation; no current adapter requires it. |
| `kotlin-test-junit5` | Automated tests have not been authorized. |
| `junit-jupiter` | Automated tests have not been authorized. |
| `kotlinx-coroutines-test` | Automated tests have not been authorized. |
| `mockk` | Automated tests have not been authorized. |
| Explicit `maven-surefire-plugin` declaration | No authorized unit-test suite exists; Maven's lifecycle default may still execute an empty test phase. |
| Explicit `maven-failsafe-plugin` declaration | No authorized integration-test suite exists. |
| `exec-maven-plugin` | It referenced a non-existent example main class. Trawler is a library and currently has no runnable application. |

Removal does not forbid a future adapter or authorized test suite from adding the narrow dependencies it genuinely needs.

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
| RSS/Atom and sitemap parsing/discovery | `trawler-web`; dependencies added only if the Phase 5 implementation requires them |
| PostgreSQL JDBC, HikariCP, Flyway | `trawler-postgres` |
| Jackson or another codec | Only the adapter selecting that codec |
| Jsoup | `trawler-web` or an example only if its actual parser requires it |
| SLF4J | A dedicated observer adapter only if demand justifies the artifact; otherwise host event consumption |
| Micrometer/OpenTelemetry | `trawler-observability-otel` or another dedicated observability artifact |
| FlareSolverr client | `trawler-antibot-flaresolverr` |
| Browser automation | `trawler-transport-browser` |
| JUnit/coroutines-test/MockK | Authorized test scope only |

Do not add any item in this table until its owning implementation is part of the requested task.
