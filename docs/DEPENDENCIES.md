# Dependency Policy and Inventory

> **Status:** Normative
>
> **Rule:** A dependency must be justified by implemented code in its owning module. Planned features do not justify dependencies.

## Current Maven modules

| Module | Packaging | Published runtime artifact | Purpose |
|---|---|---|---|
| `trawler-parent` | `pom` | No | Reactor, version, dependency, and plugin management. It has no runtime dependencies. |
| `trawler-core` | `jar` | Yes, once releases begin | Generic contracts/engine and lightweight official implementations. |

Future module ownership is defined in [`MODULES.md`](MODULES.md). Modules are not created before their implementation begins.

All project modules use the group ID `com.rmjtromp.trawler` and inherit the coordinated version from `trawler-parent`.

## Current direct runtime dependencies

| Coordinate | Version source | Scope | Owner | Purpose | Why retained |
|---|---|---|---|---|---|
| `org.jetbrains.kotlin:kotlin-stdlib` | `kotlin.version` in parent `pom.xml` | Compile | `trawler-core` | Kotlin runtime and standard APIs. | Required for all Kotlin production code. |
| `org.jetbrains.kotlinx:kotlinx-coroutines-core` | `coroutines.version` in parent `pom.xml` | Compile | `trawler-core` | Structured coroutine scopes, cancellation, suspending lifecycle, `Flow`, channels, synchronization primitives, and the bundled scheduler. | Structured concurrency is a fixed core invariant. |

No other runtime dependencies are approved at present.

## Resolved transitive runtime dependencies

The following are not declared directly. They are present because Maven resolves them from the approved direct dependencies:

| Coordinate | Introduced by | Purpose | Policy |
|---|---|---|---|
| `org.jetbrains:annotations:13.0` | Kotlin standard library | JVM annotation metadata used by Kotlin/JVM interoperability. | Do not declare directly unless a future implementation uses its API explicitly and documents that need. |
| `org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.10.2` | Kotlin coroutines core | JVM implementation selected by the multiplatform coroutines artifact. | Version follows `coroutines.version`; do not declare a second independent version. |

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

These are architectural placements, not approval to add dependencies now:

| Capability | Expected owner |
|---|---|
| Built-in coroutine scheduling | `trawler-core`; no additional dependency |
| Ktor client/CIO | `trawler-web` |
| Robots, public-web safety, RSS/Atom, sitemap | `trawler-web`; dependencies added only if implementation requires them |
| PostgreSQL JDBC, HikariCP, Flyway | `trawler-postgres` |
| Jackson or another codec | Only the adapter selecting that codec |
| Jsoup | `trawler-web` or an example only if its actual parser requires it |
| SLF4J | A dedicated observer adapter only if demand justifies the artifact; otherwise host event consumption |
| Micrometer/OpenTelemetry | `trawler-observability-otel` or another dedicated observability artifact |
| FlareSolverr client | `trawler-antibot-flaresolverr` |
| Browser automation | `trawler-transport-browser` |
| JUnit/coroutines-test/MockK | Authorized test scope only |

Do not add any item in this table until its owning implementation is part of the requested task.
