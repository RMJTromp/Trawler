# Modules and Dependencies

## Repository modules

The Maven reactor currently contains:

| Artifact | Coordinates | Role |
| --- | --- | --- |
| trawler-parent | com.rmjtromp.trawler:trawler-parent | Parent and dependency management |
| trawler-core | com.rmjtromp.trawler:trawler-core | Typed runtime and lightweight implementations |
| trawler-web | com.rmjtromp.trawler:trawler-web | Ktor-backed web transport and policies |
| trawler-postgres | com.rmjtromp.trawler:trawler-postgres | PostgreSQL adapters and migrations |
| trawler-coverage | com.rmjtromp.trawler:trawler-coverage | Coverage aggregation/build support |

The artifacts are currently versioned 0.1.0-SNAPSHOT. Later specialized adapters are not part of the current reactor.

## Dependency direction

~~~text
host application or example
        -> trawler-web or trawler-postgres
        -> trawler-core
~~~

The reverse direction is forbidden. Core public contracts must not expose adapter types. For example, a core Transport accepts and returns core model objects; it does not expose Ktor HttpClient, JDBC Connection, PostgreSQL rows, or a serializer-specific node.

## Runtime dependency ownership

| Dependency family | Owner | Why |
| --- | --- | --- |
| Kotlin standard library and coroutines | trawler-core | Typed suspending runtime |
| Ktor CIO client | trawler-web | HTTP execution |
| PostgreSQL JDBC driver | trawler-postgres | SQL state and history |
| HikariCP | trawler-postgres | Bounded connection pool |
| Flyway | trawler-postgres | Versioned adapter migrations |
| MockEngine | trawler-web tests | Test-only transport control |
| Testcontainers | trawler-postgres tests | Test-only PostgreSQL integration |

The authoritative inventory, versions, licenses, maintenance notes, and removal plans live in docs/DEPENDENCIES.md. Update that file in the same change as any dependency change.

## Adding a plugin or adapter module

Use a new Maven module only when the implementation introduces an external technology stack or a coherent adapter surface that core consumers should not inherit. A small package boundary alone is not enough.

The workflow is:

1. Read docs/MODULES.md and docs/DEPENDENCIES.md.
2. Define the capability in core only if an arbitrary caller needs the contract.
3. Implement the technology-specific owner in the adapter module.
4. Add the narrowest runtime dependency to that module.
5. Keep technology types out of core public contracts.
6. Add the module to the reactor and document its ownership.
7. Add or update the focused feature record required by docs/DOCUMENTATION.md.
8. Validate configuration before starting background work.
9. Verify cancellation and deterministic close behavior.

Do not create a POM or empty package for a future adapter before its implementation begins.

## Adding a core plugin

A core plugin must be implementable with Kotlin, coroutines, or JDK facilities and must fit an existing focused interface. Examples include:

- Discovery;
- Extractor<T>;
- Transform<T>;
- Validator<T>;
- Output<T>;
- RequestMiddleware or ResponseMiddleware;
- a replaceable policy;
- ExtensionLifecycle;
- a response or state capability implementation.

If it needs an HTTP client, database driver, parser, serializer, browser, logger, or telemetry stack, it belongs in a specialized adapter module.

## Test dependencies

Test-only dependencies must remain test-scoped and documented. The repository distinguishes unit tests named *Test from integration tests named *IT; integration tests are run by the configured Failsafe lifecycle and may require Docker or PostgreSQL. This wiki does not add tests; see [Testing and Quality](Testing-and-Quality) for the repository's verification model.

## Build commands

From the root:

~~~text
mvn -DskipTests clean compile
mvn -pl trawler-core -DskipTests compile
mvn -pl trawler-web -am -DskipTests compile
mvn -pl trawler-postgres -am -DskipTests compile
~~~

Use the focused module command while developing an adapter, then compile the reactor so dependency boundaries are exercised.

