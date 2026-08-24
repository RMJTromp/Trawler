# Trawler

Trawler is a typed, coroutine-based crawling runtime for applications that need controlled discovery, request execution, extraction, transformation, validation, and delivery. The engine coordinates work; extensions supply transport, persistence, response retention, scheduling, and output policy; the host application owns the meaning and storage of produced values.

This wiki is the operational guide for the current repository. It covers the public Kotlin contracts, the built-in database-free runtime, the web and PostgreSQL adapters, lifecycle and cancellation behavior, resumable discovery, replay, outputs, safety controls, and the boundaries of the guarantees.

## Start here

| Goal | Read |
| --- | --- |
| Run a first in-memory pipeline | [Getting Started](Getting-Started) |
| Understand the type boundary | [Core Concepts](Core-Concepts) |
| Configure the fluent DSL | [Pipeline DSL](Pipeline-DSL) and [Configuration](Configuration) |
| Build a plugin or adapter | [Extending and Plugins](Extending-and-Plugins) |
| Crawl public HTTP(S) resources | [Web Adapter](Web-Adapter) and [Security and Network Safety](Security-and-Network-Safety) |
| Use durable PostgreSQL state | [PostgreSQL Adapter](PostgreSQL-Adapter) |
| Resume a feed or incremental source | [Discovery and Follow-Ups](Discovery-and-Follow-Ups) and [Checkpoints and Recurring Harvest](Checkpoints-and-Recurring-Harvest) |
| Schedule recurring work | [Scheduling](Scheduling) |
| Retain and replay responses | [Response Storage and Replay](Response-Storage-and-Replay) |
| Deliver values reliably enough for a chosen sink | [Outputs and Outbox](Outputs-and-Outbox) |
| Diagnose a run | [Events and Observability](Events-and-Observability) and [Troubleshooting](Troubleshooting) |

## Current implementation

The repository currently contains the Phase 6 implementation:

- trawler-core contains the typed pipeline DSL, bounded runtime, built-in policies, in-memory state, response stores, outputs, discovery/checkpoint contracts, replay, retention contracts, events, and the coroutine scheduler.
- trawler-web contains the Ktor CIO transport and web policies for destination safety, robots, redirects, origin pacing/concurrency, cookies, HTTP failure classification, retries, and RSS/Atom/sitemap discovery.
- trawler-postgres contains durable PostgreSQL state, response history, typed output history, an outbox decorator, Flyway migrations, leases, fencing, recovery, checkpoints, harvest state, and retention operations.

The Maven version is 0.1.0-SNAPSHOT. The project is not presenting exactly-once delivery or a universal domain model. The [Guarantees and Limitations](Guarantees-and-Limitations) page is the contract to read before designing a production deployment.

## The governing rule

> Core owns coordination. Extensions own policy. The host owns data.

That rule has concrete consequences:

- A Pipeline<T> keeps T from Extractor<T> through Transform<T>, Validator<T>, and Output<T>.
- TrawlerEngine is intentionally non-generic so one engine can register heterogeneous pipelines without erasing each pipeline's value type during normal processing.
- A response is not a produced value. Response storage is optional and separate from output delivery.
- The ordinary core policy set is permissive. A public-web deployment must explicitly install web safety policies.
- A plugin must use the engine's RequestExecutor for discovery and follow-up requests. It must not hide a second network client behind an extension API.

## Runtime at a glance

~~~mermaid
flowchart LR
    H[Host application] --> C[Pipeline<T> DSL]
    C --> E[TrawlerEngine]
    E --> D[Discovery and checkpoints]
    E --> X[RequestExecutor]
    X --> T[Transport adapter]
    E --> P[Extractor, transforms, validators]
    P --> O[Typed outputs]
    E --> S[Crawl state]
    E --> R[Optional response store]
    E --> Q[Optional scheduler]
~~~

The host decides what T means and where accepted values ultimately live. Trawler supplies bounded coordination, explicit extension points, and observable state transitions.

## Documentation status

Pages use these labels:

- Implemented means the current source and focused repository documentation expose the behavior.
- Conceptual example means the example illustrates composition but should be adapted to the host's types and configuration.
- Planned means the repository does not currently provide the described implementation.

When the older plan text and the implementation differ, current source, module POMs, migrations, tests, and focused feature records are the authority for this wiki. See [Project Status](Project-Status) for the reconciliation.
