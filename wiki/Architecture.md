# Architecture

## Boundary model

Trawler is a small coordination core surrounded by replaceable adapters:

~~~text
Host application
    -> adapter selection and pipeline definitions
        -> trawler-web / trawler-postgres / host-owned extensions
            -> trawler-core
~~~

The valid dependency direction is adapters and integrations toward core. Core must not import Ktor, JDBC, PostgreSQL, HikariCP, Flyway, a serializer, a parser, a logger, or application/domain models.

## Runtime topology

~~~mermaid
flowchart TB
    subgraph Host
        V[Application value type T]
        C[Pipeline<T> configuration]
        A[Application-owned data sink]
    end
    subgraph Core
        E[TrawlerEngine]
        Q[Bounded work queue]
        P[Replaceable policies]
        D[Discovery and harvest]
        X[RequestExecutor]
        L[Typed pipeline stages]
        O[Output<T> fan-out]
        R[Optional response capture and replay]
    end
    subgraph Adapters
        W[KtorTransport and web policies]
        G[PostgreSQL state and history]
        H[Host-owned extension]
    end
    C --> E
    E --> Q
    E --> P
    D --> X
    X --> W
    E --> L
    L --> O
    O --> A
    E --> R
    E --> G
    C --> H
    H --> X
~~~

The graph is intentionally asymmetric. A plugin can receive a controlled request executor from core, but it does not own the engine's queue, leases, retry accounting, or redirect safety.

## Module responsibilities

| Module | Owns | Must not own |
| --- | --- | --- |
| trawler-core | Typed contracts, orchestration, bounded runtime, built-in lightweight policies and stores | HTTP client, database driver, serializer, parser, logging |
| trawler-web | Ktor CIO transport, web safety, robots, redirects, origin controls, XML discovery | Core state schema, application records, hidden transport retries |
| trawler-postgres | PostgreSQL state/response/output/outbox adapters and migrations | Core pipeline types erased into SQL rows, application schema |
| Host application | Domain types, credentials, concrete output meaning, deployment configuration | Bypassing engine-controlled network execution |

The in-memory store, coroutine scheduler, file response store, file output, generic policies, seed discovery, and callback output stay in core because they require only Kotlin, coroutines, or JDK facilities and are logically replaceable.

## One work item

At a high level, a work item passes through these phases:

1. A discovery emits a DiscoveryEmission.
2. The engine canonicalizes the request and evaluates scope, authorization, admission, identity, and deduplication.
3. The state store accepts work, optionally atomically with a checkpoint.
4. A worker claims work and receives a lease/fencing token where the store supports it.
5. The engine acquires concurrency and pacing permissions, then invokes the selected transport through RequestExecutor.
6. The response may be captured by the configured ResponseCapturePolicy.
7. The extractor returns ExtractionResult<T>.
8. Same-type transforms and validators run in declaration order.
9. Accepted values become Produced<T> and are sent to outputs in bounded batches.
10. Follow-ups are admitted through the same request path.
11. The engine records a WorkTransition and releases the lease.

The transport never decides whether a request is a new crawl job. The extractor never writes directly to crawl state. An output never mutates a lease-owned job.

## Inheritance and replacement

Engine defaults are copied into pipeline builders. Pipeline-local ordered components append after inherited components. Single-cardinality components such as transport, extractor, and each policy may be assigned once; a repeated ordinary assignment becomes a deterministic configuration issue. Use the corresponding replace... method to make replacement explicit.

This makes the effective pipeline reviewable before any coroutine starts:

~~~text
Engine defaults
    -> pipeline inheritance
        -> explicit pipeline additions
            -> explicit replacements
                -> validation
                    -> immutable Pipeline<T>
~~~

## State and response paths are independent

State is required for normal crawl coordination; the engine creates an InMemoryCrawlStateStore if no state store is supplied. Response storage is optional. A configured response store does not automatically mean that every deployment has cache, history, replay, or durable retention: the store must advertise the relevant capability and the engine validates required capabilities before startup.

The PostgreSQL adapter keeps Flyway migration histories separate for state, response, output, and outbox schemas. This prevents a response-retention decision from silently changing crawl-state tables.

## Hidden I/O is an architectural defect

An extension is in scope when it implements a declared capability. It is out of scope when it:

- starts a global coroutine;
- creates an unconfigured network client;
- retries inside a transport middleware so the engine cannot count attempts;
- opens a database transaction across a network call;
- mutates a shared application singleton;
- writes secrets into events or logs;
- bypasses the configured clock, randomness, bounds, or cancellation.

## Architecture review questions

Before adding a feature, answer:

1. Does this belong in core for arbitrary non-web, non-SQL callers?
2. Which capability owns the policy?
3. Is the lifecycle start/stop order explicit?
4. Which queues, bodies, batches, and waits are bounded?
5. What happens on cancellation, lease loss, process death, or a stale checkpoint?
6. Can a stale worker mutate state after its lease is reclaimed?
7. Is the delivery guarantee required or best effort?
8. Does a new dependency belong in one owning module and in docs/DEPENDENCIES.md?

