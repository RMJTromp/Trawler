# Extending and Plugins

Trawler does not define one universal plugin interface. It defines focused contracts so that an extension declares exactly which capability it owns. A plugin may be a discovery, typed stage, policy, transport, response store, output, observer, lifecycle component, or durable adapter.

## Choose the extension boundary

| You need to add | Implement |
| --- | --- |
| New source enumeration | Discovery or ResumableDiscovery |
| Response-to-value conversion | Extractor<T> |
| Same-type value operation | Transform<T> |
| Same-type acceptance rule | Validator<T> |
| Typed sink | Output<T> |
| Request/response decoration | RequestMiddleware or ResponseMiddleware |
| Request behavior | One policy interface from policy |
| HTTP execution | Transport in trawler-web or a host-owned transport |
| Durable coordination | CrawlStateStore capability implementation |
| Raw response retention | ResponseStore and relevant capabilities |
| Resource startup/cleanup | ExtensionLifecycle |
| Replay-only resources | ReplayLifecycle |
| Run/event observation | Observer |

If the extension needs an external technology stack, create or use an owning adapter module. Do not place Ktor, JDBC, PostgreSQL, HikariCP, Flyway, parsers, serializers, or logging libraries in trawler-core.

## A plugin owns policy, not coordination

A discovery may enumerate requests, but the engine still controls:

- canonicalization and request identity;
- scope and authorization;
- active-work deduplication;
- queue and work bounds;
- transport routing;
- pacing and concurrency;
- retry and attempt accounting;
- state transitions and checkpoints;
- cancellation.

A plugin that wants to fetch a page uses DiscoveryContext.executor. It must not create a hidden client or loop around a transport call with its own retry policy.

## Identifier discipline

Every ordered extension has a stable nonblank id. IDs are used in configuration diagnostics, events, checkpoint compatibility, and operator investigation. Do not use a random ID or include secrets, target URLs, credentials, or response contents in an ID.

## Lifecycle contract

An ExtensionLifecycle has:

~~~kotlin
interface ExtensionLifecycle {
    val id: String
    suspend fun start(context: ExtensionStartContext)
    suspend fun stop(context: ExtensionStopContext)
}
~~~

The engine starts engine-level extensions in registration order and stops them in reverse order. Pipeline-owned extensions are registered on the pipeline and follow the pipeline lifecycle. Startup failure stops already-started extensions with STARTUP_FAILURE and prevents runtime work.

Lifecycle rules:

- start must be idempotent or reject repeated start deterministically;
- stop must be safe after a failed or never-completed start;
- stop must be cancellation-aware;
- child coroutines must remain below ExtensionStartContext.scope;
- all owned clients, dispatchers, files, database pools, and subscriptions must close;
- stop must not launch detached work to finish later.

Use ReplayLifecycle when a response replay needs resources that must not activate ordinary crawl work. Replay startup must not start transports, schedulers, recovery, or crawl-state mutation.

## A useful plugin implementation sequence

1. Write down the capability and ownership boundary.
2. Identify the narrowest existing interface.
3. Define all bounds and configuration validation.
4. Define start and stop behavior, including partial startup.
5. Decide how failure, cancellation, and stale ownership are represented.
6. Implement the extension using engine-provided clocks, randomness, executors, and scopes.
7. Register the extension explicitly.
8. Add the feature record and dependency inventory entry if applicable.
9. Review logs/events for secrets and unbounded payloads.

## Configuration registration

Engine-level extensions are registered with EngineConfigurationBuilder.extension. Pipeline-level extensions are registered with PipelineBuilder.extension:

~~~kotlin
val engine = EngineConfigurationBuilder()
    .extension(extension)
    .register(
        PipelineBuilder<Int>("numbers")
            .transport(transport)
            .discover(SeedDiscovery(requests = listOf(CrawlRequest("number"))))
            .extract(IntExtractor())
            .extension(extension)
            .build(PipelineDefaults(policies = BuiltInPolicies.ordinary())),
    )
    .build()
    .let(::TrawlerEngine)
~~~

The sample is conceptual and intentionally uses the real ExtensionLifecycle, PipelineBuilder, SeedDiscovery, and other public names. A production implementation should avoid registering one state/response resource as both unrelated lifecycles unless its ownership is explicitly defined.

## Database adapter pattern

A durable adapter generally implements a focused store plus ExtensionLifecycle. The store methods use a bounded adapter-owned dispatcher. Startup runs migrations and validates configuration; stop closes the pool and dispatcher. The engine owns orchestration and passes lease/fencing information into state mutations.

The PostgreSQL implementation is the reference pattern:

- PostgresCrawlStateStore owns state migrations and durable work;
- PostgresResponseStore owns response history;
- PostgresOutput owns typed output history;
- PostgresOutboxOutput owns pending delivery rows;
- each resource is registered explicitly;
- an outbox delegate is not started implicitly by PostgresOutboxOutput.

## Anti-patterns

Avoid:

- GlobalScope;
- Thread.sleep in suspending paths;
- unbounded Flow collection or internal queues;
- hidden retries in transport middleware;
- storing produced values in core state tables;
- reflection or unchecked casts to bypass Pipeline<T>;
- broad exception swallowing;
- writing response bodies or authorization headers into events;
- treating a lifecycle callback as a license to mutate engine state directly.

