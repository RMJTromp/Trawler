# Configuration

Configuration is a deterministic phase. Build pipelines and the engine configuration first; call validation or build; only then create a TrawlerEngine and start runs or daemons. Configuration failures should be visible before background work begins.

## Engine builder

EngineConfigurationBuilder provides these groups:

| Group | Methods |
| --- | --- |
| State | stateStore, replaceStateStore |
| Responses | responseStore, replaceResponseStore, responsePolicy, replaceResponsePolicy, responseCache, replaceResponseCache |
| Inherited transport and policies | transportRouter, replaceTransportRouter, policies, replacePolicies |
| Middleware | addRequestMiddleware, addResponseMiddleware, replaceRequestMiddleware, replaceResponseMiddleware |
| Runtime | runtime, replaceRuntime, clock, replaceClock, randomSource, replaceRandomSource |
| Observability and lifecycle | observe, extension |
| Capability checks | requireStateCapability, requireResponseCapability |
| Registration | register(pipeline), register<T>(id, configure) |
| Validation | validate, build |

The builder is configuration-thread-confined. Do not mutate or reuse it concurrently.

## Defaults

If no state store is supplied, EngineConfiguration creates an InMemoryCrawlStateStore using relevant runtime bounds. If no policy bundle is supplied, the builder uses BuiltInPolicies.ordinary(runtimeOptions.workerCount, clock).

The ordinary bundle includes identity canonicalization, SHA-256 request identity, active-work-only deduplication, always-recrawl behavior, generic failure classification, no retry, immediate pacing, fixed concurrency, allow-all scope and authorization, FIFO ordering, an engine-bounded unlimited budget, continuous discovery admission, always-eligible pipelines, no-op health, and a never-open circuit breaker.

These are useful core defaults, not a public-web safety profile. Select web policies explicitly for public HTTP(S).

## Bounded runtime defaults

EngineRuntimeOptions defaults:

| Option | Default |
| --- | ---: |
| maxQueueSize | 1,024 |
| maxWorkItemsPerRun | 10,000 |
| maxDiscoveryEmissionsPerDiscovery | 1,024 |
| maxDiscoveryEmissionsPerRun | 10,000 |
| maxFollowUpRequestsPerWorkItem | 128 |
| maxValuesPerResponse | 1,024 |
| maxRequestBodyBytes | 8 MiB |
| maxResponseBodyBytes | 8 MiB |
| maxTargetBytes | 16 KiB |
| maxMethodBytes | 256 |
| maxHeaderCount | 256 |
| maxHeaderNameBytes | 1 KiB |
| maxHeaderValueBytes | 16 KiB |
| maxAttributeCount | 256 |
| maxAttributeNameBytes | 1 KiB |
| maxAttributeValueBytes | 16 KiB |
| maxProducedValuesPerRun | 100,000 |
| maxAttemptsPerWorkItem | 3 |
| maxPendingRuns | 64 |
| workerCount | 4 |
| claimBatchSize | 1 |
| maxOutputBatchSize | 128 |
| maxReplayResponsesPerRun | 1,024 |
| leaseDuration | 5 minutes |
| runTimeout | 15 minutes |
| awaitIdleTimeout | 30 seconds |
| shutdownTimeout | 10 seconds |
| maxRetainedJobs | 8,192 |
| maxRetainedResources | 8,192 |
| maxRetainedDiscoveryState | 2,048 |
| maxCheckpointPayloadBytes | 2 MiB |

All positive and bounded values are validated. Retained jobs and resources must be at least the queue size.

## Example: explicit bounds and requirements

This is a conceptual composition using real public identifiers:

~~~kotlin
val runtime = EngineRuntimeOptions(
    workerCount = 2,
    maxQueueSize = 256,
    maxResponseBodyBytes = 2 * 1024 * 1024,
    maxProducedValuesPerRun = 10_000,
)

val engine = EngineConfigurationBuilder()
    .runtime(runtime)
    .register(pipeline)
    .requireStateCapability(CrawlStateCapability.WORK_QUEUE)
    .build()
    .let(::TrawlerEngine)
~~~

Capability requirements are a configuration contract. For example, requiring DISCOVERY_CHECKPOINTS with InMemoryCrawlStateStore is rejected if the store does not advertise it. Requiring ResponseStoreCapability.REPLAY without a replay-capable response store is also rejected.

## Time and randomness

The engine accepts a Clock and RandomSource. Inject them when a policy's time or jitter affects behavior, especially for tests, retry backoff, recrawl windows, scheduler calculations, or retention. The default is SystemClock and DefaultRandomSource.

Runtime timeouts are Duration bounds for lifecycle decisions. Scheduler trigger calculations use the configured Clock; actual waiting remains cancellable coroutine delay.

## Pipeline inheritance

A PipelineBuilder<T> inherits the engine's default transport router, policies, and middleware. It can append ordered stages or replace single-cardinality values explicitly:

~~~kotlin
val pipeline = PipelineBuilder<Int>("numbers")
    .transport(transport)
    .discover(SeedDiscovery(requests = listOf(CrawlRequest("number"))))
    .extract(IntExtractor())
    .replaceRetry(NoRetryPolicy)
    .output(IntOutput(), OutputRequirement.REQUIRED)
    .build(PipelineDefaults(policies = BuiltInPolicies.ordinary()))
~~~

The exact policy replacement depends on the policy interface being replaced. See [Pipeline DSL](Pipeline-DSL) for the full surface.

## Configuration errors

Common deterministic errors include:

- no effective transport router;
- no typed extractor;
- no effective policy bundle;
- duplicate IDs in one ordered collection;
- repeated single-cardinality assignment without replace...;
- unsupported required state or response capability;
- incompatible ordering for a state adapter;
- invalid runtime bounds;
- invalid or incomplete PostgreSQL configuration.

Treat InvalidConfigurationException as a startup failure. Do not start worker scopes and hope a runtime error will resolve it.

