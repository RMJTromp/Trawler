# Examples

All examples on this page are conceptual unless the surrounding text says otherwise. They use public Trawler identifiers and safe placeholder targets. Replace host-owned transport, extractor, output, codec, credential, and persistence values with the application implementation.

## Minimal typed in-memory pipeline

~~~kotlin
val pipeline: Pipeline<Int> = pipeline<Int>("numbers") {
    transport(transport)
    discover(SeedDiscovery(requests = listOf(CrawlRequest("number"))))
    extract(extractor)
    output(output, OutputRequirement.REQUIRED)
}

val engine = EngineConfigurationBuilder()
    .register(pipeline)
    .build()
    .let(::TrawlerEngine)

try {
    engine.runOnce(RunRequest(pipelineIds = setOf("numbers"), reason = "manual"))
} finally {
    engine.shutdown()
}
~~~

The host-owned extractor and output are still typed as Extractor<Int> and Output<Int>. The core runtime uses InMemoryCrawlStateStore when no state store is selected.

## Two heterogeneous pipelines

~~~kotlin
val engine = EngineConfigurationBuilder()
    .register<Int>("numbers") {
        transport(transport)
        discover(SeedDiscovery(requests = listOf(CrawlRequest("number"))))
        extract(intExtractor)
        output(intOutput)
    }
    .register<String>("words") {
        transport(transport)
        discover(SeedDiscovery(requests = listOf(CrawlRequest("word"))))
        extract(stringExtractor)
        output(stringOutput)
    }
    .build()
    .let(::TrawlerEngine)
~~~

The engine registry is heterogeneous, but the value-processing path remains typed. An Int output cannot receive the String pipeline's values.

## Four independent daily news sources

This is a conceptual shape for four sources with separate checkpoint ownership:

~~~kotlin
val sourceA = PipelineBuilder<String>("source-a")
    .transport(transport)
    .discover(discoveryA)
    .extract(extractorA)
    .harvest(
        HistoricalThenIncrementalHarvestStrategy(
            boundaryPolicy = FixedOverlapBoundaryPolicy(Duration.ofMinutes(5)),
        ),
    )
    .output(outputA, OutputRequirement.REQUIRED)
    .build(PipelineDefaults(policies = BuiltInPolicies.ordinary()))

val sourceB = PipelineBuilder<String>("source-b")
    .transport(transport)
    .discover(discoveryB)
    .extract(extractorB)
    .harvest(WatermarkHarvestStrategy(boundaryPolicy = FixedOverlapBoundaryPolicy(Duration.ofMinutes(5))))
    .output(outputB, OutputRequirement.REQUIRED)
    .build(PipelineDefaults(policies = BuiltInPolicies.ordinary()))

val engine = EngineConfigurationBuilder()
    .register(sourceA)
    .register(sourceB)
    .register(sourceC)
    .register(sourceD)
    .requireStateCapability(CrawlStateCapability.DISCOVERY_CHECKPOINTS)
    .requireStateCapability(CrawlStateCapability.HARVEST_STATE)
    .build()
    .let(::TrawlerEngine)
~~~

The schedule belongs outside the source boundary. Use ScheduleTrigger.Daily with an explicit ZoneId and a bounded overlap policy. Four source pipelines may share an immutable transport policy bundle, but their discovery IDs, checkpoint codecs, state ownership, and output identity must remain distinct.

## Replace one built-in policy

The core ordinary policies are replaceable. For example, a host can install visible web failure retries while retaining other ordinary policies:

~~~kotlin
val pipeline = PipelineBuilder<String>("web")
    .transport(transport)
    .discover(SeedDiscovery(requests = listOf(CrawlRequest("https://example.invalid/"))))
    .extract(extractor)
    .replaceRetry(DefaultHttpRetryPolicy())
    .output(output, OutputRequirement.REQUIRED)
    .build(PipelineDefaults(policies = BuiltInPolicies.ordinary()))
~~~

Use replaceRetry when the replacement is intentional. A repeated ordinary retry assignment creates a configuration issue. Retry remains visible to engine attempt accounting.

## Safe public web transport

~~~kotlin
val transport = KtorTransport.publicWeb(
    configuration = KtorTransportConfig(
        maxResponseHeaders = 128,
        maxRedirects = 5,
    ),
    preset = PublicWebPresetConfig(
        permitsPerOrigin = 2,
        minimumOriginDelay = Duration.ofMillis(500),
    ),
)
~~~

The public preset blocks private and metadata networks, checks robots, bounds origins, and processes redirects under policy control. Use AllowAllWebDestinationPolicy only for a deliberately trusted internal deployment.

## PostgreSQL durable state

~~~kotlin
val state = PostgresCrawlStateStore(
    configuration = postgresConfiguration,
)

val engine = EngineConfigurationBuilder()
    .stateStore(state)
    .extension(state)
    .register(pipeline)
    .requireStateCapability(CrawlStateCapability.DURABLE)
    .requireStateCapability(CrawlStateCapability.LEASE_FENCING)
    .build()
    .let(::TrawlerEngine)
~~~

Create PostgresStateConfiguration from a secret-managed JDBC URL and password. Do not place the actual URL, password, or host name in source control.

## Response capture and replay

~~~kotlin
val responses = InMemoryResponseStore(maxResponses = 256)

val engine = EngineConfigurationBuilder()
    .responseStore(responses)
    .responsePolicy(CaptureResponsesWhenConfigured)
    .requireResponseCapability(ResponseStoreCapability.REPLAY)
    .register(pipeline)
    .build()
    .let(::TrawlerEngine)

val selection = pipeline.replay(
    ResponseReplayQuery.ByPipelineId("numbers"),
    limit = 32,
)
val replay = engine.replay(selection)
~~~

Replay performs zero network calls and does not create ordinary jobs. InMemoryResponseStore loses data on process restart; use PostgresResponseStore or FileResponseStore when the retention requirement is different.

## Required and best-effort outputs

~~~kotlin
val pipeline = PipelineBuilder<String>("delivery")
    .transport(transport)
    .discover(SeedDiscovery(requests = listOf(CrawlRequest("source"))))
    .extract(extractor)
    .output(primaryOutput, OutputRequirement.REQUIRED)
    .output(metricsOutput, OutputRequirement.BEST_EFFORT)
    .build(PipelineDefaults(policies = BuiltInPolicies.ordinary()))
~~~

The primary sink is part of the pipeline success contract. Metrics output failure is observable but does not itself fail the pipeline. Neither choice claims unique external delivery.

## PostgreSQL outbox duplicate window

~~~kotlin
val summary = outbox.deliverPending(
    ownerId = "delivery-worker",
    limit = 64,
)
~~~

The call is a bounded pass. If the delegate performs its side effect and the process fails before the fenced DELIVERED update, a later pass may invoke the delegate again. Make the delegate idempotent or use IdempotentOutput with a durable application-owned key.

## Deterministic shutdown

~~~kotlin
val engine = TrawlerEngine(configuration)
try {
    val daemon = engine.startDaemon(
        DaemonConfig(
            trigger = ScheduleTrigger.FixedDelay(
                delay = Duration.ofHours(24),
                maxRuns = 1,
            ),
            pipelineIds = setOf("source-a"),
        ),
    )
    daemon.completion.await()
} finally {
    engine.shutdown()
}
~~~

The engine owns the scheduler child, cancels active work, stops extensions in reverse order, and closes resources. Framework integrations should put the same shutdown call in their application close hook.

