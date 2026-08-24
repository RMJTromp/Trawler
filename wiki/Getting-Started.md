# Getting Started

This page gets a host application from an empty checkout to one explicitly configured, typed run. The code samples are conceptual examples: the public Trawler identifiers are real, but the application-defined transport, extractor, output, and value types must be supplied by the host.

## Prerequisites

The repository is a Kotlin/Maven multi-module project. The normal baseline is:

- JDK 17 or newer;
- Maven 3.9 or newer;
- Kotlin 2.3.0 as managed by the root POM;
- kotlinx-coroutines-core as managed by the root POM.

The core module has no HTTP client, parser, database, serializer, or logging dependency. Add trawler-web when the application needs the Ktor CIO transport, and add trawler-postgres when it needs PostgreSQL-backed state or history.

From the repository root:

~~~text
mvn -DskipTests clean compile
~~~

The full test commands are documented in [Testing and Quality](Testing-and-Quality). This wiki task does not add or authorize new tests.

## Choose the smallest module

| Need | Module |
| --- | --- |
| Typed coordination, in-memory state, basic discovery, outputs, replay contracts | trawler-core |
| HTTP(S), web safety, robots, redirects, RSS/Atom/sitemap | trawler-web |
| Durable state, leases, checkpoints, response history, output history, outbox | trawler-postgres |

Use the web adapter together with core. Use the PostgreSQL adapter together with core; it does not replace the engine or the typed pipeline.

## Minimal typed pipeline

The following is the shape of a database-free pipeline. StaticTransport, IntExtractor, StringExtractor, and IntOutput are application-owned implementations of existing Trawler interfaces; they are shown only to make ownership obvious.

~~~kotlin
val pipeline: Pipeline<Int> = pipeline<Int>("numbers") {
    transport(transport)
    discover(SeedDiscovery(requests = listOf(CrawlRequest("application-target"))))
    extract(IntExtractor())
    output(IntOutput(), OutputRequirement.REQUIRED)
}

val engine = EngineConfigurationBuilder()
    .register(pipeline)
    .build()
    .let(::TrawlerEngine)

val result = engine.runOnce(RunRequest(pipelineIds = setOf("numbers"), reason = "manual"))
engine.shutdown()
~~~

CrawlRequest.target is an opaque string in core. It can be a URL, an API route, a queue key, or another host-defined identity. A transport adapter decides what targets it understands.

For a real HTTP pipeline, replace the host transport with KtorTransport and install explicit web policies. See [Web Adapter](Web-Adapter).

## The smallest extractor and output contracts

The real public contracts are Extractor<T> and Output<T>. They are suspending so implementations can perform bounded asynchronous work and observe cancellation.

~~~kotlin
private class IntExtractor : Extractor<Int> {
    override val id: String = "int-extractor"

    override suspend fun extract(
        response: CrawlResponse,
        context: ExtractionContext,
    ): ExtractionResult<Int> =
        ExtractionResult.Success(
            listOf(response.body.toByteArray().decodeToString().toInt()),
        )
}

private class IntOutput : Output<Int> {
    override val id: String = "int-output"

    override suspend fun write(
        items: List<Produced<Int>>,
        context: OutputContext,
    ): OutputResult = OutputResult.Accepted(items.size)
}
~~~

This pattern is also used by the repository's public DSL compile-usage coverage. An output receives Produced<Int>, not a map, JSON envelope, or untyped record.

## Run lifecycle

TrawlerEngine does not start background work when it is constructed. A direct runOnce creates one engine run, selects pipelines, admits discoveries, executes bounded work, writes outputs, and returns a RunResult. A scheduler is opt-in through startDaemon.

Always close the engine:

~~~kotlin
val engine = TrawlerEngine { register(pipeline) }
try {
    engine.runOnce()
} finally {
    engine.shutdown()
}
~~~

The shutdown call cancels active work, stops extensions in reverse order, closes owned resources, and waits within the configured shutdown timeout. See [Lifecycle, Concurrency, and Cancellation](Lifecycle-Concurrency-and-Cancellation).

## Selective runs

RunRequest can narrow a run to a set of registered pipeline IDs:

~~~kotlin
val request = RunRequest(
    pipelineIds = setOf("numbers"),
    reason = "operator-request",
)
val result = engine.runOnce(request)
~~~

An unknown pipeline ID is a configuration/request error. A pipeline selection is not a new engine or a new type boundary; the registered pipeline object remains the owner of T.

## Before production

Before moving beyond a local run:

1. Decide whether process-local in-memory state is sufficient.
2. Select a response capture policy separately from output delivery.
3. Replace ordinary allow-all policies with domain-appropriate scope, authorization, pacing, concurrency, and failure policies.
4. Bound request, response, discovery, output, and checkpoint payload sizes.
5. Choose required versus best-effort outputs explicitly.
6. Define retention for jobs, responses, outputs, outbox rows, checkpoints, and event archives.
7. Decide how a deployment will recover abandoned work and how operators will observe it.
8. Read [Guarantees and Limitations](Guarantees-and-Limitations) before promising delivery semantics to downstream consumers.
