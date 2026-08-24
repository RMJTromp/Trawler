# Pipeline DSL

The DSL describes one typed pipeline. It does not start work. A pipeline is immutable after build, and the builder validates its effective configuration before returning it.

## Fluent form

~~~kotlin
val pipeline: Pipeline<Int> = pipeline<Int>("numbers") {
    transport(transport)
    discover(SeedDiscovery(requests = listOf(CrawlRequest("number"))))
    extract(IntExtractor())
    transform(object : Transform<Int> {
        override val id: String = "identity-transform"

        override suspend fun apply(value: Int, context: TransformContext) =
            TransformDecision.Accept(value)
    })
    validator(object : Validator<Int> {
        override val id: String = "positive-validator"

        override suspend fun validate(value: Int, context: ValidationContext) =
            if (value > 0) ValidationResult.Accepted
            else ValidationResult.Rejected("not positive")
    })
    output(IntOutput(), OutputRequirement.REQUIRED)
}
~~~

The pipeline<T>(id) helper builds with ordinary defaults. If the application needs different defaults, use PipelineBuilder<T> and pass PipelineDefaults to build.

## Required stages

A build requires exactly one effective transport router, one typed extractor, and one effective PipelinePolicies bundle. A pipeline may have zero discoveries, transforms, validators, or outputs, but a pipeline without discovery will not admit new work during a run unless work already exists in its state store.

The transport can be provided as a Transport or a TransportRouter. Use a router when different request kinds or target families need different transports.

## Ordered components

These are ordered and append in declaration order:

- discoveries;
- request middleware;
- response middleware;
- transforms;
- validators;
- output bindings;
- lifecycle extensions.

Pipeline-local middleware is appended after inherited engine middleware. replaceRequestMiddleware and replaceResponseMiddleware discard inherited middleware for that chain before local additions are appended.

## Single-cardinality components

These accept one effective value:

- transport router;
- extractor;
- policy bundle;
- each individual policy;
- harvest strategy.

Calling transport twice, for example, records a deterministic configuration issue. Calling replaceTransport communicates intentional replacement. The same pattern exists for replaceExtractor, replacePolicies, replaceRetry, replaceScope, replaceHarvest, and the other replacement methods.

## Policy surface

PipelineBuilder<T> exposes these policy setters:

| Concern | Set | Replace |
| --- | --- | --- |
| Canonicalization | canonicalize | replaceCanonicalizer |
| Identity | identity | replaceIdentity |
| Deduplication | deduplicate | replaceDeduplication |
| Recrawl | recrawl | replaceRecrawl |
| Failure classification | classifyFailures | replaceFailureClassifier |
| Retry | retry | replaceRetry |
| Pacing | pacing | replacePacing |
| Concurrency | concurrency | replaceConcurrency |
| Scope | scope | replaceScope |
| Authorization | authorize | replaceAuthorization |
| Ordering | ordering | replaceOrdering |
| Budget | budget | replaceBudget |
| Discovery admission | admission | replaceAdmission |
| Eligibility | eligibility | replaceEligibility |
| Health | health | replaceHealth |
| Circuit breaking | circuitBreaker | replaceCircuitBreaker |
| Harvest | harvest | replaceHarvest |

The API separates policy selection from the engine loop. A custom policy should return a typed decision, preserve cancellation, and avoid hidden I/O.

## Output requirements

An output is bound with OutputRequirement.REQUIRED or OutputRequirement.BEST_EFFORT. Required output failure affects the pipeline result; best-effort failure is recorded without making that sink a prerequisite for the pipeline's successful work completion. The exact duplicate semantics remain output-specific.

Multiple outputs receive the same typed values through an ordered fan-out. One output's persistence mechanism does not make another output durable.

## Heterogeneous registration

The engine can register different typed pipelines:

~~~kotlin
val engine = EngineConfigurationBuilder()
    .register<Int>("numbers") {
        transport(transport)
        discover(SeedDiscovery(requests = listOf(CrawlRequest("number"))))
        extract(IntExtractor())
    }
    .register<String>("words") {
        transport(transport)
        discover(SeedDiscovery(requests = listOf(CrawlRequest("word"))))
        extract(StringExtractor())
    }
    .build()
    .let(::TrawlerEngine)
~~~

The registry is heterogeneous, but each registration creates a typed owner. There is no public Any output path.

## Build and inspect

Use validationIssues when you want deterministic diagnostics without constructing a pipeline, or build when the result is ready:

~~~kotlin
val builder = PipelineBuilder<Int>("numbers")
val issues = builder.validationIssues(PipelineDefaults(policies = BuiltInPolicies.ordinary()))
~~~

The builder remains configuration-only. Start an engine only after the issues are empty and all required adapters have been registered.

