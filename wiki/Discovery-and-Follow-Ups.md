# Discovery and Follow-Ups

Discovery turns a source into candidate work. It is separate from transport and state. The engine owns the request execution surface and applies normal admission, identity, scope, authorization, deduplication, bounds, and checkpoint rules to every emitted request.

## Discovery contract

Discovery has an id and returns a cancellable Kotlin Flow of DiscoveryEmission:

~~~kotlin
interface Discovery {
    val id: String
    fun discover(context: DiscoveryContext): Flow<DiscoveryEmission>
}
~~~

DiscoveryContext contains CrawlerContext, an engine-provided RequestExecutor, an optional compatible DiscoveryCheckpoint, and an optional incremental window.

The discovery's Flow is collected in the engine-owned structured scope. A discovery must honor cancellation and must not create an unbounded buffer.

## Built-in discovery

SeedDiscovery copies a finite list of CrawlRequest values and bounds emissions with maxEmissions. FlowDiscovery adapts a caller-owned Flow<CrawlRequest> and requires a positive maxEmissions bound. Both emit DiscoveryEmission.Request.

~~~kotlin
val seeds = SeedDiscovery(
    id = "seeds",
    requests = listOf(
        CrawlRequest("source-a"),
        CrawlRequest("source-b"),
    ),
    maxEmissions = 2,
)
~~~

The targets are deliberately host-defined in core. The web adapter supplies HTTP-specific discoveries such as RssDiscovery, AtomDiscovery, and SitemapDiscovery.

## Discovery emissions

DiscoveryEmission has two useful forms:

| Emission | Meaning |
| --- | --- |
| Request | The engine may admit and fetch a new request |
| ExistingResponse | Discovery already obtained the response through the engine executor; the engine must not fetch it again |

Both can carry a DiscoveryCheckpointCandidate. The candidate is not advanced merely because it was emitted. The engine advances it only after the associated work is durably accepted, or applies the documented at-least-once fallback when the state store lacks atomic enqueue-and-checkpoint support.

## Engine-controlled follow-ups

An Extractor<T> can return ExtractionResult.Success with values and followUps. Follow-up CrawlRequest values:

1. are bounded by maxFollowUpRequestsPerWorkItem;
2. pass through the same canonicalization, identity, scope, authorization, deduplication, and admission path as discovery requests;
3. are accepted by state before the current work transition is finalized;
4. inherit engine policies rather than creating a private transport loop.

This makes an extractor a source of typed values and bounded next work, not a second scheduler.

## Pagination

PaginationStrategy is a focused hook for a discovery that has already obtained one response. It returns PaginationDecision.Complete or PaginationDecision.Next(request). The owning discovery controls page bounds and invokes the engine RequestExecutor for the next page.

Do not implement pagination by recursively calling a raw HTTP client. Doing so hides retries, attempt counts, redirects, robots, origin limits, and cancellation from the engine.

## Resumable discovery

ResumableDiscovery adds a DiscoveryCheckpointCodec. The codec owns:

- the stable codec ID and version;
- the strategy ID;
- the opaque payload interpretation;
- migration, reset, or quarantine decisions.

Core stores metadata such as codec ID, version, strategy, revision, completion, and generation. Core does not parse the payload.

A checkpoint codec must be deterministic and bounded. It must never use checkpoint payloads to smuggle a response body, credentials, arbitrary unbounded JSON, or a database connection.

## Web XML discoveries

The web module provides:

- RssDiscovery for RSS feeds;
- AtomDiscovery for Atom feeds;
- SitemapDiscovery for sitemap indexes and URL sets;
- XmlDiscoveryBounds for document, item, URL, depth, and document-count limits;
- XmlCursorCheckpointCodec for cursor-compatible feed state.

These discoveries use engine-controlled transport and the StAX parser with DTD/external entities disabled. See [Web Adapter](Web-Adapter) and [Security and Network Safety](Security-and-Network-Safety).

## Discovery design checklist

- Is the source finite or explicitly bounded?
- Does the discovery use DiscoveryContext.executor for every request?
- Are emitted targets bounded in size?
- Is the order meaningful and documented?
- Is a checkpoint needed to avoid rereading or to support incremental harvest?
- Is the checkpoint codec versioned and migration-safe?
- Can the source emit duplicate requests, and does deduplication handle them?
- Does cancellation stop upstream collection?
- Are follow-ups bounded and admitted through the engine?

