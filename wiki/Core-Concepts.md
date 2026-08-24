# Core Concepts

## The ownership rule

Trawler is designed around one separation:

> Core owns coordination. Extensions own policy. The host owns data.

Core coordinates work without deciding what a host's domain records mean. An extension can provide a transport, parser, database, response store, scheduler, or output implementation, but the extension must fit the core contracts and lifecycle. The host supplies the value type and decides what accepted values mean.

## A typed pipeline

Pipeline<T> is the unit that keeps a produced type intact:

~~~text
CrawlRequest
    -> Transport
CrawlResponse
    -> Extractor<T>
T values
    -> Transform<T>*
T values
    -> Validator<T>*
Produced<T>
    -> Output<T>*
~~~

The T is retained from extraction through output. Transform<T> can accept, mutate, or drop a T; Validator<T> can accept, reject, or fail it; Output<T> receives Produced<T>. Core does not convert the value to Any, JSON, a map, or a library-owned database record during normal processing.

TrawlerEngine is non-generic on purpose. It can register an Int pipeline and a String pipeline together, while each Pipeline<T> remains typed. The heterogeneous registry boundary is built during configuration and is not the normal value-processing path.

## Requests and responses

CrawlRequest contains an opaque target, a RequestMethod, immutable Headers, an optional RequestBody, and bounded ContextAttributes. Core does not assume that every target is an HTTP URL.

CrawlResponse contains the originating request, an optional status code, immutable headers, a bounded ResponseBody, a receive timestamp, an optional final target, and attributes. A transport may use statusCode = null for a non-HTTP or transport-specific response.

Headers, RequestBody, ResponseBody, and ContextAttributes are immutable boundary objects. Their constructors and size limits prevent a plugin from silently pushing unbounded bytes into the runtime.

## Contexts

The context types are deliberately stage-specific:

| Context | Used by |
| --- | --- |
| CrawlerContext | Discovery and request admission |
| RequestContext | Request middleware and transport execution |
| ResponseContext | Response middleware |
| ExtractionContext | Extractor<T> |
| TransformContext | Transform<T> |
| ValidationContext | Validator<T> |
| OutputContext | Output<T> |
| ProductionContext | Metadata attached to Produced<T> |

Contexts carry operational metadata and controlled capabilities. They are not a back door to hidden engine state or an invitation to create an unbounded child scope.

## Work identities are different things

The runtime distinguishes:

| Concept | Meaning |
| --- | --- |
| Resource identity | The canonical identity of a crawl target |
| Job identity | One admitted piece of work for a pipeline/resource |
| Lease ownership | The worker currently allowed to mutate a job |
| Execution attempt | One substantive execution of claimed work |
| Run identity | The engine or pipeline run that owns orchestration |

Acquiring a lease is not an attempt. An attempt begins when the worker performs substantive work. This distinction matters for retries, crash recovery, and operator metrics.

## State, responses, and output are separate surfaces

There are three different persistence questions:

1. Crawl state: What work is pending, claimed, completed, failed, cancelled, abandoned, or checkpointed?
2. Response storage: What raw response was retained, under which codec and lineage, for lookup, cache, history, or replay?
3. Produced output: Which typed values were accepted by an output, and which external delivery work remains pending?

Do not use a crawl state table as a produced-value table. Do not enable response capture merely because an output exists. Do not call an output durable merely because the engine recorded a work transition.

## Policy is replaceable

The core pipeline policies cover:

- canonicalization and request identity;
- active-work deduplication and recrawl;
- failure classification and retry;
- pacing and concurrency;
- scope and authorization;
- work ordering and budgets;
- discovery admission;
- eligibility, health, and circuit breaking;
- harvest mode and completion.

BuiltInPolicies.ordinary() supplies permissive defaults for a database-free runtime. In particular, its ordinary scope and authorization policies allow all targets. A web deployment must select WebPresets.publicWebPolicies(...) or another explicitly reviewed policy set.

## Process-local versus durable

InMemoryCrawlStateStore is useful for local runs and deterministic integration with a single process. It uses a process-local mutex, finite in-memory history, bounded queues, and no cross-process lease recovery.

PostgresCrawlStateStore adds durable, multi-process state with transactional claims, lease fencing, recovery, checkpoints, harvest state, and history. It still does not turn an output side effect into exactly-once delivery.

## What Trawler does not own

Core does not define:

- application record schemas;
- database tables for domain values;
- a mandatory serializer;
- a mandatory HTTP client;
- a mandatory response cache;
- a mandatory scheduler;
- logging or telemetry infrastructure;
- browser automation or challenge solving;
- a universal plugin interface that hides all capabilities.

These omissions are intentional. See [Architecture](Architecture) and [Modules and Dependencies](Modules-and-Dependencies).
