# Events and Observability

Trawler exposes a bounded event observation path for operational visibility. Events are not automatically a durable audit log.

## Event categories

CrawlerEventType covers:

| Area | Examples |
| --- | --- |
| Engine and pipeline runs | ENGINE_RUN_STARTED, ENGINE_RUN_COMPLETED, PIPELINE_RUN_FAILED |
| Discovery | DISCOVERY_STARTED, DISCOVERY_EMITTED, DISCOVERY_CHECKPOINTED, DISCOVERY_COMPLETED |
| Work | REQUEST_QUEUED, REQUEST_CLAIMED, REQUEST_STARTED, REQUEST_REDIRECTED, REQUEST_COMPLETED, REQUEST_FAILED |
| Retry and pacing | RETRY_SCHEDULED, COOLDOWN_SCHEDULED |
| Typed stages | EXTRACTION_STARTED, EXTRACTION_COMPLETED, VALUE_PRODUCED, VALUE_TRANSFORMED, VALUE_DROPPED, VALUE_ACCEPTED |
| Delivery and responses | DELIVERY_FAILED, RESPONSE_STORED, RESPONSE_STORE_FAILED |
| Configuration | CONFIGURATION_REJECTED, CAPABILITY_WARNING |
| Scheduling | SCHEDULER_TRIGGERED, SCHEDULER_TRIGGER_SKIPPED, SCHEDULER_TRIGGER_REJECTED, SCHEDULER_RUN_FAILED |

Run terminal events distinguish completed, abandoned, failed, and cancelled.

## Event shape

CrawlerEvent contains a type, EventContext, and optional bounded detail. EventContext carries safe identifiers such as engineRunId, pipelineRunId, pipelineId, requestId, workItemId, time, and bounded ContextAttributes.

Events do not carry produced values, response bodies, serializers, secrets, cookies, or authorization headers. Details must be redacted by the producer.

## Observers

Register an Observer with EngineConfigurationBuilder.observe. Observers receive events in registration order in the engine-owned structured scope. An observer returns ObserverResult.Accepted, Dropped, or Failed.

An observer should:

- remain fast or use a bounded queue it owns;
- preserve cancellation;
- avoid network/database work unless that capability is explicit and bounded;
- avoid blocking worker progress with an unbounded sink;
- classify failures without throwing away the original cause;
- keep sensitive details out of the event path.

If event delivery must be durable, implement or select an explicit observer adapter with a documented retention and delivery contract. A live observer can be lossy during process failure or cancellation.

## Metrics and logs

The core event path is not a metrics implementation. Hosts can derive metrics from events or state queries, but should avoid double-counting retries as completed work. Recommended dimensions are pipeline ID, stage, outcome, adapter ID, and safe policy ID.

Do not log full targets if they may contain credentials or personal data. Prefer a request identity or redacted target class. Do not log raw response bodies by default.

## What to watch

For a local run:

- run status and pipeline status;
- accepted, suppressed, started, produced, and delivered counts;
- failure stages and safe categories;
- response capture failures;
- output failure requirement;
- queue bound and run timeout.

For PostgreSQL:

- stale runs and reclaimed leases;
- attempts by failure classification;
- checkpoint conflicts and quarantine;
- active job count and claim latency;
- response/output retention backlog;
- outbox PENDING, retry, FAILED, DELIVERED, and stale counts;
- pool utilization and statement timeout failures.

## Event privacy review

Before installing an observer, inspect every producer detail and attribute. Verify that:

1. IDs are bounded and non-secret.
2. Target values are redacted or omitted where required.
3. Cookies and authorization are absent.
4. Response and produced payloads are not embedded.
5. Retention and access controls match the event sink.

