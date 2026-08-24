# Guarantees and Limitations

This page is the short guarantee matrix. Read the focused pages for configuration and lifecycle details.

## Guarantee matrix

| Area | Current guarantee | Not guaranteed |
| --- | --- | --- |
| Typed values | Pipeline<T> preserves T through extraction, transforms, validation, and output | A universal domain record or serializer |
| Engine registry | Heterogeneous registration is type-safe at the pipeline owner boundary | A generic TrawlerEngine |
| In-memory state | Bounded process-local coordination with focused transitions and lease semantics | Restart durability, multi-process fencing, durable run recovery |
| PostgreSQL state | Durable transactions, multi-process leases, fencing, attempts, checkpoints, recovery, history | Exactly-once external side effects |
| Discovery | Bounded, cancellable flows and engine-controlled request admission | A source's correctness or duplicate-free semantics |
| Atomic checkpoints | Atomic enqueue/checkpoint when the store advertises the capability | Exactly-once source progression without that capability |
| Response replay | Zero network calls, no ordinary crawl jobs, typed pipeline ownership | Duplicate-free external output |
| Web redirects | Every hop rechecks safety, robots, pacing, concurrency, and credential forwarding | DNS-to-socket pinning in PublicWebSafetyPolicy |
| HTTP retry | Visible core attempts when DefaultHttpRetryPolicy is selected | Hidden retry-free behavior for every custom adapter |
| Outputs | Explicit Accepted, Skipped, Failed and REQUIRED/BEST_EFFORT semantics | Exactly-once delivery by default |
| PostgreSQL outbox | Durable pending rows, bounded claims, fencing, explicit retries | Atomic transaction with an arbitrary external delegate |
| Scheduler | Structured, cancellable, bounded overlap and trigger queues | A guarantee that a run finishes before the next trigger |
| Events | Bounded live observation with safe metadata | Durable audit history unless a durable observer is selected |
| Retention | Adapter-owned bounded cleanup and protected references where supported | One global retention policy across all data |

## Delivery language

Use:

- “durably enqueued” when a transaction committed a pending state;
- “accepted by the configured filesystem” when FileOutput forced a frame;
- “acknowledged by the output” when OutputResult.Accepted was returned;
- “best effort” when failure does not fail the pipeline;
- “at least once with a duplicate window” when a crash can repeat a side effect.

Avoid:

- exactly once;
- never duplicated;
- guaranteed delivery;
- durable response retention when the store is in memory;
- replay-safe when a resource can start ordinary network work;
- completed when the run was abandoned, cancelled, or failed.

## Boundary limitations

Trawler cannot infer:

- whether a source cursor is inclusive;
- whether a timestamp is stable or mutable;
- whether an application value is safe to serialize;
- whether a destination is idempotent;
- whether a database backup violates a retention request;
- whether a caller is authorized to replay raw responses;
- whether a private network target is trusted.

Those decisions belong to the host or adapter policy and must be documented with the deployment.

