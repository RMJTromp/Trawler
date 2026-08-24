# Retention

Retention is capability-owned. Trawler does not provide one universal cleanup table or one global retention period.

## Categories

RetentionCategory separates:

| Category | Owner |
| --- | --- |
| COMPLETED_JOBS | Crawl state |
| ATTEMPTS | Crawl state |
| RUN_HISTORY | Crawl state |
| RESPONSES | Response store |
| OUTPUT_HISTORY | Output |
| OUTBOX_HISTORY | Outbox/output |
| OPERATIONAL_EVENTS | Optional event/archive owner |

The same host may select different windows for each category.

## Why separate windows matter

Operational state is needed to diagnose and recover work. Raw responses may be retained for replay or legal/analytical requirements. Produced output history may be retained for reconciliation. Outbox rows may be retained longer than delivered output to investigate duplicate or failed delivery.

Deleting one category must not be described as deleting all evidence of a crawl.

## Bounded operations

Retention requests contain a cutoff and maxEntries. Adapter operations are bounded:

- InMemoryResponseStore evicts by age or prunes a bounded protected-aware batch;
- FileOutput prunes a bounded number of old segments;
- PostgreSQL stores expose bounded retention operations with maxRetentionRows;
- state retention, response retention, output retention, and outbox retention remain separate.

Do not run an unbounded table scan or unbounded file deletion from an engine worker.

## Protection and lineage

ResponseRetentionRequest can protect a bounded set of ResponseReference values. Durable adapters may also protect lineage they own internally. A deletion count is not proof that no external lineage existed.

Output and outbox retention must account for downstream reconciliation and idempotency records. If an application deduplicates by an output key, retain that key for at least the destination's duplicate window.

## Host responsibilities

The host must decide:

- whether response bodies can contain personal, confidential, or credential-bearing data;
- whether raw files/database rows need encryption;
- who can query replay;
- how backups extend effective retention;
- whether deletion must be auditable;
- when application-owned deduplication keys can be removed;
- how a legal hold or protected lineage is represented.

## Safe cleanup sequence

For a durable deployment:

1. stop or isolate new scheduling if necessary;
2. let active delivery passes settle or cancel them safely;
3. prune terminal operational rows in bounded batches;
4. prune response history while respecting protected references;
5. prune output and outbox history after reconciliation;
6. record the retention result and remaining backlog;
7. verify backups and replicas do not contradict the requested retention.

Retention does not make a stale worker safe. Lease fencing and cancellation remain state responsibilities.

