# ADR-0008: Response replay, output, and lineage boundaries

> **Status:** Accepted
>
> **Date:** 2026-08-23

## Context

Trawler needs optional response replay and durable output without weakening the
existing separation between crawl state, raw responses, and produced data.
Replay must preserve the typed `T` path and make zero network calls. File and
database adapters need bounded caller-owned representations, while PostgreSQL
output retention must not remove a response still needed by a retained output
or outbox row. External delivery can fail after a side effect, so a generic
exactly-once claim would be misleading.

## Decision

Core owns focused `ResponseStore`, replay, cache, retention, output codec,
output decorator, and output-retention contracts. Caller/extension codecs own
all response and `Produced<T>` representations and are persisted with exact
IDs and versions. `FileResponseStore`, `FileOutput`, and in-memory response
retention are bundled in core because they require only JDK/Kotlin/coroutines.

`TrawlerEngine.replay` requires a replay-capable selected response store,
binds selection to the registered typed pipeline, and invokes the ordinary
extract/transform/validate/output path without creating crawl jobs or network
requests. Follow-ups are suppressed and reported.

PostgreSQL response history, typed output history, and outbox rows remain
output/response-owned tables rather than operational state tables. PostgreSQL
outbox delivery is an explicit bounded operation: row-lock claim, unique
sequence-backed fencing token, delegate call outside the transaction, and a
fenced terminal/requeue mutation. Local output/outbox lineages protect local
PostgreSQL response rows until ownership is pruned.

Retention is independently selected per category. Terminal state rows,
responses, output history, outbox history, and optional operational events do
not share one universal retention interface or transaction. No component
claims generic exactly-once delivery.

## Alternatives considered

1. **Put response bodies and produced values in crawl state.** Rejected because
   it couples operational recovery to retention/output concerns and violates
   the state/response/output boundary.
2. **Use a mandatory JSON or generic record envelope.** Rejected because it
   leaks a serializer and application data model into core and erases caller
   ownership of `T`.
3. **Replay by creating ordinary crawl jobs.** Rejected because replay would
   make network calls, consume crawl retry state, and mix historical processing
   with new discovery.
4. **Run outbox delivery in a hidden background coroutine.** Rejected because
   delivery, retries, cancellation, and external side effects must remain
   visible to the host and structured lifecycle.
5. **Delete response rows without lineage coordination.** Rejected because
   retained output could point at a removed response. PostgreSQL uses a local
   foreign key lineage boundary and callers can supply protected references
   for other stores.
6. **Claim exactly-once external delivery.** Rejected because a process can
   fail after delegate acceptance and before the fenced acknowledgement.

## Consequences

Positive consequences:

- ordinary crawling remains valid without response storage;
- arbitrary typed values remain `T` through replay and output;
- missing codec versions fail visibly rather than silently changing meaning;
- file and PostgreSQL stores have explicit finite bounds and cleanup ownership;
- outbox stale writers cannot complete or fail a reclaimed row;
- retention can be operated independently and preserves local response lineage.

Costs and limitations:

- callers must register lifecycle-owning adapters and provide compatible
  codecs for replay or outbox delivery;
- output and outbox adapters document duplicate windows rather than promising
  exactly-once delivery;
- cross-store response references need caller-supplied retention protection;
- operational event history and WARC/object storage remain outside this phase.

## References

- [`docs/RESPONSE-OUTPUT.md`](../RESPONSE-OUTPUT.md)
- [`docs/ARCHITECTURE.md`](../ARCHITECTURE.md)
- [`plans/FINAL-PLAN.md`](../../plans/FINAL-PLAN.md), Phase 6
