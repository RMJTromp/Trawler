# ADR-0007: Resumable discovery state boundary

> **Status:** Accepted
>
> **Date:** 2026-08-23

## Context

Recurring discovery needs to resume after interruption without placing a
serializer, site model, or produced value in core. Durable adapters also need
to prevent stale writers from overwriting progress and need an explicit path
for incompatible codec versions. A Flow's memory backpressure is not enough
to control backlog growth across runs.

## Decision

Core owns opaque checkpoint and optional harvest-state contracts with explicit
codec IDs, versions, and compare-and-set revisions. A discovery emits a
checkpoint candidate beside work; the engine advances it only after work
acceptance. Adapters may implement an atomic enqueue-plus-checkpoint boundary;
otherwise the engine uses an enqueue-then-CAS fallback with a documented
at-least-once duplicate window and active identity deduplication.

Codec resolution must return use, migrate, reset, or quarantine explicitly.
Quarantine is a focused capability, not a universal archive. Harvest state is
separate from per-emission discovery checkpoints and is required only by a
strategy that declares a state codec. Backlog admission, harvest completion,
incremental overlap, and pipeline health remain replaceable policy decisions.

RSS, Atom, and sitemap implementations belong in `trawler-web` because they
share the engine-controlled HTTP transport and its Ktor/web policy boundary.
They use JDK StAX and gzip for the currently implemented bounded behavior;
no parser dependency is introduced.

## Alternatives considered

1. **Universal JSON checkpoint envelopes.** Rejected because it leaks a
   serialization choice into core and would make arbitrary caller-owned state
   look like a domain record.
2. **Advance before enqueue.** Rejected because a queue rejection or crash
   would lose discovery work.
3. **One giant state interface.** Rejected because stateless/in-memory users
   should not implement durable checkpoint, harvest, health, or quarantine
   methods they do not select.
4. **Treat every Flow as backlog control.** Rejected because bounded
   collection prevents memory exhaustion but cannot suppress persistent
   cross-run queue growth; admission policies receive queue metrics instead.
5. **Add a parser library to `trawler-web`.** Rejected for the current RSS,
   Atom, and sitemap element set because JDK StAX and gzip provide the bounded
   behavior without another direct or transitive dependency.

## Consequences

Positive consequences:

- interrupted walks can resume without a core serializer;
- durable adapters can provide atomicity and stale-writer protection;
- incompatible state is visible and recoverable rather than silently reused;
- stateless pipelines remain free of harvest-state requirements;
- web discovery uses the same transport and safety policies as ordinary work.

Costs and limitations:

- adapters must declare focused capabilities and bound opaque payloads;
- the fallback path can replay an emission and does not claim exactly once;
- custom strategies must own their state codec and completion predicate;
- XML discovery is intentionally limited to bounded standard locations, not a
  full content parser or site-specific pagination engine.

## References

- [`docs/RECURRING-DISCOVERY.md`](../RECURRING-DISCOVERY.md)
- [`plans/FINAL-PLAN.md`](../../PLAN.md), Phase 5
- [`docs/ARCHITECTURE.md`](../ARCHITECTURE.md)
