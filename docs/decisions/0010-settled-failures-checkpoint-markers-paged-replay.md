# ADR-0010: Settled item failures, checkpoint markers, and paged replay

> **Status:** Accepted
>
> **Date:** 2026-11-08

## Context

Long-running recurring harvests exposed four gaps in the Phase 5 and Phase 6
contracts (ADR-0007, ADR-0008):

1. Any work item that failed terminally made the pipeline run `FAILED`, and
   the engine refused every harvest commit for a failed run. One permanently
   unparseable or deleted item therefore froze a cursor walk, a watermark,
   and historical completion forever, and every later incremental window
   grew from the frozen boundary.
2. `WatermarkHarvestStrategy` and the incremental phase of
   `HistoricalThenIncrementalHarvestStrategy` committed the completion time
   as the new boundary. The discovery only searched up to the window end, so
   anything changed while the run processed its work, beyond the configured
   overlap, fell between two windows and was never discovered.
3. A checkpoint could advance only beside an emitted work item. A resumable
   discovery that filters known items, or walks regions with nothing new,
   could not record that progress and repeated those regions after every
   interruption.
4. Pipeline-wide replay always returned the oldest stored responses first,
   bounded by `maxReplayResponsesPerRun`, with no way to continue. A store
   larger than one bound could not be reprocessed, and append-only history
   replayed superseded versions of recrawled targets.

These are generic coordination concerns: they apply to any recurring
discovery, domain, and output type, so they belong in core rather than in a
host application.

## Decision

**Settled item failures.** The engine distinguishes a *settled item failure*
from a blocking failure. A work item is settled when the retry policy
returns `DoNotRetry`, the failure classifier marks the failure as not
retryable by default, the failure is item-level (the exact exception raised
by the transport call, tracked by identity rather than by wrapping so callers
still see the original type; or stage `extraction`,
extraction/follow-up/response-metadata bounds, or `validation:*`), and the
terminal `Failed` transition is accepted. Other exceptions on the generic
`work` path are blocking.
Exhausted transient failures, response-capture and response-store failures,
required-output failures, attempt-start failures, and lease or state-store
failures remain blocking. A run with settled failures and no blocking
failure reports the new `HarvestRunOutcome.COMPLETED_WITH_SETTLED_ITEM_FAILURES`
and a `settledItemFailures` count to the strategy. The pipeline run status is
still `FAILED`. The engine's commit gate accepts that outcome, so the
strategy decides. Built-in cursor, watermark, and historical-then-incremental
strategies take an explicit `SettledItemFailurePolicy` whose default,
`BLOCKS_COMPLETION`, keeps the previous behavior. `ALLOWS_COMPLETION` is opt-in.

**Watermark boundary.** `HarvestCompletionContext` carries the most
conservative incremental window supplied to the run's resumable discoveries.
Watermark and incremental-phase commits record that window's exclusive end.
The historical phase has no window and still records its completion time.
That limitation is documented, and a separate watermark pipeline is the
recommended companion to a long backfill.

**Checkpoint markers.** `DiscoveryEmission.Checkpoint` advances a resumable
discovery's checkpoint with no work item. Emissions are handled in order, so
earlier work has already been accepted or suppressed. A marker after a
rejected or recrawl-deferred emission of the same walk is not applied. Markers count toward the
per-discovery emission bound but are not budget work candidates.

**Paged replay.** `ResponseReplayQuery.ByPipelineId` gains an exclusive
`after` cursor (a store-issued `ResponseReference`) and a
`ResponseReplayVersions` selection (`ALL` or `LATEST_PER_TARGET`). Results
are in ascending store order, and adapters apply ownership, cursor, and
version selection before the limit. Stores reject references they did not
issue; a reference whose response was pruned still orders correctly
because the built-in stores encode their sequence in the reference.
`ReplayResult.lastReference` is the last response that finished processing,
so callers can page until a short page.

## Alternatives considered

1. **Let a custom strategy commit after any failed run.** Rejected: the
   engine gate exists so a buggy strategy cannot claim coverage after
   infrastructure failure or data loss, such as a failed required output.
2. **Treat every terminal item failure as settled.** Rejected: an outage
   (FlareSolverr down, database unavailable, timeouts) exhausts retries for
   every item and would advance the watermark past work that was never
   processed. Only classifier-permanent, item-level failures settle.
3. **Advance the checkpoint of suppressed emissions automatically.**
   Rejected for now: the existing rule "advance only after acceptance" is
   relied on by adapters; an explicit marker keeps the decision with the
   discovery and preserves the current semantics.
4. **Offset-based replay paging.** Rejected: offsets shift when retention
   prunes rows; a store-ordered keyset cursor does not.
5. **Host-side workarounds.** Rejected: every recurring host would need to
   re-implement failure settlement and boundary tracking outside the engine's
   transactional ordering, which is exactly what core coordinates.

## Consequences

Positive consequences:

- one poison item no longer freezes recurring coverage when the host opts in;
- watermarks no longer lose changes made during long runs;
- resumed walks skip regions already proven empty;
- an arbitrarily large response history can be reprocessed in bounded pages,
  optionally only the newest version per target.

Costs and limitations:

- `HarvestRunOutcome` and `DiscoveryEmission` gained members; caller code
  with exhaustive `when` expressions over them must add a branch (unreleased
  `0.1.0-SNAPSHOT`, so no compatibility shim is provided);
- custom `ResponseReplayStore` implementations must now honor `after`,
  `versions`, and ascending order for `ByPipelineId`;
- with `ALLOWS_COMPLETION`, the host must record and revisit settled
  failures itself, because the next window does not rediscover them;
- settlement relies on the failure classifier: the core default classifier
  marks every failure non-retryable, so web pipelines should use the web or
  FlareSolverr classifier;
- the PostgreSQL schema is unchanged; latest-version replay uses the existing
  `(target, sequence)` index.

## References

- [`docs/RECURRING-DISCOVERY.md`](../RECURRING-DISCOVERY.md)
- [`docs/RESPONSE-OUTPUT.md`](../RESPONSE-OUTPUT.md)
- [ADR-0007](0007-resumable-discovery-state-boundary.md)
- [ADR-0008](0008-response-replay-output-lineage.md)
