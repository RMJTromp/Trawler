# ADR-0012: Runs over disjoint pipelines overlap

- **Status:** Accepted
- **Since:** 1.2.0

## Context

`EngineRuntime` held one mutex for the whole of every run and replay. A host with many unrelated pipelines
on separate schedules had to take turns: each selection waited for the previous one, and a selection lasted
as long as its slowest pipeline. One slow site could therefore hold back every other site and every scheduled
job in the process.

## Decision

Replace the mutex with `PipelineRunGate`, an arrival-ordered admission over pipeline IDs.

- A run is admitted once no running or earlier-queued run shares a pipeline with it. A pipeline never runs twice
  at once, so per-pipeline guarantees (one owner of a checkpoint or harvest boundary per engine) are unchanged.
- A later run never overtakes an earlier waiter it overlaps with, so an all-pipeline run is not starved.
- Replay selects every registered pipeline, so it still excludes all ordinary runs.
- Stale-run recovery uses the longest timeout among the runs active in this engine, since a shorter overlapping
  run must not declare a longer one stale.

Concurrency across runs is bounded by `maxPendingRuns`, the size of the selected pipeline sets, and each pipeline's
own policies. Requests to the same origin are still coordinated by the transport's policies, not by run admission.

## Consequences

- `runOnce` callers that relied on one run finishing before another disjoint run started must sequence them
  themselves. Sequential `runOnce` calls keep their order.
- Observers and state stores see events from overlapping engine runs. Both already had to be safe for concurrent
  pipelines inside a single run.
- A host with more independent scheduled selections than the default must raise `maxPendingRuns`.

## Alternatives considered

- One `TrawlerEngine` per pipeline group: needs hand-written sharing of store and transport lifecycles, and leaves
  replay exclusion to the host.
- Shorter runs behind the mutex: reduces the stall but keeps one slow pipeline delaying every other.
