# ADR-0003: Separate resource, job, lease, and attempt

- **Status:** Accepted
- **Date:** 2026-08-23
- **Decision owners:** Project owner
- **Related plan:** `plans/FINAL-PLAN.md`, section 9

## Context

A unique request row with a terminal success state prevents legitimate recrawling. Incrementing retry count when a lease is acquired also charges process crashes as meaningful attempts even when execution never began. Durable recurring crawls need distinct identities and histories.

## Decision drivers

- Permit repeat visits to the same logical resource.
- Suppress duplicate active work without permanent “seen forever” behavior.
- Fence stale workers after lease expiry/reclaim.
- Account for retries based on meaningful execution.
- Preserve outcomes that have no HTTP response.

## Considered options

### One row per request fingerprint

Simple, but conflates stable identity, desired visits, ownership, and retries. A global unique fingerprint breaks recurring crawling.

### Reopen terminal request rows

Allows recrawl but destroys clear visit history and complicates concurrent ownership and lineage.

### Separate resource, job, lease, and attempt

Models each lifecycle explicitly and permits append-only visit/attempt history.

## Decision

Durable state adapters model:

- **Resource:** stable logical identity chosen by the request identity policy.
- **Job:** one desired processing visit for that resource.
- **Lease:** time-bounded ownership identified by a fencing token.
- **Attempt:** meaningful processing execution beginning immediately before substantive work.

Active-job uniqueness may suppress concurrent duplicates. Completed resources remain eligible for later jobs according to recrawl policy. All lease-owned writes include the token. Merely leasing does not consume retry budget.

## Consequences

### Positive

- Recrawl and deduplication are independent.
- Crash-aware retry accounting is honest.
- Stale workers cannot commit against reclaimed jobs.
- Attempt history can record policy rejection, cancellation, and extraction failures without responses.

### Negative or accepted trade-offs

- Durable adapters have more tables/state transitions.
- Transaction boundaries and recovery require careful documentation.
- Resource codecs and identity compatibility must be versioned.

### Follow-up work

- Specify adapter-neutral state contracts in Phase 1.
- Implement the PostgreSQL model and fenced transitions in Phase 4.
- Define retention independently for resources, jobs, attempts, and operational history.

## Validation

The durable adapter must demonstrate later recrawl after success, one active job under concurrent enqueue, no retry charge before attempt start, and rejection of every stale-token mutation.

## Supersession

Any alternative must preserve repeatable recrawl, active deduplication, fenced stale-worker safety, and crash-aware attempt accounting.
