# Architecture Decision Records

Architecture Decision Records (ADRs) capture decisions that are cross-cutting, difficult to reverse, or likely to be questioned later.

## When an ADR is required

Create an ADR for:

- a change to a non-negotiable architecture invariant;
- a new core public abstraction;
- a dependency that affects multiple modules;
- a physical module-boundary decision;
- a persistent schema/codec strategy;
- a delivery or consistency guarantee;
- a public compatibility break;
- a security default with significant trade-offs;
- a choice between credible alternatives whose reasoning would otherwise be lost.

Routine implementation details inside an already-approved boundary do not need an ADR.

## Process

1. Copy [`0000-template.md`](0000-template.md).
2. Allocate the next four-digit number.
3. Use a short kebab-case filename, for example `0001-typed-pipeline-boundary.md`.
4. Set status to `Proposed` while under review.
5. Record real alternatives and consequences.
6. Change status to `Accepted` only when the decision is approved and implementation may rely on it.
7. Never rewrite the history of an accepted ADR to reverse it. Add a new ADR that supersedes it.
8. Link affected documentation and code.

## Status values

- `Proposed`
- `Accepted`
- `Rejected`
- `Deprecated`
- `Superseded by ADR-NNNN`

## Index

| ADR | Status | Decision |
|---|---|---|
| [ADR-0001](0001-typed-pipeline-core-boundary.md) | Accepted | Non-generic engine with typed, host-owned pipeline values. |
| [ADR-0002](0002-separate-state-response-and-output.md) | Accepted | Separate crawl state, response storage, and produced-data output. |
| [ADR-0003](0003-separate-resource-job-lease-and-attempt.md) | Accepted | Separate durable resource, job, lease, and attempt identities. |
| [ADR-0004](0004-cohesive-maven-module-strategy.md) | Accepted | Use a few cohesive artifacts and bundle lightweight official extensions in core. |
| [ADR-0005](0005-web-coroutines-runtime-alignment.md) | Accepted | Keep Ktor's compatible coroutines runtime alignment owned by `trawler-web`. |
| [ADR-0006](0006-postgres-operational-state-boundary.md) | Accepted | Keep durable PostgreSQL operational state behind focused core capabilities. |
| [ADR-0007](0007-resumable-discovery-state-boundary.md) | Accepted | Keep resumable checkpoints and recurring state opaque, revisioned, and capability-scoped. |
| [ADR-0008](0008-response-replay-output-lineage.md) | Accepted | Keep response replay, typed output, outbox delivery, and retention independent with caller-owned codecs and lineage protection. |
| [ADR-0009](0009-flaresolverr-adapter-boundary.md) | Accepted | Keep challenge contracts in `trawler-flaresolverr`, resolve challenges only through visible engine retries, and document that browser-side navigation bypasses per-hop safety. |
| [ADR-0010](0010-settled-failures-checkpoint-markers-paged-replay.md) | Accepted | Let opted-in harvests progress past settled item failures, commit watermarks at the searched window end, advance checkpoints without work, and page pipeline replay by store cursor. |
| [ADR-0011](0011-per-host-tls13-jdk-client.md) | Accepted | Reach TLS 1.3-only hosts through an explicitly selected, transport-owned JDK client, keeping CIO for all others and refusing runtimes where a CIO guarantee cannot be kept. |
| [ADR-0012](0012-overlapping-runs-over-disjoint-pipelines.md) | Accepted | Admit engine runs over disjoint pipeline selections concurrently; overlapping runs and replay wait in arrival order. |
| [ADR-0013](0013-forward-proxy-fails-closed.md) | Accepted | Route every `trawler-web` request through one explicit forward proxy on the JDK client, fail closed, and report proxy failures with a distinct type. |

`PLAN.md` remains the full architecture authority. These ADRs preserve its most consequential decisions in compact form.
