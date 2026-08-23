# Trawler Documentation

This directory contains focused, normative project documentation.

## Required documents

| Document | Purpose | Update when |
|---|---|---|
| [Architecture](ARCHITECTURE.md) | Core invariants, ownership, extension boundaries, and runtime models. | A public contract, boundary, state model, or capability changes. |
| [Modules and artifacts](MODULES.md) | Concrete Maven packaging, bundled built-ins, and split criteria. | A capability moves between artifacts or a module is added/removed. |
| [Dependencies](DEPENDENCIES.md) | Complete approved dependency and build-plugin inventory. | Any dependency, plugin, repository, scope, version, or owning module changes. |
| [Development](DEVELOPMENT.md) | Workflow, quality gates, test policy, and verification. | Tooling, build commands, or completion requirements change. |
| [Testing](TESTING.md) | Repository test lifecycle, deterministic test rules, fixtures, Docker requirements, and coverage policy. | Test infrastructure, naming, lifecycle, or coverage policy changes. |
| [Test matrix](TEST-MATRIX.md) | Contract-by-contract coverage plan and untested-risk register. | A public contract, documented invariant, or owning test changes. |
| [Documentation](DOCUMENTATION.md) | Feature documentation requirements and templates. | Documentation standards or required records change. |
| [Decisions](decisions/README.md) | Architecture Decision Record index and process. | A cross-cutting or difficult-to-reverse decision is made. |
| [Typed core contracts](TYPED-CORE-CONTRACTS.md) | Implemented Phase 1 public contracts, composition, validation, and registration safety. | Typed core contracts or their composition semantics change. |
| [In-memory engine](IN-MEMORY-ENGINE.md) | Implemented Phase 2 database-free runtime, state, outputs, events, and scheduler. | Runtime lifecycle, bounds, in-memory guarantees, or scheduler behavior changes. |
| [Ktor web adapter](WEB-ADAPTER.md) | Implemented Phase 3/5 HTTP transport, redirect, public-web, robots, session, origin-policy, and bounded XML discovery behavior. | Web transport, safety, robots, session, discovery, or web dependency behavior changes. |
| [Durable PostgreSQL state](POSTGRES-STATE.md) | Implemented Phase 4/5 PostgreSQL operational state, leases, attempts, run history, request codecs, checkpoints, and recurring state. | PostgreSQL state schema, fencing, recovery, codec, pool, recurring state, or retention behavior changes. |
| [Resumable and recurring discovery](RECURRING-DISCOVERY.md) | Implemented Phase 5 checkpoints, harvest strategies, admission, XML discovery, and recurring state. | Discovery checkpoints, harvest completion, backlog admission, XML bounds, or recurring state changes. |
| [Response storage, replay, outputs, and retention](RESPONSE-OUTPUT.md) | Implemented Phase 6 optional response stores, typed outputs, replay, PostgreSQL outbox, and independent retention. | Response storage, replay, output acknowledgement, outbox, lineage, or retention behavior changes. |

The full architecture plan remains [FINAL-PLAN.md](../plans/FINAL-PLAN.md). Root repository rules remain [AGENTS.md](../AGENTS.md).

## Documentation status labels

Use these labels where readers could confuse planned and implemented behavior:

- **Implemented** — exists in the repository and has passed the recorded verification.
- **In progress** — implementation is incomplete and must not be presented as usable.
- **Planned** — approved direction with no implementation claim.
- **Proposed** — under discussion and not implementation authority.
- **Deprecated** — supported temporarily with a documented replacement/removal path.

Unlabeled API snippets in planning documents are conceptual. Compiling public examples must say that they are verified and identify the version or commit to which they apply.

## Documentation ownership

The person or agent changing behavior owns the corresponding documentation update. Documentation work may not be deferred to an unspecified future contributor.
