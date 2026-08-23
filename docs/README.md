# Trawler Documentation

This directory contains focused, normative project documentation.

## Required documents

| Document | Purpose | Update when |
|---|---|---|
| [Architecture](ARCHITECTURE.md) | Core invariants, ownership, extension boundaries, and runtime models. | A public contract, boundary, state model, or capability changes. |
| [Modules and artifacts](MODULES.md) | Concrete Maven packaging, bundled built-ins, and split criteria. | A capability moves between artifacts or a module is added/removed. |
| [Dependencies](DEPENDENCIES.md) | Complete approved dependency and build-plugin inventory. | Any dependency, plugin, repository, scope, version, or owning module changes. |
| [Development](DEVELOPMENT.md) | Workflow, quality gates, test policy, and verification. | Tooling, build commands, or completion requirements change. |
| [Documentation](DOCUMENTATION.md) | Feature documentation requirements and templates. | Documentation standards or required records change. |
| [Decisions](decisions/README.md) | Architecture Decision Record index and process. | A cross-cutting or difficult-to-reverse decision is made. |

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
