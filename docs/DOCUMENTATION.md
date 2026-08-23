# Documentation Requirements

> **Status:** Normative
>
> **Principle:** A capability is not complete until its behavior and limits are documented accurately.

## Documentation goals

Documentation must let a new contributor determine:

- what is implemented now;
- what is merely planned;
- which module owns a capability;
- which guarantees are provided;
- how to configure and verify it;
- where failure, duplicate, security, and retention boundaries lie.

Documentation must not serve as marketing that outruns implementation.

## Required update mapping

| Change | Required documentation |
|---|---|
| New or changed public feature | Feature document plus relevant architecture/README update |
| New dependency/build plugin | `docs/DEPENDENCIES.md` |
| New, removed, split, or merged Maven artifact | `docs/MODULES.md`, `docs/DEPENDENCIES.md`, and an ADR when the boundary is cross-cutting |
| Cross-cutting architectural decision | ADR under `docs/decisions/` |
| New configuration | Feature document with type, default, validation, inheritance, and examples |
| State machine or durable schema | Feature document with transitions, invariants, recovery, retention, and migration |
| Public API break | ADR/migration notes and affected examples |
| New adapter | Feature document, dependency ownership, declared capabilities, guarantee matrix |
| New security behavior | Threat assumptions, safe defaults, override consequences, and redaction rules |
| Project status change | `README.md` |

## Feature document template

Create a focused Markdown file under an appropriate `docs/` section when a feature is implemented. Use this structure unless a shorter document genuinely covers every applicable concern:

```markdown
# Feature name

> Status: Implemented / In progress / Planned / Deprecated
> Owner: module or adapter
> Since: version or unreleased

## Purpose

What problem this solves.

## Non-goals

What this deliberately does not solve.

## Public contracts

Types/functions and ownership boundaries.

## Configuration

Options, defaults, validation, inheritance, and examples.

## Lifecycle and state

Startup, operation, transitions, cancellation, recovery, and cleanup.

## Capabilities and dependencies

Required adapter capabilities and external dependencies.

## Concurrency and resource bounds

Scopes, permits, queues, limits, timeouts, and blocking work.

## Failure and delivery semantics

Errors, retry, abandonment, duplicates, idempotency, and guarantees.

## Security, privacy, and retention

Sensitive data, redaction, authorization, persistence, and cleanup.

## Verification

Commands, inspections, demonstrations, and authorized tests.

## Limitations

Known unsupported cases and intentional constraints.
```

Delete sections only when they are truly inapplicable, not because the behavior has not been decided.

## API examples

- Mark non-compiling design sketches as **conceptual**.
- Compile/verify examples presented as usable API.
- Keep examples minimal and focused on public contracts.
- Do not use site-specific examples to define core semantics.
- Never include live secrets, private endpoints, or sensitive captured content.
- State required adapters and capabilities.
- Show cancellation/cleanup when resource ownership is not obvious.
- Do not imply exactly-once delivery, persistence, or replay unless the selected configuration provides it.

## State and guarantee documentation

Any durable component must document:

- stable identity and primary keys;
- state transitions and terminal states;
- transaction boundaries;
- fencing/optimistic concurrency behavior;
- crash windows;
- retry and attempt accounting;
- duplicate behavior;
- recovery process;
- schema/codec versioning;
- retention and deletion relationships.

Any output must document whether it is required/best-effort, synchronous/buffered/durable, transactional/non-transactional, and its possible duplicate/loss windows.

## Documentation quality rules

- Use precise, plain language.
- Use one canonical term for each concept.
- Separate defaults from invariants.
- Separate core behavior from adapter behavior.
- Separate implemented behavior from roadmap intent.
- Explain why a constraint exists when future contributors might remove it accidentally.
- Link to the canonical definition instead of copying large sections that will drift.
- Prefer diagrams only when they clarify ownership, state, or sequence.
- Keep code identifiers exact and searchable.
- Avoid dates or version claims that cannot be maintained.

## Documentation review checklist

- [ ] Status label is accurate.
- [ ] Owning module is named.
- [ ] Core versus adapter behavior is clear.
- [ ] Defaults and replacement paths are clear.
- [ ] Lifecycle, cancellation, and cleanup are covered.
- [ ] Capability requirements are listed.
- [ ] Failure, retry, duplicate, and delivery semantics are honest.
- [ ] Security and retention impacts are covered.
- [ ] Examples are conceptual or verified as labeled.
- [ ] Dependencies and ADRs are updated if applicable.
- [ ] README does not overclaim the feature.
- [ ] Links and terminology are consistent.
