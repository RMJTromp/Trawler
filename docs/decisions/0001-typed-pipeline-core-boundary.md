# ADR-0001: Typed pipeline and core ownership boundary

- **Status:** Accepted
- **Date:** 2026-08-23
- **Decision owners:** Project owner
- **Related plan:** `plans/FINAL-PLAN.md`, sections 1–6

## Context

Trawler must coordinate multiple unrelated scraping pipelines without owning the caller's produced data. Making the entire engine generic would restrict one engine instance to one value type. Converting every value into JSON or a universal record would make core own serialization and data representation.

## Decision drivers

- Preserve compile-time type safety from extraction through output.
- Allow unrelated output types in one engine process.
- Keep serialization and domain models outside core.
- Prevent adapter convenience from defining public core data contracts.

## Considered options

### Generic engine: `TrawlerEngine<T>`

Type-safe for one output type, but unnecessarily restricts one engine to homogeneous pipelines.

### Non-generic values using JSON or `Any`

Allows heterogeneity but loses type safety and imposes either a serializer or unsafe runtime casting throughout processing.

### Non-generic engine with typed pipeline bindings

Keeps each pipeline type-safe while isolating heterogeneous registration inside the engine.

## Decision

`TrawlerEngine` is non-generic. Each `Pipeline<T>` retains `T` through extraction, transformation, validation, `Produced<T>`, and `Output<T>`. The engine's heterogeneous registry is an isolated internal boundary constructed through typed registration.

Core must not convert `T` to JSON, `Any`, maps, a universal entity envelope, or a database record for normal processing. Serialization belongs to the adapter or host that needs it.

## Consequences

### Positive

- One engine can run unrelated pipelines.
- Host data remains host-owned and type-safe.
- Core has no universal serializer or user-data schema.
- Output adapters can choose direct mapping, callbacks, or their own codecs.

### Negative or accepted trade-offs

- Internal registration requires carefully isolated type erasure.
- The DSL must preserve compatible extractor/transform/output types.
- Type-changing pipelines require a future explicit mapper design rather than a heterogeneous transform list.

### Follow-up work

- Phase 1 implemented the typed registration proof and extension cardinality in
  [`docs/TYPED-CORE-CONTRACTS.md`](../TYPED-CORE-CONTRACTS.md).
- Preserve the no-erasure/no-cast boundary when a later runtime adds typed
  execution operations.

## Validation

The Phase 1 exit gate must register at least two unrelated pipeline output types in one engine while core contains no universal serialized produced-value representation.

## Supersession

Revisit only through a superseding ADR that preserves multi-pipeline heterogeneity and host ownership of produced data.
