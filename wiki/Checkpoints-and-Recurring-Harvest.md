# Checkpoints and Recurring Harvest

Recurring harvesting is a coordination policy over discovery. It is not the same as a scheduler: scheduling decides when to run; harvest state decides what portion of a source to request and when a run is complete.

## Harvest modes

The core strategies are:

| Strategy | Use |
| --- | --- |
| StatelessHarvestStrategy | Re-read a bounded source each run |
| CursorHarvestStrategy | Resume from a source cursor |
| WatermarkHarvestStrategy | Use a monotonic boundary such as a timestamp or sequence |
| HistoricalThenIncrementalHarvestStrategy | Finish a historical walk, then switch to incremental windows |
| Custom | Host-owned strategy with an explicit lifecycle and codec |

Non-stateless strategies need a ResumableDiscovery. Strategies with a state codec need a HarvestStateStore capability.

## Checkpoint progression

The safe progression is:

~~~text
load compatible checkpoint
    -> discovery emits work plus candidate
        -> admission and deduplication
            -> accepted work is persisted
                -> checkpoint compare-and-set advances
~~~

The engine assigns revision and completion metadata. A discovery cannot mark an unaccepted request as complete.

When the state store implements AtomicEnqueueAndCheckpointStore, accepted work and checkpoint advancement are committed atomically. Without that capability, the engine uses enqueue then checkpoint compare-and-set, which has an at-least-once duplicate window after a crash. It must not claim exactly-once source progression.

## Checkpoint compatibility

DiscoveryCheckpointCodecDescriptor contains a stable codec ID and version. The codec's strategyId also participates in compatibility. On a mismatch, a codec can:

- use the checkpoint;
- migrate it;
- reset;
- quarantine it.

Quarantine is preferable to silently interpreting an unknown payload. A quarantined checkpoint is observable and does not block the rest of the engine from starting unless the configured policy requires it.

Checkpoint payloads are opaque, bounded by maxCheckpointPayloadBytes, and owned by the discovery/harvest implementation. They are not mandatory JSON and must not embed secrets.

## Generation and completion

A recurring historical run can only complete conservatively when:

- the discovery naturally reaches its end;
- accepted work has settled;
- the current generation has no open work;
- the run outcome is COMPLETED;
- the strategy's completion predicate passes.

An ABANDONED, CANCELLED, or FAILED run cannot be used to claim historical completion. This prevents a timeout from incorrectly advancing a source boundary.

## Boundary policies

FixedOverlapBoundaryPolicy adds a deliberate overlap window to a watermark or historical/incremental boundary. Overlap trades duplicate source reads for protection against late-arriving or out-of-order source data. Downstream deduplication or idempotency must handle the overlap.

The boundary policy is a host decision. Trawler cannot infer whether a source timestamp is inclusive, exclusive, mutable, or trustworthy.

## Four-source daily harvest shape

Conceptual example using real public strategy names:

~~~kotlin
val daily = PipelineBuilder<String>("daily-source")
    .transport(transport)
    .discover(resumableDiscovery)
    .extract(StringExtractor())
    .harvest(
        HistoricalThenIncrementalHarvestStrategy(
            boundaryPolicy = FixedOverlapBoundaryPolicy(Duration.ofMinutes(5)),
        ),
    )
    .output(output, OutputRequirement.REQUIRED)
    .build(PipelineDefaults(policies = BuiltInPolicies.ordinary()))

val engine = EngineConfigurationBuilder()
    .register(daily)
    .requireStateCapability(CrawlStateCapability.DISCOVERY_CHECKPOINTS)
    .requireStateCapability(CrawlStateCapability.HARVEST_STATE)
    .build()
    .let(::TrawlerEngine)
~~~

For four independent sources, register four pipelines or four discovery instances with distinct IDs and checkpoint ownership. Do not share one mutable cursor object across pipelines unless its concurrency and ownership contract explicitly supports it.

## Recovery behavior

If a worker or process dies after accepted work but before checkpoint advancement, the next run can see the old checkpoint and re-emit work. This is the expected duplicate window without atomic enqueue-and-checkpoint. The correct response is idempotent downstream handling, not a hidden in-memory “already seen” set.

If a checkpoint is corrupt or incompatible, quarantine or reset according to policy. Do not guess a payload format or silently skip the source.

