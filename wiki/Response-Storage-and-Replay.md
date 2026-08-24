# Response Storage and Replay

Response storage is optional raw-response retention. It is independent of crawl state and produced-data outputs.

## Capabilities

A ResponseStore advertises a set of ResponseStoreCapability values:

| Capability | Meaning |
| --- | --- |
| LOOKUP | Find one stored response |
| CACHE | Serve a response as a cache candidate |
| HISTORY | Retain multiple historical responses |
| REPLAY | Enumerate stored responses for typed replay |
| TTL_EVICTION | Evict by age |
| RANGE_QUERY | Adapter-defined bounded range query |

The engine validates required capabilities at configuration time. A response store can support lookup without supporting replay or history.

## Capture and cache policies

ResponseCapturePolicy controls whether responses are retained:

- CaptureResponsesWhenConfigured stores responses whenever a response store is selected;
- DoNotCaptureResponses leaves the response store unused for normal capture;
- RequireResponseCapture requires a store and treats inability to retain a response as a failure.

ResponseCachePolicy is independent. NoResponseCachePolicy is the default. LatestTargetResponseCachePolicy can use a recent response only when the selected store advertises LOOKUP and CACHE and the stored response belongs to the requesting pipeline.

Enabling a response store does not silently enable a cache. Choose both policies explicitly.

## In-memory response store

InMemoryResponseStore is a bounded process-local ring:

- maxResponses defaults to 1,024;
- maxBodyBytes defaults to 1 MiB;
- lookup, cache, history, replay, and TTL eviction are supported;
- the oldest entry is evicted at capacity;
- data disappears on process restart.

It is useful for local development and small replay loops, not as a restart or durability guarantee.

## File response store

FileResponseStore is a bounded JDK file implementation. Its defaults include:

- maxResponses: 1,024;
- maxBodyBytes: 8 MiB;
- maxEncodedResponseBytes: 8 MiB;
- dispatcherParallelism: 1.

It uses caller-supplied ResponseCodec values and versioned ResponseCodecDescriptor entries. It keeps a fixed ring of numeric files and replaces a slot only after the new file is written and renamed.

The host owns filesystem permissions, encryption, backups, root-directory lifecycle, and deletion policy. A local file path is not automatically private or durable.

## PostgreSQL response history

PostgresResponseStore owns a separate response migration and stores encoded responses with codec ID/version and lineage. It advertises lookup, cache, history, replay, TTL eviction, and range query. Register it as an engine extension so its pool and migration lifecycle are explicit.

Missing historical codecs cause a visible decode failure. The adapter never guesses a codec or silently interprets old bytes as a new format.

## Replay contract

Replay is a typed reprocessing path:

~~~kotlin
val selection = pipeline.replay(
    ResponseReplayQuery.ByPipelineId("numbers"),
    limit = 100,
)
val result = engine.replay(selection)
~~~

The selection carries the exact Pipeline<T> instance, not just a string ID. The engine verifies that the exact pipeline is registered and that the response store advertises REPLAY.

Replay:

- reads stored responses only;
- makes zero network calls;
- does not create ordinary crawl jobs;
- does not advance discovery checkpoints;
- does not perform ordinary run recovery;
- applies pipeline ownership before limiting results;
- runs the typed extractor, transforms, validators, and outputs;
- activates only ReplayLifecycle resources where needed.

Replay output delivery can still have the duplicate semantics of the selected output. Replay being network-free does not make an external sink exactly-once.

## Replay safety

Replay-safe resources may open database/file handles needed to read stored responses. They must not start a transport, scheduler, ordinary state recovery, or background crawler. If a response store implements ReplayLifecycle, the engine calls startForReplay and stopForReplay around the bounded replay operation.

## Retention and lineage

Response retention uses ResponseRetentionRequest with a cutoff, a bounded maxEntries, and protected references. An adapter can evict before a cutoff or prune a bounded batch. Protected references and adapter-owned lineage must not be removed accidentally.

Do not retain credentials, cookies, authorization headers, or private response bodies without a documented retention and redaction policy. See [Security and Network Safety](Security-and-Network-Safety) and [Retention](Retention).

