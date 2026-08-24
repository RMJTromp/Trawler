# Outputs and Outbox

Outputs receive typed Produced<T> batches. They own encoding, destination transactions, idempotency, retention, and external delivery semantics.

## Output contract

Output<T> exposes an id and a suspending write method:

~~~kotlin
interface Output<T : Any> {
    val id: String
    suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult
}
~~~

The engine passes bounded batches up to maxOutputBatchSize. OutputResult is one of Accepted, Skipped, or Failed. A cancellation exception propagates; an ordinary output error is represented as Failed with its original cause.

## Built-in outputs

| Output | Use | Important behavior |
| --- | --- | --- |
| CallbackOutput<T> | Host callback | Runs in the structured pipeline scope |
| FlowOutput<T> | Typed in-process stream | Bounded channel, capacity 128 by default, applies backpressure |
| FileOutput<T> | JDK file sink | Versioned codec, fixed ring of forced segment files |
| NoOutput<T> | Explicitly discard delivery | Returns Skipped |
| PostgresOutput<T> | PostgreSQL output history | Adapter-owned encoding and migration |
| PostgresOutboxOutput<T> | Durable pending delivery | Explicit bounded delivery pass |

FileOutput writes and forces frames before returning Accepted, but that is durable acceptance to the configured filesystem, not external exactly-once delivery.

FileOutput defaults to 64 segment slots, 16 MiB per segment, 1,024 items per segment, 1 MiB per encoded item, and one file dispatcher thread. The output's root is host-selected and is never derived from a target or produced value.

## Required and best-effort outputs

Bind each output explicitly:

~~~kotlin
val pipeline = PipelineBuilder<String>("delivery")
    .transport(transport)
    .discover(SeedDiscovery(requests = listOf(CrawlRequest("source"))))
    .extract(extractor)
    .output(requiredOutput, OutputRequirement.REQUIRED)
    .output(observabilityOutput, OutputRequirement.BEST_EFFORT)
    .build(PipelineDefaults(policies = BuiltInPolicies.ordinary()))
~~~

Required failure affects the pipeline run result. Best-effort failure remains observable but does not by itself make the pipeline unsuccessful.

This decision is about pipeline completion, not about whether the sink is durable or duplicate-free. A required callback can still be non-transactional; a best-effort PostgreSQL history output can still retain an accepted value.

## Output decorators

The core output decorators are:

- BatchedOutput splits a bounded engine batch into smaller synchronous delegate writes;
- RetryingOutput retries a complete delegate batch using an OutputRetryPolicy;
- IdempotentOutput invokes a typed OutputIdempotencyHook before forwarding.

RetryingOutput defaults to three attempts with a zero delay. A delegate that performs an external side effect and then fails can receive a duplicate on retry. Use IdempotentOutput with a durable application-owned key when the destination supports that model.

Decorators close a wrapped OutputLifecycle when they close. Register the owning lifecycle once and document who owns the delegate.

## Codec ownership

FileOutput, PostgresOutput, and PostgresOutboxOutput use OutputCodec<T> and OutputCodecRegistry<T>. The codec owns the representation of Produced<T>, including application mapping or serialization. Core never chooses JSON or a database record shape.

Codec descriptors are versioned. A missing historical descriptor is a visible failure, not silent reinterpretation.

## PostgreSQL outbox

PostgresOutboxOutput<T> separates durable enqueue from external delivery:

~~~text
pipeline output write
    -> commit PENDING outbox row
        -> bounded deliverPending pass
            -> claim with row lock and fence token
                -> delegate outside transaction
                    -> fenced DELIVERED, retry, or FAILED
~~~

The delegate is not started implicitly. Register it separately if it implements ExtensionLifecycle.

The explicit delivery API accepts ownerId, a bounded limit, and an optional clock time. It does not launch an infinite worker. A host scheduler or operator loop decides when to call it.

Possible outcomes:

- delegate acknowledges: the row becomes DELIVERED;
- delegate fails: the row is retried up to maxOutboxAttempts;
- retry limit is exhausted: the row becomes FAILED;
- claim is stale: the old worker cannot mutate the row;
- process dies after the side effect but before DELIVERED: a later claim may duplicate the side effect.

The duplicate window is inherent to calling an external delegate outside the database transaction. Use an idempotent delegate or application-owned deduplication key. Do not advertise exactly-once delivery.

## Output delivery checklist

- Define what Accepted means for the destination.
- Bound batch size, encoded item size, and local buffers.
- Preserve cancellation and close resources deterministically.
- Choose REQUIRED or BEST_EFFORT deliberately.
- Document duplicate windows and idempotency requirements.
- Keep output history separate from crawl state.
- Use a versioned codec for durable bytes.
- For outbox delivery, monitor PENDING, DELIVERED, retry, and FAILED counts.
