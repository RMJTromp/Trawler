package com.rmjtromp.trawler.output

import com.rmjtromp.trawler.pipeline.Output
import com.rmjtromp.trawler.pipeline.OutputResult
import com.rmjtromp.trawler.pipeline.Produced
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.retention.RetentionCategory
import java.time.Duration

/**
 * Caller-supplied encoding boundary for file and PostgreSQL outputs.
 *
 * The codec owns the representation of `Produced<T>`, including any mapping,
 * serialization, schema, or application identity it needs. Core only bounds
 * the returned bytes and writes them through the selected output. It never
 * converts `T` to JSON, a map, a database row, or an entity envelope.
 */
public interface OutputCodec<T : Any> {
    /** Stable identity persisted beside encoded output bytes. */
    public val descriptor: OutputCodecDescriptor

    /** Encodes one typed produced value without network or database access. */
    public fun encode(item: Produced<T>): ByteArray

    /** Decodes one payload for output-owned outbox delivery. */
    public fun decode(payload: ByteArray): Produced<T>
}

/** Versioned identity for one caller-supplied output codec. */
public data class OutputCodecDescriptor(
    public val id: String,
    public val version: Int,
) {
    init {
        require(id.isNotBlank()) { "Output codec ID must not be blank" }
        require(version >= 0) { "Output codec version must not be negative" }
    }
}

/** Bounded registry for active and historical output codecs. */
public class OutputCodecRegistry<T : Any>(
    codecs: List<OutputCodec<T>>,
    active: OutputCodecDescriptor? = null,
) {
    private val codecs: List<OutputCodec<T>> = codecs.toList()
    private val activeDescriptor: OutputCodecDescriptor = active ?: this.codecs.firstOrNull()?.descriptor
        ?: error("At least one output codec is required")

    init {
        require(this.codecs.isNotEmpty()) { "At least one output codec is required" }
        require(this.codecs.map { it.descriptor }.distinct().size == this.codecs.size) {
            "Output codec ID/version pairs must be unique"
        }
        require(this.codecs.any { it.descriptor == activeDescriptor }) {
            "Active output codec is not registered"
        }
    }

    /** Returns the active codec used for new output rows. */
    public fun active(): OutputCodec<T> = codec(activeDescriptor)

    /** Decodes a row with the exact persisted codec descriptor. */
    public fun decode(descriptor: OutputCodecDescriptor, payload: ByteArray): Produced<T> =
        codec(descriptor).decode(payload.copyOf())

    private fun codec(descriptor: OutputCodecDescriptor): OutputCodec<T> =
        codecs.firstOrNull { it.descriptor == descriptor }
            ?: throw IllegalStateException(
                "No output codec is registered for ${descriptor.id}@${descriptor.version}",
            )
}

/** Bounded request for pruning one output-owned history category. */
public class OutputRetentionRequest(
    public val category: RetentionCategory,
    public val cutoff: java.time.Instant,
    public val maxEntries: Int,
) {
    init {
        require(category == RetentionCategory.OUTPUT_HISTORY ||
            category == RetentionCategory.OUTBOX_HISTORY
        ) { "Output retention supports only output-owned categories" }
        require(maxEntries > 0) { "Output retention batch must be positive" }
    }
}

/** Focused retention capability owned by one output or outbox adapter. */
public interface OutputRetentionStore {
    /** Categories this output owns and can prune. */
    public val retentionCategories: Set<RetentionCategory>

    /** Prunes at most [request.maxEntries] terminal output rows. */
    public suspend fun prune(request: OutputRetentionRequest): com.rmjtromp.trawler.retention.RetentionSummary
}

/** How an output retry policy handles one failed delegate attempt. */
public sealed interface OutputRetryDecision {
    /** Invoke the delegate again after the decorator's configured delay. */
    public data object Retry : OutputRetryDecision

    /** Return the failure without another delegate call. */
    public data object Stop : OutputRetryDecision
}

/** Replaceable policy for output-local retries. */
public interface OutputRetryPolicy {
    /** Stable identifier used by configuration and diagnostics. */
    public val id: String

    /** Decides whether [attempt] may be retried after [error]. */
    public fun decide(attempt: Int, error: Throwable): OutputRetryDecision
}

/** Fixed bounded output retry policy with an injected suspending delay. */
public class FixedOutputRetryPolicy(
    public val maxAttempts: Int = 3,
    public val delay: Duration = Duration.ZERO,
) : OutputRetryPolicy {
    override val id: String = "fixed-output-retry"

    init {
        require(maxAttempts > 0) { "Output retry attempt bound must be positive" }
        require(!delay.isNegative) { "Output retry delay must not be negative" }
    }

    override fun decide(attempt: Int, error: Throwable): OutputRetryDecision =
        if (attempt < maxAttempts) OutputRetryDecision.Retry else OutputRetryDecision.Stop
}

/** Expected decision for a typed idempotency hook. */
public sealed interface OutputIdempotencyDecision {
    /** Pass the item to the wrapped output. */
    public data object Write : OutputIdempotencyDecision

    /** Treat the item as acknowledged without forwarding it again. */
    public data object Suppress : OutputIdempotencyDecision

    /** Reject this item as an output failure. */
    public data class Reject(public val reason: String) : OutputIdempotencyDecision {
        init {
            require(reason.isNotBlank()) { "Idempotency rejection reason must not be blank" }
        }
    }
}

/** Caller-owned, typed idempotency decision hook. */
public fun interface OutputIdempotencyHook<T : Any> {
    /** Classifies one item without erasing its `T` value. */
    public suspend fun decide(item: Produced<T>, context: OutputContext): OutputIdempotencyDecision
}

/**
 * Batches each bounded engine write into smaller delegate writes and waits for
 * every delegate acknowledgement. This is a synchronous batching decorator:
 * it never acknowledges items merely because they entered an unflushed local
 * buffer, so it is suitable for required outputs. The largest in-memory batch
 * is [maxBatchSize].
 */
public class BatchedOutput<T : Any>(
    private val delegate: Output<T>,
    public val maxBatchSize: Int = 128,
    override val id: String = "${delegate.id}-batched",
) : Output<T>, OutputLifecycle {
    init {
        require(maxBatchSize > 0) { "Output batch size must be positive" }
        require(id.isNotBlank()) { "Batched output ID must not be blank" }
    }

    override suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult {
        var accepted = 0
        for (batch in items.asSequence().chunked(maxBatchSize)) {
            val result = try {
                delegate.write(batch, context)
            } catch (cancelled: kotlinx.coroutines.CancellationException) {
                throw cancelled
            } catch (error: Throwable) {
                OutputResult.Failed(error)
            }
            when (result) {
                is OutputResult.Accepted -> {
                    if (result.itemCount != batch.size) {
                        return OutputResult.Failed(
                            IllegalStateException("Batched output delegate acknowledged an unexpected item count"),
                        )
                    }
                    accepted += result.itemCount
                }

                is OutputResult.Skipped -> return result
                is OutputResult.Failed -> return result
            }
        }
        return OutputResult.Accepted(accepted)
    }

    override suspend fun close() {
        (delegate as? OutputLifecycle)?.close()
    }
}

/**
 * Retries a complete bounded delegate batch according to [policy]. A failure
 * after an external side effect may cause the same batch to be delivered more
 * than once; callers must use an idempotency hook or destination semantics
 * appropriate to that duplicate window.
 */
public class RetryingOutput<T : Any>(
    private val delegate: Output<T>,
    private val policy: OutputRetryPolicy = FixedOutputRetryPolicy(),
    public val maxAttempts: Int = 3,
    override val id: String = "${delegate.id}-retry",
) : Output<T>, OutputLifecycle {
    init {
        require(maxAttempts > 0) { "Output retry attempt bound must be positive" }
        require(id.isNotBlank()) { "Retrying output ID must not be blank" }
    }

    override suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult {
        var attempt = 1
        while (true) {
            val result = try {
                delegate.write(items, context)
            } catch (cancelled: kotlinx.coroutines.CancellationException) {
                throw cancelled
            } catch (error: Throwable) {
                OutputResult.Failed(error)
            }
            if (result !is OutputResult.Failed) return result
            if (attempt >= maxAttempts) return result
            when (policy.decide(attempt, result.error)) {
                OutputRetryDecision.Stop -> return result
                OutputRetryDecision.Retry -> {
                    if (policy is FixedOutputRetryPolicy && !policy.delay.isZero) {
                        kotlinx.coroutines.delay(policy.delay.toMillis())
                    }
                    attempt++
                }
            }
        }
    }

    override suspend fun close() {
        (delegate as? OutputLifecycle)?.close()
    }
}

/**
 * Applies a typed idempotency hook before forwarding to [delegate]. Suppressed
 * items count as acknowledged by this decorator; the hook owner is
 * responsible for durable keys/claims and their retention. A rejection is an
 * explicit output failure and never silently drops the value.
 */
public class IdempotentOutput<T : Any>(
    private val delegate: Output<T>,
    private val hook: OutputIdempotencyHook<T>,
    override val id: String = "${delegate.id}-idempotent",
) : Output<T>, OutputLifecycle {
    init {
        require(id.isNotBlank()) { "Idempotent output ID must not be blank" }
    }

    override suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult {
        val forwarded = ArrayList<Produced<T>>(items.size)
        for (item in items) {
            when (val decision = hook.decide(item, context)) {
                OutputIdempotencyDecision.Write -> forwarded += item
                OutputIdempotencyDecision.Suppress -> Unit
                is OutputIdempotencyDecision.Reject ->
                    return OutputResult.Failed(IllegalStateException(decision.reason))
            }
        }
        if (forwarded.isEmpty()) return OutputResult.Accepted(items.size)
        return when (val result = delegate.write(forwarded, context)) {
            is OutputResult.Accepted -> {
                if (result.itemCount == forwarded.size) OutputResult.Accepted(items.size)
                else OutputResult.Failed(
                    IllegalStateException("Idempotent output delegate acknowledged an unexpected item count"),
                )
            }

            is OutputResult.Skipped -> result
            is OutputResult.Failed -> result
        }
    }

    override suspend fun close() {
        (delegate as? OutputLifecycle)?.close()
    }
}
