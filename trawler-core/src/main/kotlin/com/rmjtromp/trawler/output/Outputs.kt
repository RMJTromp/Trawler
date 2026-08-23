package com.rmjtromp.trawler.output

import com.rmjtromp.trawler.pipeline.Output
import com.rmjtromp.trawler.pipeline.OutputResult
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.pipeline.Produced
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow

/**
 * Callback-backed typed output for host-owned delivery.
 *
 * The engine supplies a bounded batch and waits for the callback to finish.
 * The callback runs in the pipeline's structured coroutine, so cancellation
 * propagates to it. A callback exception becomes [OutputResult.Failed] while
 * cancellation is rethrown; the host owns any side effects and duplicate
 * handling.
 */
public class CallbackOutput<T : Any>(
    override val id: String = "callback",
    private val callback: suspend (Produced<T>) -> Unit,
) : Output<T> {
    init {
        require(id.isNotBlank()) { "Callback output ID must not be blank" }
    }

    override suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult {
        return try {
            for (item in items) callback(item)
            OutputResult.Accepted(items.size)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Throwable) {
            OutputResult.Failed(error)
        }
    }
}

/**
 * Explicitly selected output that acknowledges no destination work.
 *
 * It is useful when a host consumes [FlowOutput.flow] or intentionally wants
 * a pipeline to produce no external delivery. It never retains produced
 * values and always reports an expected skip.
 */
public class NoOutput<T : Any>(
    override val id: String = "no-output",
) : Output<T> {
    init {
        require(id.isNotBlank()) { "No-output ID must not be blank" }
    }

    override suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult =
        OutputResult.Skipped("No output destination selected")
}

/** Lifecycle surface for outputs that own a bounded buffer or other resource. */
public interface OutputLifecycle {
    /** Closes the output's owned resources; repeated calls are harmless. */
    public suspend fun close()
}

/**
 * Direct typed `Flow<Produced<T>>` collection output.
 *
 * The channel is bounded and applies backpressure to the pipeline when the
 * host is not collecting quickly enough. Values remain `Produced<T>`; no
 * heterogeneous engine value collection is performed. The flow remains open
 * across runs and closes when the engine shuts down or [close] is called.
 */
public class FlowOutput<T : Any>(
    override val id: String = "flow",
    capacity: Int = 128,
) : Output<T>, OutputLifecycle {
    private val channel = Channel<Produced<T>>(capacity = requirePositiveCapacity(capacity))

    /** The bounded typed stream owned by this output. */
    public val flow: Flow<Produced<T>> = channel.receiveAsFlow()

    init {
        require(id.isNotBlank()) { "Flow output ID must not be blank" }
        require(capacity > 0) { "Flow output capacity must be positive" }
    }

    override suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult {
        return try {
            for (item in items) channel.send(item)
            OutputResult.Accepted(items.size)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Throwable) {
            OutputResult.Failed(error)
        }
    }

    override suspend fun close() {
        channel.close()
    }
}

private fun requirePositiveCapacity(capacity: Int): Int {
    require(capacity > 0) { "Flow output capacity must be positive" }
    return capacity
}
