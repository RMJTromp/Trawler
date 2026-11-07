package com.rmjtromp.trawler.output

import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.pipeline.Output
import com.rmjtromp.trawler.pipeline.OutputResult
import com.rmjtromp.trawler.pipeline.Produced
import com.rmjtromp.trawler.pipeline.ProductionContext
import com.rmjtromp.trawler.retention.RetentionCategory
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class OutputsTest {
    @Test
    fun `callback output preserves callback failures and rethrows cancellation`() = runTest {
        val callbackFailure = IllegalStateException("callback failed")
        val seen = mutableListOf<Int>()
        val output = CallbackOutput<Int> { item ->
            seen += item.value
            if (item.value == 2) throw callbackFailure
        }

        val result = output.write(listOf(produced(1), produced(2), produced(3)), context)
        val failure = assertIs<OutputResult.Failed>(result)
        assertSame(callbackFailure, failure.error)
        assertEquals(listOf(1, 2), seen)
        assertEquals(OutputResult.Accepted(0), output.write(emptyList(), context))
        assertEquals(listOf(1, 2), seen)

        val cancellation = CancellationException("callback cancelled")
        val cancelling = CallbackOutput<Int> { throw cancellation }
        val thrown = assertFailsWith<CancellationException> {
            cancelling.write(listOf(produced(1)), context)
        }
        assertSame(cancellation, thrown)
    }

    @Test
    fun `flow output preserves order applies bounded backpressure and closes`() = runTest {
        val output = FlowOutput<Int>(capacity = 1)
        val write = async(start = CoroutineStart.UNDISPATCHED) {
            output.write(listOf(produced(1), produced(2)), context)
        }

        assertFalse(write.isCompleted)
        assertEquals(1, output.flow.first().value)
        assertEquals(OutputResult.Accepted(2), write.await())
        assertEquals(2, output.flow.first().value)
        assertEquals(OutputResult.Accepted(0), output.write(emptyList(), context))

        output.close()
        output.close()
        val afterClose = assertIs<OutputResult.Failed>(
            output.write(listOf(produced(3)), context),
        )
        assertTrue(afterClose.error is kotlinx.coroutines.channels.ClosedSendChannelException)
    }

    @Test
    fun `flow output cancellation releases a blocked write without closing the channel`() = runTest {
        val output = FlowOutput<Int>(capacity = 1)
        val write = async(start = CoroutineStart.UNDISPATCHED) {
            output.write(listOf(produced(1), produced(2)), context)
        }
        assertFalse(write.isCompleted)
        write.cancel()
        assertFailsWith<CancellationException> { write.await() }
        assertEquals(1, output.flow.first().value)
        output.close()
    }

    @Test
    fun `batched output splits input and stops on acknowledgement outcomes`() = runTest {
        val delegate = RecordingOutput<Int> { batch, _ -> OutputResult.Accepted(batch.size) }
        val output = BatchedOutput(delegate, maxBatchSize = 2)

        assertEquals(OutputResult.Accepted(5), output.write((1..5).map(::produced), context))
        assertEquals(listOf(listOf(1, 2), listOf(3, 4), listOf(5)), delegate.calls.map { it.map(Produced<Int>::value) })
        assertEquals(OutputResult.Accepted(0), output.write(emptyList(), context))
        assertEquals(3, delegate.calls.size)

        val skipped = RecordingOutput<Int> { _, _ -> OutputResult.Skipped("not selected") }
        val skippedResult = BatchedOutput(skipped, maxBatchSize = 2).write((1..4).map(::produced), context)
        assertEquals(OutputResult.Skipped("not selected"), skippedResult)
        assertEquals(1, skipped.calls.size)

        val failure = IllegalArgumentException("delegate failed")
        val failed = RecordingOutput<Int> { _, _ -> OutputResult.Failed(failure) }
        val failedResult = BatchedOutput(failed, maxBatchSize = 2).write((1..4).map(::produced), context)
        assertSame(failure, assertIs<OutputResult.Failed>(failedResult).error)
        assertEquals(1, failed.calls.size)
    }

    @Test
    fun `batched output translates throwing and partial delegates and delegates close`() = runTest {
        val thrown = IllegalStateException("delegate threw")
        val throwing = RecordingOutput<Int> { _, _ -> throw thrown }
        val throwingResult = BatchedOutput(throwing, maxBatchSize = 2).write(listOf(produced(1)), context)
        assertSame(thrown, assertIs<OutputResult.Failed>(throwingResult).error)

        val partial = RecordingOutput<Int> { _, _ -> OutputResult.Accepted(0) }
        val partialResult = BatchedOutput(partial, maxBatchSize = 2).write(listOf(produced(1), produced(2)), context)
        val partialFailure = assertIs<OutputResult.Failed>(partialResult)
        assertTrue(partialFailure.error.message!!.contains("unexpected item count"))

        val release = CompletableDeferred<Unit>()
        val started = CompletableDeferred<Unit>()
        val cancellable = RecordingOutput<Int> { _, _ ->
            started.complete(Unit)
            release.await()
            OutputResult.Accepted(1)
        }
        val output = BatchedOutput(cancellable, maxBatchSize = 1)
        val write = async(start = CoroutineStart.UNDISPATCHED) { output.write(listOf(produced(1)), context) }
        started.await()
        write.cancel()
        assertFailsWith<CancellationException> { write.await() }
        output.close()
        assertEquals(1, cancellable.closeCalls)
    }

    @Test
    fun `retrying output obeys policy and attempt bounds under virtual time`() = runTest {
        val failure = IllegalStateException("temporary")
        var attempts = 0
        val delegate = RecordingOutput<Int> { batch, _ ->
            attempts++
            if (attempts < 3) OutputResult.Failed(failure)
            else OutputResult.Accepted(batch.size)
        }
        val output = RetryingOutput(
            delegate = delegate,
            policy = FixedOutputRetryPolicy(maxAttempts = 4, delay = Duration.ofSeconds(1)),
            maxAttempts = 3,
        )
        val write = async(start = CoroutineStart.UNDISPATCHED) {
            output.write(listOf(produced(1)), context)
        }
        assertEquals(1, delegate.calls.size)
        advanceTimeBy(999)
        assertEquals(1, delegate.calls.size)
        advanceTimeBy(1)
        runCurrent()
        assertEquals(2, delegate.calls.size)
        advanceTimeBy(1_000)
        runCurrent()
        assertEquals(OutputResult.Accepted(1), write.await())
        assertEquals(3, delegate.calls.size)
        assertEquals(
            listOf(listOf(1), listOf(1), listOf(1)),
            delegate.calls.map { batch -> batch.map(Produced<Int>::value) },
        )
    }

    @Test
    fun `retrying output stops early preserves thrown causes and propagates cancellation`() = runTest {
        val failure = IllegalStateException("permanent")
        val stopPolicy = RecordingRetryPolicy(OutputRetryDecision.Stop)
        val stopDelegate = RecordingOutput<Int> { _, _ -> OutputResult.Failed(failure) }
        val stopped = RetryingOutput(stopDelegate, stopPolicy, maxAttempts = 3)
        assertSame(failure, assertIs<OutputResult.Failed>(stopped.write(listOf(produced(1)), context)).error)
        assertEquals(listOf(1), stopPolicy.attempts)
        assertEquals(1, stopDelegate.calls.size)

        val thrown = IllegalArgumentException("delegate exception")
        val throwing = RecordingOutput<Int> { _, _ -> throw thrown }
        val thrownResult = RetryingOutput(throwing, RecordingRetryPolicy(OutputRetryDecision.Stop))
            .write(listOf(produced(1)), context)
        assertSame(thrown, assertIs<OutputResult.Failed>(thrownResult).error)

        var zeroDelayAttempts = 0
        val zeroDelayDelegate = RecordingOutput<Int> { batch, _ ->
            zeroDelayAttempts++
            if (zeroDelayAttempts == 1) OutputResult.Failed(failure)
            else OutputResult.Accepted(batch.size)
        }
        assertEquals(
            OutputResult.Accepted(1),
            RetryingOutput(
                zeroDelayDelegate,
                FixedOutputRetryPolicy(maxAttempts = 2),
                maxAttempts = 2,
            ).write(listOf(produced(1)), context),
        )
        assertEquals(2, zeroDelayDelegate.calls.size)

        val exhaustedDelegate = RecordingOutput<Int> { _, _ -> OutputResult.Failed(failure) }
        val exhausted = RetryingOutput(
            exhaustedDelegate,
            FixedOutputRetryPolicy(maxAttempts = 5),
            maxAttempts = 2,
        )
        assertSame(
            failure,
            assertIs<OutputResult.Failed>(exhausted.write(listOf(produced(1)), context)).error,
        )
        assertEquals(2, exhaustedDelegate.calls.size)

        val retryDelegate = RecordingOutput<Int> { _, _ -> OutputResult.Failed(failure) }
        val retrying = RetryingOutput(
            retryDelegate,
            FixedOutputRetryPolicy(maxAttempts = 3, delay = Duration.ofSeconds(1)),
        )
        val write = async(start = CoroutineStart.UNDISPATCHED) { retrying.write(listOf(produced(1)), context) }
        assertEquals(1, retryDelegate.calls.size)
        write.cancel()
        assertFailsWith<CancellationException> { write.await() }
        assertEquals(1, retryDelegate.calls.size)
    }

    @Test
    fun `idempotent output writes suppresses rejects and preserves forwarded order`() = runTest {
        val delegate = RecordingOutput<Int> { batch, _ -> OutputResult.Accepted(batch.size) }
        val hook = OutputIdempotencyHook<Int> { item, _ ->
            when (item.value) {
                2 -> OutputIdempotencyDecision.Suppress
                else -> OutputIdempotencyDecision.Write
            }
        }
        val output = IdempotentOutput(delegate, hook)
        assertEquals(OutputResult.Accepted(3), output.write((1..3).map(::produced), context))
        assertEquals(listOf(1, 3), delegate.calls.single().map(Produced<Int>::value))

        val allSuppressedDelegate = RecordingOutput<Int> { _, _ -> error("forbidden delegate call") }
        val allSuppressed = IdempotentOutput(
            allSuppressedDelegate,
            OutputIdempotencyHook { _, _ -> OutputIdempotencyDecision.Suppress },
        )
        assertEquals(OutputResult.Accepted(2), allSuppressed.write(listOf(produced(1), produced(2)), context))
        assertTrue(allSuppressedDelegate.calls.isEmpty())

        val rejectedDelegate = RecordingOutput<Int> { _, _ -> error("forbidden delegate call") }
        val rejected = IdempotentOutput(
            rejectedDelegate,
            OutputIdempotencyHook { item, _ ->
                if (item.value == 2) OutputIdempotencyDecision.Reject("duplicate key")
                else OutputIdempotencyDecision.Write
            },
        )
        val rejectedResult = rejected.write(listOf(produced(1), produced(2), produced(3)), context)
        val rejectedFailure = assertIs<OutputResult.Failed>(rejectedResult)
        assertEquals("duplicate key", rejectedFailure.error.message)
        assertTrue(rejectedDelegate.calls.isEmpty())
    }

    @Test
    fun `idempotent output translates acknowledgement and preserves hook exceptions and close`() = runTest {
        val hookFailure = IllegalStateException("hook failed")
        val hookThrowing = IdempotentOutput(
            RecordingOutput<Int> { _, _ -> OutputResult.Accepted(1) },
            OutputIdempotencyHook { _, _ -> throw hookFailure },
        )
        assertSame(hookFailure, assertFailsWith<IllegalStateException> {
            hookThrowing.write(listOf(produced(1)), context)
        })

        val partial = IdempotentOutput(
            RecordingOutput<Int> { _, _ -> OutputResult.Accepted(0) },
            OutputIdempotencyHook { _, _ -> OutputIdempotencyDecision.Write },
        )
        val partialFailure = assertIs<OutputResult.Failed>(partial.write(listOf(produced(1)), context))
        assertTrue(partialFailure.error.message!!.contains("unexpected item count"))

        val skipped = IdempotentOutput(
            RecordingOutput<Int> { _, _ -> OutputResult.Skipped("later") },
            OutputIdempotencyHook { _, _ -> OutputIdempotencyDecision.Write },
        )
        assertEquals(OutputResult.Skipped("later"), skipped.write(listOf(produced(1)), context))

        val delegateFailure = IllegalArgumentException("delegate failed")
        val throwingDelegate = IdempotentOutput(
            RecordingOutput<Int> { _, _ -> throw delegateFailure },
            OutputIdempotencyHook { _, _ -> OutputIdempotencyDecision.Write },
        )
        assertSame(delegateFailure, assertFailsWith<IllegalArgumentException> {
            throwingDelegate.write(listOf(produced(1)), context)
        })

        val lifecycle = RecordingOutput<Int> { _, _ -> OutputResult.Accepted(1) }
        IdempotentOutput(lifecycle, OutputIdempotencyHook { _, _ -> OutputIdempotencyDecision.Write }).close()
        assertEquals(1, lifecycle.closeCalls)
    }

    @Test
    fun `output decorators validate positive bounds and fixed retry delay`() {
        assertFailsWith<IllegalArgumentException> { FlowOutput<Int>(capacity = 0) }
        assertFailsWith<IllegalArgumentException> {
            BatchedOutput(RecordingOutput<Int> { _, _ -> OutputResult.Accepted(0) }, maxBatchSize = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            RetryingOutput(RecordingOutput<Int> { _, _ -> OutputResult.Accepted(0) }, maxAttempts = 0)
        }
        assertFailsWith<IllegalArgumentException> { FixedOutputRetryPolicy(maxAttempts = 0) }
        assertFailsWith<IllegalArgumentException> { FixedOutputRetryPolicy(delay = Duration.ofNanos(-1)) }
        assertFailsWith<IllegalArgumentException> {
            CallbackOutput<Int>(id = " ") { }
        }
    }

    @Test
    fun `no output reports an explicit skip without retaining values`() = runTest {
        val output = NoOutput<Int>()
        assertEquals("no-output", output.id)
        assertEquals(
            OutputResult.Skipped("No output destination selected"),
            output.write(listOf(produced(1), produced(2)), context),
        )
        assertEquals(OutputResult.Skipped("No output destination selected"), output.write(emptyList(), context))
        assertEquals("custom", NoOutput<Int>("custom").id)
        assertEquals(
            "No-output ID must not be blank",
            assertFailsWith<IllegalArgumentException> { NoOutput<Int>(" ") }.message,
        )
    }

    @Test
    fun `flow output default capacity buffers writes until the host collects`() = runTest {
        val output = FlowOutput<Int>()
        assertEquals("flow", output.id)
        assertEquals(OutputResult.Accepted(128), output.write((1..128).map(::produced), context))
        val blocked = async(start = CoroutineStart.UNDISPATCHED) { output.write(listOf(produced(129)), context) }
        assertFalse(blocked.isCompleted)
        assertEquals(1, output.flow.first().value)
        assertEquals(OutputResult.Accepted(1), blocked.await())
        output.close()
        assertEquals(
            "Flow output ID must not be blank",
            assertFailsWith<IllegalArgumentException> { FlowOutput<Int>(id = " ") }.message,
        )
    }

    @Test
    fun `retrying output delegates custom policies without delay and propagates delegate cancellation`() = runTest {
        val failure = IllegalStateException("temporary")
        var attempts = 0
        val delegate = RecordingOutput<Int> { batch, _ ->
            attempts++
            if (attempts < 3) OutputResult.Failed(failure) else OutputResult.Accepted(batch.size)
        }
        val policy = RecordingRetryPolicy(OutputRetryDecision.Retry)
        val output = RetryingOutput(delegate, policy, maxAttempts = 5)
        assertEquals("recording-output-retry", output.id)
        assertEquals(OutputResult.Accepted(1), output.write(listOf(produced(1)), context))
        assertEquals(listOf(1, 2), policy.attempts)
        assertEquals(0L, testScheduler.currentTime)

        val cancellation = CancellationException("delegate cancelled")
        val cancelling = RetryingOutput(
            RecordingOutput<Int> { _, _ -> throw cancellation },
            RecordingRetryPolicy(OutputRetryDecision.Retry),
        )
        assertSame(cancellation, assertFailsWith<CancellationException> {
            cancelling.write(listOf(produced(1)), context)
        })

        output.close()
        assertEquals(1, delegate.closeCalls)
        RetryingOutput(PlainOutput()).close()
        assertEquals(
            "Retrying output ID must not be blank",
            assertFailsWith<IllegalArgumentException> { RetryingOutput(PlainOutput(), id = " ") }.message,
        )
    }

    @Test
    fun `fixed output retry policy stops exactly at its attempt bound`() {
        val policy = FixedOutputRetryPolicy(maxAttempts = 2)
        val error = IllegalStateException("failed")
        assertEquals("fixed-output-retry", policy.id)
        assertEquals(Duration.ZERO, policy.delay)
        assertEquals(OutputRetryDecision.Retry, policy.decide(1, error))
        assertEquals(OutputRetryDecision.Stop, policy.decide(2, error))
        assertEquals(OutputRetryDecision.Stop, policy.decide(3, error))
    }

    @Test
    fun `decorators close only lifecycle delegates and reject blank identifiers`() = runTest {
        val plain = PlainOutput()
        BatchedOutput(plain).close()
        IdempotentOutput(plain, OutputIdempotencyHook { _, _ -> OutputIdempotencyDecision.Write }).close()
        assertEquals(0, plain.writes)

        val lifecycle = RecordingOutput<Int> { batch, _ -> OutputResult.Accepted(batch.size) }
        val batched = BatchedOutput(lifecycle)
        assertEquals("recording-output-batched", batched.id)
        assertEquals(128, batched.maxBatchSize)
        batched.close()
        assertEquals(1, lifecycle.closeCalls)
        assertEquals(
            "recording-output-idempotent",
            IdempotentOutput(lifecycle, OutputIdempotencyHook { _, _ -> OutputIdempotencyDecision.Write }).id,
        )

        assertEquals(
            "Batched output ID must not be blank",
            assertFailsWith<IllegalArgumentException> { BatchedOutput(plain, id = " ") }.message,
        )
        assertEquals(
            "Idempotent output ID must not be blank",
            assertFailsWith<IllegalArgumentException> {
                IdempotentOutput(plain, OutputIdempotencyHook { _, _ -> OutputIdempotencyDecision.Write }, id = " ")
            }.message,
        )
        assertEquals(
            "Idempotency rejection reason must not be blank",
            assertFailsWith<IllegalArgumentException> { OutputIdempotencyDecision.Reject(" ") }.message,
        )
    }

    @Test
    fun `idempotent output returns delegate failures unchanged`() = runTest {
        val failure = IllegalStateException("delegate failed")
        val output = IdempotentOutput(
            RecordingOutput<Int> { _, _ -> OutputResult.Failed(failure) },
            OutputIdempotencyHook { item, _ ->
                if (item.value == 1) OutputIdempotencyDecision.Suppress else OutputIdempotencyDecision.Write
            },
        )
        val result = assertIs<OutputResult.Failed>(output.write(listOf(produced(1), produced(2)), context))
        assertSame(failure, result.error)
    }

    @Test
    fun `output retention requests accept only output-owned categories and positive batches`() {
        val output = OutputRetentionRequest(RetentionCategory.OUTPUT_HISTORY, Instant.EPOCH, 1)
        assertEquals(RetentionCategory.OUTPUT_HISTORY, output.category)
        assertEquals(Instant.EPOCH, output.cutoff)
        assertEquals(1, output.maxEntries)
        assertEquals(
            RetentionCategory.OUTBOX_HISTORY,
            OutputRetentionRequest(RetentionCategory.OUTBOX_HISTORY, Instant.EPOCH, 2).category,
        )
        RetentionCategory.entries
            .filterNot { it == RetentionCategory.OUTPUT_HISTORY || it == RetentionCategory.OUTBOX_HISTORY }
            .forEach { category ->
                assertEquals(
                    "Output retention supports only output-owned categories",
                    assertFailsWith<IllegalArgumentException> {
                        OutputRetentionRequest(category, Instant.EPOCH, 1)
                    }.message,
                )
            }
        assertEquals(
            "Output retention batch must be positive",
            assertFailsWith<IllegalArgumentException> {
                OutputRetentionRequest(RetentionCategory.OUTPUT_HISTORY, Instant.EPOCH, 0)
            }.message,
        )
    }

    private val context: OutputContext = OutputContext(
        CrawlerContext("engine", "pipeline-run", "pipeline"),
    )

    private fun produced(value: Int): Produced<Int> = Produced(
        value = value,
        context = ProductionContext(
            pipelineId = "pipeline",
            engineRunId = "engine",
            pipelineRunId = "pipeline-run",
            requestId = "request-$value",
            responseReference = null,
            originalTarget = "target-$value",
            finalTarget = null,
            producedAt = Instant.EPOCH,
        ),
    )

    private class PlainOutput : Output<Int> {
        override val id: String = "plain"
        var writes: Int = 0

        override suspend fun write(items: List<Produced<Int>>, context: OutputContext): OutputResult {
            writes++
            return OutputResult.Accepted(items.size)
        }
    }

    private class RecordingRetryPolicy(
        private val decision: OutputRetryDecision,
    ) : OutputRetryPolicy {
        override val id: String = "recording-retry"
        val attempts: MutableList<Int> = mutableListOf()

        override fun decide(attempt: Int, error: Throwable): OutputRetryDecision {
            attempts += attempt
            return decision
        }
    }

    private class RecordingOutput<T : Any>(
        private val behavior: suspend (List<Produced<T>>, OutputContext) -> OutputResult,
    ) : Output<T>, OutputLifecycle {
        override val id: String = "recording-output"
        val calls: MutableList<List<Produced<T>>> = mutableListOf()
        var closeCalls: Int = 0

        override suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult {
            calls += items.toList()
            return behavior(items, context)
        }

        override suspend fun close() {
            closeCalls++
        }
    }
}
