package com.rmjtromp.trawler.pipeline

import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

@OptIn(ExperimentalCoroutinesApi::class)
class PipelineRunGateTest {
    private val gate = PipelineRunGate()
    private val started = mutableListOf<String>()

    private fun TestScope.hold(name: String, ids: Set<String>, release: CompletableDeferred<Unit>): Deferred<Unit> =
        async(start = CoroutineStart.UNDISPATCHED) {
            gate.withPipelines(ids) {
                started += name
                release.await()
            }
        }

    @Test
    fun `disjoint selections are admitted at the same time`() = runTest {
        val release = CompletableDeferred<Unit>()
        val first = hold("first", setOf("a"), release)
        val second = hold("second", setOf("b"), release)
        runCurrent()
        assertEquals(listOf("first", "second"), started)
        release.complete(Unit)
        first.await()
        second.await()
    }

    @Test
    fun `an overlapping selection waits for the holder to finish`() = runTest {
        val releaseFirst = CompletableDeferred<Unit>()
        val first = hold("first", setOf("a", "b"), releaseFirst)
        val second = hold("second", setOf("b", "c"), CompletableDeferred(Unit))
        runCurrent()
        assertEquals(listOf("first"), started)
        releaseFirst.complete(Unit)
        first.await()
        second.await()
        assertEquals(listOf("first", "second"), started)
    }

    @Test
    fun `a later narrow run cannot overtake an earlier waiter it overlaps with`() = runTest {
        val releaseFirst = CompletableDeferred<Unit>()
        val releaseAll = CompletableDeferred<Unit>()
        val first = hold("first", setOf("a"), releaseFirst)
        val all = hold("all", setOf("a", "b"), releaseAll)
        val narrow = hold("narrow", setOf("b"), CompletableDeferred(Unit))
        runCurrent()
        assertEquals(listOf("first"), started, "'narrow' is free of the holder but queued behind 'all'")
        releaseFirst.complete(Unit)
        first.await()
        runCurrent()
        assertEquals(listOf("first", "all"), started)
        releaseAll.complete(Unit)
        all.await()
        narrow.await()
        assertEquals(listOf("first", "all", "narrow"), started)
    }

    @Test
    fun `a run disjoint from every waiter passes the queue`() = runTest {
        val release = CompletableDeferred<Unit>()
        val first = hold("first", setOf("a"), release)
        val waiting = hold("waiting", setOf("a"), CompletableDeferred(Unit))
        val other = hold("other", setOf("z"), CompletableDeferred(Unit))
        runCurrent()
        assertEquals(listOf("first", "other"), started)
        release.complete(Unit)
        first.await()
        waiting.await()
        other.await()
    }

    @Test
    fun `a cancelled waiter leaves the queue and lets later waiters in`() = runTest {
        val release = CompletableDeferred<Unit>()
        val first = hold("first", setOf("a"), release)
        val cancelled = hold("cancelled", setOf("a", "b"), CompletableDeferred(Unit))
        val blocked = hold("blocked", setOf("b"), CompletableDeferred(Unit))
        runCurrent()
        assertEquals(listOf("first"), started)
        cancelled.cancel()
        runCurrent()
        assertEquals(listOf("first", "blocked"), started, "only the cancelled waiter was holding 'blocked' back")
        assertTrue(cancelled.isCancelled)
        release.complete(Unit)
        first.await()
        blocked.await()
    }

    @Test
    fun `a failing or cancelled holder releases its pipelines`() = runTest {
        assertFailsWith<IllegalStateException> { gate.withPipelines(setOf("a")) { error("boom") } }
        val holder = hold("holder", setOf("a"), CompletableDeferred())
        runCurrent()
        holder.cancel()
        runCurrent()
        var entered = false
        gate.withPipelines(setOf("a")) { entered = true }
        assertTrue(entered)
    }

    @Test
    fun `an empty selection is admitted even while everything is held`() = runTest {
        val release = CompletableDeferred<Unit>()
        val first = hold("first", setOf("a"), release)
        var entered = false
        gate.withPipelines(emptySet()) { entered = true }
        assertTrue(entered)
        assertFalse(first.isCompleted)
        release.complete(Unit)
        first.await()
    }
}
