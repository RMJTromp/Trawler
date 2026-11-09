package com.rmjtromp.trawler.pipeline

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred

/**
 * Admits engine runs whose pipeline selections are disjoint at the same time.
 *
 * A run that shares a pipeline with a running or earlier-queued run waits. Waiters are considered in arrival
 * order and a later waiter never overtakes an earlier one it overlaps with, so an all-pipeline run cannot be
 * starved by a stream of narrower runs. Selections are fixed for the lifetime of one admission.
 *
 * Thread-safe. Cancelling a waiter removes it from the queue and lets later waiters re-evaluate; leaving
 * [withPipelines] always releases the admission, including under cancellation.
 */
internal class PipelineRunGate {
    private class Admission(val pipelineIds: Set<String>) {
        val admitted = CompletableDeferred<Unit>()
        var running = false
    }

    private val lock = Any()
    private val running = HashSet<String>()
    private val waiting = ArrayDeque<Admission>()

    /** Runs [block] once no other admission holds any of [pipelineIds]. */
    suspend fun <R> withPipelines(pipelineIds: Set<String>, block: suspend () -> R): R {
        val admission = Admission(pipelineIds)
        synchronized(lock) {
            waiting.addLast(admission)
            admitWaiting()
        }
        try {
            admission.admitted.await()
        } catch (cancelled: CancellationException) {
            leave(admission)
            throw cancelled
        }
        try {
            return block()
        } finally {
            leave(admission)
        }
    }

    private fun leave(admission: Admission) {
        synchronized(lock) {
            if (admission.running) {
                running -= admission.pipelineIds
                admission.running = false
            } else {
                waiting.remove(admission)
            }
            admitWaiting()
        }
    }

    private fun admitWaiting() {
        val claimed = HashSet(running)
        val iterator = waiting.iterator()
        while (iterator.hasNext()) {
            val candidate = iterator.next()
            if (candidate.pipelineIds.none { it in claimed }) {
                iterator.remove()
                running += candidate.pipelineIds
                candidate.running = true
                candidate.admitted.complete(Unit)
            }
            claimed += candidate.pipelineIds
        }
    }
}
