package com.rmjtromp.trawler.pipeline

import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class RuntimeConfigurationTest {
    @Test
    fun `all minimum runtime bounds are accepted`() {
        val options = EngineRuntimeOptions(
            maxQueueSize = 1,
            maxWorkItemsPerRun = 1,
            maxDiscoveryEmissionsPerDiscovery = 1,
            maxDiscoveryEmissionsPerRun = 1,
            maxFollowUpRequestsPerWorkItem = 0,
            maxValuesPerResponse = 1,
            maxRequestBodyBytes = 1,
            maxResponseBodyBytes = 1,
            maxTargetBytes = 1,
            maxMethodBytes = 1,
            maxHeaderCount = 0,
            maxHeaderNameBytes = 1,
            maxHeaderValueBytes = 0,
            maxAttributeCount = 0,
            maxAttributeNameBytes = 1,
            maxAttributeValueBytes = 0,
            maxProducedValuesPerRun = 1,
            maxAttemptsPerWorkItem = 1,
            maxPendingRuns = 1,
            workerCount = 1,
            claimBatchSize = 1,
            maxOutputBatchSize = 1,
            maxReplayResponsesPerRun = 1,
            leaseDuration = Duration.ofNanos(1),
            runTimeout = Duration.ofNanos(1),
            awaitIdleTimeout = Duration.ofNanos(1),
            shutdownTimeout = Duration.ofNanos(1),
            maxRetainedJobs = 1,
            maxRetainedResources = 1,
            maxRetainedDiscoveryState = 1,
            maxCheckpointPayloadBytes = 1,
        )

        assertEquals(1, options.maxQueueSize)
        assertEquals(0, options.maxFollowUpRequestsPerWorkItem)
        assertEquals(0, options.maxHeaderCount)
        assertEquals(Duration.ofNanos(1), options.shutdownTimeout)
    }

    @Test
    fun `each invalid runtime option reports its own validation branch`() {
        assertInvalid("Runtime queue size must be positive") { EngineRuntimeOptions(maxQueueSize = 0) }
        assertInvalid("Runtime work-item bound must be positive") { EngineRuntimeOptions(maxWorkItemsPerRun = 0) }
        assertInvalid("Runtime discovery emission bound must be positive") {
            EngineRuntimeOptions(maxDiscoveryEmissionsPerDiscovery = 0)
        }
        assertInvalid("Runtime discovery-per-run bound must be positive") {
            EngineRuntimeOptions(maxDiscoveryEmissionsPerRun = 0)
        }
        assertInvalid("Runtime follow-up bound must not be negative") {
            EngineRuntimeOptions(maxFollowUpRequestsPerWorkItem = -1)
        }
        assertInvalid("Runtime attempt bound must be positive") {
            EngineRuntimeOptions(maxAttemptsPerWorkItem = 0)
        }
        assertInvalid("Runtime response value bound must be positive") {
            EngineRuntimeOptions(maxValuesPerResponse = 0)
        }
        assertInvalid("Runtime request body bound must be positive") {
            EngineRuntimeOptions(maxRequestBodyBytes = 0)
        }
        assertInvalid("Runtime response body bound must be positive") {
            EngineRuntimeOptions(maxResponseBodyBytes = 0)
        }
        assertInvalid("Runtime target bound must be positive") {
            EngineRuntimeOptions(maxTargetBytes = 0)
        }
        assertInvalid("Runtime method bound must be positive") {
            EngineRuntimeOptions(maxMethodBytes = 0)
        }
        assertInvalid("Runtime header count bound must not be negative") {
            EngineRuntimeOptions(maxHeaderCount = -1)
        }
        assertInvalid("Runtime header-name bound must be positive") {
            EngineRuntimeOptions(maxHeaderNameBytes = 0)
        }
        assertInvalid("Runtime header-value bound must not be negative") {
            EngineRuntimeOptions(maxHeaderValueBytes = -1)
        }
        assertInvalid("Runtime attribute count bound must not be negative") {
            EngineRuntimeOptions(maxAttributeCount = -1)
        }
        assertInvalid("Runtime attribute-name bound must be positive") {
            EngineRuntimeOptions(maxAttributeNameBytes = 0)
        }
        assertInvalid("Runtime attribute-value bound must not be negative") {
            EngineRuntimeOptions(maxAttributeValueBytes = -1)
        }
        assertInvalid("Runtime produced-value bound must be positive") {
            EngineRuntimeOptions(maxProducedValuesPerRun = 0)
        }
        assertInvalid("Runtime worker count must be positive") { EngineRuntimeOptions(workerCount = 0) }
        assertInvalid("Runtime pending-run bound must be positive") { EngineRuntimeOptions(maxPendingRuns = 0) }
        assertInvalid("Runtime claim batch size must be positive") { EngineRuntimeOptions(claimBatchSize = 0) }
        assertInvalid("Runtime output batch size must be positive") { EngineRuntimeOptions(maxOutputBatchSize = 0) }
        assertInvalid("Runtime replay response bound must be positive") {
            EngineRuntimeOptions(maxReplayResponsesPerRun = 0)
        }
        assertInvalid("Runtime lease duration must be positive") {
            EngineRuntimeOptions(leaseDuration = Duration.ZERO)
        }
        assertInvalid("Runtime lease duration must be positive") {
            EngineRuntimeOptions(leaseDuration = Duration.ofNanos(-1))
        }
        assertInvalid("Runtime run timeout must be positive") { EngineRuntimeOptions(runTimeout = Duration.ZERO) }
        assertInvalid("Runtime run timeout must be positive") {
            EngineRuntimeOptions(runTimeout = Duration.ofNanos(-1))
        }
        assertInvalid("Runtime idle timeout must be positive") {
            EngineRuntimeOptions(awaitIdleTimeout = Duration.ZERO)
        }
        assertInvalid("Runtime idle timeout must be positive") {
            EngineRuntimeOptions(awaitIdleTimeout = Duration.ofNanos(-1))
        }
        assertInvalid("Runtime shutdown timeout must be positive") {
            EngineRuntimeOptions(shutdownTimeout = Duration.ZERO)
        }
        assertInvalid("Runtime shutdown timeout must be positive") {
            EngineRuntimeOptions(shutdownTimeout = Duration.ofNanos(-1))
        }
        assertInvalid("Runtime retained jobs must be at least the queue size") {
            EngineRuntimeOptions(maxQueueSize = 2, maxRetainedJobs = 1)
        }
        assertInvalid("Runtime retained resources must be at least the queue size") {
            EngineRuntimeOptions(maxQueueSize = 2, maxRetainedResources = 1)
        }
        assertInvalid("Runtime retained discovery state bound must be positive") {
            EngineRuntimeOptions(maxRetainedDiscoveryState = 0)
        }
        assertInvalid("Runtime checkpoint payload bound must be positive") {
            EngineRuntimeOptions(maxCheckpointPayloadBytes = 0)
        }
    }

    @Test
    fun `run and daemon selections snapshot caller collections`() {
        val runIds = linkedSetOf("one")
        val run = RunRequest(runIds)
        runIds += "two"

        assertEquals(setOf("one"), run.selectedPipelineIds)
        @Suppress("UNCHECKED_CAST")
        val mutableRunIds = run.selectedPipelineIds as MutableSet<String>
        assertFailsWith<UnsupportedOperationException> { mutableRunIds += "forbidden" }

        val daemonIds = linkedSetOf("one")
        val daemon = DaemonConfig(pipelineIds = daemonIds)
        daemonIds.clear()

        assertEquals(setOf("one"), daemon.selectedPipelineIds)
        val selectedPipelineIds = assertNotNull(daemon.selectedPipelineIds)
        assertTrue(selectedPipelineIds.contains("one"))
        assertFalse(selectedPipelineIds.isEmpty())
    }

    @Test
    fun `run and daemon constructors reject invalid selections and timeouts`() {
        assertFailsWith<IllegalArgumentException> { RunRequest(setOf(" ")) }
        assertFailsWith<IllegalArgumentException> { RunRequest(reason = " ") }
        assertFailsWith<IllegalArgumentException> { RunRequest(timeout = Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { DaemonConfig(pipelineIds = emptySet()) }
        assertFailsWith<IllegalArgumentException> { DaemonConfig(pipelineIds = setOf(" ")) }
        assertFailsWith<IllegalArgumentException> { RunFailure(" ", IllegalStateException()) }
    }

    private fun assertInvalid(expectedMessage: String, create: () -> EngineRuntimeOptions) {
        val error = assertFailsWith<IllegalArgumentException> { create() }
        assertEquals(expectedMessage, error.message)
    }
}
