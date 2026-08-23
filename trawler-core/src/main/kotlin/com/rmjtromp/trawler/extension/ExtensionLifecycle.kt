package com.rmjtromp.trawler.extension

import kotlinx.coroutines.CoroutineScope

/** Context supplied during ordered extension startup. */
public data class ExtensionStartContext(
    /** Engine-owned scope; child work must remain structured beneath it. */
    public val scope: CoroutineScope,
)

/**
 * Reason supplied when an extension is asked to stop. Pipeline-run reasons
 * apply to pipeline-owned extensions; engine shutdown applies to shared
 * engine extensions. A stop call remains cancellation-aware and may report
 * its own failure to the owning runtime.
 */
public enum class ExtensionStopReason {
    ENGINE_SHUTDOWN,
    STARTUP_FAILURE,
    CANCELLATION,
    CONFIGURATION_REJECTED,
    PIPELINE_RUN_COMPLETED,
    PIPELINE_RUN_FAILED,
    PIPELINE_RUN_ABANDONED,
}

/** Context supplied during deterministic extension shutdown. */
public data class ExtensionStopContext(
    public val reason: ExtensionStopReason,
)

/**
 * Lifecycle-only contract for an installed extension.
 *
 * This is intentionally not a universal plugin or storage abstraction:
 * transport, state, response, output, policy, and observer contracts remain
 * focused. The engine starts extensions in configuration order and stops them
 * in reverse order. Startup failure prevents runtime work and causes already
 * started extensions to receive [ExtensionStopReason.STARTUP_FAILURE]. Stop
 * must be cancellation-aware, idempotent, safe after a failed or never-started
 * start, and release adapter-owned resources deterministically.
 */
public interface ExtensionLifecycle {
    /** Stable identifier unique within the effective lifecycle list. */
    public val id: String

    /** Starts the extension under the engine-owned structured scope. */
    public suspend fun start(context: ExtensionStartContext)

    /** Stops the extension and releases all resources it owns. */
    public suspend fun stop(context: ExtensionStopContext)
}

/**
 * Lifecycle explicitly safe to activate for response replay.
 *
 * Replay startup must not activate crawl-state recovery, transports, schedulers,
 * or other ordinary engine work. An adapter that owns resources needed by
 * replay implements this alongside [ExtensionLifecycle]. The replay methods
 * must initialize and release only replay-safe resources, honor cancellation,
 * and remain idempotent. After [stopForReplay], the owner must be safe to
 * activate for another replay until ordinary [stop] is called; implementations
 * are not allowed to claim that a replay lifecycle permits network calls or
 * ordinary crawl-state mutation.
 */
public interface ReplayLifecycle : ExtensionLifecycle {
    /** Starts only resources required by the selected replay operation. */
    public suspend fun startForReplay(context: ExtensionStartContext)

    /** Stops resources started for replay and releases them deterministically. */
    public suspend fun stopForReplay(context: ExtensionStopContext)
}
