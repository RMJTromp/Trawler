package com.rmjtromp.trawler.extension

import kotlinx.coroutines.CoroutineScope

/** Context supplied during ordered extension startup. */
public data class ExtensionStartContext(
    /** Engine-owned scope; child work must remain structured beneath it. */
    public val scope: CoroutineScope,
)

/** Reason supplied when an extension is asked to stop. */
public enum class ExtensionStopReason {
    ENGINE_SHUTDOWN,
    STARTUP_FAILURE,
    CANCELLATION,
    CONFIGURATION_REJECTED,
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
 * must be cancellation-aware and release adapter-owned resources
 * deterministically.
 */
public interface ExtensionLifecycle {
    /** Stable identifier unique within the effective lifecycle list. */
    public val id: String

    /** Starts the extension under the engine-owned structured scope. */
    public suspend fun start(context: ExtensionStartContext)

    /** Stops the extension and releases all resources it owns. */
    public suspend fun stop(context: ExtensionStopContext)
}
