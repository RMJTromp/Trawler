# Lifecycle, Concurrency, and Cancellation

Trawler treats lifecycle and cancellation as correctness behavior. A run is not complete merely because a coroutine returned; owned resources, leases, buffers, schedulers, and output handles must have deterministic ownership.

## Engine lifecycle

The normal engine state sequence is:

~~~text
NEW
    -> STARTING
        -> RUNNING
            -> SHUTTING_DOWN
                -> CLOSED
~~~

The engine may also enter a failed startup path. Configuration rejection happens before runtime work. A startup failure stops already-started extensions with STARTUP_FAILURE.

Creating TrawlerEngine is configuration materialization. Direct runOnce starts the engine's ordinary structured runtime on demand. startDaemon adds a structured scheduler child. shutdown cancels active runs and daemons, stops extensions in reverse order, closes owned stores and outputs, and waits within shutdownTimeout.

## Extension order

Engine and pipeline extensions start in registration order. They stop in reverse order. This gives an output decorator or store a deterministic opportunity to release resources after dependants stop.

Use ExtensionStopReason to distinguish:

- ENGINE_SHUTDOWN;
- STARTUP_FAILURE;
- CANCELLATION;
- CONFIGURATION_REJECTED;
- PIPELINE_RUN_COMPLETED;
- PIPELINE_RUN_FAILED;
- PIPELINE_RUN_ABANDONED.

Stop must be safe after partial startup and must preserve the original failure when a cleanup operation also fails.

## Structured concurrency

The engine owns a root SupervisorJob and derived scopes. Extensions receive ExtensionStartContext.scope and must keep child work below it. GlobalScope is forbidden.

The runtime bounds:

- pending engine runs;
- work queue size;
- worker count;
- discovery emissions;
- follow-up requests;
- output batches;
- replay response count;
- request/response bodies;
- checkpoint payloads;
- retained in-memory history;
- scheduler trigger backlog.

An extension may impose tighter bounds, but it must not silently remove the engine's bounds.

## Worker concurrency

Core workerCount defaults to 4. ConcurrencyPolicy is replaceable and can be narrower or broader within state/runtime capacity. Web origin concurrency and pacing are separate policies from pipeline concurrency.

Permits must be released in finally blocks. Do not hold a database transaction or a concurrency permit across unrelated network work. Do not hold a lease while waiting on an unbounded output buffer.

## Leases and fencing

Durable state separates lease ownership from attempts. A worker claims a bounded item with a lease and fencing token. Every lease-owned mutation includes that token. If the lease expires and another worker reclaims the item, the stale worker's transition, response attachment, checkpoint, or output state mutation is rejected.

The in-memory state store preserves the same focused transition semantics within one process but cannot provide cross-process fencing or restart recovery.

## Run timeouts and idle waits

EngineRuntimeOptions.runTimeout bounds a run. awaitIdleTimeout bounds an idle wait. shutdownTimeout bounds cooperative shutdown. A timeout does not mean that all external code instantly stops; cancellation must propagate and adapters must use their own bounded I/O timeouts.

An adapter that ignores cancellation can delay shutdown until its own connection, statement, socket, or dispatcher bound expires. Document that delay rather than claiming instantaneous cancellation.

## Output and cancellation

The engine awaits output acknowledgements. CallbackOutput and FlowOutput propagate cancellation. File, response, and database adapters close their dispatchers or pools under a non-cancellable cleanup section after active work has been cancelled.

If cancellation happens after an external side effect but before a durable acknowledgement, the destination may observe a duplicate on retry or replay. Output contracts must describe this.

## Deterministic shutdown pattern

~~~kotlin
val engine = TrawlerEngine(configuration)
try {
    val handle = engine.startDaemon(
        DaemonConfig(
            trigger = ScheduleTrigger.Manual(),
            pipelineIds = setOf("numbers"),
        ),
    )
    handle.trigger()
} finally {
    engine.shutdown()
}
~~~

The example is conceptual but uses the public lifecycle API. In an application framework, place engine.shutdown in the framework's structured close hook.

## Resource ownership questions

For every extension, record:

1. Who creates the client, pool, dispatcher, file root, or subscription?
2. Who registers its lifecycle?
3. Which stop reason is used when a run is cancelled?
4. What happens if start fails halfway through?
5. Does replay need a separate ReplayLifecycle?
6. What operation remains bounded if cancellation is delayed?

