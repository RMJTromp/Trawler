# Failure, Retry, and Recovery

Failures remain typed and stage-aware. The engine does not reduce all failures to strings or hide policy decisions inside transport middleware.

## Failure stages

A failure can occur while:

- validating configuration;
- admitting discovery;
- canonicalizing or identifying a request;
- applying scope or authorization;
- claiming work;
- executing a transport;
- capturing a response;
- extracting;
- transforming;
- validating;
- writing output;
- advancing a checkpoint;
- recovering or retaining state.

RunResult and PipelineRunResult expose bounded RunFailure values with a stage and original Throwable. Expected decisions such as Ignore, Drop, Rejected, Skipped, Retry, or Cancelled remain distinguishable from exceptional failures.

## Failure classification

FailureClassifier maps an exception and FailureContext to FailureClassification. The classification contains a stable category, retryable-by-default information, and optional safe detail. A retry policy then returns RetryDecision.RetryAt or RetryDecision.DoNotRetry.

The core default is DefaultFailureClassifier plus NoRetryPolicy. The web adapter supplies DefaultHttpFailureClassifier for inspectable HTTP statuses and DefaultWebFailureClassifier for network/policy exceptions.

## Attempt accounting

An acquired lease does not count as a meaningful attempt. A substantive transport/extraction/output operation starts an attempt. Retry decisions are evaluated with AttemptSummary and remain visible to the engine.

Do not put a retry loop inside:

- Ktor client configuration;
- a discovery's private HTTP client;
- a response middleware;
- an output decorator that the engine cannot observe.

Output-local retries are valid when they are documented as output behavior and preserve their own duplicate window. They are not crawl-attempt retries.

## Core retry flow

~~~text
substantive work fails
    -> classify Throwable
        -> inspect attempt bound and policy
            -> DoNotRetry
            -> RetryAt(time)
                -> requeue with visible attempt
~~~

The engine's maxAttemptsPerWorkItem defaults to 3. A pipeline RetryPolicy can further narrow or shape retry timing. Time and jitter should use injected Clock and RandomSource.

## Web retry

DefaultHttpRetryPolicy retries failures classified as retryable by default, up to three attempts, with a one-second initial delay and a thirty-second maximum delay. It does not retry non-2xx responses invisibly; those responses remain available for HTTP classification and extraction policy.

Retry-After is applied to origin pacing through RetryAfterPolicy. It is not permission to bypass core attempt accounting.

## Work transitions

Durable and in-memory state use explicit WorkTransition outcomes:

- Succeeded;
- Retry with notBefore;
- Failed with reason;
- Skipped with reason;
- Cancelled with reason;
- Abandoned with reason.

Abandoned means the runtime or worker could not finish safely, commonly because a lease or run was lost. Cancelled means a caller or engine requested cancellation. Failed means work completed with an error outcome. Do not collapse these states in an operator dashboard.

## Recovery

PostgresCrawlStateStore performs bounded stale-run summary recovery at run start. It recovers rows according to lease and stale thresholds without pretending that an unobserved process had completed a side effect.

Recovery must remain explicit:

- stale run summaries are not active work;
- lease reclamation is fenced;
- pending work may be retried;
- output or external delivery may still duplicate;
- a checkpoint is not advanced from a failed or abandoned historical run.

The in-memory store has no restart recovery. Process-local jobs disappear when the process ends.

## Checkpoint failure

If accepted work and checkpoint advancement are not atomic, a crash can leave the old checkpoint with already-enqueued work. A later run may emit a duplicate. If checkpoint compare-and-set fails, surface the failure or quarantine according to the state capability; do not skip silently.

## Output failure

For REQUIRED outputs, output failure affects the pipeline run result. For BEST_EFFORT outputs, the failure remains observable but does not by itself fail the pipeline. Neither setting proves external delivery uniqueness.

The PostgreSQL outbox changes the failure boundary: write commits PENDING state first, and a separate deliverPending pass handles delegate failure and retries. A delegate success followed by process loss before the fenced terminal update can duplicate.

## Failure review checklist

- Preserve the original cause.
- Redact details at observability boundaries, not inside internal state.
- Reclassify only at explicit policy boundaries.
- Keep cancellation exceptions propagating.
- Bound retry count and delay.
- Record the stage and pipeline/run IDs without secrets.
- Distinguish failed, cancelled, and abandoned.
- Verify stale workers cannot mutate reclaimed work.

