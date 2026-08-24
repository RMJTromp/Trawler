# Troubleshooting

## Configuration fails before startup

Check the InvalidConfigurationException issues. Common causes:

- a pipeline has no transport or extractor;
- a default policy bundle was not supplied when building PipelineBuilder directly;
- a duplicate ID exists in one ordered list;
- a single-cardinality component was assigned twice without replace...;
- a required state/response capability is not advertised;
- PostgreSQL ordering or codec requirements are incompatible;
- a runtime, web, or database bound is invalid.

Call EngineConfigurationBuilder.validate or PipelineBuilder.validationIssues before build to display deterministic issues.

## The run is completed with no requests

Check:

- whether RunRequest selected the intended pipeline IDs;
- whether discoveries are registered on the pipeline;
- whether the state store contains active or retryable work;
- whether scope, authorization, eligibility, health, circuit breaker, or admission rejected the source;
- whether deduplication suppressed already-active work;
- whether a recrawl policy deferred completed resources.

Use Events-and-Observability to distinguish discovery, admission, and suppression.

## A request is rejected by the web adapter

Inspect the safe policy category, not the raw target:

- target is not a valid HTTP(S) URI;
- URI userinfo was present;
- destination resolved to a blocked address;
- robots denied the target;
- robots was unavailable under DENY/FAIL;
- redirect crossed into a blocked or unauthorized destination;
- origin capacity or pacing bound was exceeded;
- a header, body, target, or redirect exceeded its limit.

If the target is intentionally internal, review the trust boundary before selecting AllowAllWebDestinationPolicy.

## Retries do not happen

Core defaults to NoRetryPolicy. Install an explicit RetryPolicy such as DefaultHttpRetryPolicy. Confirm that the failure classifier marks the failure retryable and that maxAttemptsPerWorkItem has not been reached.

HTTP non-2xx responses are inspectable responses; they are not automatically transport exceptions. Configure HTTP status classification and extractor behavior accordingly.

## The run is abandoned

Look for:

- runTimeout;
- shutdown or caller cancellation;
- lease loss or stale worker rejection;
- state queue/capacity bounds;
- output backpressure;
- response capture failure under RequireResponseCapture;
- adapter timeout or pool exhaustion.

An abandoned run must not advance a historical checkpoint as completed. A durable next run can recover stale state in bounded batches.

## Replay returns no responses

Check:

- the response store advertises REPLAY;
- the selected Pipeline<T> is the exact instance registered in the engine;
- CaptureResponsesWhenConfigured or an explicit capture policy was selected;
- responses have not been evicted;
- the ResponseReplayQuery ownership matches pipeline metadata;
- the replay limit is positive and within runtime bounds;
- a historical response codec remains registered.

Replay makes no network calls. If the host sees network traffic, a resource was incorrectly activated outside ReplayLifecycle.

## Output is failing or duplicating

Check whether the output is REQUIRED or BEST_EFFORT, and whether the delegate is transactional or idempotent. RetryingOutput can repeat a batch after a side effect. PostgreSQL outbox delivery can repeat a delegate call after process loss before DELIVERED.

Use application-owned idempotency keys and retain them for the full duplicate window. Do not solve duplicates by hiding output failures.

## PostgreSQL startup fails

Check:

- JDBC URL begins with jdbc:postgresql:;
- schema and pool identifiers are valid;
- connection timeout is at least 250 ms;
- dispatcher parallelism does not exceed maximumPoolSize;
- database credentials and TLS settings are available;
- the database user can create/use the configured schemas;
- the correct adapter lifecycle was registered;
- migration history is not being shared across state/response/output/outbox paths.

The PostgreSQL adapter requires bounded statements and may fail visibly on timeout or cancellation.

## Shutdown hangs

Check for an extension that:

- ignores cancellation;
- blocks on an unbounded queue;
- owns a client with no request timeout;
- holds a database operation beyond statement timeout;
- created detached coroutines;
- does not close its dispatcher or pool.

Engine shutdown is bounded by shutdownTimeout, but an external operation may still need its own adapter timeout. Fix the extension ownership rather than increasing a global timeout indefinitely.

