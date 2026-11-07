# ADR-0009: FlareSolverr adapter boundary and browser-side safety limits

- **Status:** Accepted
- **Date:** 2026-11-07
- **Decision owners:** Project owner (scope requested for the `trawler-flaresolverr` change)
- **Related plan:** `PLAN.md` sections 10.2, 11.3, 11.5, 16, and Phase 7

## Context

Phase 7 lists a FlareSolverr challenge resolver with origin/session
single-flight as a demand-driven integration. `PLAN.md` §11.5 describes
`ChallengeDetector` and `ChallengeResolver` as optional, vendor-neutral
extension families and permits generic hooks early; §10.2 lists "invoke
challenge resolution" among possible retry outcomes and forbids retry loops
hidden in transport or middleware; §11.3 requires every redirect hop to rerun
destination safety, authorization, concurrency, and pacing.

FlareSolverr is an external service that drives a real browser. A request to
it is one call from Trawler's perspective, but the browser may follow
redirects, load subresources, and run scripts that Trawler cannot observe.
FlareSolverr v3 also reports a hard-coded status 200 and no response headers.

Three decisions therefore need a durable record: where challenge contracts
live, how challenge resolution stays visible to engine retry accounting, and
how the per-hop safety rule applies to browser-side navigation.

## Decision drivers

- Core must stay free of anti-bot behavior and vendor shapes (AGENTS.md §4.2).
- Retry must remain visible to engine attempt accounting (AGENTS.md §4.7).
- Every Trawler-controlled hop must keep per-hop safety; claims must not
  exceed what the adapter can enforce.
- No `trawler-core` or `trawler-web` change unless unavoidable.
- One external technology stack per artifact; no hidden network clients for
  target fetches.

## Considered options

### Put `ChallengeDetector`/`ChallengeResolver` in `trawler-core`

Matches the wording of `PLAN.md` §11.5. It adds a core public abstraction with
only one implementation and one consumer, and the contracts need web concepts
(`WebOrigin`, cookies, User-Agent) to be useful. The owner explicitly asked to
keep these contracts inside the adapter for this change.

### Keep challenge contracts adapter-local (selected)

The contracts live in `com.rmjtromp.trawler.flaresolverr.challenge`. Core is
unchanged. Challenge kinds are open values (`ChallengeKind`), not a closed
vendor enum, so the contracts can be promoted to core later without redesign
if a second, non-FlareSolverr provider demonstrates a shared need.

### Resolve challenges inside middleware or transport with an internal retry

Simplest wiring, but it hides a second fetch inside one attempt and corrupts
attempt accounting and observability. Rejected.

### Resolve through the visible engine retry path (selected)

A response middleware converts a detected challenge into
`ChallengeDetectedException`; a classifier decorator maps it to an extensible
category; a retry decorator returns `RetryNow`; the next engine attempt's
request middleware performs a per-origin single-flight resolution before the
request proceeds. Every target fetch remains one counted attempt.

### Pretend FlareSolverr fetches satisfy per-hop redirect safety

Not achievable: Trawler cannot see browser-side hops. Rejected.

### Check what Trawler can see and document the rest (selected)

The adapter enforces an HTTP(S)-only target scheme regardless of policy,
evaluates the initial target with a mandatory caller-supplied
`WebDestinationPolicy` before the call, and evaluates the solution's final URL
when it differs, discarding the result on rejection. Browser-side redirects,
subresources, and scripts are explicitly documented as outside per-hop
checking; operators needing that guarantee must isolate FlareSolverr's network
egress.

## Decision

1. `trawler-flaresolverr` is a separate published artifact depending on
   `trawler-core` and `trawler-web`. It owns its own Ktor CIO client for the
   FlareSolverr endpoint (the transport backend, as JDBC is for
   `trawler-postgres`) and `kotlinx-serialization-json-jvm` for the protocol.
2. Challenge detection/resolution contracts and clearance types are
   adapter-local. Core gains no anti-bot abstraction.
3. Challenge resolution uses only the engine-visible retry path described
   above; no adapter component loops or retries.
4. Clearance reuse integrates through existing `trawler-web` extension
   points: request middleware sets the clearance `User-Agent` (which
   `KtorTransport` preserves) and a `CookieSession` decorator merges clearance
   cookies per hop for the exact origin. No `trawler-web` change is made.
5. FlareSolverr fetches are evaluated by destination policy at the initial and
   final URL only; browser-side navigation is a documented limitation, not a
   claimed guarantee.

## Consequences

### Positive

- Core and `trawler-web` are unchanged; users who omit the artifact inherit
  nothing.
- Attempt counts, retry events, and failure categories reflect every
  challenge and resolution.
- Clearance cookies stay origin-scoped and keep the jar's own cookies.

### Negative or accepted trade-offs

- `PLAN.md` §11.5's core extension families are not created now; a future
  second provider may require promoting the contracts (a public API move).
- Mode B needs coordinated installation of four pieces (request/response
  middleware, cookie-session decorator, classifier/retry decorators) and
  explicit `KtorTransport.create` policy assembly.
- A FlareSolverr-loaded page may reach destinations the configured policy
  would have blocked had it seen the hop.
- FlareSolverr's synthetic status 200 limits what mode A can classify.

### Follow-up work

- Reconsider core challenge contracts if a second provider is implemented.

## Validation

`mvn -B clean compile` and `mvn -B -pl trawler-flaresolverr verify` pass; the
dependency tree adds only `kotlinx-serialization-json-jvm:1.11.0`. The module
suite asserts the visible-retry path end to end (`ChallengeClearanceEngineTest`),
single-flight resolution, and initial/final destination checks.
A manual smoke check against FlareSolverr 3.5.2 in Docker verified the
mode A mapping and initial/final destination rejection, and a mode B engine
run in which three concurrent challenged work items caused exactly one
resolution and completed through visible `RetryNow` attempts. See
[`FLARESOLVERR.md`](../FLARESOLVERR.md#verification).

## Supersession

Revisit when a second challenge provider is implemented (promote contracts to
core), when FlareSolverr exposes real status/headers or per-hop hooks, or if
`trawler-web` adds a first-class clearance/session-provider extension point.

## References

- [`docs/FLARESOLVERR.md`](../FLARESOLVERR.md)
- [`docs/MODULES.md`](../MODULES.md)
- [`PLAN.md`](../../PLAN.md), sections 10.2, 11.3, 11.5, Phase 7
