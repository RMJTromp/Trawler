# FlareSolverr Adapter

> **Status:** Implemented; covered by unit, engine end-to-end, and loopback CIO integration tests
>
> **Owner:** `trawler-flaresolverr`
>
> **Since:** 1.0.0. Since 1.3.2 a challenge met on a cross-origin redirect hop is cleared for that hop's origin.

## Purpose

`trawler-flaresolverr` integrates a separately operated
[FlareSolverr](https://github.com/FlareSolverr/FlareSolverr) v3 service with
Trawler web pipelines. It offers two independent, opt-in capabilities:

- **A. FlareSolverr transport** — `FlareSolverrTransport` executes a
  `CrawlRequest` through FlareSolverr's `/v1` API, so the target is loaded by
  FlareSolverr's real browser.
- **B. Solve-then-reuse challenge clearance** — ordinary requests keep using
  `KtorTransport`; when a Cloudflare-style challenge is detected, FlareSolverr
  solves it once per origin and the resulting clearance cookies and browser
  User-Agent are reused by later Ktor requests to that origin.

Nothing is activated by depending on the artifact. Each piece must be
constructed and installed explicitly.

## Non-goals

- No anti-bot contract is added to `trawler-core`. `ChallengeDetector`,
  `ChallengeResolver`, `ChallengeKind`, and clearance types live in this
  module ([ADR-0009](decisions/0009-flaresolverr-adapter-boundary.md)).
- No browser automation, CAPTCHA solving, Turnstile token handling,
  screenshots, or `waitInSeconds`/`disableMedia` options.
- No durable clearance or session storage; all state is process-local.
- No automatic retry, fallback transport, or proactive solving.
- Managing or securing the FlareSolverr service itself (deployment, logs,
  resource limits) is the operator's responsibility.

## Public contracts

| Area | Public types | Owner and boundary |
|---|---|---|
| Service connection | `FlareSolverrServiceConfig`, `FlareSolverrProxy` | Endpoint, Trawler-side call timeouts, JSON response bound, call concurrency, and the FlareSolverr browser proxy. |
| Transport (A) | `FlareSolverrTransport`, `FlareSolverrTransportConfig`, `FlareSolverrSessionConfig`, `FlareSolverrResponseAttributes` | The transport owns its FlareSolverr client and optional session pool; it is a core `Transport` + `ExtensionLifecycle` + `AutoCloseable`. |
| Detection (B) | `ChallengeDetector`, `ChallengeDetection`, `ChallengeKind`, `CloudflareChallengeDetector` | Pure, replaceable policy over a bounded core response. |
| Resolution (B) | `ChallengeResolver`, `ChallengeResolutionRequest`, `FlareSolverrChallengeResolver`, `FlareSolverrResolverConfig` | One resolution per call, no internal retry; the FlareSolverr resolver owns its client. |
| Clearance state (B) | `Clearance`, `ClearanceCookie`, `StoredClearance`, `ClearanceStore`, `ClearanceStoreConfig` | Bounded, expiring, process-local, exact-origin store. |
| Coordination (B) | `ChallengeClearance` (`requestMiddleware`, `responseMiddleware`, `cookieSession(delegate)`), `ChallengeRetryPolicy` | Per-origin single-flight resolution (including the origin of a challenged cross-origin redirect hop), per-hop User-Agent application, cookie merging, and visible retry. |
| Failures | `FlareSolverrException` (sealed: `FlareSolverrUnavailableException`, `FlareSolverrErrorException`, `FlareSolverrProtocolException`, `FlareSolverrResponseLimitException`, `UnsupportedFlareSolverrRequestException`), `ChallengeDetectedException`, `ChallengeResolutionException`, `FlareSolverrFailureClassifier`, `FlareSolverrFailureCategories` | Classification into extensible core categories; retry stays with the selected core `RetryPolicy`. |

Ktor and kotlinx-serialization types are used only internally; no public
signature in this module exposes them, and no type reaches `trawler-core`.

## FlareSolverr protocol mapping

Verified against the FlareSolverr README and v3 source (`dtos.py`,
`flaresolverr_service.py`) and against a running FlareSolverr 3.5.2 container
(see [Verification](#verification)).

| Trawler request | FlareSolverr command |
|---|---|
| `GET`, no body (or empty body) | `request.get` with `url`, `maxTimeout` |
| `POST` with exactly one `Content-Type: application/x-www-form-urlencoded…` header and a UTF-8 body | `request.post` with `postData` |
| Any other method, a `GET` body, a non-form or non-UTF-8 `POST` body | rejected with `UnsupportedFlareSolverrRequestException` before any call |
| `Cookie` request header | `cookies: [{name, value}]` (bounded; malformed pairs are rejected) |
| Other request headers, including `User-Agent` | not forwarded; FlareSolverr v3 removed header support and uses its browser's User-Agent |
| Transport session pool (optional) | `session` and optional `session_ttl_minutes`; sessions created with `sessions.create` (with proxy) and removed with `sessions.destroy` |
| Service proxy, no sessions | per-request `proxy: {url}` (credentials are not accepted there by FlareSolverr) |
| Challenge resolver | `request.get` with `returnOnlyCookies: true` and optional `proxy: {url}` |

Response mapping (`status: "ok"`): `solution.status` → `CrawlResponse.statusCode`,
`solution.headers` → headers, `solution.response` → UTF-8 body bytes,
`solution.url` → `finalTarget`, `solution.cookies`/`solution.userAgent` →
attributes and optional clearance storage. `status: "error"` (FlareSolverr
returns HTTP 500) becomes `FlareSolverrErrorException`; any other shape is a
`FlareSolverrProtocolException`.

**FlareSolverr v3 limitation:** the service hard-codes `solution.status = 200`
and returns empty `headers` because its browser driver cannot observe them. A
target that refuses connections is reported as `ok` with status 200 and
Chrome's error page as the body (observed against 3.5.2). Callers of mode A
must not treat the status as the origin's real status; extraction should
validate the page content.

### Response attributes (mode A)

`FlareSolverrResponseAttributes` defines the only attributes added. All are
non-sensitive:

| Attribute | Value |
|---|---|
| `trawler.flaresolverr.user-agent` | The browser User-Agent. |
| `trawler.flaresolverr.cookie-names` | Comma-separated cookie **names** (present only when cookies exist). |
| `trawler.flaresolverr.cookie-count` | Number of browser cookies. |
| `trawler.flaresolverr.challenge` | `solved`, `not-detected`, or `unknown` (from FlareSolverr's message). |
| `trawler.flaresolverr.body-charset` | Always `UTF-8`; the body is the UTF-8 encoding of the rendered DOM, regardless of any page charset. |

Cookie values are deliberately **not** attributes, because core defines
attributes as non-sensitive diagnostics that may reach observers and response
stores. A host that needs the solved cookie values passes a `ClearanceStore`
to `FlareSolverrTransport.create`; solved cookies that domain-match the final
origin, plus the browser User-Agent, are stored there for that origin.

## Configuration

`FlareSolverrServiceConfig` (validated at construction):

| Setting | Default | Validation / behavior |
|---|---:|---|
| `endpoint` | none (required) | Absolute `http`/`https` URL, no user-info, no fragment, ≤ 2048 chars. Typically `http://localhost:8191/v1`. Not evaluated by the target destination policy. |
| `maxTimeout` | 60 s | Sent as FlareSolverr `maxTimeout`; 1 s ≤ value ≤ `Int.MAX_VALUE` ms. |
| `responseTimeoutMargin` | 30 s | Trawler-side solve-call bound is `maxTimeout + margin`. |
| `sessionCommandTimeout` | 60 s | Bound for `sessions.create`/`sessions.destroy`. |
| `connectTimeout` | 10 s | Ktor connect timeout to the service. |
| `maxServiceResponseBytes` | 16 MiB | Raw JSON bound, checked by `Content-Length` and while streaming. |
| `maxConcurrentCalls` | 2 | 1–64 in-flight calls per consumer (each call drives a browser). |
| `proxy` | `null` | `FlareSolverrProxy(url, username?, password?)`; scheme `http`, `socks4`, or `socks5`; credentials only via the fields, never URL user-info. `withProxyOf(transportConfig)` sets it from `KtorTransportConfig.proxy`, so one definition serves both. |

`FlareSolverrTransportConfig` defaults: `id = "flaresolverr"`, no sessions,
target ≤ 16 KiB, POST body ≤ 64 KiB, ≤ 64 request cookies, response body
≤ 8 MiB (UTF-8 bytes), ≤ 256 response headers (name ≤ 1024, value ≤ 16 KiB),
≤ 128 response cookies (value ≤ 8 KiB), User-Agent ≤ 1024 chars. Exceeding a
response bound raises `FlareSolverrResponseLimitException`; nothing is
silently truncated. Proxy credentials without `sessions` are rejected because
FlareSolverr accepts them only on `sessions.create`.

`FlareSolverrSessionConfig`: `count` 1–32 (default 1), optional `ttl`
(≥ 1 minute, sent in whole minutes).

`FlareSolverrResolverConfig`: `id = "flaresolverr-challenge-resolver"`,
target ≤ 16 KiB, ≤ 64 retained cookies (1–1024), cookie value ≤ 8 KiB,
User-Agent ≤ 1024. Proxy credentials are rejected (the resolver uses no
session).

`ClearanceStoreConfig`: ≤ 256 origins (least recently used evicted),
clearance lifetime 30 min, challenge-marker lifetime 10 min, resolution
failure cooldown 30 s (0 disables), ≤ 64 cookies per origin, merged `Cookie`
header ≤ 16 KiB.

`CloudflareChallengeDetector`: statuses `{403, 503}`, inspects at most the
first 64 KiB of the body, markers `/cdn-cgi/challenge-platform/`,
`window._cf_chl_opt`, `<title>Just a moment...</title>`. A response is a
challenge only with a matching status **and** either a `cf-mitigated` header
containing `challenge`, or Cloudflare evidence (`Server: cloudflare` or a
`CF-RAY` header) plus a body marker. Replace it with any `ChallengeDetector`.

`ChallengeRetryPolicy(delegate, maxChallengeAttempts = 3)`.

### Mode A wiring

```kotlin
// Conceptual usage sketch; not a compiled repository source. The same calls
// were compiled and executed by the manual smoke check described below.
val resolver = JdkNetworkAddressResolver()
val flare = FlareSolverrTransport.create(
    FlareSolverrTransportConfig(FlareSolverrServiceConfig(endpoint = "http://localhost:8191/v1")),
    destination = PublicWebSafetyPolicy(resolver),
)
val engine = EngineConfigurationBuilder()
    .extension(flare)                                   // owns client + sessions
    .register(
        PipelineBuilder<String>("protected-pages")
            .transport(flare)
            .discover(SeedDiscovery(requests = listOf(CrawlRequest("https://example.test/"))))
            .classifyFailures(FlareSolverrFailureClassifier(DefaultWebFailureClassifier))
            .retry(DefaultHttpRetryPolicy())
            .extract(myExtractor)
            .output(CallbackOutput { consume(it.value) })
            .build(PipelineDefaults(policies = BuiltInPolicies.ordinary())),
    )
    .build()
    .let(::TrawlerEngine)
try { engine.runOnce() } finally { engine.shutdown(); resolver.close() }
```

### Mode B wiring

All Mode B pieces must be installed together and share one
`ChallengeClearance`. `KtorTransport.publicWeb()` assembles its own cookie
session, so Mode B uses `KtorTransport.create` with an explicit
`WebTransportPolicies` whose `cookies` is the clearance decorator:

```kotlin
// Conceptual usage sketch; not a compiled repository source. The same wiring
// was compiled and executed end to end by the manual smoke check below.
val dns = JdkNetworkAddressResolver()
val safety = PublicWebSafetyPolicy(dns)
val service = FlareSolverrServiceConfig(endpoint = "http://localhost:8191/v1")
val clearance = ChallengeClearance(
    resolver = FlareSolverrChallengeResolver.create(FlareSolverrResolverConfig(service), safety),
    detector = CloudflareChallengeDetector(),
    store = ClearanceStore(),
)
val ktor = KtorTransport.create(
    policies = WebPresets.publicWebPolicies(dns).let { preset ->
        preset.copy(cookies = clearance.cookieSession(preset.cookies))
    },
)
val pipeline = PipelineBuilder<String>("pages")
    .transport(ktor)
    .addRequestMiddleware(clearance.requestMiddleware)
    .addResponseMiddleware(clearance.responseMiddleware)   // before other response middleware
    .classifyFailures(FlareSolverrFailureClassifier(DefaultWebFailureClassifier))
    .retry(ChallengeRetryPolicy(DefaultHttpRetryPolicy()))
    // discover / extract / output as usual
    .build(PipelineDefaults(policies = BuiltInPolicies.ordinary()))
val engine = EngineConfigurationBuilder()
    .extension(ktor)
    .extension(clearance)            // owns the resolver lifecycle; do not also register the resolver
    .register(pipeline)
    .build()
    .let(::TrawlerEngine)
try { engine.runOnce() } finally { engine.shutdown(); dns.close() }
```

`KtorTransport.create` does not own the DNS resolver, so the host closes it.

## Mode B sequence and integration with `KtorTransport`

```text
attempt 1  Ktor GET (default UA, jar cookies)
           -> 403 + cf-mitigated: challenge
           -> responseMiddleware: detector => Detected
              store.recordChallenge(origin)            (marker, no network)
              throw ChallengeDetectedException          (attempt fails visibly)
           -> FlareSolverrFailureClassifier: flaresolverr-challenge-detected
           -> ChallengeRetryPolicy: RetryNow            (engine attempt accounting)
attempt 2  requestMiddleware: marker present, no clearance
              per-origin mutex: first caller resolves, others wait and reuse
              FlareSolverr request.get(returnOnlyCookies) -> cookies + userAgent
              store.store(clearance)                    (generation N)
           -> request User-Agent := clearance UA; attribute generation=N
           -> KtorTransport: cookieSession decorator adds clearance cookies
              and the clearance UA for the matching origin, merged with
              jar cookies, per hop
           -> 200
```

### Challenge on a cross-origin redirect hop

`KtorTransport` follows redirects inside one attempt, so request middleware
only ever sees the request's own target. A challenge served by a hop on
another origin (for example `http://www.example.aw/x.jpg` → `301` →
`https://www.example.aw/x.jpg`, or `https://example.aw/` →
`http://www.example.aw/wp/`) is handled like this:

```text
attempt 1  GET A -> 301 -> GET B -> 403 + cf-mitigated: challenge
           -> responseMiddleware: challenged origin = B (final hop)
              store.recordChallenge(B)                  (marker for B)
              store.recordRedirectedChallenge(A target -> B hop target)
              throw ChallengeDetectedException(origin = B)
attempt 2  requestMiddleware: A has no marker or clearance
              link for A target -> B has a marker -> resolve B once,
              FlareSolverr loads the B hop target, store.store(clearance B)
           -> request goes out unchanged
           -> KtorTransport hop A: no clearance (A has none)
           -> KtorTransport hop B: cookieSession decorator adds B's cookies
              and B's User-Agent
           -> 200
```

The link is keyed by the request target, which the engine keeps across
attempts of the same work item; other work that redirects to B afterwards
already receives B's clearance on that hop. Hop clearance is never forwarded
to the redirecting origin: each hop gets only the clearance of its own exact
origin.

A clearance whose cookies are all `Secure` can never be sent to a plain
`http` hop. When a challenge is met on such a target while its origin holds
that clearance, the response middleware throws `ChallengeResolutionException`
with reason `clearance-not-applicable` instead of `ChallengeDetectedException`,
so the work fails clearly instead of spending its remaining attempts on a
retry that cannot succeed.

Why this integration: `KtorTransport` keeps a caller-supplied `User-Agent`
header (it adds its configured UA only when none is present) and consults the
selected `CookieSession` for every hop after redirect-header stripping.
`InMemoryCookieSession.apply` adds nothing when a `Cookie` header is already
present, so injecting `Cookie` from request middleware would silently drop the
jar's cookies and would bypass per-hop origin matching. The decorator instead
merges clearance cookies into the delegate's result for the exact origin of
each hop, and replaces the hop's `User-Agent` with that origin's clearance
User-Agent, because clearance is bound to it. A caller-supplied `Cookie`
header opts the hop out of clearance entirely (no cookies, no User-Agent
change). **No `trawler-web` or `trawler-core` change was needed.**

The clearance generation lets the response middleware distinguish "the
current clearance was rejected" (invalidate and re-mark) from "a request sent
before the clearance existed was challenged" (keep the newer clearance). The
generation attribute describes the request origin only. For a challenge on a
hop to another origin, the clearance counts as sent when the hop request that
the transport reports in `CrawlResponse.request` targets that origin and its
`Cookie` header carries one of the clearance cookies (for a clearance without
cookies, when it carries the clearance User-Agent). `KtorTransport` reports
the effective final hop request. A transport that reports the original
request instead never invalidates a hop clearance this way; the clearance
then lives until its lifetime ends.

## Lifecycle and state

- `FlareSolverrTransport.start` creates `sessions.count` FlareSolverr
  sessions (none when sessions are not configured). A partial failure
  destroys the created sessions before rethrowing. `stop` destroys every
  created session under `NonCancellable`, each bounded by
  `sessionCommandTimeout`, then closes the client; it is idempotent and safe
  after a failed or missing start, and rethrows the first destroy failure
  after attempting all. `close` releases only the local client; service-side
  sessions survive until FlareSolverr restarts (or rotates them with a TTL).
  A stopped transport cannot be restarted. Executing with configured sessions
  before `start` fails with `IllegalStateException`.
- `FlareSolverrChallengeResolver.start` performs no network work; `stop`/
  `close` close its client. It uses no FlareSolverr session.
- `ChallengeClearance.start`/`stop` forward to the resolver when it is an
  `ExtensionLifecycle`; `stop` also clears the store.
- `ClearanceStore` per origin: no state → *challenge marker* (on detection,
  expires after `challengeMarkerLifetime`) → *clearance* (on successful
  resolution; clears marker and cooldown; expires after
  `maxClearanceLifetime`; individual cookies also expire by their own
  `expires`) → back to *marker* when a request carrying the current
  generation (or a redirect hop carrying the current clearance) is
  challenged. A failed resolution records a cooldown timestamp.
- `ClearanceStore` redirect links: request target → challenged hop origin and
  target, recorded when a challenge is met on a hop to another origin,
  replaced by a newer link for the same request target, expiring after
  `challengeMarkerLifetime`, and removed by `clear(origin)` for either side
  or by `clearAll`. A link triggers a resolution only while the hop origin
  has a challenge marker.

## Concurrency and resource bounds

- Each consumer (transport or resolver) owns one CIO client with at most
  `maxConcurrentCalls` connections and a semaphore of the same size. The
  service may hold the connection silently while solving, so the per-call
  request and socket timeouts both equal the call bound. Waiting for a call
  permit is caller-bounded (engine run timeout/cancellation).
- The session pool is a bounded channel of `count` IDs; each request borrows
  one exclusively and returns it in `finally`.
- Single-flight: one `Mutex` per origin, held only while that origin is being
  resolved; the gate map holds only origins with active callers. A cancelled
  waiter does not affect the resolution; a cancelled leader cancels its own
  call and the next waiter becomes the leader. Different origins resolve in
  parallel subject to `maxConcurrentCalls`.
- Waiting for a resolution happens inside the waiting request's engine
  attempt, so its core concurrency permit is held meanwhile (bounded by the
  service timeouts).
- The store is bounded by `maxOrigins` and per-origin cookie counts; the
  merged cookie header is bounded and an overflow raises `trawler-web`'s
  `CookieHeaderLimitException`. Redirect links are separately bounded by
  `maxOrigins` (expired links evicted first, then least recently used); each
  holds two caller-bounded target strings and no clearance value.
- A request whose target origin and linked hop origin both have markers
  resolves them one after the other inside its attempt, each through that
  origin's single-flight gate.
- No `GlobalScope`, no blocking sleeps, no background coroutines.

## Failure and retry semantics

| Failure | Category | Retryable by default |
|---|---|---|
| `ChallengeDetectedException` | `flaresolverr-challenge-detected` (detail: kind) | yes |
| `ChallengeResolutionException` | `flaresolverr-challenge-resolution-failed` (detail: `resolver-failed`, `resolution-cooldown`, `origin-mismatch`, `clearance-not-applicable`) | yes for cooldown or an unavailable/erroring service; otherwise no |
| `FlareSolverrUnavailableException` | `flaresolverr-unavailable` | yes |
| `FlareSolverrErrorException` | `flaresolverr-service-error` | yes |
| `FlareSolverrProtocolException` | `flaresolverr-protocol` | no |
| `FlareSolverrResponseLimitException` | `flaresolverr-response-limit` (detail: limit kind) | no |
| `UnsupportedFlareSolverrRequestException` | `flaresolverr-unsupported-request` (detail: reason) | no |
| Destination rejection (`WebPolicyRejectedException`) | delegated (`web-policy-rejected`) | no |

`FlareSolverrFailureClassifier` inspects the error and up to eight causes,
then delegates. `ChallengeRetryPolicy` returns `RetryNow` for a detected
challenge while the failed attempt number is below `maxChallengeAttempts`,
and delegates everything else, including resolution failures. With
`DefaultHttpRetryPolicy` as delegate, a retryable resolution failure becomes
`RetryAt` with backoff; by core semantics that defers the work beyond the
current run (the pipeline run reports `ABANDONED` with work retained). The
engine's `maxAttemptsPerWorkItem` still bounds every work item.

Every FlareSolverr call happens at most once per transport execution or
resolution; no adapter code loops or retries. A resolution failure starts the
origin's cooldown so concurrent waiters fail fast instead of each calling
FlareSolverr. Mode A makes no delivery claim beyond core's; FlareSolverr
`POST` is not idempotent, and a retried `POST` may be submitted twice.

## Security, privacy, and retention

- **Sensitive values:** clearance cookie values, solved cookie values, proxy
  credentials, and FlareSolverr session IDs are never placed in response
  attributes, events, exception messages, or `toString` output.
  `Clearance`, `ClearanceCookie`, `StoredClearance`, `SolvedCookie`, and
  `FlareSolverrProxy` redact. FlareSolverr error messages are bounded to 512
  characters, stripped of control characters, and scrubbed of the configured
  proxy username/password; they may still contain the target URL, so the
  classifier does not copy them into failure details. The smoke check
  asserted that the clearance token never appeared in engine events.
- **Effective request headers:** `KtorTransport` records the effective hop
  request (including the merged `Cookie` header and clearance User-Agent) in
  `CrawlResponse.request`. A host that stores responses must apply
  `KtorTransport.redactHeadersForDiagnostics` or its own redaction before
  persisting or emitting them; this is existing `trawler-web` behavior.
- **Egress binding:** Cloudflare binds clearance to the User-Agent and,
  normally, to the client IP. FlareSolverr and `KtorTransport` must share the
  same egress IP/proxy for reuse to work; the adapter cannot enforce this.
  Define the proxy once on `KtorTransportConfig` and call
  `FlareSolverrServiceConfig.withProxyOf` to share it. Omitting that call leaves
  the browser on its own network path. `FlareSolverrProxy` configures only
  FlareSolverr's browser: it is never applied to Trawler's connection to the
  service (as for any Ktor CIO client without a proxy, a JVM-wide
  `ProxySelector` still affects that connection), the address must be reachable from FlareSolverr's network (a loopback address
  means the FlareSolverr container itself), and a browser proxy failure
  surfaces as a FlareSolverr error rather than `WebProxyFailureException`.
- **Destination safety:** the initial target must be `http`/`https`
  (enforced regardless of policy, so FlareSolverr's browser is never sent to
  `file:`/`data:` URLs) and is evaluated by the mandatory
  `WebDestinationPolicy` before the call; the final URL is checked again when
  it differs, and a rejection discards the solution. **Redirects,
  subresources, scripts, and other navigation inside FlareSolverr's browser
  cannot be checked per hop by Trawler**, and DNS for the browser is resolved
  inside the FlareSolverr host. Use network isolation (egress firewall) on the
  FlareSolverr deployment when per-hop safety matters
  ([ADR-0009](decisions/0009-flaresolverr-adapter-boundary.md)).
- **Origin scoping:** clearance is stored and applied per exact origin; only
  cookies whose domain matches the origin host are kept; `Secure` and `Path`
  attributes are honored (a `Secure` cookie is never sent to an `http` hop);
  cross-origin redirect hops never receive another origin's clearance. A hop
  whose own origin has clearance receives that clearance and its
  User-Agent; the User-Agent then stays on later hops of the same chain, as
  any request `User-Agent` does (it is not a credential).
- **FlareSolverr's own logs:** FlareSolverr logs each incoming request body at
  INFO level, which can include request cookies and, for `sessions.create`,
  proxy credentials. Operators must restrict FlareSolverr log access and
  retention.
- **Robots and authorization:** Mode A does not consult robots policy
  (FlareSolverr fetches directly); install an engine `AuthorizationPolicy` if
  robots enforcement is required. In Mode B the target fetch still goes
  through `KtorTransport`'s robots policy, but the single resolver fetch does
  not.
- **Retention:** clearance is held in memory only, for at most the configured
  lifetime, and cleared on `ChallengeClearance.stop`. Nothing is persisted.

## Capabilities and dependencies

Direct runtime dependencies (see [`DEPENDENCIES.md`](DEPENDENCIES.md)):
`trawler-core`, `trawler-web`, `kotlinx-coroutines-core:1.11.0` (the Ktor
alignment from [ADR-0005](decisions/0005-web-coroutines-runtime-alignment.md)),
`ktor-client-cio-jvm:3.5.1` (used directly for the FlareSolverr client), and
`kotlinx-serialization-json-jvm:1.11.0` (tree API only; no compiler plugin).
No core capability is required; any state store works.

## Verification

Performed for this change:

- `mvn -B clean compile` for the full reactor.
- `mvn -B -pl trawler-flaresolverr verify`: 105 unit tests (Surefire) and 6
  integration tests (Failsafe) pass. The suite was run five consecutive times
  without a failure.
- Kotlin compilation of the module's main and test sources with zero compiler
  warnings.
- JaCoCo (`trawler-flaresolverr/target/site/jacoco/jacoco.xml`): 99.47% line
  (946/951) and 96.09% branch (859/894) coverage for the module.
- `mvn -pl trawler-flaresolverr dependency:tree -Dscope=runtime -Dverbose`:
  the only new resolved coordinate is
  `kotlinx-serialization-json-jvm:1.11.0`; its
  `kotlinx-serialization-core-jvm:1.11.0` already arrives through Ktor.
- Manual smoke check (not committed): a throwaway Kotlin program outside the
  repository, against `ghcr.io/flaresolverr/flaresolverr` 3.5.2 in Docker and
  a local fixture HTTP server imitating a Cloudflare challenge
  (403 + `cf-mitigated: challenge`, clearance only for the browser UA plus
  `cf_clearance` plus a jar cookie). It verified: mode A GET/POST mapping,
  `Cookie` → `cookies`, attributes without cookie values, synthetic 200 and
  empty headers, rejection of PUT/JSON POST/`file:` targets, initial and final
  URL destination rejection, public-web safety blocking a private target before
  any call, unreachable service and FlareSolverr timeout classification, a
  two-session pool created on start and destroyed on stop; and mode B
  end to end in a `TrawlerEngine` run with `KtorTransport`: three concurrent
  challenged work items, exactly one FlareSolverr resolution, three visible
  `RETRY_SCHEDULED` events, six attempts, all three cleared on retry with the
  browser UA and merged jar + clearance cookies, no clearance token in events,
  redacted `toString`, store cleared on stop. No live third-party site was
  contacted. The container and fixture were removed afterwards.

Performed for the cross-origin redirect hop fix (issue #3):

- `mvn -B clean verify` for the full reactor passes; `trawler-flaresolverr`
  runs 121 unit tests and 7 integration tests. JaCoCo for the module: 99.50%
  line (1000/1005) and 96.06% branch (903/940) coverage.
- With the coordinator sources reverted to 1.3.1 and the new tests kept,
  11 tests fail, including both engine-level redirect scenarios: the
  resolver is never called and the work fails after its challenge attempts.
- No manual check against a real FlareSolverr or a real protected site was
  run for this fix.

### Automated tests

All tests live in `trawler-flaresolverr/src/test/kotlin` and use no Docker,
no live FlareSolverr, and no public network:

| Test | Scope |
|---|---|
| `FlareSolverrConfigurationTest` | Validation of every config bound; proxy/credential placement; proxy redaction in `toString`; exception messages. |
| `FlareSolverrClientTest` | JSON command posting; every protocol-error path (non-JSON, non-object, missing/unknown status, `ok` with non-200); `status: error` sanitization including credential scrubbing before truncation; declared and streamed size bounds at the boundary; IO/DNS failures → unavailable; permit bound and permit release on cancellation; solution parsing of every field, default, shape error, and bound. |
| `FlareSolverrTransportTest` | GET/POST encoding (`maxTimeout`, cookies, proxy, session, TTL); every unsupported method/body/cookie/target case before any call; initial and final URL destination checks; response and attribute mapping without secrets; clearance-store hook; session pool create/borrow/destroy, failure during start with rollback and successful restart, idempotent stop, destroy-failure aggregation, exclusive borrowing and return on cancellation. |
| `FlareSolverrChallengeResolverTest` | `returnOnlyCookies` command; domain/value/duplicate cookie filtering and `maxCookies`; origin/scheme/length rejection; initial and final destination checks; typed failures; closed-resolver lifecycle. |
| `FlareSolverrFailureClassifierTest` | Every category and retryability, cause-chain depth bound, delegation of destination rejections and foreign errors, no secret in details. |
| `challenge/CloudflareChallengeDetectorTest` | Header and marker positives, Cloudflare-evidence requirement, status negatives, inspected-prefix boundary, replaceable statuses/markers, value-type validation. |
| `challenge/ClearanceStoreTest` | Cookie/clearance validation and redaction, expiry, exact-origin keying, cookie bound, generations, expiry-first then LRU eviction, marker lifetime, generation-checked invalidation, cooldown; redirect-link lifetime, replacement, expiry-first then LRU bound, and removal by `clear`/`clearAll`. |
| `challenge/ChallengeClearanceTest` | Pass-through, User-Agent replacement and generation tagging, single-flight (8 concurrent requests → 1 resolution, virtual time), independent origins, leader cancellation hand-off, failure cooldown fast-fail, origin mismatch, marker expiry while queued, stale vs current generation, lifecycle forwarding, `ChallengeRetryPolicy`; cross-origin hop challenge resolved for the hop origin with the hop target on the next attempt, request and hop origins both resolved, hop resolution failure and cooldown, hop clearance invalidated only when the reported hop carried it (cookies, or User-Agent without cookies), `clearance-not-applicable` for `Secure`-only clearance on `http`. |
| `challenge/ClearanceCookieSessionTest` | Merge with a real `InMemoryCookieSession`, same-name replacement, path/secure/expiry/exact-origin rules, caller `Cookie` header honored, header bound, clearance User-Agent per hop, and per-hop application through `KtorTransport` redirects (never sent cross-origin; a cross-origin hop gets its own origin's clearance and User-Agent). |
| `ChallengeClearanceEngineTest` | `TrawlerEngine` + `KtorTransport` (MockEngine target site) + resolver (fake FlareSolverr): 3 concurrent challenged items → 1 resolution, 6 attempts, 3 `RETRY_SCHEDULED`, no token in events; a site that keeps rejecting clearance → 2 resolutions, 3 attempts, `FAILED`; a failed resolution → `RetryAt`, `ABANDONED` with retained work; a challenge after an `http`→`https` or cross-host `301` → 1 resolution of the hop URL, 2 attempts, `COMPLETED`; `Secure`-only clearance for an `http` hop → `clearance-not-applicable`, 2 attempts, `FAILED`. |
| `FlareSolverrCioIT` | Real Ktor CIO against a loopback JDK `HttpServer`: session lifecycle and mapping, resolver, HTTP 500 error, declared and chunked size bounds, refused connection, and the call timeout. |

The FlareSolverr client accepts an internal `HttpClientEngine` seam
(`FlareSolverrTransport.createWithEngine`,
`FlareSolverrChallengeResolver.createWithEngine`) so unit tests use Ktor
MockEngine; it is `internal` and not part of the public API, and the public
factories always use CIO.

Two production defects were found and fixed while adding the suite:

- a failed `FlareSolverrTransport.start` left destroyed session IDs in the
  session pool, so a later `start` blocked forever on the full pool and
  requests could borrow destroyed sessions;
- `FlareSolverrErrorException` message sanitization truncated before removing
  proxy credentials, so a credential split by the truncation boundary could
  survive partially.

## Limitations

- Tests use a controlled fake FlareSolverr. Compatibility with real
  FlareSolverr releases is evidenced only by the manual 3.5.2 smoke check; a
  protocol change upstream would not be caught by the default suite.
- FlareSolverr v3 reports a synthetic status 200 and no headers, including for
  connection failures (Chrome error page as body).
- Only `GET` and form-urlencoded `POST`; request headers other than `Cookie`
  are not forwarded.
- Navigation inside FlareSolverr's browser bypasses per-hop redirect safety,
  robots, pacing, and origin concurrency.
- A challenge reached through a cross-origin redirect is linked to the
  request by its exact target string. A request middleware installed before
  `ChallengeClearance.requestMiddleware` that changes the target between
  attempts breaks the link; the request then exhausts its challenge attempts
  without resolving the hop origin.
- Whether a hop clearance was sent is read from the hop request the
  transport reports in `CrawlResponse.request` (see
  [Mode B sequence](#mode-b-sequence-and-integration-with-ktortransport)).
- A clearance made only of `Secure` cookies cannot clear a plain `http` hop;
  such a challenge fails with `clearance-not-applicable`. Point the crawl at
  the `https` form of the target instead.
- A caller-supplied `Cookie` header disables clearance cookies for that
  request (the `CookieSession` contract).
- A resolution that returns no clearance cookies is accepted (User-Agent only);
  if the site still challenges, the next attempt re-resolves within the
  attempt bounds.
- Clearance and sessions are process-local and not shared across processes.
- Clearance reuse depends on the shared-egress requirement above.
