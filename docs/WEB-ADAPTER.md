# Ktor Web Adapter

> **Status:** Implemented
>
> **Owner:** `trawler-web`
>
> **Since:** 0.1.0-SNAPSHOT, unreleased

## Purpose

`trawler-web` supplies the optional HTTP execution boundary for Trawler. It
adapts Ktor CIO requests and responses to the core `Transport` contract and
assembles replaceable public-web policies for destination safety, robots,
redirects, cookies, origin concurrency, pacing, and HTTP classification.

The module is deliberately separate from `trawler-core`. Core still owns
pipeline coordination and typed `T`; this module owns Ktor, sockets, DNS
lookups, HTTP semantics, and web-specific safety decisions.

## Non-goals

This phase does not implement PostgreSQL, durable response storage, browser or
FlareSolverr behavior, HTML parsing, external scheduling, or automatic retries
hidden inside transport. Phase 5 XML discovery is intentionally limited to
bounded RSS/Atom/sitemap locations and does not provide site-specific parsing.

## Public contracts and ownership

| Area | Public types | Owner |
|---|---|---|
| Transport | `KtorTransport`, `KtorTransportConfig` | The transport owns the Ktor client, request execution, response body lifecycle, and adapter resources. |
| Policy composition | `WebTransportPolicies`, `WebPresets` | The caller selects each web policy; the bundle does not erase request or produced values. |
| Destination safety | `WebDestinationPolicy`, `PublicWebSafetyPolicy`, `NetworkAddressResolver` | The policy owns scheme, target, DNS, and address admission. |
| Redirects and diagnostics | `RedirectHistory`, `RedirectHop`, `RedirectHeaderPolicy`, `HeaderRedactionPolicy` | The transport owns the bounded hop loop; redirect headers own credential forwarding and the redactor supplies safe diagnostic copies without changing execution headers. |
| Robots | `RobotsPolicy`, `CachingRobotsPolicy`, `RobotsFetcher` | The robots policy owns bounded rules/cache state; the transport supplies the shared-client fetcher. |
| Origin controls | `OriginConcurrencyPolicy`, `OriginPacingPolicy`, `RetryAfterPolicy` | Policies own permits, pacing state, server cooldowns, and their bounds. |
| Sessions | `CookieSession`, `InMemoryCookieSession` | The session owns process-local cookie values and retention. It is not durable. |
| HTTP outcomes | `HttpFailureClassifier`, `DefaultWebFailureClassifier`, `DefaultHttpRetryPolicy` | Classification and retry remain distinct; core still owns visible retry accounting. |
| XML discovery | `RssDiscovery`, `AtomDiscovery`, `SitemapDiscovery`, `XmlDiscoveryBounds`, `XmlCursorCheckpointCodec` | The adapter owns bounded JDK StAX/gzip parsing and calls only the engine-provided `RequestExecutor`. |

Ktor types appear only in this adapter's public `KtorTransport` construction
surface. No Ktor type is imported by or exposed from `trawler-core`.

## Configuration

`KtorTransport.publicWeb()` creates one CIO client, one bounded JDK DNS
resolver, and the public-web policy preset. Register the transport as an
engine-owned `ExtensionLifecycle` when the engine owns its resources:

```kotlin
// Conceptual usage sketch; this document is not a compiled example source.
val transport = KtorTransport.publicWeb()
val engine = TrawlerEngine {
    extension(transport)
    register<String>("pages") {
        discover(SeedDiscovery(requests = listOf(CrawlRequest("https://example.test/"))))
        transport(transport)
        extract(myExtractor)
        classifyFailures(DefaultWebFailureClassifier)
        retry(DefaultHttpRetryPolicy())
        output(CallbackOutput { produced -> consume(produced.value) })
    }
}
try {
    engine.runOnce()
} finally {
    engine.shutdown()
}
```

The public-web preset defaults are immutable and validated at construction:

| Setting | Default | Bound/behavior |
|---|---:|---|
| Connect timeout | 10 s | Ktor `HttpTimeout` and CIO endpoint timeout. |
| Per-hop request timeout | 30 s | Whole Ktor call for one hop. |
| Socket inactivity timeout | 30 s | Maximum idle interval between packets. |
| Total redirect-chain timeout | 2 min | Outer bound for robots, hops, and body reads. |
| Compressed/decompressed body | 8 MiB / 8 MiB | Raw and decoded limits are independent. |
| Robots body | 128 KiB | Robots fetches use the smaller dedicated bound. |
| Redirects | 8 | Redirect history and loop detection are bounded. |
| Origin permits | 2 | Page and robots calls share the origin limit. |
| Minimum origin delay | 250 ms | Merged with robots delay and `Retry-After`. |
| Maximum pacing wait | 5 min | Longer server/robots waits fail visibly instead of being silently shortened. |
| Cookie entries | 512 | In-memory, oldest-entry eviction. Domain matching follows RFC 6265: IP-literal hosts match only exactly, and `Max-Age` takes precedence over `Expires`. |
| Diagnostic header redaction | Authorization, proxy authorization, cookies, common API/cloud credential headers | Diagnostic copies redact values; execution headers remain unchanged. |

`KtorTransport.create(configuration, policies)` accepts a complete
`WebTransportPolicies` value. Replacing one policy is therefore explicit and
does not require accepting all preset decisions. `fromClient` can wrap a
caller-owned Ktor client by deriving a transport-owned client with
`expectSuccess = false`, `followRedirects = false`, and the configured
`HttpTimeout`; client ownership is controlled by `closeClient`.

`DefaultHeaderRedactionPolicy` is selected by the public-web preset. Hosts and
observer adapters can call `redactHeadersForDiagnostics` before emitting
headers; transport execution retains the original caller-visible headers so
HTTP inspection and cookie handling are not silently altered. The default
redaction set covers authorization, proxy authorization, cookies, common API
keys, and cloud credential headers, and is replaceable for an internal policy.

For an internal or controlled target, replace the public destination policy
explicitly with `AllowAllWebDestinationPolicy` or a narrower caller policy,
and select `NoRobotsPolicy` only when that is an intentional authorization
decision. The public-web preset continues to block loopback and private
addresses.

## Transport lifecycle and cancellation

The transport owns one client and closes it, together with resources supplied
by its factory, when `stop` or `close` is called. `start` performs no network
work. Engine-owned registration gives startup/shutdown ordering; a shared
caller-owned client must instead be closed by its owner.

Every page and robots hop acquires an origin permit and pacing reservation in
that order. The reservation is acquired inside a `try` scope, and both are
released in `finally` before the next redirect hop. Cancellation propagates
through Ktor, body decoding, DNS lookup, robots evaluation, and pacing delay.
The DNS implementation uses a bounded adapter-owned executor rather than a
global dispatcher. The total request timeout also bounds a redirect chain;
when it expires the transport throws Ktor's `HttpRequestTimeoutException`
(classified `http-request-timeout`, retryable) rather than coroutine
cancellation, so an expired chain is a visible, retryable failure. Caller
cancellation still propagates as cancellation.

## Redirect security and ordering

Ktor automatic redirects are disabled. The transport handles each redirect as
a first-class transition:

```text
resolve Location
  -> destination policy and DNS revalidation
  -> robots policy for the new path/origin
  -> destination origin concurrency and pacing
  -> request execution with destination cookie state
  -> bounded body/header adaptation
  -> release permits and evaluate the next hop
```

The default redirect header policy strips `Authorization`, proxy
authorization, cookies, common API-key headers, and entity/framing headers on
cross-origin or body-changing transitions. The destination cookie session is
consulted after this stripping step. 301/302 POST redirects and 303 redirects
become GET; 307/308 preserve method and body. The maximum hop count and exact
target loop set are bounded. Only a redirect count and classification metadata
are added to the core response attributes; raw redirect URLs are not emitted
to default events or logs.

Core request middleware runs in the declared chain before this transport, with
inherited defaults preceding pipeline-local additions. Core response
middleware runs in declared order after the final adapted response. The web
module provides small opt-in header/attribute middleware helpers; it does not
create a second Ktor interceptor chain or hide retries in middleware.

## RSS, Atom, and sitemap discovery

The Phase 5 discovery implementations are resumable core `Discovery` values.
Each fetches its document through the pipeline's engine-controlled executor,
emits bounded request/checkpoint pairs, and lets core apply scope,
authorization, pacing, concurrency, middleware, and active deduplication.
RSS item links and Atom entry links are resolved against the feed target.
Sitemap traversal accepts both `urlset` and `sitemapindex`, follows index
locations breadth first, and resolves each child against the document that
listed it. Recursion depth and document count are finite; reaching a bound
raises a visible discovery failure rather than silently truncating historical
coverage.

The parser disables and explicitly rejects DTD and external-entity events. It
accepts a gzip member by magic-byte detection and bounds both input/compressed
and decompressed bytes.
The transport normally performs content-encoding adaptation first, so this
additional gzip handling covers a bounded raw gzip document supplied to a
custom transport. Locations must resolve to absolute HTTP(S) targets.

Custom paginated discovery uses core's `PaginationStrategy` hook. The hook
returns at most one next request per response; the owning discovery remains
responsible for page bounds, checkpoint candidates, and cancellation.

## Bodies, headers, and response adaptation

The transport disables Ktor `expectSuccess`, so non-2xx responses remain
ordinary inspectable `CrawlResponse` values. Request targets and methods,
header counts, header names, and header values have explicit bounds.
The response channel is drained inside Ktor's statement scope and then closed
by Ktor.

The adapter accepts identity, gzip, and deflate content encodings. It reads no
more than the compressed limit and streams decompression into the independent
decompressed limit. Brotli and chained encodings are rejected predictably.
Request and response bodies are copied into core's immutable byte wrappers;
no serializer or domain model is introduced.

## Public-web safety and threat assumptions

`PublicWebSafetyPolicy` permits configured schemes (HTTP and HTTPS by
default), rejects URI user-info, resolves every hostname, and blocks loopback,
private/site-local, link-local, multicast, unspecified, carrier-grade NAT,
benchmark, and known metadata addresses by default. IPv4 addresses embedded in
IPv6 (mapped `::ffff:0:0/96`, compatible `::/96`, and NAT64 `64:ff9b::/96`) are
judged by the full IPv4 rules, and IPv6 metadata addresses are compared as raw
bytes. An oversized DNS answer is classified `web-dns-answer-limit` and is not
retried. The check runs again for every redirect destination.

This is a fail-closed policy check, not a claim of perfect DNS-to-socket
pinning: the CIO connection may resolve after the policy lookup, so a strict
operator requiring TOCTOU-resistant address pinning must supply a transport
that owns that capability. Internal-network use is possible only through an
explicit replacement policy. Hosts remain responsible for authorization,
terms, privacy, credentials, and retention.

Sensitive headers and cookie values are never placed in the module's default
response attributes, core events, or logs. The response itself can contain
headers because it is the caller-visible transport result; callers selecting
response storage must apply `redactHeadersForDiagnostics` or their own
retention/redaction policy before emitting or persisting diagnostics.

## Robots behavior

`CachingRobotsPolicy` keeps a bounded, process-local origin cache with an
injected clock, TTL, crawler identity, and bounded robots body. The parser
supports user-agent groups, allow/disallow prefix/glob rules (matched with a
linear wildcard matcher, so hostile patterns cannot cause regex backtracking),
and crawl-delay. Crawl delays too large to represent are capped and handled by
the long-delay behavior, never shortened; a target that is not a valid URI is
disallowed without a fetch.
The selected user-agent group is evaluated before the page path. A 404 is an
empty rules document; other unavailable statuses or fetch failures follow the
configured `ALLOW`, `DENY`, or `FAIL` behavior. The default is `DENY`.

An excessive crawl delay is never silently shortened: it can be explicitly
allowed without applying the delay, denied, or failed according to the
configured behavior. Robots fetches use the same Ktor client and ordinary
origin permit/pacing path, but bypass recursive robots checking. The default
origin pacing merges the robots delay and valid `Retry-After` responses.

## Failure, retry, and delivery semantics

`DefaultHttpFailureClassifier` classifies response status without changing the
response. `DefaultWebFailureClassifier` classifies failures that prevent a
response from reaching extraction, including distinct body, header-count,
header-name, and header-value bound categories. `DefaultHttpRetryPolicy` is an explicit
core `RetryPolicy` with bounded deterministic exponential backoff; callers
must install it in the pipeline if they want it. No Ktor retry plugin or
transport loop is installed. Retry therefore remains visible to core attempt
accounting and observers.

`Retry-After` changes the selected origin pacing state. It does not trigger an
automatic retry. A delay beyond the configured pacing bound fails visibly on
the next acquisition, avoiding an unbounded suspended wait. Non-transactional
outputs retain core's documented duplicate window; this module makes no
exactly-once claim.

## Capabilities and dependencies

Direct runtime dependencies are limited to:

- `trawler-core` for core contracts and lifecycle;
- `kotlinx-coroutines-core:1.11.0` for bounded suspending policy state and the
  DNS dispatcher; this web-only alignment leaves core on its existing 1.10.2
  runtime graph;
- `io.ktor:ktor-client-cio-jvm:3.5.1` for the one Ktor CIO client.

Ktor brings its required client, HTTP, network/TLS, serialization plumbing,
Kotlinx I/O, and SLF4J API transitives. No Ktor retry, content-negotiation,
Jackson, Jsoup, parser, database, browser, or telemetry backend is declared.
Ktor MockEngine is a test-scoped dependency owned only by this module. CIO
integration tests use a small JDK `ServerSocket` fixture bound to loopback and
add no runtime dependency. The XML discovery uses JDK StAX and gzip, so Phase
5 adds no runtime dependency. Those transitive APIs are not exposed through
core and are not used to serialize produced values.

## Verification

The implementation is verified with the reactor compile/verify commands, the
module-focused build, Maven dependency-tree inspection, compiler/static source
scans, deterministic `WebPolicyTest`, `PublicWebSafetyPolicyTest`,
`RobotsPolicyTest`, `HttpOutcomePolicyTest`, `CookieSessionTest`,
`CookieSessionRulesTest`, `XmlDiscoveryTest`, `XmlDiscoveryHostileInputTest`,
`KtorTransportTest`, `KtorTransportSafetyTest`, `WebTransportContractsTest`,
and `KtorTransportIT` suites, gated at 95% line / 90% branch coverage. The transport unit tests use
Ktor MockEngine; the CIO integration tests use only process-local loopback
servers and no public website. Together they cover request/response metadata,
body and header bounds, identity/gzip/deflate decoding, redirect method and
credential rules, policy re-entry, permit cleanup, client ownership,
non-2xx inspection, and deterministic cancellation/failure paths. Underlying
CIO socket timeout enforcement is not tested with wall-clock timing; the
deterministic timeout exception and outer cancellation paths are covered.
MockEngine ownership and the remaining matrix are recorded in
[`TESTING.md`](TESTING.md) and [`TEST-MATRIX.md`](TEST-MATRIX.md).

## Limitations

- The module is process-local; cookie and robots caches are not durable.
- The public-web DNS check is revalidation, not address-pinned socket policy.
- The robots parser is intentionally bounded and does not claim complete
  support for every robots extension. Non-404 4xx robots responses are treated
  as unavailable (default `DENY`), which is stricter than RFC 9309, and robots
  redirects are not followed.
- The public-web safety preset does not block 240.0.0.0/4, 192.0.0.0/24,
  TEST-NET ranges, 6to4 (`2002::/16`), Teredo, or `64:ff9b:1::/48`; supply a
  stricter `WebDestinationPolicy` if those must be rejected.
- The cookie session has no public-suffix list, stores `Secure` cookies set
  over plain HTTP, and ignores the dashed `01-Jan-1970` date format.
- Redirect history is retained during one request and exposed as bounded
  metadata, not as a durable audit record.
- HTML parsing, response replay, and site-specific recurring strategies remain
  later-phase or host-owned behavior.
