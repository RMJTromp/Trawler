# Ktor Web Adapter

> **Status:** Implemented
>
> **Owner:** `trawler-web`
>
> **Since:** 1.0.0

## Purpose

`trawler-web` supplies the optional HTTP execution boundary for Trawler. It
adapts Ktor CIO requests and responses (and, for explicitly selected hosts, JDK
`java.net.http` exchanges) to the core `Transport` contract and
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
| Forward proxy | `WebProxy`, `KtorTransportConfig.proxy`, `WebProxyFailureException` | The operator owns the proxy address; the transport owns routing every hop through it and reporting its failures. |
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
| TLS 1.3 hosts (`tls13Hosts`) | Empty | Exact hostnames using the JDK path; see [Per-host TLS 1.3 path](#per-host-tls-13-path). |
| Forward proxy (`proxy`) | `null` (routing unchanged) | Explicit `http://host:port`; see [Forward proxy](#forward-proxy). |
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

The transport owns one client (plus the JDK client when `tls13Hosts` is
non-empty) and closes them, together with resources supplied
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

## Per-host TLS 1.3 path

> **Since:** 1.1.0

### Purpose and non-goals

Ktor's CIO TLS implementation offers at most TLS 1.2: `TLSVersion` stops at
`TLS12` and the ClientHello sends no `supported_versions` extension, through
Ktor 3.6.1. Servers that accept only TLS 1.3 therefore refuse every CIO
handshake. `KtorTransportConfig.tls13Hosts` routes named hosts through a
transport-owned JDK `java.net.http` client so those servers can be reached.
The JDK client negotiates the best version the server offers, so a configured
host may still use TLS 1.2 if that is all it supports.

This is not TLS fallback or fingerprint management. It never selects a path by
observing a failure, and a TLS failure on either path remains an ordinary
visible failure for core retry accounting.

### Configuration

| Setting | Type | Default | Validation and behavior |
|---|---|---|---|
| `KtorTransportConfig.tls13Hosts` | `Set<String>` | `emptySet()` | Exact, case-insensitive hostnames; bracket IPv6 literals. Scheme, port, path, or user-info entries are rejected at construction. Subdomains are not implied. |

```kotlin
// Conceptual usage sketch; this document is not a compiled example source.
KtorTransport.create(
    configuration = KtorTransportConfig(tls13Hosts = setOf("ombudsman.aw", "dvg.aw")),
    policies = policies,
)
```

Selection applies to any scheme on a listed host and is evaluated for every
request and redirect hop, including robots fetches. All three factories honor
it. With an empty set no JDK client, thread, or executor exists and CIO
behavior is unchanged. `trawler-flaresolverr` keeps its own CIO client.

### Required runtime

A non-empty set requires both of the following. Each is checked when the
transport is constructed, before any client exists, and fails with an
`IllegalStateException` rather than weakening a guarantee:

- **Java 21 or newer.** `HttpClient` only became closable in Java 21; on Java
  17 its selector thread and pooled sockets could not be released
  deterministically.
- **The JVM option `-Djdk.httpclient.redirects.retrylimit=1`.** By default the
  JDK client transparently re-sends a GET or HEAD after an abrupt connection
  or handshake failure (once on Java 21, up to five attempts on Java 26), which
  would be a retry hidden from engine attempt accounting. The limit is JVM-wide
  and read once by the JDK, so the host owns it; the library does not change it
  behind other clients in the process.

### Guarantees compared with the CIO path

The transport's redirect loop, destination, robots, origin concurrency,
pacing, cookie, redirect-header, classification, and diagnostic code is shared
and does not know which client ran a hop.

| Guarantee | JDK path behavior |
|---|---|
| Destination safety | Same per-hop policy. As on CIO, the connection resolves after the policy lookup, so this is revalidation and not address pinning. |
| Redirects | The JDK client never follows redirects; the shared loop handles every hop. |
| Retries | None in the transport; JDK re-sends are disabled by the required option. |
| Body limits | Non-blocking subscriber cancels the stream at the compressed bound; `Content-Length` is rejected early; decompression shares the CIO decoder and bound. |
| Headers | Same count/name/value bounds before the body is accepted. Same restricted-header filter and `User-Agent`, `Accept`, `Accept-Encoding`, and `Content-Type` defaults. |
| Timeouts | Per-hop request and total-chain bounds as on CIO, connect via the JDK client, and socket inactivity measured between response bytes because the JDK has no such setting. Before the first response bytes the inactivity allowance also includes the connect timeout. |
| Connection bounds | Exchanges are limited to `maxClientConnections` and `maxConnectionsPerOrigin`; waiting counts against the per-hop request timeout. |
| Cancellation | Cancels the JDK exchange with `cancel(true)`, closing its connection. |
| Cookies | The JDK cookie store is unset; the shared session owns cookies. |
| Proxy | With `proxy` unset the JDK path is always direct and the JVM default `ProxySelector` is not consulted. Ktor CIO does consult that selector when no proxy is configured, so the two paths differ there. See [Forward proxy](#forward-proxy). |
| Lifecycle | `stop`/`close` abort in-flight exchanges (callers see `IOException`) and shut down the JDK client and its bounded adapter-owned executor. |
| Credentials | Same redirect stripping; hostname verification and the JVM default trust store apply; no JDK client logging is enabled by the transport. |

The JDK client uses HTTP/1.1 only, as CIO does. Duplicate request headers are
sent as separate lines instead of CIO's joined line. `Expect` and `Upgrade`
are forbidden by the JDK client and fail visibly with
`IllegalArgumentException` before anything is sent.

### Failure categories

Timeouts map to the same Ktor and JDK exception types the classifier already
recognizes. TLS failures surface as `SSLException` subtypes (CIO: `TLSException`
or `EOFException`) and are classified `web-transport-failure`, retryable.

## Forward proxy

> **Since:** 1.3.0

### Purpose and non-goals

`KtorTransportConfig.proxy` sends every request of a transport through one
forward proxy, so an origin never sees the host's address. It applies to both
clients and to every page hop, redirect hop, and robots fetch. If the proxy
cannot carry a request, the request fails and is never sent directly.

Proxy authentication, SOCKS, HTTPS to the proxy, per-host proxies, rotation,
PAC files, and egress-IP verification are out of scope.

One definition can also serve FlareSolverr: `FlareSolverrServiceConfig.withProxyOf(transportConfig)`
points FlareSolverr's browser at the same address. Trawler's own connection to
the FlareSolverr service never uses this proxy. See
[`FLARESOLVERR.md`](FLARESOLVERR.md).

### Configuration

| Setting | Type | Default | Validation and behavior |
|---|---|---|---|
| `KtorTransportConfig.proxy` | `WebProxy?` | `null` | `null` leaves today's routing unchanged. |
| `WebProxy.url` | `String` | Required | Exactly `http://host:port`: scheme `http`, a host, an explicit port from 1 to 65535, and no user-info, path, query, or fragment. Bracket IPv6 literals. Checked at construction; messages never repeat the value because a rejected URL may hold credentials. |

```kotlin
// Conceptual usage sketch; this document is not a compiled example source.
KtorTransport.publicWeb(
    configuration = KtorTransportConfig(proxy = WebProxy("http://10.8.0.1:8888")),
)
```

`create` and `publicWeb` honor the setting. `fromClient` rejects a non-null
`proxy` with `IllegalArgumentException`, because a proxy cannot be added to an
already built client and requests would otherwise leave directly. JVM proxy
properties and environment variables never select this proxy, and a configured
proxy never consults the JVM default `ProxySelector`. Requests using any spelling
of the `CONNECT` method, or a target containing user-info, are refused before
any connection exists, because the JDK client routes `CONNECT` around its proxy
and absolute-form requests would expose user-info to it. A target's fragment is
not sent.

### Routing and policies

HTTPS targets use a `CONNECT` tunnel; plain HTTP targets are sent to the proxy
in absolute form. The proxy address is operator configuration like the
FlareSolverr endpoint, so the destination policy does not check it. Destination,
robots, pacing, cookie, and redirect policies still run per hop against the real
target. Neither client retries through the proxy or falls back to a direct
connection.

### Concurrency, limits, and pooling

Ktor CIO treats the proxy as the single route for every origin, so its
per-route limit would become a limit across all origins. With a proxy set, the
transport enforces `maxClientConnections` and `maxConnectionsPerOrigin` itself
on the CIO path and sets CIO's per-route limit to the client total. Waiting for
a slot counts against the per-hop request timeout, as on the JDK path. CIO opens
one connection per request; the JDK client pools HTTPS tunnels per destination,
so a tunnel never carries a different origin's traffic. Plain HTTP on the JDK
path may reuse one connection to the proxy across origins, which is ordinary
forward-proxy behavior because each request names its full target.

### Failures

A failure caused by the proxy throws `WebProxyFailureException`, classified by
`DefaultWebFailureClassifier` as `web-proxy-failure` (retryable). `connectStatus`
holds the proxy's status where available and `cause` holds the client's original
exception. A proxy failure during a robots fetch propagates the same way and is
not cached as a robots outcome.

| Condition | CIO observed | JDK observed | `connectStatus` |
|---|---|---|---|
| Proxy refuses the connection | `ConnectException` | `ConnectException` | `null` |
| TCP connect to the proxy times out | `ConnectTimeoutException` | `HttpConnectTimeoutException`, mapped to `ConnectTimeoutException` | `null` |
| CONNECT rejected (not 407) | `IOException("Can not establish tunnel connection")` | `IOException("Tunnel failed, got: N")` | CIO `null`; JDK `N` |
| CONNECT answered 407 | Same as above | Synthetic 407 response | CIO `null`; JDK `407` |
| Proxy closes before answering CONNECT | `EOFException` | `IOException` (no bytes) | `null` |
| Proxy resets the connection | `SocketException` | `SocketException` or the no-bytes `IOException` | `null` |
| Proxy cuts off its CONNECT answer | `EOFException` (`Unexpected end of stream ...`) | `IOException` (`parsing HTTP/1.1 ...`) | `null` |
| Proxy answers CONNECT with garbage | `ParserException` | `ProtocolException` | `null` |
| Proxy accepts but never answers | `SocketTimeoutException` at `socketTimeout` | Inactivity or request timeout | `null` |
| Total chain timeout during connection setup | Ordinary timeout as cause | Ordinary timeout as cause | `null` |
| Proxy answers 407 to plain HTTP | Response status | Response status | `407` |

A 407 inside an established tunnel is the site's own response and stays an
ordinary `CrawlResponse`. Other failures after the tunnel exists, such as TLS
certificate errors or a site that never answers, keep their normal exceptions
and categories. Lifecycle and cancellation match the rest of the transport:
cancelling a hop closes its connection to the proxy, and `close` aborts in-flight
exchanges without reporting them as proxy failures.

### Security

The destination safety check still resolves DNS on the host, while the proxy
resolves the target itself. DNS lookups for targets therefore still leave from
the host, and the two answers can differ. On the CIO path this also happens for
every HTTPS hop regardless of the destination policy, because Ktor resolves the
target locally to name the TLS peer. The proxy host's firewall must block
access to its own private network, because the destination policy cannot.
`WebProxy` cannot hold credentials, so none can reach logs or events through it.

### Limitations

- Ktor CIO discards the CONNECT status, so a rejected tunnel on the CIO path
  reports `connectStatus = null`; only the JDK path reports the status.
- Ktor CIO accepts any 2xx or 3xx answer to CONNECT as success. A 3xx then fails
  as a TLS error, still without reaching the origin directly.
- Plain HTTP errors produced by the proxy (for example a 502) cannot be told
  apart from the origin's and stay ordinary responses. Only HTTPS tunnel
  failures and 407 answers are typed.
- A CONNECT status such as 502 can mean the proxy could not reach the site; the
  failure is still typed as a proxy failure.
- A TLS handshake that stalls or is reset behind a working tunnel is reported as
  a proxy failure on both clients, because neither exposes the moment the tunnel
  is ready.
- If TLS setup is cut off, Ktor CIO's engine can reconnect by itself until the
  per-hop timeout or another failure ends the request. This happens with or
  without a proxy and cannot be configured. Each attempt goes through the proxy,
  but these attempts are not visible to engine attempt accounting, and if the
  per-hop timeout ends the loop the request is reported as a proxy failure.
- A host whose name contains an underscore cannot be used, because `java.net.URI`
  does not parse it as a host.
- Ktor CIO consults the JVM default `ProxySelector` when `proxy` is `null`. That
  is existing behavior and is left unchanged.

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

The per-host TLS 1.3 path uses only the JDK's `java.net.http` module and adds
no dependency.

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
`WebProxyTest`, `KtorTransportIT`, `KtorTransportTls13IT`, and `KtorTransportProxyIT` suites, gated at 95% line / 90% branch coverage. The transport unit tests use
Ktor MockEngine; the CIO integration tests use only process-local loopback
servers and no public website. Together they cover request/response metadata,
body and header bounds, identity/gzip/deflate decoding, redirect method and
credential rules, policy re-entry, permit cleanup, client ownership,
non-2xx inspection, and deterministic cancellation/failure paths. Underlying
CIO socket timeout enforcement is not tested with wall-clock timing; the
deterministic timeout exception and outer cancellation paths are covered.
`KtorTransportTls13IT` drives the JDK path against loopback servers restricted
to TLS 1.3 and also proves the default CIO path is refused by them, on Java 21
and the build JDK.
`KtorTransportProxyIT` runs both clients through a loopback forward proxy that
records CONNECT and absolute-form targets, and proves each origin was reached
only through it.
MockEngine ownership and the remaining matrix are recorded in
[`TESTING.md`](TESTING.md) and [`TEST-MATRIX.md`](TEST-MATRIX.md).

## Limitations

- The module is process-local; cookie and robots caches are not durable.
- The public-web DNS check is revalidation, not address-pinned socket policy.
- JDK-path connect timeout is not exercised (loopback cannot stall a connect
  deterministically), and the TLS 1.3 path has not been run against a real
  third-party server by the test suite.
- Forward proxy limits are listed under [Forward proxy](#forward-proxy).
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
