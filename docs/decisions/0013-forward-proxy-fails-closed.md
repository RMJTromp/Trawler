# ADR-0013: Fail-closed forward proxy in the web transport

- **Status:** Accepted
- **Date:** 2026-11-09
- **Since:** 1.3.0; the decision on which client carries proxied requests changed in 1.3.1
- **Decision owners:** Project owner (scope requested for 1.3.0)
- **Related plan:** `AGENTS.md` sections 4.2, 4.7, and 6

## Context

A host needs every crawl request to leave through a forward proxy so that an
origin never sees its real address; one request that bypasses the proxy is a
serious failure. The transport has two clients, Ktor CIO and the per-host JDK
client, and neither had a proxy option.

1.3.0 sent proxied requests through both. In production Ktor CIO 3.5.1 proved
unusable behind a proxy: it writes an absolute-form request line inside HTTPS
tunnels and offers no option to change that, so 8 of 35 sites failed on every run
with 404s, malformed redirects, dropped connections, or 406s, while the same URLs
returned 200 in origin form.

## Decision drivers

- No request may leave directly, including redirect hops and robots fetches.
- Hosts must be able to tell a proxy outage from a site failure.
- `null` must keep today's behavior; no new module or dependency.
- Per-origin limits must keep meaning per origin.

## Considered options

### A pluggable routing abstraction in core

Rejected: core must not know about HTTP proxies, and one proxy per transport has
one real consumer.

### Passing a proxy to `fromClient`

Rejected: a built Ktor engine cannot take a proxy, so the configuration would be
silently ignored and requests would leave directly. It is refused instead.

### Keeping Ktor CIO for proxied requests

Rejected in 1.3.1: the tunnel code that writes the request line, and that discards
the CONNECT status, is internal to CIO and offers no injection point.

## Decision

`KtorTransportConfig.proxy` holds an optional immutable `WebProxy`, validated at
construction and unable to carry credentials. With a proxy set, no CIO client is
built and the JDK client carries every hop for every host, offering TLS 1.2 and 1.3.
Failures attributable to the proxy throw `WebProxyFailureException`, classified
`web-proxy-failure` and retryable through core's visible retry accounting; the
transport never retries directly.
`FlareSolverrServiceConfig.withProxyOf` copies the transport's proxy to
FlareSolverr's browser, so one explicit definition serves both without ambient
configuration; this proxy is never applied to the FlareSolverr service connection.
Connection setup is told apart from waiting on the site by observing when the
client first reads the request body's length.

## Consequences

- A stalled proxy and a stalled site can be told apart, except that a TLS
  handshake stalling behind a working tunnel counts as a proxy failure.
- A host must still make the `withProxyOf` call; nothing can detect that it was
  omitted, because the transport and FlareSolverr share no runtime link.
- A proxy host now needs what the JDK client needs: Java 21 and
  `-Djdk.httpclient.redirects.retrylimit=1`, both checked at construction. A patch
  release adds this requirement because the proxy cannot work on CIO.
- Proxied requests differ from direct CIO requests in the small ways listed in
  `docs/WEB-ADAPTER.md`.
- Error mapping depends on exception types and messages observed in the JDK; JDK
  upgrades can break it, and `KtorTransportProxyIT` is the check that catches that.
- DNS for targets still resolves on the host for the destination check, so the
  proxy host's firewall must block access to its own private network.

## Compliance and verification

`KtorTransportProxyIT` and `WebProxyTest` cover routing, raw request lines, TLS
versions, wire parity with direct CIO, limits, failures, timeouts, cancellation,
and validation. Connect-timeout mapping is unit-tested
only and recorded with other residual risks in `docs/TEST-MATRIX.md`.
