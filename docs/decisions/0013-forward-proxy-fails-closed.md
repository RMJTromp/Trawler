# ADR-0013: Fail-closed forward proxy in the web transport

- **Status:** Accepted
- **Date:** 2026-11-09
- **Since:** 1.3.0
- **Decision owners:** Project owner (scope requested for 1.3.0)
- **Related plan:** `AGENTS.md` sections 4.2, 4.7, and 6

## Context

A host needs every crawl request to leave through a forward proxy so that an
origin never sees its real address; one request that bypasses the proxy is a
serious failure. The transport has two clients, Ktor CIO and the per-host JDK
client, and neither had a proxy option.

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

### Reporting a proxy's CONNECT status on CIO through a custom tunnel

Rejected: CIO's tunnel code is internal and offers no injection point, so the
status is unavailable without replacing the engine.

## Decision

`KtorTransportConfig.proxy` holds an optional immutable `WebProxy`, validated at
construction and unable to carry credentials. Both clients use it for every hop.
Failures attributable to the proxy throw `WebProxyFailureException`, classified
`web-proxy-failure` and retryable through core's visible retry accounting; the
transport never retries directly. CIO counts its per-route limit against the
proxy, so the transport enforces the configured limits itself in proxy mode.
`FlareSolverrServiceConfig.withProxyOf` copies the transport's proxy to
FlareSolverr's browser, so one explicit definition serves both without ambient
configuration; this proxy is never applied to the FlareSolverr service connection.
Connection setup is told apart from waiting on the site by observing when each
client first reads the request body's length.

## Consequences

- A stalled proxy and a stalled site can be told apart, except that a TLS
  handshake stalling behind a working tunnel counts as a proxy failure.
- A host must still make the `withProxyOf` call; nothing can detect that it was
  omitted, because the transport and FlareSolverr share no runtime link.
- A rejected tunnel on the CIO path carries no status because Ktor discards it.
- Error mapping depends on exception types and messages observed in Ktor 3.5.1
  and the JDK; dependency or JDK upgrades can break it, and
  `KtorTransportProxyIT` is the check that catches that.
- DNS for targets still resolves on the host for the destination check, and on
  the CIO path for every HTTPS hop, so the proxy host's firewall must block
  access to its own private network.

## Compliance and verification

`KtorTransportProxyIT` and `WebProxyTest` cover routing, limits, failures,
timeouts, cancellation, and validation. Connect-timeout mapping is unit-tested
only and recorded with other residual risks in `docs/TEST-MATRIX.md`.
