# ADR-0011: Per-host TLS 1.3 through a transport-owned JDK client

- **Status:** Accepted
- **Date:** 2026-11-09
- **Decision owners:** Project owner (scope requested for 1.1.0)
- **Related plan:** `AGENTS.md` sections 4.6, 4.7, and 6

## Context

Ktor CIO's TLS implementation offers at most TLS 1.2 through Ktor 3.6.1, so a
dependency bump cannot reach servers that accept only TLS 1.3. About 30 other
sites work through CIO today, several behind Cloudflare, where a change of TLS
or HTTP-client fingerprint can change how requests are treated.

## Decision drivers

- Hosts that do not need TLS 1.3 must keep exactly today's CIO behavior.
- Every existing transport guarantee must hold on the new path.
- Retries must stay visible to engine attempt accounting.
- No new dependency unless necessary.

## Considered options

### Bump Ktor

Rejected: `TLSVersion` still ends at `TLS12` in 3.6.1.

### Ktor Java engine (`ktor-client-java`)

Rejected. Its `close` drops the JDK client reference without shutting it down,
it has no socket-inactivity timeout, its `sendAsync(...).await()` cancels with
`cancel(false)` so a coroutine cancellation does not abort the exchange, and it
defaults to HTTP/2 and the JVM proxy selector. It would also add a dependency.

### Third-party engine (OkHttp or Apache)

Rejected: a new HTTP stack is not necessary when the JDK client can satisfy
every guarantee.

### Automatic fallback on TLS failure

Rejected: a hidden second attempt violates visible retry accounting and
changes a host's fingerprint without an operator decision.

## Decision

`KtorTransportConfig.tls13Hosts`, empty by default, selects a transport-owned
client built directly on `java.net.http`. Selection is exact-host and per hop.
`KtorTransport` keeps owning every policy and shares its decoding and header
bounds, so only the single-hop exchange differs. The transport refuses a
non-empty set unless the runtime is Java 21 or newer and the JVM option
`-Djdk.httpclient.redirects.retrylimit=1` is set.

## Consequences

- Hosts using this feature must supply one JVM option and a Java 21+ runtime;
  failing to do so is a clear startup failure rather than a weakened guarantee.
- The JVM option also applies to any other JDK `HttpClient` in that process.
- Stale pooled connections fail visibly instead of being re-sent, which surfaces
  as an ordinary retryable failure.
- A configured host presents a JDK TLS and HTTP fingerprint, which is why
  selection is per host and opt-in.

## Compliance and verification

`KtorTransportTls13IT` covers selection, redirects, limits, timeouts,
cancellation, lifecycle, proxy isolation, and the construction refusals.
Connect-timeout mapping and real third-party servers remain untested and are
recorded in `docs/TEST-MATRIX.md`.
