# Trawler FlareSolverr

> **Status:** Phase 7 implemented and tested; unreleased.

`trawler-flaresolverr` is the optional adapter for a separately operated
[FlareSolverr](https://github.com/FlareSolverr/FlareSolverr) v3 service. It
depends on `trawler-core` and `trawler-web` and provides two independent,
opt-in capabilities:

- `FlareSolverrTransport` executes `GET` and form-urlencoded `POST` requests
  through FlareSolverr's `/v1` API, with an optional lifecycle-owned session
  pool. Register it as an engine extension.
- `ChallengeClearance` keeps ordinary requests on `KtorTransport`: a detected
  challenge fails the attempt visibly, the next engine attempt resolves it once
  per origin through `FlareSolverrChallengeResolver`, and the clearance cookies
  and User-Agent are reused through request middleware and a `CookieSession`
  decorator.

Nothing is enabled implicitly. Target safety is an explicit, mandatory
`WebDestinationPolicy` for every FlareSolverr-loaded target and final URL;
redirects and subresources inside FlareSolverr's browser cannot be checked
per hop by Trawler. FlareSolverr v3 reports a synthetic status 200 and no
headers. Clearance cookies, proxy credentials, and session IDs are kept out of
attributes, events, and messages. Reusing clearance requires FlareSolverr and
`KtorTransport` to share the same egress IP/proxy.

Challenge detection and resolution contracts are local to this module; core
has no anti-bot abstraction ([ADR-0009](../docs/decisions/0009-flaresolverr-adapter-boundary.md)).

See the [FlareSolverr feature record](../docs/FLARESOLVERR.md) for the protocol
mapping, wiring, bounds, lifecycle, failure categories, security notes, and
verification.

## Build

```bash
mvn -pl trawler-flaresolverr -am clean verify
```

Unit tests (`*Test`, Surefire) drive the FlareSolverr client through Ktor
MockEngine and the engine end to end through `KtorTransport`; loopback CIO
integration tests (`*IT`, Failsafe) use a process-local JDK `HttpServer`. No
Docker, live FlareSolverr, or public network is required. Commands and
coverage are documented in [`docs/TESTING.md`](../docs/TESTING.md) and
[`docs/TEST-MATRIX.md`](../docs/TEST-MATRIX.md).
