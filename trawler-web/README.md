# Trawler Web

> **Status:** Phase 3 implemented; unreleased.

`trawler-web` is the optional Ktor/CIO adapter for Trawler. It depends on
`trawler-core` and owns HTTP execution, redirect processing, bounded response
bodies, public-network safety, robots, cookies, origin concurrency, pacing,
and HTTP failure classification.

It does not add Ktor types to core, create hidden clients from discovery or
middleware, or retry inside the transport. Register one `KtorTransport` as an
engine-owned lifecycle extension when the engine owns its client; use
`KtorTransport.fromClient` when a host owns a shared client. The public-web
preset is opt-in and blocks loopback/private/metadata addresses. Internal
targets require an explicit replacement destination policy.

See the [web adapter feature record](../docs/WEB-ADAPTER.md) for public
contracts, bounds, lifecycle, redirect security, robots behavior, dependency
ownership, and verification.

## Build

```bash
mvn -pl trawler-web -am clean verify
```

No automated tests or test dependencies are included; tests require explicit
project-owner authorization.
