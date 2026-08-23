# Trawler Web

> **Status:** Phase 3 and Phase 5 implemented; unreleased.

`trawler-web` is the optional Ktor/CIO adapter for Trawler. It depends on
`trawler-core` and owns HTTP execution, redirect processing, bounded response
bodies, public-network safety, robots, cookies, origin concurrency, pacing,
and HTTP failure classification. Phase 5 adds bounded RSS, Atom, and sitemap
discovery through the normal engine-controlled request path.

It does not add Ktor types to core, create hidden clients from discovery or
middleware, or retry inside the transport. Register one `KtorTransport` as an
engine-owned lifecycle extension when the engine owns its client; use
`KtorTransport.fromClient` when a host owns a shared client. The public-web
preset is opt-in and blocks loopback/private/metadata addresses. Internal
targets require an explicit replacement destination policy.

`RssDiscovery`, `AtomDiscovery`, and `SitemapDiscovery` are resumable
discoveries. They use JDK StAX/gzip, bounded XML documents, breadth-first
sitemap recursion, and the engine-provided `RequestExecutor`; no parser or
second HTTP client is introduced.

See the [web adapter feature record](../docs/WEB-ADAPTER.md) for public
contracts, bounds, lifecycle, redirect security, robots behavior, dependency
ownership, and verification.

## Build

```bash
mvn -pl trawler-web -am clean verify
```

No automated tests or test dependencies are included; tests require explicit
project-owner authorization.
