# Project Status

## Current status

The current repository contains the Phase 6 implementation:

| Area | Status |
| --- | --- |
| Typed core pipeline and engine | Implemented |
| Bounded database-free runtime | Implemented |
| In-memory crawl state | Implemented |
| Coroutine scheduler | Implemented and opt-in |
| Resumable discovery and harvest | Implemented |
| File and in-memory response stores | Implemented |
| Network-free typed replay | Implemented |
| Typed callback, flow, file, and decorator outputs | Implemented |
| Ktor CIO transport | Implemented |
| Public-web safety, robots, redirects, cookies, origin controls | Implemented |
| RSS, Atom, and sitemap discovery | Implemented |
| PostgreSQL state and Flyway migrations | Implemented |
| PostgreSQL response/output history | Implemented |
| PostgreSQL outbox | Implemented with explicit delivery passes |
| Exactly-once external delivery | Not claimed |
| Later specialized adapters | Planned |

The Maven version is 0.1.0-SNAPSHOT and the artifacts are not presented as a stable published compatibility surface.

## Authority reconciliation

Some older planning prose describes the repository as a Phase 1 skeleton with no production implementation. That is historical planning context and is stale relative to the current source tree. For this wiki, current Kotlin source, POMs, migrations, focused docs, and existing verification are authoritative.

The architecture direction in plans/FINAL-PLAN.md remains useful where it agrees with the implemented contracts. It must not be used to claim that an unimplemented later phase exists.

## What is safe to say

The repository currently supports:

- typed pipelines with heterogeneous engine registration;
- bounded in-memory coordination;
- opt-in scheduling;
- resumable discovery and recurring harvest policies;
- optional response capture and zero-network replay;
- explicit typed output acknowledgement;
- Ktor web transport with replaceable safety policies;
- PostgreSQL durable state, history, and outbox adapters.

It does not promise:

- exactly-once delivery;
- automatic public-web safety for ordinary core policies;
- universal serializers or domain schemas;
- automatic outbox delivery workers;
- restart recovery for in-memory state;
- DNS-to-socket pinning in the default public-web policy;
- a durable event archive without an explicit observer adapter.

## Roadmap discipline

Future adapter ideas must remain labeled planned until their module, dependencies, lifecycle, capabilities, failure behavior, retention, and verification are implemented and documented. Do not create placeholder production classes, empty modules, or fake APIs to make a roadmap item appear complete.
