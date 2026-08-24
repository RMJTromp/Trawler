# Testing and Quality

This page describes how to verify a Trawler change and how to interpret repository test coverage. The current wiki work is documentation-only: it adds no tests and does not authorize running the automated test suite.

## Build verification

For documentation-only changes, review Markdown structure, links, identifiers, defaults, and status claims. For production/build changes, the repository's normal commands are:

~~~text
mvn clean test
mvn clean verify
~~~

The complete lifecycle includes unit tests, integration tests, and JaCoCo reporting. A compile-only check that avoids tests is:

~~~text
mvn -DskipTests clean compile
~~~

## Focused modules

~~~text
mvn -pl trawler-core -am clean verify
mvn -pl trawler-web -am clean verify
mvn -pl trawler-postgres -am clean verify
~~~

Unit tests are named *Test and run in Surefire. Integration tests are named *IT and run in Failsafe during verify. PostgreSQL integration tests require Docker/Testcontainers. A skipped integration test is an environment limitation, not proof of database correctness.

## What to test when tests are authorized

Do not add or refactor tests without explicit user authorization. When tests are authorized, prioritize:

- typed isolation across heterogeneous pipelines;
- configuration conflicts and capability validation;
- discovery bounds and checkpoint CAS/atomicity;
- cancellation and extension cleanup;
- lease fencing and stale writer rejection;
- retry attempt accounting;
- redirect safety and credential stripping;
- response replay with zero network calls;
- required versus best-effort outputs;
- outbox duplicate and retry windows;
- retention protection and codec version failures.

Use controlled transports, fixed responses, injected Clock and RandomSource, bounded channels, barriers, virtual coroutine time, temporary directories, and isolated PostgreSQL containers. Never use live third-party sites or private captured fixtures.

## Static quality review

Review changed code for:

- dependency direction;
- no GlobalScope or Thread.sleep;
- bounded queues, bodies, recursion, batches, and waits;
- cancellation propagation;
- deterministic lifecycle start/stop;
- original failure causes;
- no hidden transport retries;
- no secret-bearing logs/events;
- no erased Pipeline<T> values or unchecked casts;
- documented retention and delivery semantics.

For Markdown, also check:

- every sidebar target exists;
- every internal wiki link resolves;
- code identifiers are searchable in source or explicitly marked as host-owned;
- defaults match constructors;
- implemented/planned labels match current source;
- no live secrets, private endpoints, or live-site examples.

## Verification reporting

A handoff should say exactly which commands ran and which did not. “Tests not added or run” is a valid result for documentation-only work. Do not report a full build or integration verification when only a link review or compile check was performed.

