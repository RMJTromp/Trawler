# Trawler 🌊

> **A high-performance, modular, and fault-tolerant web crawling and data extraction library in Kotlin.**

Trawler is designed from the ground up as a general-purpose, asynchronous scraping framework powered by Kotlin Coroutines, PostgreSQL persistence, and adaptive anti-bot bypass. It decouples discovery, transport, extraction, and persistence into clean, pluggable extension points.

---

## 📑 Table of Contents

- [Key Features](#-key-features)
- [System Architecture](#-system-architecture)
- [Repository & Plan Structure](#-repository--plan-structure)
- [Architectural Guidelines](#-architectural-guidelines)
- [Quick Start](#-quick-start)
- [Engine Configuration DSL](#-engine-configuration-dsl)
- [Writing a Source Plugin](#-writing-a-source-plugin)
- [PostgreSQL Storage & Schemas](#-postgresql-storage--schemas)
- [Adaptive Anti-Bot & FlareSolverr](#-adaptive-anti-bot--flaresolverr)
- [Building & Testing](#-building--testing)

---

## ✨ Key Features

- **⚡ Multi-Source Parallelism & Politeness:** Crawl multiple distinct sources concurrently with independent rate limiters, token buckets, and adaptive jitter per domain.
- **🛡️ Adaptive Challenge Resolution:** Automatically detect Cloudflare Turnstile/v2, DataDome, and Akamai challenges. Solves once via FlareSolverr using a domain-scoped single-flight lock and propagates session cookies across all workers.
- **💾 PostgreSQL Atomic State & Crash Recovery:** Resilient checkpointing state machine (`QUEUED` ➔ `IN_PROGRESS` ➔ `COMPLETED` / `FAILED` / `RETRY_SCHEDULED`). Automatic stale-lock reclamation prevents orphaned jobs.
- **📦 Immutable Raw HTTP Cache & Offline Re-extraction:** All raw byte responses and HTTP headers are archived in PostgreSQL, enabling 100% offline re-parsing when extraction schemas evolve without making live network requests.
- **🧩 Pluggable Pipeline Architecture:** Deep interceptor chains for discovery feeds/sitemaps, TLS fingerprinting, content transforms, SimHash deduplication, language filtering, and multi-sink dispatching (Postgres, WARC, Vector DBs).

---

## 🏛️ System Architecture

Trawler is organized into five decoupled subsystems coordinated by the central `ScraperEngine`:

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                          ScraperEngine                                           │
│                       (SupervisorScope / Dispatchers.IO / Event Bus)                             │
├──────────────────────────────┬───────────────────────────────────┬───────────────────────────────┤
│    1. Discovery Subsystem    │   2. Transport & Interceptors     │   3. Extraction & Transform   │
│  ┌────────────────────────┐  │  ┌─────────────────────────────┐  │  ┌─────────────────────────┐  │
│  │ SourcePlugin.discover()│  │  │ Proxy Router / TLS Engine   │  │  │ SourcePlugin.extract()  │  │
│  │ Sitemap & RSS Crawlers │  │  │ RateLimiter & Politeness    │  │  │ Readability Extractor   │  │
│  │ Wayback Fallback       │  │  │ Cache Interceptor (Postgres)│  │  │ SimHash / Language Gate │  │
│  └───────────┬────────────┘  │  │ Challenge Resolvers (Solver)│  │  │ Media Downloader        │  │
│              │               │  └──────────────┬──────────────┘  │  └────────────┬────────────┘  │
└──────────────┼───────────────┴─────────────────┼─────────────────┴───────────────┼───────────────┘
               ▼                                 ▼                                 ▼
┌──────────────────────────────┐ ┌───────────────────────────────┐ ┌───────────────────────────────┐
│     Checkpoint Store         │ │        Raw Cache Store        │ │         Item Sinks            │
│ (State, Locks, Crash Recovery│ │ (Immutable HTTP Transactions) │ │ (PostgreSQL, WARC, Vectors)   │
└──────────────────────────────┘ └───────────────────────────────┘ └───────────────────────────────┘
```

### Execution Lifecycle

```
[Lease Checkpoint Batch] ──► [Check Raw Cache Table]
                                      │
               ┌──────────────────────┴──────────────────────┐
          (Cache Hit)                                   (Cache Miss)
               │                                             │
               │                                    [Acquire Rate Limit]
               │                                             │
               │                                    [Execute Transport Chain]
               │                                             │
               │                                    [Detect Challenge?]
               │                                     ├── Yes: Solve & Escalate
               │                                     └── No: Continue
               │                                             │
               │                                    [Save to Raw Cache]
               │                                             │
               └──────────────────────┬──────────────────────┘
                                      │
                          [Execute Extractor Chain]
                                      │
                         [Apply Transform Plugins]
                                      │
                        [Batch Dispatch to Sinks]
                                      │
                     [Mark Checkpoint: COMPLETED]
```

---

## 📂 Repository & Plan Structure

To maintain architectural clarity and preserve end-goal alignment, the repository includes foundational plans and guidelines:

- [`plans/ORIGINAL-REQUEST.md`](plans/ORIGINAL-REQUEST.md): The original client specification, requirements, philosophy, and reference use case.
- [`plans/PLAN-1.md`](plans/PLAN-1.md): Comprehensive system design, interfaces, data contracts, and PostgreSQL schemas.

```
Trawler
├── README.md                      # Project documentation and guidelines
├── plans/
│   ├── ORIGINAL-REQUEST.md        # Original problem statement & reference use case
│   └── PLAN-1.md                  # Comprehensive architectural blueprint
├── pom.xml                        # Maven configuration
└── src/
    ├── main/kotlin/com/rmjtromp/trawler/
    │   ├── core/                  # Engine, storage, transport, politeness, transform
    │   └── plugins/               # Reusable & reference source plugins
    └── test/kotlin/               # Unit & integration tests
```

---

## 🏛️ Architectural Guidelines

Core design invariants and guidelines for development:

1. **Library-First Scope:** Never hardcode domain/website parsing logic in the core engine. All site-specific scrapers must be implemented as `SourcePlugin<T>` modules.
2. **Coroutines & Structured Concurrency:** No blocking threads (`Thread.sleep()` is strictly forbidden). Always use `SupervisorScope`, `Flow`, and non-blocking delays/channels.
3. **Mandatory Persistence & Caching:** All states must be transactionally checkpointed in PostgreSQL, and raw responses must be saved in `raw_http_cache` for zero-network offline reprocessing.
4. **Politeness Isolation:** Each source must possess independent rate limiters, token buckets, and automatic `Retry-After` compliance.
5. **FlareSolverr Coordination:** Anti-bot challenge resolution must use single-flight domain locks and session cookie propagation.

---

## 🚀 Quick Start

### Engine Configuration DSL

```kotlin
import com.rmjtromp.trawler.core.engine.ScraperEngine
import com.rmjtromp.trawler.core.model.CachePolicy
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

val engine = ScraperEngine.create {
    // Concurrency & Coroutines
    concurrency {
        globalMaxConcurrency = 64
        staleLockTimeout = 5.minutes
    }

    // Storage & PostgreSQL Persistence
    storage {
        usePostgresStorage {
            dataSource = hikariDataSource
            checkpointBatchSize = 100
        }
        installSink(PostgresEntitySink(hikariDataSource))
    }

    // Transport & Anti-Bot Escalation
    transport {
        defaultTimeout = 30.seconds
        adaptiveEscalation {
            detector = CompositeChallengeDetector(CloudflareDetector(), DataDomeDetector())
            resolver = FlareSolverrResolver(endpoint = "http://127.0.0.1:8191")
            cooldownPeriod = 15.minutes
        }
    }

    // Source Plugin Registration
    registerSource(PapiamentoNewsPlugin()) {
        politeness {
            maxConcurrency = 4
            delay = 1500.milliseconds
            jitter = 400.milliseconds
            respectRetryAfter = true
        }
        cache {
            defaultTtl = 7.days
            policy = CachePolicy.CACHE_FIRST
        }
        discovery {
            enableSitemapDiscovery = true
            enableRssPolling = true
            pollInterval = 10.minutes
        }
    }
}

// Start crawling asynchronously
engine.start()
```

---

## 🔌 Writing a Source Plugin

A source plugin implements `SourcePlugin<T>` to provide URL discovery and content extraction:

```kotlin
class ExampleNewsPlugin : SourcePlugin<ArticleEntity> {
    override val sourceId: String = "example_news"
    override val domain: String = "example.com"
    override val defaultCacheDuration: Duration = 7.days

    override fun discover(context: CrawlContext): Flow<CrawlRequest> = flow {
        // Emit initial seed URLs or fetch sitemaps/RSS feeds
        emit(
            CrawlRequest(
                id = UUID.randomUUID().toString(),
                url = "https://example.com/feed.xml",
                sourceId = sourceId,
                entityType = "rss_feed"
            )
        )
    }

    override fun canExtract(response: CrawlResponse): Boolean {
        return response.url.contains("/articles/")
    }

    override suspend fun extract(
        response: CrawlResponse,
        context: CrawlContext
    ): ExtractionResult<ArticleEntity> {
        val html = response.utf8Body()
        val doc = Jsoup.parse(html)

        val title = doc.selectFirst("h1.entry-title")?.text() ?: return ExtractionResult.Ignored("Missing title")
        val content = doc.selectFirst("div.entry-content")?.text() ?: return ExtractionResult.Ignored("Missing body")

        val article = ArticleEntity(
            url = response.url,
            title = title,
            content = content,
            publishedAt = Instant.now()
        )

        return ExtractionResult.Success(items = listOf(article))
    }
}
```

---

## 🗄️ PostgreSQL Storage & Schemas

Trawler requires PostgreSQL for state tracking and caching. Execute the following DDL to initialize the required tables:

```sql
-- 1. Checkpoint & State Management Table
CREATE TABLE IF NOT EXISTS crawl_checkpoints (
    request_id VARCHAR(64) PRIMARY KEY,
    source_id VARCHAR(64) NOT NULL,
    url TEXT NOT NULL,
    url_hash CHAR(64) NOT NULL, -- SHA-256 for fast indexed lookups
    entity_type VARCHAR(64) NOT NULL,
    priority INT NOT NULL DEFAULT 1,
    status VARCHAR(24) NOT NULL DEFAULT 'QUEUED',
    locked_by_worker VARCHAR(64),
    locked_at TIMESTAMP WITH TIME ZONE,
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    last_error TEXT,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_lease 
ON crawl_checkpoints (source_id, status, priority DESC, next_retry_at)
WHERE status IN ('QUEUED', 'RETRY_SCHEDULED');

CREATE UNIQUE INDEX IF NOT EXISTS idx_checkpoints_dedup 
ON crawl_checkpoints (source_id, url_hash);

-- 2. Raw HTTP Response Cache (Immutable History for Zero-Network Reprocessing)
CREATE TABLE IF NOT EXISTS raw_http_cache (
    id BIGSERIAL PRIMARY KEY,
    source_id VARCHAR(64) NOT NULL,
    url_hash CHAR(64) NOT NULL,
    url TEXT NOT NULL,
    status_code INT NOT NULL,
    headers JSONB NOT NULL,
    response_body BYTEA NOT NULL,
    is_compressed BOOLEAN NOT NULL DEFAULT FALSE,
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE,
    transport_metadata JSONB DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_cache_lookup 
ON raw_http_cache (source_id, url_hash);

CREATE INDEX IF NOT EXISTS idx_raw_cache_expiry 
ON raw_http_cache (expires_at) 
WHERE expires_at IS NOT NULL;

-- 3. Extracted Entity Record Sink Table
CREATE TABLE IF NOT EXISTS extracted_entities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id VARCHAR(64) NOT NULL,
    request_id VARCHAR(64) REFERENCES crawl_checkpoints(request_id) ON DELETE SET NULL,
    url TEXT NOT NULL,
    entity_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    content_hash CHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entities_lookup 
ON extracted_entities (source_id, entity_type);

CREATE INDEX IF NOT EXISTS idx_entities_content_hash 
ON extracted_entities (source_id, content_hash);
```

---

## 🛡️ Adaptive Anti-Bot & FlareSolverr

When scraping sites protected by Cloudflare Turnstile or other WAFs:

```
Standard HTTP (Fast) ───[Challenge Detected (403/503)]───► Single-Flight Mutex ───► FlareSolverr / Headless
        ▲                                                                                   │
        │                                                                                   │
        └───────────── Session Re-injection (Cookies, User-Agent, Fingerprint) ◄────────────┘
```

1. Outbound requests run via lightweight HTTP client.
2. If a challenge is encountered, the domain's **Single-Flight Mutex** is locked.
3. One worker triggers FlareSolverr to solve the challenge.
4. The cleared cookies (`cf_clearance`), headers, and session tokens are saved in `SessionRegistry`.
5. Concurrent workers immediately resume without duplicate solves.

---

## 🛠️ Building & Testing

### Build with Maven
```bash
mvn clean compile
```

### Run Tests
```bash
mvn test
```

---

## 📜 License & Acknowledgments

Trawler is developed as a high-performance open-source Kotlin crawling library. Contributions and plugins are welcome!
