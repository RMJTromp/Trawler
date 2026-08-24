# Web Adapter

The web adapter provides an HTTP transport and web-specific policies. It is deliberately separate from core because HTTP client behavior, DNS resolution, robots, redirects, cookies, and origin controls are not universal crawler requirements.

## Create a public-web transport

KtorTransport.publicWeb creates one bounded CIO client, one bounded JDK resolver, the public-web policy bundle, and an ExtensionLifecycle that closes the owned resources:

~~~kotlin
val transport = KtorTransport.publicWeb(
    configuration = KtorTransportConfig(
        userAgent = "TrawlerBot/0.1",
        maxRedirects = 8,
    ),
    preset = PublicWebPresetConfig(
        permitsPerOrigin = 2,
        minimumOriginDelay = Duration.ofMillis(250),
    ),
)
~~~

Register the transport as a pipeline transport and as an engine or pipeline lifecycle extension according to its ownership. The transport does not own the pipeline definition.

For an existing Ktor HttpClient, use KtorTransport.fromClient. It derives a controlled client with redirects disabled and non-2xx responses inspectable. The caller's client is closed only when closeClient is true; the derived client is always closed by the transport.

## Web transport limits

KtorTransportConfig defaults:

| Option | Default |
| --- | ---: |
| connectTimeout | 10 seconds |
| requestTimeout | 30 seconds |
| socketTimeout | 30 seconds |
| totalRequestTimeout | 2 minutes |
| maxRequestBodyBytes | 8 MiB |
| maxCompressedBodyBytes | 8 MiB |
| maxDecompressedBodyBytes | 8 MiB |
| maxRobotsBodyBytes | 128 KiB |
| maxResponseHeaders | 256 |
| maxRequestHeaders | 256 |
| maxTargetLength | 16 KiB |
| maxMethodLength | 256 |
| maxHeaderNameLength | 1 KiB |
| maxHeaderValueLength | 16 KiB |
| maxRedirects | 8 |
| maxLocationLength | 16 KiB |
| maxClientConnections | 64 |
| maxConnectionsPerOrigin | 8 |
| userAgent | TrawlerBot/0.1 |

The transport fails when a request, response, compressed body, decompressed body, header collection, redirect location, or target exceeds its configured bound.

## Public-web policies

WebPresets.publicWebPolicies assembles:

- PublicWebSafetyPolicy;
- CachingRobotsPolicy;
- BoundedOriginConcurrencyPolicy;
- FixedOriginPacingPolicy;
- InMemoryCookieSession;
- DefaultHttpFailureClassifier;
- DefaultRetryAfterPolicy;
- DefaultRedirectHeaderPolicy;
- DefaultHeaderRedactionPolicy.

PublicWebPresetConfig defaults to two permits per origin, at most 256 origins, a 250 ms minimum origin delay, a 5 minute maximum pacing wait, and bounded process-local cookies.

The public destination policy allows HTTP and HTTPS by default and blocks loopback, private, link-local, multicast, metadata, carrier-grade NAT, and benchmark address ranges. It rejects URI userinfo. DNS is revalidated for each redirect hop, but the policy does not claim DNS-to-socket pinning.

AllowAllWebDestinationPolicy is an explicit permissive option for trusted internal targets or a host-owned safety policy. It must not be confused with the public-web preset.

## Robots

CachingRobotsPolicy evaluates robots.txt before a page request and on every redirect destination. Defaults:

- cache TTL: 1 hour;
- max entries: 256;
- max body: 128 KiB;
- unavailable behavior: DENY;
- maximum crawl delay: 5 minutes;
- excessive delay behavior: FAIL.

NoRobotsPolicy is an explicit opt-out for a trusted caller. If robots are unavailable under DENY or FAIL, the page request is not silently allowed.

## Redirects

Automatic Ktor redirects are disabled so the transport can enforce policy at every hop. Each hop re-runs:

- destination safety and DNS resolution;
- robots;
- origin concurrency;
- origin pacing;
- credential-forwarding rules;
- response limits and redirect limits.

Default cross-origin redirect handling strips authorization, cookies, common API keys, and other sensitive headers. Host, content length, transfer encoding, and other restricted transport headers are controlled by the transport.

Method behavior follows standard web semantics implemented by the adapter: 301 and 302 convert POST to GET, 303 converts non-HEAD requests to GET, and 307 and 308 preserve the method and body. A redirect loop or excessive hop/location count is a failure.

## HTTP status and retry

Non-2xx responses are returned as inspectable CrawlResponse values. Ktor does not hide a retry loop inside the client. HTTP status classification is handled by HttpFailureClassifier; transport exceptions are handled by DefaultWebFailureClassifier.

DefaultHttpRetryPolicy is an explicit core RetryPolicy with default maximum attempts of 3, initial delay of 1 second, and maximum delay of 30 seconds. Install it explicitly on a pipeline when HTTP retry is wanted:

~~~kotlin
val pipeline = PipelineBuilder<String>("web")
    .transport(transport)
    .classifyFailures(DefaultWebFailureClassifier)
    .retry(DefaultHttpRetryPolicy())
    .discover(SeedDiscovery(requests = listOf(CrawlRequest("https://example.invalid/"))))
    .extract(StringExtractor())
    .build(PipelineDefaults(policies = BuiltInPolicies.ordinary()))
~~~

Retry-After is observed by pacing; it does not create an invisible transport retry.

## XML discoveries

RssDiscovery, AtomDiscovery, and SitemapDiscovery parse bounded XML through the engine-controlled transport. XmlDiscoveryBounds defaults include a 2 MiB document limit, 10,000 items, 16 KiB locations, depth 4, and at most 256 documents. DTD and external entities are disabled.

XML discovery can be combined with XmlCursorCheckpointCodec when the source's cursor semantics are compatible. The parser does not own response storage or crawl state.

## Cookies and sessions

InMemoryCookieSession is process-local. Its defaults bound the session to 512 cookies, 8 KiB per value, 16 KiB per header, and a 30-day lifetime. Cookies can carry identity and sensitive state; use a dedicated session only when the host accepts that retention and isolation model.

Cookies are stripped on unsafe cross-origin redirects by the default RedirectHeaderPolicy. Do not put credentials in CrawlRequest targets or checkpoint payloads.

## Web adapter checklist

- Choose KtorTransport.publicWeb for public targets.
- Supply AllowAllWebDestinationPolicy only after reviewing internal-network risk.
- Keep redirect processing enabled in the transport policy path and disabled in Ktor's hidden redirect plugin.
- Decide what robots-unavailable means for the application.
- Install DefaultHttpRetryPolicy explicitly if retries are desired.
- Set request and response bounds tighter when the source permits.
- Register the transport lifecycle and close the engine.
- Keep production tests on controlled MockEngine or a host-owned fake; do not depend on live sites.

