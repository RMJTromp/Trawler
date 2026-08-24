# Security and Network Safety

Security is a replaceable but explicit policy choice. Core's ordinary policies are not a public-network safety profile.

## Public-network preset

For public HTTP(S), choose KtorTransport.publicWeb or assemble WebPresets.publicWebPolicies with a reviewed NetworkAddressResolver. The preset:

- allows HTTP and HTTPS by default;
- rejects URI userinfo;
- limits target, headers, bodies, redirects, and origins;
- resolves DNS before admission;
- blocks loopback, private, link-local, multicast, metadata, carrier-grade NAT, and benchmark ranges;
- re-runs destination checks on every redirect hop;
- evaluates robots before each page hop;
- applies bounded origin concurrency and pacing;
- redacts sensitive headers for diagnostics;
- strips sensitive credentials on unsafe cross-origin redirects.

PublicWebSafetyPolicy revalidates DNS but does not claim DNS-to-socket pinning. If a deployment needs stronger anti-rebinding guarantees, it must supply a transport/policy pair that owns that capability.

## Internal targets

AllowAllWebDestinationPolicy is intentionally explicit. It is appropriate only when the host has already defined the internal network trust boundary, authorization, DNS behavior, and credential policy. Do not enable it merely to make a local test pass in production configuration.

## Redirect security

Redirects are processed by the engine-controlled KtorTransport rather than Ktor's hidden follow-redirect path. Every hop re-runs:

- URI validation;
- destination address checks;
- robots;
- origin limits;
- pacing;
- redirect count and loop checks;
- header forwarding rules.

Default cross-origin redirect handling removes authorization, cookies, common API keys, host, content length, transfer encoding, and other restricted headers. A custom RedirectHeaderPolicy must be reviewed like credential-forwarding code.

## Robots and authorization

Robots enforcement and authorization are different:

- robots expresses a site's crawl instructions;
- AuthorizationPolicy decides whether the configured crawler may perform this request.

Both are evaluated. A request allowed by robots can still be rejected by authorization or scope, and an authorized request can still be denied by robots.

## Secrets and sensitive data

Do not put these in IDs, logs, events, checkpoint payloads, fixtures, examples, or default archives:

- passwords;
- authorization headers;
- cookies;
- proxy credentials;
- API keys;
- private endpoint addresses;
- raw response bodies unless retention is explicitly approved;
- configured sensitive attributes.

PostgresStateConfiguration excludes its password from toString and redacts JDBC query parameters in its diagnostic string. That is a boundary safeguard, not a substitute for secret management.

DefaultHeaderRedactionPolicy is for diagnostics. It does not remove secrets from the live request when the destination requires them.

## Cookies

InMemoryCookieSession is process-local, bounded, and potentially sensitive. Its default lifetime is 30 days, with a maximum of 512 cookies, 8 KiB values, and a 16 KiB cookie header. Choose NoCookieSession when sessions are not required.

Do not share one cookie session across unrelated pipelines unless their identity and authorization boundaries are intentionally shared.

## Parser safety

RSS, Atom, and sitemap discovery uses bounded XML input and disables DTD/external entities. XML document, item, URL, depth, and document-count limits are configured by XmlDiscoveryBounds.

Any host-owned parser must preserve the same rules: bounded reads, no external entity resolution, no unbounded recursion, and cancellation-aware execution.

## Data retention and replay authorization

Response replay can expose raw historical responses and may re-run outputs. Protect replay behind an application authorization boundary. Require REPLAY only for pipelines and stores that need it. Apply pipeline ownership before the replay limit and keep response retention separate from output retention.

When deleting response history, account for protected references and application lineage. A retention job must not assume that deleting a raw response deletes a downstream produced record.

## Network safety checklist

- Use the public-web preset for public targets.
- Review DNS resolver limits and rebinding requirements.
- Keep redirects under engine control.
- Strip credentials on cross-origin redirects by default.
- Set an explicit robots-unavailable behavior.
- Bound all request, response, XML, checkpoint, and output bytes.
- Keep secrets out of events and diagnostics.
- Use a dedicated cookie-session boundary.
- Review replay authorization and retention.
- Treat external side effects as at-least-once unless the destination proves a narrower guarantee.

