package com.rmjtromp.trawler.web.policy

import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import java.util.Locale

/** Redacts sensitive header values when a host creates diagnostics. */
public interface HeaderRedactionPolicy {
    /** Stable identifier used by transport configuration diagnostics. */
    public val id: String

    /** Returns an immutable diagnostic copy without changing transport headers. */
    public fun redact(headers: Headers): Headers
}

/** Conservative header/cookie redaction policy for logs, events, and diagnostics. */
public class DefaultHeaderRedactionPolicy(
    sensitiveHeaderNames: Set<String> = setOf(
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "x-api-key",
        "x-auth-token",
        "x-amz-security-token",
        "x-amz-credential",
        "x-goog-user-project",
    ),
    private val redactedValue: String = "[REDACTED]",
) : HeaderRedactionPolicy {
    override val id: String = "sensitive-header-redaction"

    private val sensitiveNames = sensitiveHeaderNames.map { it.lowercase(Locale.ROOT) }.toSet()

    init {
        require(sensitiveNames.all { it.isNotBlank() }) { "Sensitive header names must not be blank" }
        require(redactedValue.isNotBlank()) { "Redacted header value must not be blank" }
    }

    override fun redact(headers: Headers): Headers = Headers.from(
        headers.entries.map { header ->
            if (header.name.lowercase(Locale.ROOT) in sensitiveNames) {
                header.copy(value = redactedValue)
            } else {
                header
            }
        },
    )
}

/** Selects which request headers may cross a redirect transition. */
public interface RedirectHeaderPolicy {
    /** Stable identifier used by transport diagnostics. */
    public val id: String

    /**
     * Returns headers for the next hop. Cross-origin calls must strip
     * credential-bearing headers by default; body changes also remove entity
     * framing headers that no longer describe the request.
     */
    public fun headersForRedirect(
        headers: Headers,
        crossOrigin: Boolean,
        bodyRemoved: Boolean,
    ): Headers
}

/**
 * Safe default redirect-header policy.
 *
 * The set is intentionally conservative and can be replaced for an
 * authenticated internal service. Cookie state for the destination is added
 * later by the selected [com.rmjtromp.trawler.web.session.CookieSession].
 */
public class DefaultRedirectHeaderPolicy(
    sensitiveHeaderNames: Set<String> = setOf(
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "x-api-key",
        "x-auth-token",
        "x-amz-security-token",
        "x-amz-credential",
        "x-goog-user-project",
    ),
) : RedirectHeaderPolicy {
    override val id: String = "safe-redirect-headers"

    private val sensitiveNames = sensitiveHeaderNames.map { it.lowercase(Locale.ROOT) }.toSet()

    init {
        require(sensitiveNames.all { it.isNotBlank() }) { "Sensitive header names must not be blank" }
    }

    override fun headersForRedirect(
        headers: Headers,
        crossOrigin: Boolean,
        bodyRemoved: Boolean,
    ): Headers = Headers.from(
        headers.entries.filterNot { header ->
            (crossOrigin && header.name.lowercase(Locale.ROOT) in sensitiveNames) ||
                header.name.equals("Host", ignoreCase = true) ||
                header.name.equals("Content-Length", ignoreCase = true) ||
                header.name.equals("Transfer-Encoding", ignoreCase = true) ||
                (bodyRemoved && header.name.equals("Content-Type", ignoreCase = true))
        },
    )
}

/** Adds a single immutable request header without creating a Ktor dependency in core. */
public class AddRequestHeaderMiddleware(
    override val id: String,
    private val header: Header,
) : com.rmjtromp.trawler.transport.RequestMiddleware {
    init {
        require(id.isNotBlank()) { "Middleware ID must not be blank" }
    }

    override suspend fun intercept(
        request: com.rmjtromp.trawler.model.CrawlRequest,
        context: com.rmjtromp.trawler.model.RequestContext,
        next: com.rmjtromp.trawler.transport.RequestExecutor,
    ): com.rmjtromp.trawler.model.CrawlResponse =
        next.execute(request.copy(headers = request.headers.plus(header)), context)
}

/** Adds bounded non-sensitive response metadata before later response middleware. */
public class AddResponseAttributeMiddleware(
    override val id: String,
    private val attribute: com.rmjtromp.trawler.model.ContextAttribute,
) : com.rmjtromp.trawler.transport.ResponseMiddleware {
    init {
        require(id.isNotBlank()) { "Middleware ID must not be blank" }
    }

    override suspend fun intercept(
        response: com.rmjtromp.trawler.model.CrawlResponse,
        context: com.rmjtromp.trawler.model.ResponseContext,
        next: com.rmjtromp.trawler.transport.ResponseHandler,
    ): com.rmjtromp.trawler.model.CrawlResponse =
        next.handle(response.copy(attributes = response.attributes.plus(attribute)), context)
}
