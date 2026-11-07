package com.rmjtromp.trawler.flaresolverr

import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.flaresolverr.challenge.ClearanceStore
import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.SystemClock
import com.rmjtromp.trawler.transport.Transport
import com.rmjtromp.trawler.web.transport.WebDestinationPolicy
import io.ktor.client.engine.HttpClientEngine
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Transport that executes requests through a FlareSolverr v3 service.
 *
 * Each [execute] call issues exactly one `request.get` or `request.post`
 * command; there are no hidden retries, and failures surface as exceptions
 * for the engine's classifier and retry policy. Only `GET` without a body and
 * `POST` with an `application/x-www-form-urlencoded` UTF-8 body are
 * supported. Request headers are not forwarded because FlareSolverr v3 has
 * no header parameter; a `Cookie` header is translated into FlareSolverr's
 * `cookies` parameter, and the browser chooses its own User-Agent.
 *
 * The initial target must be HTTP(S) and is evaluated by the caller-supplied
 * [WebDestinationPolicy] before the call; the solution's final URL is
 * evaluated again when it differs. Redirects, subresources, and scripts run
 * inside FlareSolverr's browser and cannot be checked per hop by Trawler.
 *
 * Responses use FlareSolverr's reported status and headers; FlareSolverr v3
 * currently always reports status 200 and no headers because its browser
 * driver cannot observe them. The body is the UTF-8 encoding of the rendered
 * DOM. Non-sensitive metadata is exposed through [FlareSolverrResponseAttributes];
 * solved cookie values are never placed in attributes. When a
 * [ClearanceStore] is supplied, solved cookies and the browser User-Agent are
 * stored for the final origin so an ordinary transport can reuse them.
 *
 * The transport owns one Ktor CIO client for the FlareSolverr endpoint and,
 * when [FlareSolverrTransportConfig.sessions] is set, a fixed pool of
 * FlareSolverr browser sessions created by [start] and destroyed by [stop].
 * Register it as an engine extension. [close] releases the local client only;
 * use [stop] to destroy service-side sessions.
 */
public class FlareSolverrTransport private constructor(
    private val configuration: FlareSolverrTransportConfig,
    private val destination: WebDestinationPolicy,
    private val clearanceStore: ClearanceStore?,
    private val clock: Clock,
    engine: HttpClientEngine?,
) : Transport, ExtensionLifecycle, AutoCloseable {
    override val id: String = configuration.id

    private val client = FlareSolverrClient(configuration.service, engine)
    private val closed = AtomicBoolean(false)
    private val lifecycleMutex = Mutex()
    private var started = false
    private val sessionIds = ArrayList<String>()
    private val sessionPool: Channel<String>? = configuration.sessions?.let { Channel(it.count) }
    private val limits = SolutionLimits(
        maxTargetLength = configuration.maxTargetLength,
        maxResponseHeaders = configuration.maxResponseHeaders,
        maxHeaderNameLength = configuration.maxHeaderNameLength,
        maxHeaderValueLength = configuration.maxHeaderValueLength,
        maxResponseCookies = configuration.maxResponseCookies,
        maxCookieValueLength = configuration.maxCookieValueLength,
        maxUserAgentLength = configuration.maxUserAgentLength,
    )

    /**
     * Creates the configured session pool. Performs no network work when no
     * sessions are configured. On partial failure, already created sessions
     * are destroyed before the original failure is rethrown.
     */
    override suspend fun start(context: ExtensionStartContext) {
        lifecycleMutex.withLock {
            check(!closed.get()) { "FlareSolverr transport is closed" }
            if (started) return
            val sessions = configuration.sessions
            if (sessions != null) {
                try {
                    repeat(sessions.count) {
                        val sessionId = createSession()
                        sessionIds += sessionId
                        checkNotNull(sessionPool).send(sessionId)
                    }
                } catch (error: Throwable) {
                    withContext(NonCancellable) { destroySessions()?.let(error::addSuppressed) }
                    drainSessionPool()
                    throw error
                }
            }
            started = true
        }
    }

    /**
     * Destroys every session this transport created, then closes the client.
     * Idempotent and safe after a failed or never-started start. The first
     * destroy failure is rethrown after all sessions were attempted.
     */
    override suspend fun stop(context: ExtensionStopContext) {
        lifecycleMutex.withLock {
            if (closed.get()) return
            val failure = try {
                withContext(NonCancellable) { destroySessions() }
            } finally {
                started = false
                close()
            }
            failure?.let { throw it }
        }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        sessionPool?.close()
        client.close()
    }

    override suspend fun execute(request: CrawlRequest): CrawlResponse {
        check(!closed.get()) { "FlareSolverr transport is closed" }
        val command = commandFor(request)
        requireHttpTarget(request.target, configuration.maxTargetLength)
        admitDestination(destination, request)
        val pool = sessionPool
        val root = if (pool == null) {
            client.execute(command.build(sessionId = null), configuration.service.solveCallTimeout)
        } else {
            check(started) { "FlareSolverr transport sessions are not started; register the transport as an extension" }
            val sessionId = pool.receive()
            try {
                client.execute(command.build(sessionId), configuration.service.solveCallTimeout)
            } finally {
                pool.trySend(sessionId)
            }
        }
        return adapt(request, root)
    }

    private suspend fun adapt(request: CrawlRequest, root: JsonObject): CrawlResponse {
        val solution = parseSolution(root, limits)
        val (finalTarget, finalOrigin) = admitFinalTarget(
            policy = destination,
            request = request,
            solutionUrl = solution.url,
            maxTargetLength = configuration.maxTargetLength,
        )
        val html = solution.response ?: throw FlareSolverrProtocolException("solution has no response body")
        val body = html.toByteArray(Charsets.UTF_8)
        if (body.size > configuration.maxResponseBodyBytes) {
            throw FlareSolverrResponseLimitException("response body bytes", configuration.maxResponseBodyBytes.toLong())
        }
        val now = clock.now()
        val userAgent = solution.userAgent
        if (clearanceStore != null && userAgent != null) {
            clearanceStore.store(
                clearanceFrom(finalOrigin, solution.cookies, userAgent, now, clearanceStore.configuration.maxCookiesPerOrigin),
            )
        }
        val attributes = buildList {
            add(ContextAttribute(FlareSolverrResponseAttributes.BODY_CHARSET, "UTF-8"))
            add(ContextAttribute(FlareSolverrResponseAttributes.CHALLENGE, challengeOutcome(root)))
            add(ContextAttribute(FlareSolverrResponseAttributes.COOKIE_COUNT, solution.cookies.size.toString()))
            if (solution.cookies.isNotEmpty()) {
                add(
                    ContextAttribute(
                        FlareSolverrResponseAttributes.COOKIE_NAMES,
                        solution.cookies.joinToString(",") { it.name },
                    ),
                )
            }
            if (userAgent != null) add(ContextAttribute(FlareSolverrResponseAttributes.USER_AGENT, userAgent))
        }
        return CrawlResponse(
            request = request,
            statusCode = solution.status,
            headers = Headers.from(solution.headers.map { (name, value) -> Header(name, value) }),
            body = ResponseBody.from(body),
            receivedAt = now,
            finalTarget = finalTarget,
            attributes = ContextAttributes.from(attributes),
        )
    }

    private fun commandFor(request: CrawlRequest): PendingCommand {
        val method = request.method.value.uppercase(Locale.ROOT)
        val body = request.body
        val postData = when (method) {
            "GET" -> {
                if (body != null && body.size > 0) throw UnsupportedFlareSolverrRequestException("get-with-body")
                null
            }

            "POST" -> {
                if (body == null) throw UnsupportedFlareSolverrRequestException("post-without-body")
                if (body.size > configuration.maxRequestBodyBytes) {
                    throw UnsupportedFlareSolverrRequestException("request-body-bound")
                }
                val contentType = request.headers["Content-Type"]
                if (contentType.size != 1 ||
                    !contentType.single().trim().lowercase(Locale.ROOT).startsWith(FORM_URLENCODED)
                ) {
                    throw UnsupportedFlareSolverrRequestException("post-body-not-form-urlencoded")
                }
                decodeUtf8(body.toByteArray())
            }

            else -> throw UnsupportedFlareSolverrRequestException("method-not-supported")
        }
        return PendingCommand(
            command = if (postData == null) "request.get" else "request.post",
            target = request.target,
            postData = postData,
            cookies = requestCookies(request.headers),
        )
    }

    private inner class PendingCommand(
        val command: String,
        val target: String,
        val postData: String?,
        val cookies: List<Pair<String, String>>,
    ) {
        fun build(sessionId: String?): JsonObject = buildJsonObject {
            put("cmd", command)
            put("url", target)
            put("maxTimeout", configuration.service.maxTimeout.toMillis())
            if (postData != null) put("postData", postData)
            if (cookies.isNotEmpty()) {
                putJsonArray("cookies") {
                    cookies.forEach { (name, value) ->
                        addJsonObject {
                            put("name", name)
                            put("value", value)
                        }
                    }
                }
            }
            if (sessionId != null) {
                put("session", sessionId)
                configuration.sessions?.ttl?.let { put("session_ttl_minutes", it.toMinutes()) }
            } else {
                configuration.service.proxy?.let { putProxy(it, includeCredentials = false) }
            }
        }
    }

    private fun requestCookies(headers: Headers): List<Pair<String, String>> {
        val pairs = headers["Cookie"]
            .flatMap { it.split(';') }
            .map { it.trim() }
            .filter { it.isNotEmpty() }
        if (pairs.size > configuration.maxRequestCookies) {
            throw UnsupportedFlareSolverrRequestException("request-cookie-bound")
        }
        return pairs.map { pair ->
            val separator = pair.indexOf('=')
            if (separator <= 0) throw UnsupportedFlareSolverrRequestException("invalid-cookie-header")
            pair.substring(0, separator).trim() to pair.substring(separator + 1).trim()
        }
    }

    private suspend fun createSession(): String {
        val root = client.execute(
            buildJsonObject {
                put("cmd", "sessions.create")
                configuration.service.proxy?.let { putProxy(it, includeCredentials = true) }
            },
            configuration.service.sessionCommandTimeout,
        )
        return root.string("session")?.takeIf { it.isNotBlank() && it.length <= MAX_SESSION_ID_LENGTH }
            ?: throw FlareSolverrProtocolException("sessions.create returned no usable session")
    }

    /** Removes destroyed session IDs so a later start cannot block on a full pool or reuse them. */
    private fun drainSessionPool() {
        val pool = sessionPool ?: return
        while (pool.tryReceive().isSuccess) Unit
    }

    /** Destroys all created sessions; returns the first failure with later ones suppressed. */
    private suspend fun destroySessions(): Throwable? {
        var failure: Throwable? = null
        sessionIds.forEach { sessionId ->
            try {
                client.execute(
                    buildJsonObject {
                        put("cmd", "sessions.destroy")
                        put("session", sessionId)
                    },
                    configuration.service.sessionCommandTimeout,
                )
            } catch (error: Throwable) {
                val first = failure
                if (first == null) failure = error else first.addSuppressed(error)
            }
        }
        sessionIds.clear()
        return failure
    }

    private fun challengeOutcome(root: JsonObject): String = when (root.string("message")) {
        "Challenge solved!" -> "solved"
        "Challenge not detected!" -> "not-detected"
        else -> "unknown"
    }

    private fun decodeUtf8(bytes: ByteArray): String = try {
        Charsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
            .decode(ByteBuffer.wrap(bytes))
            .toString()
    } catch (_: CharacterCodingException) {
        throw UnsupportedFlareSolverrRequestException("post-body-not-utf8")
    }

    public companion object {
        private const val FORM_URLENCODED: String = "application/x-www-form-urlencoded"
        private const val MAX_SESSION_ID_LENGTH: Int = 256

        /**
         * Creates a transport for [configuration]. [destination] is mandatory
         * so target safety is an explicit choice: pass `PublicWebSafetyPolicy`
         * for public-web crawling or a narrower/`AllowAllWebDestinationPolicy`
         * policy deliberately. [clearanceStore], when supplied, receives
         * solved cookies and the browser User-Agent for reuse.
         */
        public fun create(
            configuration: FlareSolverrTransportConfig,
            destination: WebDestinationPolicy,
            clearanceStore: ClearanceStore? = null,
            clock: Clock = SystemClock,
        ): FlareSolverrTransport = FlareSolverrTransport(configuration, destination, clearanceStore, clock, null)

        /** Module-internal seam: uses [engine] instead of CIO for the FlareSolverr client. */
        internal fun createWithEngine(
            configuration: FlareSolverrTransportConfig,
            destination: WebDestinationPolicy,
            engine: HttpClientEngine,
            clearanceStore: ClearanceStore? = null,
            clock: Clock = SystemClock,
        ): FlareSolverrTransport = FlareSolverrTransport(configuration, destination, clearanceStore, clock, engine)
    }
}

/**
 * Names and typed accessors for non-sensitive response attributes added by
 * [FlareSolverrTransport]. Cookie values, session IDs, and proxy settings are
 * never exposed as attributes.
 */
public object FlareSolverrResponseAttributes {
    /** Browser User-Agent FlareSolverr used; required to reuse its clearance cookies. */
    public const val USER_AGENT: String = "trawler.flaresolverr.user-agent"

    /** Comma-separated names (not values) of cookies the browser held after solving. */
    public const val COOKIE_NAMES: String = "trawler.flaresolverr.cookie-names"

    /** Number of cookies the browser held after solving. */
    public const val COOKIE_COUNT: String = "trawler.flaresolverr.cookie-count"

    /** `solved`, `not-detected`, or `unknown`, derived from FlareSolverr's message. */
    public const val CHALLENGE: String = "trawler.flaresolverr.challenge"

    /** Always `UTF-8`: the body is the UTF-8 encoding of the rendered DOM, whatever the page declared. */
    public const val BODY_CHARSET: String = "trawler.flaresolverr.body-charset"

    /** Returns the FlareSolverr browser User-Agent recorded on [response], if any. */
    public fun userAgent(response: CrawlResponse): String? = response.attributes[USER_AGENT]

    /** Returns the solved cookie names recorded on [response]. */
    public fun cookieNames(response: CrawlResponse): List<String> =
        response.attributes[COOKIE_NAMES]?.split(',')?.filter { it.isNotEmpty() } ?: emptyList()

    /** Returns the challenge outcome recorded on [response], if any. */
    public fun challengeOutcome(response: CrawlResponse): String? = response.attributes[CHALLENGE]
}
