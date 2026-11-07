package com.rmjtromp.trawler.web.transport

import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers as CoreHeaders
import com.rmjtromp.trawler.model.RequestMethod
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.SystemClock
import com.rmjtromp.trawler.web.policy.InvalidWebRedirectException
import com.rmjtromp.trawler.web.policy.RedirectLimitExceededException
import com.rmjtromp.trawler.web.policy.RobotsDecision
import com.rmjtromp.trawler.web.policy.RobotsFetchResult
import com.rmjtromp.trawler.web.policy.RobotsPolicy
import com.rmjtromp.trawler.web.policy.RobotsUnavailableException
import com.rmjtromp.trawler.web.policy.UnsupportedWebContentEncodingException
import com.rmjtromp.trawler.web.policy.WebPolicyRejectedException
import com.rmjtromp.trawler.web.policy.WebResponseHeaderValueTooLargeException
import com.rmjtromp.trawler.web.policy.WebResponseHeaderNameTooLargeException
import com.rmjtromp.trawler.web.policy.WebResponseHeadersTooLargeException
import com.rmjtromp.trawler.web.policy.WebResponseTooLargeException
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpRequestTimeoutException
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.headers
import io.ktor.client.request.prepareRequest
import io.ktor.client.request.setBody
import io.ktor.client.request.url
import io.ktor.client.plugins.timeout
import io.ktor.client.statement.bodyAsChannel
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.contentType
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withTimeoutOrNull
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.readAvailable
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.net.URI
import java.util.Collections
import java.util.LinkedHashSet
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.GZIPInputStream
import java.util.zip.InflaterInputStream

/**
 * Ktor-backed HTTP transport that adapts all responses into core models.
 *
 * The transport owns the controlled client derived from the supplied client.
 * Discovery, middleware,
 * and policies receive no client handle and therefore cannot create a hidden
 * network path. Retry is deliberately absent: non-2xx responses are returned
 * for classification and the core retry policy remains engine-visible.
 */
public class KtorTransport private constructor(
    private val client: HttpClient,
    private val configuration: KtorTransportConfig,
    private val policies: WebTransportPolicies,
    private val clock: Clock,
    ownedResources: List<AutoCloseable>,
) : com.rmjtromp.trawler.transport.Transport, ExtensionLifecycle, AutoCloseable {
    override val id: String = configuration.id

    private val closed = AtomicBoolean(false)
    private val resources: List<AutoCloseable> = Collections.unmodifiableList(ownedResources.toList())

    override suspend fun start(context: ExtensionStartContext) {
        check(!closed.get()) { "Ktor transport is closed" }
    }

    override suspend fun stop(context: ExtensionStopContext) {
        close()
    }

    override suspend fun execute(request: CrawlRequest): CrawlResponse {
        check(!closed.get()) { "Ktor transport is closed" }
        require((request.body?.size ?: 0) <= configuration.maxRequestBodyBytes) {
            "Request body exceeds configured limit of ${configuration.maxRequestBodyBytes} bytes"
        }
        val totalTimeoutMillis = configuration.totalRequestTimeout.toMillis()
        // A TimeoutCancellationException would look like caller cancellation to the engine,
        // so an expired chain timeout is reported as an ordinary, classifiable timeout failure.
        return withTimeoutOrNull(totalTimeoutMillis) {
            executeRedirectChain(request)
        } ?: throw HttpRequestTimeoutException(request.target, totalTimeoutMillis)
    }

    /**
     * Returns a diagnostic-only immutable header copy using the selected
     * redaction policy. It performs no network work and never changes the
     * request or response headers used by transport execution.
     */
    public fun redactHeadersForDiagnostics(headers: CoreHeaders): CoreHeaders =
        policies.headerRedaction.redact(headers)

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        var failure: Throwable? = null
        resources.asReversed().forEach { resource ->
            try {
                resource.close()
            } catch (error: Throwable) {
                if (failure == null) failure = error else failure.addSuppressed(error)
            }
        }
        failure?.let { throw it }
    }

    private suspend fun executeRedirectChain(initialRequest: CrawlRequest): CrawlResponse {
        var currentRequest = initialRequest
        var history = RedirectHistory.empty(configuration.maxRedirects)
        val seenTargets = LinkedHashSet<String>()
        seenTargets += currentRequest.target

        while (true) {
            when (val destination = policies.destination.evaluate(currentRequest, history)) {
                DestinationDecision.Allowed -> Unit
                is DestinationDecision.Rejected -> throw WebPolicyRejectedException(
                    policies.destination.id,
                    destination.reason,
                )
            }
            val origin = try {
                WebOrigin.fromTarget(currentRequest.target)
            } catch (error: IllegalArgumentException) {
                throw WebPolicyRejectedException(policies.destination.id, "invalid-web-origin")
                    .also { it.initCause(error) }
            }
            when (val robots = policies.robots.evaluate(
                target = currentRequest.target,
                fetcher = robotsFetcher,
            )) {
                is RobotsDecision.Allowed -> robots.crawlDelay?.let {
                    policies.pacing.observeRobotsDelay(origin, it)
                }

                is RobotsDecision.Disallowed -> throw WebPolicyRejectedException(
                    policies.robots.id,
                    robots.reason,
                )

                is RobotsDecision.Unavailable -> throw RobotsUnavailableException(robots.reason)
            }

            val requestToSend = prepareRequest(currentRequest)
            val permit = policies.concurrency.acquire(origin, WebRequestKind.PAGE)
            var pacing: OriginPacingReservation? = null
            val response = try {
                pacing = policies.pacing.acquire(origin, WebRequestKind.PAGE)
                executeSingleHop(requestToSend)
            } finally {
                pacing?.release()
                permit.release()
            }
            policies.pacing.observe(origin, response)
            policies.cookies.observe(
                target = requestToSend.target,
                setCookieHeaders = response.headers["Set-Cookie"],
            )
            val decorated = decorateResponse(response, history)
            val location = response.headers[HttpHeaders.Location].firstOrNull()?.trim()
            if (response.statusCode !in REDIRECT_STATUSES || location.isNullOrEmpty()) {
                return decorated
            }
            if (history.size >= configuration.maxRedirects) {
                throw RedirectLimitExceededException(configuration.maxRedirects)
            }
            if (location.length > configuration.maxLocationLength) {
                throw InvalidWebRedirectException("Location header exceeds configured bound")
            }
            val redirectTarget = resolveRedirect(requestToSend.target, location)
            if (!seenTargets.add(redirectTarget)) {
                throw InvalidWebRedirectException("Redirect loop detected")
            }
            val nextOrigin = try {
                WebOrigin.fromTarget(redirectTarget)
            } catch (error: IllegalArgumentException) {
                throw InvalidWebRedirectException("Redirect target has no valid web origin")
                    .also { it.initCause(error) }
            }
            val nextMethod = redirectMethod(response.statusCode, requestToSend.method)
            val bodyRemoved = nextMethod != requestToSend.method ||
                (response.statusCode == 303 && requestToSend.body != null)
            val hop = RedirectHop(
                fromTarget = requestToSend.target,
                toTarget = redirectTarget,
                statusCode = requireNotNull(response.statusCode),
                crossOrigin = origin != nextOrigin,
            )
            history = history.plus(hop)
            currentRequest = requestToSend.copy(
                target = redirectTarget,
                method = nextMethod,
                headers = policies.redirectHeaders.headersForRedirect(
                    headers = requestToSend.headers,
                    crossOrigin = hop.crossOrigin,
                    bodyRemoved = bodyRemoved,
                ),
                body = if (bodyRemoved) null else requestToSend.body,
            )
        }
    }

    private suspend fun prepareRequest(request: CrawlRequest): CrawlRequest {
        var headers = request.headers
        if (headers[HttpHeaders.UserAgent].isEmpty()) {
            headers = headers.plus(Header(HttpHeaders.UserAgent, configuration.userAgent))
        }
        if (headers[HttpHeaders.AcceptEncoding].isEmpty()) {
            headers = headers.plus(Header(HttpHeaders.AcceptEncoding, "gzip, deflate"))
        }
        headers = policies.cookies.apply(request.target, headers)
        return request.copy(headers = headers)
    }

    private suspend fun executeSingleHop(request: CrawlRequest): CrawlResponse {
        val requestBody = request.body
        validateRequestTarget(request)
        validateRequestHeaders(request.headers)
        return client.prepareRequest {
            url(request.target)
            method = HttpMethod(request.method.value)
            headers {
                request.headers.entries.forEach { header ->
                    if (!isRestrictedRequestHeader(header.name)) append(header.name, header.value)
                }
            }
            if (requestBody != null) {
                setBody(requestBody.toByteArray())
                if (request.headers[HttpHeaders.ContentType].isEmpty()) {
                    contentType(ContentType.Application.OctetStream)
                }
            }
            timeout {
                requestTimeoutMillis = configuration.requestTimeout.toMillis()
                connectTimeoutMillis = configuration.connectTimeout.toMillis()
                socketTimeoutMillis = configuration.socketTimeout.toMillis()
            }
        }.execute { response ->
            val coreHeaders = copyHeaders(response.headers)
            val body = readBody(
                response = response,
                maxCompressedBytes = configuration.maxCompressedBodyBytes,
                maxDecompressedBytes = configuration.maxDecompressedBodyBytes,
            )
            CrawlResponse(
                request = request,
                statusCode = response.status.value,
                headers = coreHeaders,
                body = ResponseBody.from(body),
                receivedAt = clock.now(),
                finalTarget = request.target,
            )
        }
    }

    private fun decorateResponse(response: CrawlResponse, history: RedirectHistory): CrawlResponse {
        val classification = policies.failureClassifier.classify(response)
        val retryAfter = policies.retryAfter.delay(response, clock.now())
        val attributes = response.attributes
            .plus(ContextAttribute("trawler.web.redirect-count", history.size.toString()))
            .plus(ContextAttribute("trawler.web.redirect-limit", configuration.maxRedirects.toString()))
            .plus(ContextAttribute("trawler.web.http-category", classification.category))
            .plus(ContextAttribute("trawler.web.http-retryable", classification.retryableByDefault.toString()))
            .let { current ->
                if (retryAfter == null) current else current.plus(
                    ContextAttribute("trawler.web.retry-after-millis", retryAfter.toMillis().toString()),
                )
            }
        return response.copy(attributes = attributes, finalTarget = response.request.target)
    }

    private suspend fun fetchRobots(origin: WebOrigin, userAgent: String): RobotsFetchResult {
        val target = robotsTarget(origin)
        val request = CrawlRequest(
            target = target,
            method = RequestMethod.GET,
            headers = CoreHeaders.of(
                Header(HttpHeaders.UserAgent, userAgent),
                Header(HttpHeaders.Accept, "text/plain"),
                Header(HttpHeaders.AcceptEncoding, "gzip, deflate"),
            ),
        )
        val permit = policies.concurrency.acquire(origin, WebRequestKind.ROBOTS)
        var pacing: OriginPacingReservation? = null
        return try {
            pacing = policies.pacing.acquire(origin, WebRequestKind.ROBOTS)
            val response = executeSingleHop(
                request,
                maxBodyBytes = configuration.maxRobotsBodyBytes,
            )
            policies.pacing.observe(origin, response)
            val text = String(response.body.toByteArray(), Charsets.UTF_8)
            RobotsFetchResult.Available(
                statusCode = response.statusCode ?: 0,
                body = text,
                fetchedAt = response.receivedAt,
            )
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Throwable) {
            RobotsFetchResult.Unavailable("robots-fetch-failed:${error::class.simpleName ?: "failure"}")
        } finally {
            pacing?.release()
            permit.release()
        }
    }

    private suspend fun executeSingleHop(request: CrawlRequest, maxBodyBytes: Int): CrawlResponse {
        validateRequestTarget(request)
        validateRequestHeaders(request.headers)
        return client.prepareRequest {
            url(request.target)
            method = HttpMethod(request.method.value)
            headers {
                request.headers.entries.forEach { header ->
                    if (!isRestrictedRequestHeader(header.name)) append(header.name, header.value)
                }
            }
            timeout {
                requestTimeoutMillis = configuration.requestTimeout.toMillis()
                connectTimeoutMillis = configuration.connectTimeout.toMillis()
                socketTimeoutMillis = configuration.socketTimeout.toMillis()
            }
        }.execute { response ->
            val coreHeaders = copyHeaders(response.headers)
            val body = readBody(response, maxBodyBytes, maxBodyBytes)
            CrawlResponse(
                request = request,
                statusCode = response.status.value,
                headers = coreHeaders,
                body = ResponseBody.from(body),
                receivedAt = clock.now(),
                finalTarget = request.target,
            )
        }
    }

    private fun validateRequestHeaders(headers: CoreHeaders) {
        require(headers.entries.size <= configuration.maxRequestHeaders) {
            "HTTP request headers exceed configured limit of ${configuration.maxRequestHeaders} entries"
        }
        require(headers.entries.all { it.name.length <= configuration.maxHeaderNameLength }) {
            "HTTP request header name exceeds configured limit of ${configuration.maxHeaderNameLength} characters"
        }
        require(headers.entries.all { it.value.length <= configuration.maxHeaderValueLength }) {
            "HTTP request header value exceeds configured limit of ${configuration.maxHeaderValueLength} characters"
        }
    }

    private fun validateRequestTarget(request: CrawlRequest) {
        require(request.target.length <= configuration.maxTargetLength) {
            "HTTP request target exceeds configured limit of ${configuration.maxTargetLength} characters"
        }
        require(request.method.value.length <= configuration.maxMethodLength) {
            "HTTP request method exceeds configured limit of ${configuration.maxMethodLength} characters"
        }
    }

    private fun isRestrictedRequestHeader(name: String): Boolean =
        RESTRICTED_REQUEST_HEADERS.any { it.equals(name, ignoreCase = true) }

    private fun copyHeaders(headers: io.ktor.http.Headers): CoreHeaders {
        val entries = ArrayList<Header>()
        headers.forEach { name, values ->
            if (name.length > configuration.maxHeaderNameLength) {
                throw WebResponseHeaderNameTooLargeException(configuration.maxHeaderNameLength)
            }
            values.forEach { value ->
                if (entries.size >= configuration.maxResponseHeaders) {
                    throw WebResponseHeadersTooLargeException(configuration.maxResponseHeaders)
                }
                if (value.length > configuration.maxHeaderValueLength) {
                    throw WebResponseHeaderValueTooLargeException(configuration.maxHeaderValueLength)
                }
                entries += Header(name, value)
            }
        }
        return CoreHeaders.from(entries)
    }

    private suspend fun readBody(
        response: io.ktor.client.statement.HttpResponse,
        maxCompressedBytes: Int,
        maxDecompressedBytes: Int,
    ): ByteArray {
        val contentLength = response.headers[HttpHeaders.ContentLength]?.toLongOrNull()
        if (contentLength != null && contentLength > maxCompressedBytes.toLong()) {
            throw WebResponseTooLargeException("compressed", maxCompressedBytes)
        }
        val raw = readChannel(response.bodyAsChannel(), maxCompressedBytes)
        val encoding = response.headers[HttpHeaders.ContentEncoding]
            ?.split(',')
            ?.map { it.trim().lowercase() }
            ?.filter { it.isNotEmpty() }
            ?: emptyList()
        if (encoding.size > 1) {
            throw UnsupportedWebContentEncodingException(encoding.joinToString(","))
        }
        return when (encoding.firstOrNull()) {
            null, "identity" -> {
                if (raw.size > maxDecompressedBytes) {
                    throw WebResponseTooLargeException("decompressed", maxDecompressedBytes)
                }
                raw
            }

            "gzip" -> GZIPInputStream(ByteArrayInputStream(raw)).use {
                readInput(it, maxDecompressedBytes)
            }

            "deflate" -> InflaterInputStream(ByteArrayInputStream(raw)).use {
                readInput(it, maxDecompressedBytes)
            }

            else -> throw UnsupportedWebContentEncodingException(encoding.first())
        }
    }

    private suspend fun readChannel(channel: ByteReadChannel, maximum: Int): ByteArray {
        val output = ByteArrayOutputStream(minOf(maximum, 8 * 1024))
        val buffer = ByteArray(minOf(maximum, 8 * 1024).coerceAtLeast(1))
        while (true) {
            val read = channel.readAvailable(buffer, 0, buffer.size)
            if (read == -1) break
            if (read == 0) continue
            if (output.size() > maximum - read) {
                throw WebResponseTooLargeException("compressed", maximum)
            }
            output.write(buffer, 0, read)
        }
        return output.toByteArray()
    }

    private fun readInput(input: InputStream, maximum: Int): ByteArray {
        val output = ByteArrayOutputStream(minOf(maximum, 8 * 1024))
        val buffer = ByteArray(minOf(maximum, 8 * 1024).coerceAtLeast(1))
        while (true) {
            val read = input.read(buffer)
            if (read == -1) break
            if (output.size() > maximum - read) {
                throw WebResponseTooLargeException("decompressed", maximum)
            }
            output.write(buffer, 0, read)
        }
        return output.toByteArray()
    }

    private fun resolveRedirect(currentTarget: String, location: String): String {
        return try {
            val resolved = URI(currentTarget).resolve(location).toASCIIString()
            resolved.substringBefore('#').takeIf { it.isNotBlank() }
                ?: throw InvalidWebRedirectException("Redirect resolved to an empty target")
        } catch (error: IllegalArgumentException) {
            throw InvalidWebRedirectException("Unable to resolve redirect target")
                .also { it.initCause(error) }
        }
    }

    private fun robotsTarget(origin: WebOrigin): String {
        val host = if (origin.host.contains(':')) "[${origin.host}]" else origin.host
        val defaultPort = (origin.scheme == "http" && origin.port == 80) ||
            (origin.scheme == "https" && origin.port == 443)
        return buildString {
            append(origin.scheme).append("://").append(host)
            if (!defaultPort) append(':').append(origin.port)
            append("/robots.txt")
        }
    }

    private fun redirectMethod(status: Int?, method: RequestMethod): RequestMethod {
        return when (status) {
            303 -> if (method.value.equals("HEAD", ignoreCase = true)) method else RequestMethod.GET
            301, 302 -> if (method.value.equals("POST", ignoreCase = true)) RequestMethod.GET else method
            else -> method
        }
    }

    public companion object {
        private val REDIRECT_STATUSES = setOf(301, 302, 303, 307, 308)
        private val RESTRICTED_REQUEST_HEADERS = setOf(
            HttpHeaders.Host,
            HttpHeaders.ContentLength,
            HttpHeaders.TransferEncoding,
            HttpHeaders.Connection,
        )

        /**
         * Creates a transport with a new CIO client and the caller-supplied
         * replaceable policy set. The transport owns and closes the client.
         */
        public fun create(
            configuration: KtorTransportConfig = KtorTransportConfig(),
            policies: WebTransportPolicies,
            clock: Clock = SystemClock,
        ): KtorTransport {
            val client = newClient(configuration)
            return KtorTransport(client, configuration, policies, clock, listOf(client))
        }

        /**
         * Creates a public-web transport with one bounded CIO client and one
         * bounded JDK resolver. Both resources are closed by [stop] or
         * [close]. Each assembled policy remains replaceable through
         * [create] and [WebTransportPolicies].
         */
        public fun publicWeb(
            configuration: KtorTransportConfig = KtorTransportConfig(),
            preset: PublicWebPresetConfig = PublicWebPresetConfig(),
            clock: Clock = SystemClock,
        ): KtorTransport {
            val resolver = JdkNetworkAddressResolver(maxParallelLookups = 4)
            val policies = WebPresets.publicWebPolicies(resolver, preset, clock)
            val client = newClient(configuration)
            return KtorTransport(client, configuration, policies, clock, listOf(resolver, client))
        }

        /**
         * Wraps a caller-owned Ktor client by deriving a transport-owned client
         * with redirects disabled, non-2xx responses inspectable, and the
         * configured timeout plugin installed or replaced. The caller's client
         * is closed only when [closeClient] is true; the derived client is
         * always closed by this transport.
         */
        public fun fromClient(
            client: HttpClient,
            configuration: KtorTransportConfig,
            policies: WebTransportPolicies,
            clock: Clock = SystemClock,
            closeClient: Boolean = false,
            ownedResources: List<AutoCloseable> = emptyList(),
        ): KtorTransport {
            val controlledClient = client.config {
                expectSuccess = false
                followRedirects = false
                installOrReplace(HttpTimeout) {
                    requestTimeoutMillis = configuration.requestTimeout.toMillis()
                    connectTimeoutMillis = configuration.connectTimeout.toMillis()
                    socketTimeoutMillis = configuration.socketTimeout.toMillis()
                }
            }
            return KtorTransport(
            client = controlledClient,
            configuration = configuration,
            policies = policies,
            clock = clock,
            ownedResources = ownedResources + listOf(controlledClient) +
                if (closeClient) listOf(client) else emptyList(),
            )
        }

        private fun newClient(configuration: KtorTransportConfig): HttpClient = HttpClient(CIO) {
            expectSuccess = false
            followRedirects = false
            install(HttpTimeout) {
                requestTimeoutMillis = configuration.requestTimeout.toMillis()
                connectTimeoutMillis = configuration.connectTimeout.toMillis()
                socketTimeoutMillis = configuration.socketTimeout.toMillis()
            }
            engine {
                maxConnectionsCount = configuration.maxClientConnections
                endpoint.maxConnectionsPerRoute = configuration.maxConnectionsPerOrigin
                endpoint.connectTimeout = configuration.connectTimeout.toMillis()
                endpoint.socketTimeout = configuration.socketTimeout.toMillis()
            }
        }
    }

    private val robotsFetcher: com.rmjtromp.trawler.web.policy.RobotsFetcher
        get() = object : com.rmjtromp.trawler.web.policy.RobotsFetcher {
            override suspend fun fetch(origin: WebOrigin, userAgent: String): RobotsFetchResult =
                fetchRobots(origin, userAgent)
        }
}
