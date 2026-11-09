package com.rmjtromp.trawler.web.transport

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.web.policy.WebProxyFailureException
import com.rmjtromp.trawler.web.policy.WebResponseHeaderNameTooLargeException
import com.rmjtromp.trawler.web.policy.WebResponseHeaderValueTooLargeException
import com.rmjtromp.trawler.web.policy.WebResponseHeadersTooLargeException
import com.rmjtromp.trawler.web.policy.WebResponseTooLargeException
import io.ktor.client.network.sockets.ConnectTimeoutException
import io.ktor.client.plugins.HttpRequestTimeoutException
import io.ktor.http.HttpHeaders
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.net.ProxySelector
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpConnectTimeoutException
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Flow
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.cancellation.CancellationException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/** One completed JDK-path exchange before content decoding. */
internal class Tls13Exchange(
    val statusCode: Int,
    val headers: Headers,
    val rawBody: ByteArray,
)

/**
 * Transport-owned JDK `java.net.http` client used only for hosts explicitly
 * selected by [KtorTransportConfig.tls13Hosts].
 *
 * This class exists because Ktor's own CIO TLS stops at TLS 1.2 and Ktor's
 * Java engine neither closes its JDK client nor enforces socket inactivity.
 * It mirrors the CIO path's HTTP/1.1, no-redirect, no-cookie-store,
 * no-decompression configuration and its direct or proxied routing; [KtorTransport] keeps
 * owning every policy, redirect, cookie, and content-decoding decision.
 *
 * Instances are coroutine-safe. [exchange] is cancellable, aborts the
 * underlying exchange on cancellation, and never retries a failed exchange
 * itself. [close] aborts in-flight exchanges and shuts the client down.
 */
internal class Tls13HttpClient(
    private val configuration: KtorTransportConfig,
) : AutoCloseable {
    init {
        requireNoHiddenJdkRetries()
    }

    private val closed = AtomicBoolean(false)
    private val inFlight = ConcurrentHashMap.newKeySet<CompletableFuture<*>>()
    private val limiter = ConnectionLimiter(
        configuration.maxClientConnections,
        configuration.maxConnectionsPerOrigin,
    )
    private val executor: ExecutorService = Executors.newFixedThreadPool(EXECUTOR_THREADS) { task ->
        Thread(task, "trawler-tls13-${THREAD_IDS.incrementAndGet()}").apply { isDaemon = true }
    }
    private val client: HttpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .followRedirects(HttpClient.Redirect.NEVER)
        .proxy(configuration.proxy?.let { ProxySelector.of(it.socketAddress()) } ?: HttpClient.Builder.NO_PROXY)
        .connectTimeout(configuration.connectTimeout)
        .executor(executor)
        .build()
    private val shutdownHandle: AutoCloseable = try {
        requireShutdownCapability(client)
    } catch (error: Throwable) {
        executor.shutdownNow()
        throw error
    }

    /**
     * Performs one exchange. Header and content-length bounds are enforced by
     * [adaptHeaders] and [maxCompressedBytes] before the body is accepted, and
     * the raw body is never buffered beyond [maxCompressedBytes].
     */
    suspend fun exchange(
        request: CrawlRequest,
        headers: List<Header>,
        maxCompressedBytes: Int,
        adaptHeaders: (Map<String, List<String>>) -> Headers,
        proxyHop: ProxyHop?,
    ): Tls13Exchange {
        val timeoutMillis = configuration.requestTimeout.toMillis()
        return withTimeoutOrNull(timeoutMillis) {
            limiter.withPermits(WebOrigin.fromTarget(request.target)) {
                proxyHop?.connecting()
                perform(request, headers, maxCompressedBytes, adaptHeaders, proxyHop)
            }
        } ?: throw HttpRequestTimeoutException(request.target, timeoutMillis)
    }

    private suspend fun perform(
        request: CrawlRequest,
        headers: List<Header>,
        maxCompressedBytes: Int,
        adaptHeaders: (Map<String, List<String>>) -> Headers,
        proxyHop: ProxyHop?,
    ): Tls13Exchange {
        val outgoing = buildRequest(request, headers, proxyHop)
        val activity = Channel<Unit>(Channel.CONFLATED)
        val handler = BoundedBodyHandler(maxCompressedBytes, activity, adaptHeaders) { status ->
            configuration.proxyRejection(request.target, status)
        }
        check(!closed.get()) { "TLS 1.3 client is closed" }
        val future = client.sendAsync(outgoing, handler)
        inFlight += future
        // A concurrent close may have swept inFlight just before this registration.
        if (closed.get()) future.cancel(true)
        val response = try {
            coroutineScope {
                val watchdog = launch { watchInactivity(activity) }
                try {
                    awaitExchange(future)
                } finally {
                    watchdog.cancel()
                }
            }
        } finally {
            inFlight -= future
        }
        // The JDK client answers a CONNECT 407 with a synthetic response that skips the body handler.
        if (handler.adaptedHeaders == null && response.statusCode() == PROXY_AUTHENTICATION_REQUIRED) {
            throw WebProxyFailureException(PROXY_AUTHENTICATION_REQUIRED, null)
        }
        return Tls13Exchange(response.statusCode(), checkNotNull(handler.adaptedHeaders), response.body())
    }

    private fun buildRequest(request: CrawlRequest, headers: List<Header>, proxyHop: ProxyHop?): HttpRequest {
        val builder = HttpRequest.newBuilder(URI(configuration.wireTarget(request.target)))
        headers.forEach { builder.header(it.name, it.value) }
        val publisher = request.body
            ?.let { HttpRequest.BodyPublishers.ofByteArray(it.toByteArray()) }
            ?: HttpRequest.BodyPublishers.noBody()
        val observed = if (proxyHop == null) publisher else DispatchObservingPublisher(publisher, proxyHop)
        return builder.method(request.method.value, observed).build()
    }

    /**
     * JDK clients have no socket inactivity setting, so idle time is measured
     * here. Connection setup reports no progress, so the wait for the first
     * response bytes may additionally consume the connect budget.
     */
    private suspend fun watchInactivity(activity: Channel<Unit>) {
        val socketMillis = configuration.socketTimeout.toMillis()
        var budgetMillis = configuration.connectTimeout.toMillis() + socketMillis
        while (true) {
            withTimeoutOrNull(budgetMillis) { activity.receive() }
                ?: throw SocketTimeoutException("No response data within $budgetMillis ms")
            budgetMillis = socketMillis
        }
    }

    private suspend fun <T> awaitExchange(future: CompletableFuture<T>): T =
        suspendCancellableCoroutine { continuation ->
            future.whenComplete { value, error ->
                if (error == null) {
                    continuation.resume(value)
                } else {
                    continuation.resumeWithException(translateFailure(error))
                }
            }
            // kotlinx future.await() cancels with false, which leaves the JDK exchange running.
            continuation.invokeOnCancellation { future.cancel(true) }
        }

    private fun translateFailure(error: Throwable): Throwable {
        // The JDK client wraps failures raised by a body subscriber in IOException.
        generateSequence(error) { it.cause }.take(MAX_CAUSE_DEPTH).firstOrNull(::isAdapterBoundFailure)
            ?.let { return it }
        var cause = error
        while (cause is CompletionException || cause is ExecutionException) {
            cause = cause.cause ?: break
        }
        // With the required retry limit of 1, the JDK reports the original failure as the cause.
        if (cause is IOException && cause.message == JDK_RETRY_LIMIT_MESSAGE) {
            cause = cause.cause ?: cause
        }
        return when {
            cause is HttpConnectTimeoutException ->
                ConnectTimeoutException(cause.message ?: "JDK client connect timeout", cause)

            // Only close() cancels the future without cancelling the awaiting coroutine.
            cause is CancellationException && closed.get() ->
                IOException("Transport closed while the exchange was in progress", cause)

            else -> cause
        }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        inFlight.toList().forEach { it.cancel(true) }
        try {
            shutdownHandle.close()
        } finally {
            executor.shutdownNow()
        }
    }

    private companion object {
        const val EXECUTOR_THREADS = 4
        const val MAX_CAUSE_DEPTH = 8
        val THREAD_IDS = AtomicInteger()
    }
}

private const val JDK_RETRY_LIMIT_MESSAGE = "Too many retries"
internal const val JDK_RETRY_LIMIT_PROPERTY = "jdk.httpclient.redirects.retrylimit"

/**
 * Refuses to build the client unless the JVM disables the JDK client's internal
 * re-sends. By default it re-sends a GET or HEAD after an abrupt connection or
 * handshake failure, which would be a retry hidden from engine attempt
 * accounting. The limit is JVM-wide and read once by the JDK, so the host must
 * set it; a library must not change it behind other clients in the process.
 */
internal fun requireNoHiddenJdkRetries() {
    check(System.getProperty(JDK_RETRY_LIMIT_PROPERTY)?.trim() == "1") {
        "KtorTransportConfig.tls13Hosts requires the JVM option -D$JDK_RETRY_LIMIT_PROPERTY=1 " +
            "so the JDK HTTP client cannot retry requests outside engine attempt accounting"
    }
}

/**
 * Returns the JDK client's shutdown handle. `HttpClient` only became
 * `AutoCloseable` in Java 21, and earlier runtimes offer no way to release its
 * selector thread and pooled sockets, so they are refused instead of leaking.
 */
internal fun requireShutdownCapability(client: Any): AutoCloseable =
    client as? AutoCloseable ?: throw IllegalStateException(
        "KtorTransportConfig.tls13Hosts requires a Java 21 or newer runtime for deterministic client shutdown",
    )

private fun isAdapterBoundFailure(error: Throwable): Boolean =
    error is WebProxyFailureException ||
        error is WebResponseTooLargeException ||
        error is WebResponseHeadersTooLargeException ||
        error is WebResponseHeaderNameTooLargeException ||
        error is WebResponseHeaderValueTooLargeException

/** Sends the delegate unchanged and reports when the JDK client begins writing the request. */
private class DispatchObservingPublisher(
    private val delegate: HttpRequest.BodyPublisher,
    private val hop: ProxyHop,
) : HttpRequest.BodyPublisher by delegate {
    override fun contentLength(): Long = delegate.contentLength().also { hop.dispatched() }
}

/** Bounds exchanges the way CIO bounds its total and per-route connections. */
internal class ConnectionLimiter(maxTotal: Int, private val maxPerOrigin: Int) {
    private class OriginEntry(permits: Int) {
        val semaphore = Semaphore(permits)
        var users = 0
    }

    private val total = Semaphore(maxTotal)
    private val origins = HashMap<WebOrigin, OriginEntry>()

    suspend fun <T> withPermits(origin: WebOrigin, block: suspend () -> T): T {
        val entry = synchronized(origins) {
            origins.getOrPut(origin) { OriginEntry(maxPerOrigin) }.also { it.users++ }
        }
        try {
            return entry.semaphore.withPermit { total.withPermit { block() } }
        } finally {
            synchronized(origins) {
                entry.users--
                if (entry.users == 0) origins.remove(origin)
            }
        }
    }
}

/** Rejects oversized responses before or while their body arrives. */
private class BoundedBodyHandler(
    private val maxBytes: Int,
    private val activity: Channel<Unit>,
    private val adaptHeaders: (Map<String, List<String>>) -> Headers,
    private val statusRejection: (Int) -> Throwable?,
) : HttpResponse.BodyHandler<ByteArray> {
    @Volatile
    var adaptedHeaders: Headers? = null
        private set

    override fun apply(info: HttpResponse.ResponseInfo): HttpResponse.BodySubscriber<ByteArray> {
        activity.trySend(Unit)
        val rejection = try {
            statusRejection(info.statusCode()) ?: run {
                adaptedHeaders = adaptHeaders(info.headers().map())
                val contentLength = info.headers().firstValue(HttpHeaders.ContentLength).orElse(null)?.toLongOrNull()
                if (contentLength != null && contentLength > maxBytes.toLong()) {
                    WebResponseTooLargeException("compressed", maxBytes)
                } else {
                    null
                }
            }
        } catch (error: RuntimeException) {
            error
        }
        return BoundedBodySubscriber(maxBytes, activity, rejection)
    }
}

/** Non-blocking body collector that cancels the stream at its byte bound. */
private class BoundedBodySubscriber(
    private val maxBytes: Int,
    private val activity: Channel<Unit>,
    private val rejection: Throwable?,
) : HttpResponse.BodySubscriber<ByteArray> {
    private val output = ByteArrayOutputStream(minOf(maxBytes, 8 * 1024))
    private val body = CompletableFuture<ByteArray>()

    @Volatile
    private var subscription: Flow.Subscription? = null

    override fun getBody(): CompletionStage<ByteArray> = body

    override fun onSubscribe(subscription: Flow.Subscription) {
        this.subscription = subscription
        if (rejection != null) {
            fail(rejection)
        } else {
            subscription.request(1)
        }
    }

    override fun onNext(items: List<ByteBuffer>) {
        if (body.isDone) return
        activity.trySend(Unit)
        items.forEach { buffer ->
            if (buffer.remaining() > maxBytes - output.size()) {
                fail(WebResponseTooLargeException("compressed", maxBytes))
                return
            }
            val chunk = ByteArray(buffer.remaining())
            buffer.get(chunk)
            output.write(chunk)
        }
        subscription?.request(1)
    }

    override fun onError(throwable: Throwable) {
        body.completeExceptionally(throwable)
    }

    override fun onComplete() {
        body.complete(output.toByteArray())
    }

    private fun fail(failure: Throwable) {
        body.completeExceptionally(failure)
        subscription?.cancel()
    }
}
