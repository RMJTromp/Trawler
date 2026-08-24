package com.rmjtromp.trawler.web.transport

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.RequestBody
import com.rmjtromp.trawler.model.RequestMethod
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.web.policy.AllowAllWebDestinationPolicy
import com.rmjtromp.trawler.web.policy.DefaultHeaderRedactionPolicy
import com.rmjtromp.trawler.web.policy.DefaultHttpFailureClassifier
import com.rmjtromp.trawler.web.policy.DefaultRedirectHeaderPolicy
import com.rmjtromp.trawler.web.policy.DefaultRetryAfterPolicy
import com.rmjtromp.trawler.web.policy.NoRobotsPolicy
import com.rmjtromp.trawler.web.session.CookieSession
import com.rmjtromp.trawler.web.session.InMemoryCookieSession
import com.rmjtromp.trawler.web.session.NoCookieSession
import java.io.ByteArrayOutputStream
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.IOException
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.DeflaterOutputStream
import java.util.zip.GZIPOutputStream
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

/** CIO integration tests using only process-local loopback HTTP servers. */
class KtorTransportIT {
    @Test
    fun `CIO sends bounded request metadata and adapts the local response`(): Unit = runBlocking {
        val receivedAt = Instant.parse("2026-01-02T03:04:05Z")
        val server = LocalHttpServer { _ ->
            ServerResponse(
                status = 201,
                headers = listOf(
                    "X-Response" to "one",
                    "X-Response" to "two",
                ),
                body = "created".toByteArray(),
            )
        }
        val transport = KtorTransport.create(
            configuration = KtorTransportConfig(maxRequestBodyBytes = 64),
            policies = testPolicies(),
            clock = FixedClock(receivedAt),
        )
        try {
            val target = server.url("/submit")
            val body = "payload".toByteArray()
            val response = transport.execute(
                CrawlRequest(
                    target = target,
                    method = RequestMethod("POST"),
                    headers = Headers.of(
                        Header("X-Duplicate", "one"),
                        Header("X-Duplicate", "two"),
                        Header("User-Agent", "caller-agent"),
                        Header("Host", "caller-host"),
                        Header("Content-Length", "999"),
                        Header("Connection", "close"),
                    ),
                    body = RequestBody.from(body),
                ),
            )

            val request = server.requests.single()
            assertEquals("POST", request.method)
            assertContentEquals(body, request.body)
            assertEquals(listOf("one,two"), request.header("x-duplicate"))
            assertEquals(listOf("caller-agent"), request.header("user-agent"))
            assertEquals(listOf("gzip, deflate"), request.header("accept-encoding"))
            assertEquals(listOf("application/octet-stream"), request.header("content-type"))
            assertNotEquals(listOf("999"), request.header("content-length"))
            assertFalse(request.header("host").contains("caller-host"))

            assertEquals(201, response.statusCode)
            assertContentEquals("created".toByteArray(), response.body.toByteArray())
            assertEquals(listOf("one", "two"), response.headers["X-Response"])
            assertEquals(target, response.finalTarget)
            assertEquals(receivedAt, response.receivedAt)
        } finally {
            transport.close()
            server.close()
        }
    }

    @Test
    fun `CIO follows cross origin redirect and applies destination cookies`(): Unit = runBlocking {
        lateinit var destination: LocalHttpServer
        val source = LocalHttpServer { _ ->
            ServerResponse(
                status = 302,
                headers = listOf("Location" to "${destination.url("/final")}#fragment"),
            )
        }
        destination = LocalHttpServer { _ -> ServerResponse(body = "destination".toByteArray()) }
        val clock = FixedClock(Instant.parse("2026-01-02T03:04:05Z"))
        val cookies = InMemoryCookieSession(clock = clock)
        cookies.observe(destination.url("/final"), listOf("destination=accepted; Path=/"))
        val destinationPolicy = RecordingDestination()
        val transport = KtorTransport.create(
            policies = testPolicies(destination = destinationPolicy, cookies = cookies),
            clock = clock,
        )
        try {
            val start = source.url("/start")
            val response = transport.execute(
                CrawlRequest(
                    target = start,
                    headers = Headers.of(
                        Header("Authorization", "Bearer source-secret"),
                        Header("Cookie", "caller=source"),
                    ),
                ),
            )

            val sourceRequest = source.requests.single()
            val destinationRequest = destination.requests.single()
            assertEquals(listOf("Bearer source-secret"), sourceRequest.header("authorization"))
            assertEquals(listOf("caller=source"), sourceRequest.header("cookie"))
            assertTrue(destinationRequest.header("authorization").isEmpty())
            assertEquals(listOf("destination=accepted"), destinationRequest.header("cookie"))
            assertEquals(listOf(start, destination.url("/final")), destinationPolicy.targets)
            assertContentEquals("destination".toByteArray(), response.body.toByteArray())
            assertEquals(destination.url("/final"), response.finalTarget)
            assertEquals("1", response.attributes["trawler.web.redirect-count"])
        } finally {
            transport.close()
            source.close()
            destination.close()
        }
    }

    @Test
    fun `CIO decodes local gzip and deflate and does not retry non success`(): Unit = runBlocking {
        val calls = CopyOnWriteArrayList<String>()
        val server = LocalHttpServer { request ->
            calls += request.path
            when (request.path) {
                "/gzip" -> ServerResponse(
                    headers = listOf("Content-Encoding" to "gzip"),
                    body = gzip("gzip-body".toByteArray()),
                )

                "/deflate" -> ServerResponse(
                    headers = listOf("Content-Encoding" to "deflate"),
                    body = deflate("deflate-body".toByteArray()),
                )

                "/failure" -> ServerResponse(
                    status = 503,
                    headers = listOf("Retry-After" to "4"),
                    body = "still inspectable".toByteArray(),
                )

                else -> error("unexpected local path ${request.path}")
            }
        }
        val transport = KtorTransport.create(policies = testPolicies())
        try {
            assertContentEquals(
                "gzip-body".toByteArray(),
                transport.execute(CrawlRequest(server.url("/gzip"))).body.toByteArray(),
            )
            assertContentEquals(
                "deflate-body".toByteArray(),
                transport.execute(CrawlRequest(server.url("/deflate"))).body.toByteArray(),
            )
            val failure = transport.execute(CrawlRequest(server.url("/failure")))
            assertEquals(503, failure.statusCode)
            assertContentEquals("still inspectable".toByteArray(), failure.body.toByteArray())
            assertEquals("4000", failure.attributes["trawler.web.retry-after-millis"])
            assertEquals(listOf("/gzip", "/deflate", "/failure"), calls)
        } finally {
            transport.close()
            server.close()
        }
    }

    private fun testPolicies(
        destination: WebDestinationPolicy = AllowAllWebDestinationPolicy,
        cookies: CookieSession = NoCookieSession,
    ): WebTransportPolicies = WebTransportPolicies(
        destination = destination,
        robots = NoRobotsPolicy,
        concurrency = NoOriginConcurrencyPolicy,
        pacing = NoOriginPacingPolicy,
        cookies = cookies,
        failureClassifier = DefaultHttpFailureClassifier,
        retryAfter = DefaultRetryAfterPolicy,
        redirectHeaders = DefaultRedirectHeaderPolicy(),
        headerRedaction = DefaultHeaderRedactionPolicy(),
    )

    private fun gzip(bytes: ByteArray): ByteArray = ByteArrayOutputStream().use { output ->
        GZIPOutputStream(output).use { it.write(bytes) }
        output.toByteArray()
    }

    private fun deflate(bytes: ByteArray): ByteArray = ByteArrayOutputStream().use { output ->
        DeflaterOutputStream(output).use { it.write(bytes) }
        output.toByteArray()
    }

    private class FixedClock(private val instant: Instant) : Clock {
        override fun now(): Instant = instant
    }

    private class RecordingDestination : WebDestinationPolicy {
        override val id: String = "recording-destination-it"
        val targets = CopyOnWriteArrayList<String>()

        override suspend fun evaluate(
            request: CrawlRequest,
            history: RedirectHistory,
        ): DestinationDecision {
            targets += request.target
            return DestinationDecision.Allowed
        }
    }

    private data class RecordedRequest(
        val method: String,
        val path: String,
        val headers: Map<String, List<String>>,
        val body: ByteArray,
    ) {
        fun header(name: String): List<String> = headers[name.lowercase()].orEmpty()
    }

    private data class ServerResponse(
        val status: Int = 200,
        val headers: List<Pair<String, String>> = emptyList(),
        val body: ByteArray = ByteArray(0),
    )

    private class LocalHttpServer(
        private val responder: (RecordedRequest) -> ServerResponse,
    ) : AutoCloseable {
        private val server = ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())
        private val executor = Executors.newFixedThreadPool(4)
        private val accepting = AtomicBoolean(true)
        private val acceptThread = Thread(::acceptLoop, "trawler-test-http-${server.localPort}")
        val requests = CopyOnWriteArrayList<RecordedRequest>()
        private val port: Int = server.localPort

        init {
            acceptThread.isDaemon = true
            acceptThread.start()
        }

        fun url(path: String): String = "http://127.0.0.1:$port$path"

        private fun acceptLoop() {
            while (accepting.get()) {
                try {
                    val socket = server.accept()
                    executor.execute { socket.use(::handle) }
                } catch (_: SocketException) {
                    if (accepting.get()) throw IllegalStateException("local test server stopped unexpectedly")
                }
            }
        }

        private fun handle(socket: Socket) {
            socket.use { current ->
                val input = BufferedInputStream(current.getInputStream())
                val requestLine = readLine(input)
                    ?: throw IOException("local test server received an empty request")
                val parts = requestLine.split(' ', limit = 3)
                require(parts.size == 3) { "invalid HTTP request line: $requestLine" }
                val headerMap = linkedMapOf<String, MutableList<String>>()
                while (true) {
                    val line = readLine(input) ?: throw IOException("truncated HTTP headers")
                    if (line.isEmpty()) break
                    val separator = line.indexOf(':')
                    require(separator > 0) { "invalid HTTP header: $line" }
                    headerMap.getOrPut(line.substring(0, separator).lowercase()) { mutableListOf() }
                        .add(line.substring(separator + 1).trim())
                }
                val contentLength = headerMap["content-length"]?.firstOrNull()?.toIntOrNull() ?: 0
                require(contentLength >= 0) { "negative content length" }
                val body = input.readExactly(contentLength)
                val request = RecordedRequest(
                    method = parts[0],
                    path = URI(parts[1]).rawPath ?: "/",
                    headers = headerMap.mapValues { it.value.toList() },
                    body = body,
                )
                requests += request
                writeResponse(current, responder(request))
            }
        }

        private fun writeResponse(socket: Socket, response: ServerResponse) {
            val output = BufferedOutputStream(socket.getOutputStream())
            val reason = when (response.status) {
                200 -> "OK"
                201 -> "Created"
                302 -> "Found"
                503 -> "Service Unavailable"
                else -> "Test Response"
            }
            output.write("HTTP/1.1 ${response.status} $reason\r\n".toByteArray(StandardCharsets.ISO_8859_1))
            response.headers.forEach { (name, value) ->
                output.write("$name: $value\r\n".toByteArray(StandardCharsets.ISO_8859_1))
            }
            output.write("Content-Length: ${response.body.size}\r\nConnection: close\r\n\r\n".toByteArray(StandardCharsets.ISO_8859_1))
            output.write(response.body)
            output.flush()
        }

        private fun readLine(input: BufferedInputStream): String? {
            val line = ByteArrayOutputStream()
            while (true) {
                val value = input.read()
                if (value < 0) return if (line.size() == 0) null else line.toString(StandardCharsets.ISO_8859_1)
                if (value == '\r'.code) {
                    require(input.read() == '\n'.code) { "HTTP line is not CRLF terminated" }
                    return line.toString(StandardCharsets.ISO_8859_1)
                }
                line.write(value)
            }
        }

        private fun BufferedInputStream.readExactly(length: Int): ByteArray {
            val result = ByteArray(length)
            var offset = 0
            while (offset < length) {
                val read = read(result, offset, length - offset)
                if (read < 0) throw IOException("truncated HTTP request body")
                offset += read
            }
            return result
        }

        override fun close() {
            if (!accepting.compareAndSet(true, false)) return
            server.close()
            acceptThread.join()
            executor.shutdownNow()
        }
    }
}
