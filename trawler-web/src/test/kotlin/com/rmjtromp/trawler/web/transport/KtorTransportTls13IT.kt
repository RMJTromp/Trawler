package com.rmjtromp.trawler.web.transport

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.RequestBody
import com.rmjtromp.trawler.model.RequestMethod
import com.rmjtromp.trawler.web.policy.AllowAllWebDestinationPolicy
import com.rmjtromp.trawler.web.policy.CachingRobotsPolicy
import com.rmjtromp.trawler.web.policy.DefaultHeaderRedactionPolicy
import com.rmjtromp.trawler.web.policy.DefaultHttpFailureClassifier
import com.rmjtromp.trawler.web.policy.DefaultRedirectHeaderPolicy
import com.rmjtromp.trawler.web.policy.DefaultRetryAfterPolicy
import com.rmjtromp.trawler.web.policy.NoRobotsPolicy
import com.rmjtromp.trawler.web.policy.RobotsPolicy
import com.rmjtromp.trawler.web.policy.WebResponseHeaderNameTooLargeException
import com.rmjtromp.trawler.web.policy.WebResponseHeaderValueTooLargeException
import com.rmjtromp.trawler.web.policy.WebResponseHeadersTooLargeException
import com.rmjtromp.trawler.web.policy.WebResponseTooLargeException
import com.rmjtromp.trawler.web.session.CookieSession
import com.rmjtromp.trawler.web.session.InMemoryCookieSession
import com.rmjtromp.trawler.web.session.NoCookieSession
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import com.sun.net.httpserver.HttpsConfigurator
import com.sun.net.httpserver.HttpsExchange
import com.sun.net.httpserver.HttpsParameters
import com.sun.net.httpserver.HttpsServer
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.HttpRequestTimeoutException
import io.ktor.http.HttpStatusCode
import io.ktor.network.tls.TlsException
import java.io.ByteArrayOutputStream
import java.io.EOFException
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Proxy
import java.net.ProxySelector
import java.net.ServerSocket
import java.net.SocketAddress
import java.net.SocketTimeoutException
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPOutputStream
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Loopback HTTPS tests for the [KtorTransportConfig.tls13Hosts] path. Servers
 * accept only TLS 1.3 and use a certificate generated for this test run, and
 * `localhost` and `127.0.0.1` act as two distinct hosts on the same machine.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KtorTransportTls13IT {
    private lateinit var keyDirectory: Path
    private lateinit var serverContext: SSLContext
    private lateinit var previousDefaultContext: SSLContext

    @BeforeAll
    fun createCertificateAndTrustIt() {
        keyDirectory = Files.createTempDirectory("trawler-tls13-it")
        val keyStorePath = keyDirectory.resolve("server.p12")
        val keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString()
        val process = ProcessBuilder(
            keytool, "-genkeypair", "-alias", "server", "-keyalg", "EC", "-groupname", "secp256r1",
            "-sigalg", "SHA256withECDSA", "-dname", "CN=localhost", "-validity", "2",
            "-ext", "SAN=dns:localhost,ip:127.0.0.1",
            "-storetype", "PKCS12", "-keystore", keyStorePath.toString(),
            "-storepass", STORE_PASSWORD, "-keypass", STORE_PASSWORD,
        ).redirectErrorStream(true).start()
        val output = process.inputStream.readAllBytes().decodeToString()
        assertTrue(process.waitFor(30, TimeUnit.SECONDS) && process.exitValue() == 0, output)

        val keyStore = KeyStore.getInstance("PKCS12")
        Files.newInputStream(keyStorePath).use { keyStore.load(it, STORE_PASSWORD.toCharArray()) }
        val keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
            .apply { init(keyStore, STORE_PASSWORD.toCharArray()) }
        val trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
            .apply { init(keyStore) }
        serverContext = SSLContext.getInstance("TLS").apply { init(keyManagers.keyManagers, null, null) }
        previousDefaultContext = SSLContext.getDefault()
        SSLContext.setDefault(SSLContext.getInstance("TLS").apply { init(null, trustManagers.trustManagers, null) })
    }

    @AfterAll
    fun restoreTrustAndDeleteCertificate() {
        SSLContext.setDefault(previousDefaultContext)
        keyDirectory.toFile().deleteRecursively()
    }

    @Test
    fun `configured host negotiates TLS 1_3 over HTTP 1_1 and adapts the response`(): Unit = runBounded {
        Tls13Server { exchange ->
            exchange.responseHeaders.add("X-Response", "one")
            exchange.responseHeaders.add("X-Response", "two")
            exchange.respond(201, "created".toByteArray())
        }.use { server ->
            transport(setOf("localhost")).use { transport ->
                val target = server.url("localhost", "/page")
                val response = transport.execute(CrawlRequest(target))

                assertEquals(201, response.statusCode)
                assertContentEquals("created".toByteArray(), response.body.toByteArray())
                assertEquals(listOf("one", "two"), response.headers["X-Response"])
                assertEquals(target, response.finalTarget)
                val recorded = server.requests.single()
                assertEquals("TLSv1.3", recorded.tlsProtocol)
                assertEquals("HTTP/1.1", recorded.httpProtocol)
            }
        }
    }

    @Test
    fun `default path is refused by a TLS 1_3 only server without reaching HTTP`(): Unit = runBounded {
        Tls13Server { it.respond(200, "unreachable".toByteArray()) }.use { server ->
            transport(emptySet()).use { transport ->
                assertCioHandshakeRefused { transport.execute(CrawlRequest(server.url("localhost", "/page"))) }
                assertTrue(server.requests.isEmpty())
            }
        }
    }

    @Test
    fun `host matching is exact so other hosts stay on the default path`(): Unit = runBounded {
        Tls13Server { it.respond(200, "unreachable".toByteArray()) }.use { server ->
            transport(setOf("localhost")).use { transport ->
                assertCioHandshakeRefused { transport.execute(CrawlRequest(server.url("127.0.0.1", "/page"))) }
                assertTrue(server.requests.isEmpty())
            }
        }
    }

    @Test
    fun `host matching is case insensitive`(): Unit = runBounded {
        Tls13Server { it.respond(200, "ok".toByteArray()) }.use { server ->
            transport(setOf("LOCALHOST")).use { transport ->
                val response = transport.execute(CrawlRequest(server.url("Localhost", "/page")))
                assertEquals(200, response.statusCode)
            }
        }
    }

    @Test
    fun `redirect from the default path selects the JDK path for a configured destination`(): Unit = runBounded {
        Tls13Server { it.respond(200, "final".toByteArray()) }.use { tls ->
            LoopbackHttpServer { exchange ->
                exchange.responseHeaders.add("Location", tls.url("localhost", "/final"))
                exchange.respond(302, ByteArray(0))
            }.use { plain ->
                val destinations = RecordingDestination()
                val concurrency = RecordingConcurrency()
                transport(setOf("localhost"), destination = destinations, concurrency = concurrency).use { transport ->
                    val start = plain.url("127.0.0.1", "/start")
                    val response = transport.execute(CrawlRequest(start))

                    assertContentEquals("final".toByteArray(), response.body.toByteArray())
                    assertEquals("1", response.attributes["trawler.web.redirect-count"])
                    assertEquals("TLSv1.3", tls.requests.single().tlsProtocol)
                    assertEquals(listOf(start, tls.url("localhost", "/final")), destinations.targets)
                    assertEquals(listOf("127.0.0.1", "localhost"), concurrency.hosts)
                    assertEquals(2, concurrency.released.get())
                }
            }
        }
    }

    @Test
    fun `redirect from the JDK path selects the default path for an unconfigured destination`(): Unit = runBounded {
        Tls13Server { it.respond(200, "unreachable".toByteArray()) }.use { destination ->
            Tls13Server { exchange ->
                exchange.responseHeaders.add("Location", destination.url("127.0.0.1", "/final"))
                exchange.respond(302, ByteArray(0))
            }.use { source ->
                transport(setOf("localhost")).use { transport ->
                    assertCioHandshakeRefused { transport.execute(CrawlRequest(source.url("localhost", "/start"))) }
                    assertEquals(1, source.requests.size)
                    assertTrue(destination.requests.isEmpty())
                }
            }
        }
    }

    @Test
    fun `JDK client does not follow redirects and credentials are stripped across origins`(): Unit = runBounded {
        Tls13Server { it.respond(200, "destination".toByteArray()) }.use { destination ->
            Tls13Server { exchange ->
                exchange.responseHeaders.add("Location", destination.url("localhost", "/final"))
                exchange.respond(302, ByteArray(0))
            }.use { source ->
                transport(setOf("localhost")).use { transport ->
                    val response = transport.execute(
                        CrawlRequest(
                            target = source.url("localhost", "/start"),
                            headers = Headers.of(
                                Header("Authorization", "Bearer source-secret"),
                                Header("Cookie", "caller=source"),
                            ),
                        ),
                    )
                    assertEquals(200, response.statusCode)
                    assertEquals(1, source.requests.size)
                    assertEquals(listOf("Bearer source-secret"), source.requests.single().header("authorization"))
                    val forwarded = destination.requests.single()
                    assertTrue(forwarded.header("authorization").isEmpty())
                    assertTrue(forwarded.header("cookie").isEmpty())
                }
            }
        }
    }

    @Test
    fun `robots fetches for a configured host use the JDK path`(): Unit = runBounded {
        Tls13Server { exchange ->
            when (exchange.requestURI.path) {
                "/robots.txt" -> exchange.respond(200, "User-agent: *\nDisallow: /private\n".toByteArray())
                else -> exchange.respond(200, "page".toByteArray())
            }
        }.use { server ->
            transport(setOf("localhost"), robots = CachingRobotsPolicy()).use { transport ->
                val response = transport.execute(CrawlRequest(server.url("localhost", "/page")))
                assertEquals(200, response.statusCode)
                assertEquals(listOf("/robots.txt", "/page"), server.requests.map { it.path })
                assertTrue(server.requests.all { it.tlsProtocol == "TLSv1.3" })
            }
        }
    }

    @Test
    fun `cookies set on the JDK path are observed and replayed`(): Unit = runBounded {
        val cookies = InMemoryCookieSession()
        Tls13Server { exchange ->
            exchange.responseHeaders.add("Set-Cookie", "session=abc; Path=/")
            exchange.respond(200, "ok".toByteArray())
        }.use { server ->
            transport(setOf("localhost"), cookies = cookies).use { transport ->
                transport.execute(CrawlRequest(server.url("localhost", "/first")))
                transport.execute(CrawlRequest(server.url("localhost", "/second")))

                assertTrue(server.requests.first().header("cookie").isEmpty())
                assertEquals(listOf("session=abc"), server.requests.last().header("cookie"))
            }
        }
    }

    @Test
    fun `JDK path sends the same default headers as the CIO path`(): Unit = runBounded {
        LoopbackHttpServer { it.respond(200, "ok".toByteArray()) }.use { server ->
            val requestBody = RequestBody.from("payload".toByteArray())
            val request = CrawlRequest(
                target = server.url("127.0.0.1", "/submit"),
                method = RequestMethod("POST"),
                body = requestBody,
            )
            transport(emptySet()).use { it.execute(request) }
            transport(setOf("127.0.0.1")).use { it.execute(request) }

            val (cio, jdk) = server.requests.toList()
            assertEquals(cio.headers.keys, jdk.headers.keys)
            COMPARED_HEADERS.forEach { name -> assertEquals(cio.header(name), jdk.header(name), name) }
            assertContentEquals("payload".toByteArray(), jdk.body)
            assertEquals(listOf("application/octet-stream"), jdk.header("content-type"))
        }
    }

    @Test
    fun `caller restricted headers are filtered as on the CIO path`(): Unit = runBounded {
        Tls13Server { it.respond(200, "ok".toByteArray()) }.use { server ->
            transport(setOf("localhost")).use { transport ->
                transport.execute(
                    CrawlRequest(
                        target = server.url("localhost", "/page"),
                        headers = Headers.of(
                            Header("Host", "caller-host"),
                            Header("Connection", "close"),
                            Header("Content-Length", "999"),
                            Header("Transfer-Encoding", "chunked"),
                            Header("User-Agent", "caller-agent"),
                        ),
                    ),
                )
                val recorded = server.requests.single()
                assertFalse(recorded.header("host").contains("caller-host"))
                assertEquals(listOf("caller-agent"), recorded.header("user-agent"))
                assertTrue(recorded.header("transfer-encoding").isEmpty())
            }
        }
    }

    @Test
    fun `headers the JDK client forbids fail visibly before any request is sent`(): Unit = runBounded {
        Tls13Server { it.respond(200, "unreachable".toByteArray()) }.use { server ->
            transport(setOf("localhost")).use { transport ->
                assertFailsWith<IllegalArgumentException> {
                    transport.execute(
                        CrawlRequest(
                            target = server.url("localhost", "/page"),
                            headers = Headers.of(Header("Expect", "100-continue")),
                        ),
                    )
                }
                assertTrue(server.requests.isEmpty())
            }
        }
    }

    @Test
    fun `gzip and deflate decode and non success stays inspectable without retry`(): Unit = runBounded {
        Tls13Server { exchange ->
            when (exchange.requestURI.path) {
                "/gzip" -> {
                    exchange.responseHeaders.add("Content-Encoding", "gzip")
                    exchange.respond(200, gzip("gzip-body".toByteArray()))
                }

                else -> exchange.respond(503, "still inspectable".toByteArray())
            }
        }.use { server ->
            transport(setOf("localhost")).use { transport ->
                val decoded = transport.execute(CrawlRequest(server.url("localhost", "/gzip")))
                assertContentEquals("gzip-body".toByteArray(), decoded.body.toByteArray())

                val failure = transport.execute(CrawlRequest(server.url("localhost", "/failure")))
                assertEquals(503, failure.statusCode)
                assertContentEquals("still inspectable".toByteArray(), failure.body.toByteArray())
                assertEquals(listOf("/gzip", "/failure"), server.requests.map { it.path })
            }
        }
    }

    @Test
    fun `a failed handshake is reported without a second connection attempt`(): Unit = runBounded {
        val accepted = AtomicInteger()
        val socket = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
        val acceptThread = Thread {
            try {
                while (!socket.isClosed) socket.accept().use { accepted.incrementAndGet() }
            } catch (_: IOException) {
                // Closing the listener ends the accept loop.
            }
        }.apply { isDaemon = true; start() }
        try {
            transport(setOf("localhost")).use { transport ->
                assertFailsWith<IOException> {
                    transport.execute(CrawlRequest("https://localhost:${socket.localPort}/page"))
                }
            }
        } finally {
            socket.close()
            acceptThread.join(1_000)
        }
        assertEquals(1, accepted.get())
    }

    @Test
    fun `compressed bound applies to advertised and streamed bodies at the exact boundary`(): Unit = runBounded {
        Tls13Server { exchange ->
            when (exchange.requestURI.path) {
                "/exact" -> exchange.respond(200, ByteArray(BODY_LIMIT))
                "/advertised" -> exchange.respond(200, ByteArray(BODY_LIMIT + 1))
                else -> exchange.respondChunked(ByteArray(BODY_LIMIT + 1))
            }
        }.use { server ->
            transport(setOf("localhost"), KtorTransportConfig(maxCompressedBodyBytes = BODY_LIMIT, tls13Hosts = setOf("localhost"))).use { transport ->
                assertEquals(
                    BODY_LIMIT,
                    transport.execute(CrawlRequest(server.url("localhost", "/exact"))).body.toByteArray().size,
                )
                listOf("/advertised", "/streamed").forEach { path ->
                    val failure = assertFailsWith<WebResponseTooLargeException> {
                        transport.execute(CrawlRequest(server.url("localhost", path)))
                    }
                    assertEquals("compressed", failure.limitKind)
                }
            }
        }
    }

    @Test
    fun `decompressed bound applies independently of the compressed bound`(): Unit = runBounded {
        val bomb = gzip(ByteArray(BODY_LIMIT + 1))
        Tls13Server { exchange ->
            exchange.responseHeaders.add("Content-Encoding", "gzip")
            exchange.respond(200, bomb)
        }.use { server ->
            transport(setOf("localhost"), KtorTransportConfig(maxDecompressedBodyBytes = BODY_LIMIT, tls13Hosts = setOf("localhost"))).use { transport ->
                val failure = assertFailsWith<WebResponseTooLargeException> {
                    transport.execute(CrawlRequest(server.url("localhost", "/bomb")))
                }
                assertEquals("decompressed", failure.limitKind)
            }
        }
    }

    @Test
    fun `response header bounds are enforced on the JDK path`(): Unit = runBounded {
        Tls13Server { exchange ->
            exchange.responseHeaders.add("X-Bounded-Name-Long", "value-that-is-long")
            exchange.respond(200, "ok".toByteArray())
        }.use { server ->
            val target = server.url("localhost", "/page")
            transport(setOf("localhost"), KtorTransportConfig(maxResponseHeaders = 1, tls13Hosts = setOf("localhost"))).use { transport ->
                assertFailsWith<WebResponseHeadersTooLargeException> { transport.execute(CrawlRequest(target)) }
            }
            transport(setOf("localhost"), KtorTransportConfig(maxHeaderNameLength = 17, tls13Hosts = setOf("localhost"))).use { transport ->
                assertFailsWith<WebResponseHeaderNameTooLargeException> { transport.execute(CrawlRequest(target)) }
            }
            transport(setOf("localhost"), KtorTransportConfig(maxHeaderValueLength = 15, tls13Hosts = setOf("localhost"))).use { transport ->
                assertFailsWith<WebResponseHeaderValueTooLargeException> { transport.execute(CrawlRequest(target)) }
            }
        }
    }

    @Test
    fun `request timeout bounds a stalled response`(): Unit = runBounded {
        val release = CountDownLatch(1)
        Tls13Server { exchange ->
            release.await(TEST_BUDGET_MILLIS, TimeUnit.MILLISECONDS)
            exchange.respond(200, "late".toByteArray())
        }.use { server ->
            val configuration = KtorTransportConfig(
                requestTimeout = Duration.ofMillis(300),
                tls13Hosts = setOf("localhost"),
            )
            try {
                transport(setOf("localhost"), configuration).use { transport ->
                    assertFailsWith<HttpRequestTimeoutException> {
                        transport.execute(CrawlRequest(server.url("localhost", "/stall")))
                    }
                }
            } finally {
                release.countDown()
            }
        }
    }

    @Test
    fun `socket inactivity bounds a stalled body even with a long request timeout`(): Unit = runBounded {
        val release = CountDownLatch(1)
        Tls13Server { exchange ->
            exchange.sendResponseHeaders(200, 0)
            exchange.responseBody.write("partial".toByteArray())
            exchange.responseBody.flush()
            release.await(TEST_BUDGET_MILLIS, TimeUnit.MILLISECONDS)
            exchange.close()
        }.use { server ->
            val configuration = KtorTransportConfig(
                socketTimeout = Duration.ofMillis(300),
                requestTimeout = Duration.ofMinutes(1),
                tls13Hosts = setOf("localhost"),
            )
            try {
                transport(setOf("localhost"), configuration).use { transport ->
                    assertFailsWith<SocketTimeoutException> {
                        transport.execute(CrawlRequest(server.url("localhost", "/stall")))
                    }
                }
            } finally {
                release.countDown()
            }
        }
    }

    @Test
    fun `total chain timeout is a visible timeout failure on the JDK path`(): Unit = runBounded {
        val release = CountDownLatch(1)
        Tls13Server { exchange ->
            release.await(TEST_BUDGET_MILLIS, TimeUnit.MILLISECONDS)
            exchange.respond(200, "late".toByteArray())
        }.use { server ->
            val configuration = KtorTransportConfig(
                totalRequestTimeout = Duration.ofMillis(300),
                tls13Hosts = setOf("localhost"),
            )
            try {
                transport(setOf("localhost"), configuration).use { transport ->
                    assertFailsWith<HttpRequestTimeoutException> {
                        transport.execute(CrawlRequest(server.url("localhost", "/stall")))
                    }
                }
            } finally {
                release.countDown()
            }
        }
    }

    @Test
    fun `cancellation aborts the exchange and releases connection capacity`(): Unit = runBounded {
        val bodyStarted = CountDownLatch(1)
        val cancelled = CountDownLatch(1)
        val writeFailed = CountDownLatch(1)
        Tls13Server { exchange ->
            if (exchange.requestURI.path != "/stream") {
                exchange.respond(200, "after".toByteArray())
                return@Tls13Server
            }
            exchange.sendResponseHeaders(200, 0)
            exchange.responseBody.write(ByteArray(16))
            exchange.responseBody.flush()
            bodyStarted.countDown()
            cancelled.await(TEST_BUDGET_MILLIS, TimeUnit.MILLISECONDS)
            try {
                repeat(MAX_WRITES_AFTER_CANCEL) {
                    exchange.responseBody.write(ByteArray(16 * 1024))
                    exchange.responseBody.flush()
                }
            } catch (_: IOException) {
                writeFailed.countDown()
            } finally {
                exchange.close()
            }
        }.use { server ->
            val configuration = KtorTransportConfig(maxClientConnections = 1, tls13Hosts = setOf("localhost"))
            transport(setOf("localhost"), configuration).use { transport ->
                val running = async(Dispatchers.IO, start = CoroutineStart.UNDISPATCHED) {
                    transport.execute(CrawlRequest(server.url("localhost", "/stream")))
                }
                assertTrue(bodyStarted.await(TEST_BUDGET_MILLIS, TimeUnit.MILLISECONDS))
                running.cancel()
                assertFailsWith<CancellationException> { running.await() }
                cancelled.countDown()

                assertTrue(writeFailed.await(TEST_BUDGET_MILLIS, TimeUnit.MILLISECONDS))
                val next = transport.execute(CrawlRequest(server.url("localhost", "/after")))
                assertContentEquals("after".toByteArray(), next.body.toByteArray())
            }
        }
    }

    @Test
    fun `connection bounds serialize exchanges on the JDK path`(): Unit = runBounded {
        val active = AtomicInteger()
        val peak = AtomicInteger()
        val handlers = CountDownLatch(4)
        Tls13Server { exchange ->
            val current = active.incrementAndGet()
            peak.accumulateAndGet(current, ::maxOf)
            handlers.countDown()
            exchange.respond(200, "ok".toByteArray())
            active.decrementAndGet()
        }.use { server ->
            val configuration = KtorTransportConfig(maxConnectionsPerOrigin = 1, tls13Hosts = setOf("localhost"))
            transport(setOf("localhost"), configuration).use { transport ->
                (0 until 4).map { index ->
                    async(Dispatchers.IO) {
                        transport.execute(CrawlRequest(server.url("localhost", "/page-$index")))
                    }
                }.awaitAll()
                assertTrue(handlers.await(TEST_BUDGET_MILLIS, TimeUnit.MILLISECONDS))
                assertEquals(1, peak.get())
            }
        }
    }

    @Test
    fun `JDK client ignores the JVM default proxy selector`(): Unit = runBounded {
        val previous = ProxySelector.getDefault()
        val consulted = AtomicBoolean(false)
        ProxySelector.setDefault(
            object : ProxySelector() {
                override fun select(uri: URI?): List<Proxy> {
                    consulted.set(true)
                    return listOf(Proxy.NO_PROXY)
                }

                override fun connectFailed(uri: URI?, sa: SocketAddress?, ioe: IOException?) = Unit
            },
        )
        try {
            Tls13Server { it.respond(200, "direct".toByteArray()) }.use { server ->
                transport(setOf("localhost")).use { transport ->
                    assertEquals(200, transport.execute(CrawlRequest(server.url("localhost", "/page"))).statusCode)
                }
            }
            assertFalse(consulted.get())
        } finally {
            ProxySelector.setDefault(previous)
        }
    }

    @Test
    fun `closing the transport aborts in flight exchanges and stops every JDK client thread`(): Unit = runBounded {
        val handlerEntered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val threadsBefore = jdkClientThreads()
        Tls13Server { exchange ->
            handlerEntered.countDown()
            release.await(TEST_BUDGET_MILLIS, TimeUnit.MILLISECONDS)
            exchange.respond(200, "late".toByteArray())
        }.use { server ->
            val transport = transport(setOf("localhost"))
            try {
                val running = async(Dispatchers.IO, start = CoroutineStart.UNDISPATCHED) {
                    runCatching { transport.execute(CrawlRequest(server.url("localhost", "/stall"))) }
                }
                assertTrue(handlerEntered.await(TEST_BUDGET_MILLIS, TimeUnit.MILLISECONDS))
                val started = jdkClientThreads() - threadsBefore
                assertTrue(started.isNotEmpty())

                transport.close()
                transport.close()

                assertIs<IOException>(running.await().exceptionOrNull())
                started.forEach { thread ->
                    thread.join(TEST_BUDGET_MILLIS)
                    assertFalse(thread.isAlive, "${thread.name} survived close")
                }
                assertFailsWith<IllegalStateException> {
                    transport.execute(CrawlRequest(server.url("localhost", "/after-close")))
                }
            } finally {
                release.countDown()
                transport.close()
            }
        }
    }

    @Test
    fun `an empty host set creates no JDK client`(): Unit = runBounded {
        val threadsBefore = jdkClientThreads()
        LoopbackHttpServer { it.respond(200, "cio".toByteArray()) }.use { server ->
            transport(emptySet()).use { transport ->
                assertEquals(200, transport.execute(CrawlRequest(server.url("127.0.0.1", "/page"))).statusCode)
                assertTrue((jdkClientThreads() - threadsBefore).isEmpty())
            }
        }
    }

    @Test
    fun `fromClient routes configured hosts to the JDK path and leaves the caller client open`(): Unit = runBounded {
        val mockCalls = CopyOnWriteArrayList<String>()
        val callerClient = HttpClient(
            MockEngine { request ->
                mockCalls += request.url.host
                respond("mock", HttpStatusCode.OK)
            },
        )
        Tls13Server { it.respond(200, "jdk".toByteArray()) }.use { server ->
            val transport = KtorTransport.fromClient(
                client = callerClient,
                configuration = KtorTransportConfig(tls13Hosts = setOf("localhost")),
                policies = policies(),
            )
            transport.use {
                val tls = it.execute(CrawlRequest(server.url("localhost", "/page")))
                val mock = it.execute(CrawlRequest("https://other.test/page"))
                assertContentEquals("jdk".toByteArray(), tls.body.toByteArray())
                assertContentEquals("mock".toByteArray(), mock.body.toByteArray())
            }
            assertEquals(listOf("other.test"), mockCalls)
            assertTrue(callerClient.engine.coroutineContext[kotlinx.coroutines.Job]?.isActive == true)
        }
        callerClient.close()
    }

    @Test
    fun `configured hosts and proxies fail before any client exists unless JDK retries are disabled`() {
        val previous = System.clearProperty(JDK_RETRY_LIMIT_PROPERTY)
        val threadsBefore = jdkClientThreads()
        try {
            listOf(
                KtorTransportConfig(tls13Hosts = setOf("localhost")),
                KtorTransportConfig(proxy = WebProxy("http://127.0.0.1:8888")),
            ).forEach { configuration ->
                val failure = assertFailsWith<IllegalStateException> { KtorTransport.create(configuration, policies()) }
                assertTrue(failure.message.orEmpty().contains("-D$JDK_RETRY_LIMIT_PROPERTY=1"))
            }
            assertTrue((jdkClientThreads() - threadsBefore).isEmpty())
            KtorTransport.create(KtorTransportConfig(), policies()).close()
        } finally {
            if (previous != null) System.setProperty(JDK_RETRY_LIMIT_PROPERTY, previous)
        }
    }

    @Test
    fun `runtimes without a closable JDK client are refused`() {
        val closeable = AutoCloseable { }
        assertEquals(closeable, requireShutdownCapability(closeable))
        assertFailsWith<IllegalStateException> { requireShutdownCapability(Any()) }
    }

    @Test
    fun `publicWeb owns and closes the JDK client when hosts or a proxy are configured`(): Unit = runBounded {
        listOf(
            KtorTransportConfig(tls13Hosts = setOf("example.test")),
            KtorTransportConfig(proxy = WebProxy("http://127.0.0.1:8888")),
        ).forEach { configuration ->
            val threadsBefore = jdkClientThreads()
            val transport = KtorTransport.publicWeb(configuration)
            val started = jdkClientThreads() - threadsBefore
            transport.close()
            started.forEach { thread ->
                thread.join(TEST_BUDGET_MILLIS)
                assertFalse(thread.isAlive, "${thread.name} survived close")
            }
        }
    }

    /**
     * The refusal is a `ProtocolVersion` alert or, when the local JSSE server
     * closes without an alert, an EOF during the handshake read.
     */
    private suspend fun assertCioHandshakeRefused(block: suspend () -> Unit) {
        val failure = runCatching { block() }.exceptionOrNull()
        val alert = failure is TlsException && failure.message.orEmpty().contains("ProtocolVersion")
        assertTrue(alert || failure is EOFException, "unexpected CIO outcome: $failure")
    }

    private fun jdkClientThreads(): Set<Thread> = Thread.getAllStackTraces().keys
        .filterTo(LinkedHashSet()) { it.name.startsWith("trawler-tls13-") || it.name.contains("SelectorManager") }

    private fun runBounded(block: suspend kotlinx.coroutines.CoroutineScope.() -> Unit): Unit = runBlocking {
        withTimeout(TEST_BUDGET_MILLIS, block)
    }

    private fun transport(
        hosts: Set<String>,
        configuration: KtorTransportConfig = KtorTransportConfig(tls13Hosts = hosts),
        destination: WebDestinationPolicy = AllowAllWebDestinationPolicy,
        concurrency: OriginConcurrencyPolicy = NoOriginConcurrencyPolicy,
        robots: RobotsPolicy = NoRobotsPolicy,
        cookies: CookieSession = NoCookieSession,
    ): KtorTransport = KtorTransport.create(
        configuration = configuration,
        policies = policies(destination, concurrency, robots, cookies),
    )

    private fun policies(
        destination: WebDestinationPolicy = AllowAllWebDestinationPolicy,
        concurrency: OriginConcurrencyPolicy = NoOriginConcurrencyPolicy,
        robots: RobotsPolicy = NoRobotsPolicy,
        cookies: CookieSession = NoCookieSession,
    ): WebTransportPolicies = WebTransportPolicies(
        destination = destination,
        robots = robots,
        concurrency = concurrency,
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

    private fun HttpExchange.respond(status: Int, body: ByteArray) {
        sendResponseHeaders(status, if (body.isEmpty()) -1 else body.size.toLong())
        if (body.isNotEmpty()) responseBody.write(body)
        close()
    }

    private fun HttpExchange.respondChunked(body: ByteArray) {
        sendResponseHeaders(200, 0)
        responseBody.write(body)
        close()
    }

    private class RecordingDestination : WebDestinationPolicy {
        override val id: String = "recording-destination-tls13"
        val targets = CopyOnWriteArrayList<String>()

        override suspend fun evaluate(request: CrawlRequest, history: RedirectHistory): DestinationDecision {
            targets += request.target
            return DestinationDecision.Allowed
        }
    }

    private class RecordingConcurrency : OriginConcurrencyPolicy {
        override val id: String = "recording-concurrency-tls13"
        val hosts = CopyOnWriteArrayList<String>()
        val released = AtomicInteger()

        override suspend fun acquire(origin: WebOrigin, kind: WebRequestKind): OriginConcurrencyPermit {
            hosts += origin.host
            return OriginConcurrencyPermit { released.incrementAndGet() }
        }
    }

    private class RecordedExchange(
        val path: String,
        val headers: Map<String, List<String>>,
        val body: ByteArray,
        val httpProtocol: String,
        val tlsProtocol: String?,
    ) {
        fun header(name: String): List<String> = headers[name.lowercase()].orEmpty()
    }

    private abstract class RecordingServer(
        private val handler: (HttpExchange) -> Unit,
    ) : AutoCloseable {
        val requests = CopyOnWriteArrayList<RecordedExchange>()
        private val executor = Executors.newFixedThreadPool(8)

        protected abstract val server: HttpServer
        protected abstract val scheme: String

        protected fun configure(target: HttpServer) {
            target.executor = executor
            target.createContext("/") { exchange ->
                try {
                    val body = exchange.requestBody.readAllBytes()
                    requests += RecordedExchange(
                        path = exchange.requestURI.path,
                        headers = exchange.requestHeaders.entries.associate { (name, values) ->
                            name.lowercase() to values.toList()
                        },
                        body = body,
                        httpProtocol = exchange.protocol,
                        tlsProtocol = (exchange as? HttpsExchange)?.sslSession?.protocol,
                    )
                    handler(exchange)
                } catch (_: IOException) {
                    // A client abort is the condition several tests provoke.
                } finally {
                    exchange.close()
                }
            }
            target.start()
        }

        fun url(host: String, path: String): String = "$scheme://$host:${server.address.port}$path"

        override fun close() {
            server.stop(0)
            executor.shutdownNow()
        }
    }

    private inner class Tls13Server(handler: (HttpExchange) -> Unit) : RecordingServer(handler) {
        override val scheme: String = "https"
        override val server: HttpsServer =
            HttpsServer.create(InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0).apply {
                httpsConfigurator = object : HttpsConfigurator(serverContext) {
                    override fun configure(parameters: HttpsParameters) {
                        val ssl = serverContext.defaultSSLParameters
                        ssl.protocols = arrayOf("TLSv1.3")
                        parameters.setSSLParameters(ssl)
                    }
                }
            }

        init {
            configure(server)
        }
    }

    private class LoopbackHttpServer(handler: (HttpExchange) -> Unit) : RecordingServer(handler) {
        override val scheme: String = "http"
        override val server: HttpServer =
            HttpServer.create(InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0)

        init {
            configure(server)
        }
    }

    private companion object {
        const val STORE_PASSWORD = "throwaway-test-only"
        const val BODY_LIMIT = 1_024
        const val TEST_BUDGET_MILLIS = 20_000L
        const val MAX_WRITES_AFTER_CANCEL = 4_096
        val COMPARED_HEADERS = listOf("accept", "accept-encoding", "user-agent", "content-type")
    }
}
