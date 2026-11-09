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
import com.rmjtromp.trawler.web.policy.WebProxyFailureException
import com.rmjtromp.trawler.web.session.NoCookieSession
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import com.sun.net.httpserver.HttpsConfigurator
import com.sun.net.httpserver.HttpsParameters
import com.sun.net.httpserver.HttpsServer
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Proxy
import java.net.ProxySelector
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketAddress
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import io.ktor.client.plugins.HttpRequestTimeoutException
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
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
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

/**
 * Loopback tests for [KtorTransportConfig.proxy] on both clients. A fixture
 * forward proxy records every CONNECT and absolute-form request and sits in
 * front of loopback origins, whose accepted peers prove nothing bypassed it.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KtorTransportProxyIT {
    /** Selects a client: CIO offers TLS 1.2 at most, so each origin matches its client. */
    enum class ClientPath(val tlsProtocol: String, val tls13Hosts: Set<String>) {
        CIO("TLSv1.2", emptySet()),
        JDK("TLSv1.3", setOf("localhost")),
    }

    private lateinit var keyDirectory: Path
    private lateinit var serverContext: SSLContext
    private lateinit var previousDefaultContext: SSLContext
    private var previousTrustStore: String? = null
    private var previousTrustStoreType: String? = null
    private var previousTrustStorePassword: String? = null

    @BeforeAll
    fun createCertificateAndTrustIt() {
        keyDirectory = Files.createTempDirectory("trawler-proxy-it")
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
        // CIO builds its trust manager from the JVM trust store settings.
        previousTrustStore = System.getProperty("javax.net.ssl.trustStore")
        previousTrustStoreType = System.getProperty("javax.net.ssl.trustStoreType")
        previousTrustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword")
        System.setProperty("javax.net.ssl.trustStore", keyStorePath.toString())
        System.setProperty("javax.net.ssl.trustStoreType", "PKCS12")
        System.setProperty("javax.net.ssl.trustStorePassword", STORE_PASSWORD)
        // Loads the new trust store now, before tests connect concurrently.
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()).init(null as KeyStore?)
    }

    @AfterAll
    fun restoreTrustAndDeleteCertificate() {
        SSLContext.setDefault(previousDefaultContext)
        restoreProperty("javax.net.ssl.trustStore", previousTrustStore)
        restoreProperty("javax.net.ssl.trustStoreType", previousTrustStoreType)
        restoreProperty("javax.net.ssl.trustStorePassword", previousTrustStorePassword)
        keyDirectory.toFile().deleteRecursively()
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `HTTPS is tunneled with CONNECT and the origin only sees the proxy`(path: ClientPath): Unit = runBounded {
        HttpsOrigin(path) { it.respond(200, "tunneled") }.use { origin ->
            FixtureProxy().use { proxy ->
                transport(path, proxy).use { transport ->
                    val response = transport.execute(CrawlRequest(origin.url("/page")))

                    assertEquals(200, response.statusCode)
                    assertEquals("tunneled", response.body.toByteArray().decodeToString())
                    assertEquals(listOf(origin.authority), proxy.connectTargets)
                    assertTrue(proxy.absoluteTargets.isEmpty())
                    assertEquals(path.tlsProtocol, origin.tlsProtocols.single())
                    assertOnlyProxyReachedOrigin(proxy, origin)
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `request body and content type cross the tunnel unchanged`(path: ClientPath): Unit = runBounded {
        HttpsOrigin(path) { it.respond(200, "ok") }.use { origin ->
            FixtureProxy().use { proxy ->
                transport(path, proxy).use { transport ->
                    val response = transport.execute(
                        CrawlRequest(
                            target = origin.url("/submit"),
                            method = RequestMethod("POST"),
                            headers = Headers.of(Header("Content-Type", "application/xml")),
                            body = RequestBody.from("<a/>".toByteArray()),
                        ),
                    )

                    assertEquals(200, response.statusCode)
                    val recorded = origin.requests.single()
                    assertEquals("POST", recorded.method)
                    assertEquals(listOf("application/xml"), recorded.headers["content-type"])
                    assertEquals(listOf("4"), recorded.headers["content-length"])
                    assertContentEquals("<a/>".toByteArray(), recorded.body)
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `plain HTTP targets go through the proxy in absolute form`(path: ClientPath): Unit = runBounded {
        PlainOrigin { it.respond(200, "plain") }.use { origin ->
            FixtureProxy().use { proxy ->
                transport(path, proxy).use { transport ->
                    val response = transport.execute(CrawlRequest(origin.url("/page?q=1")))

                    assertEquals("plain", response.body.toByteArray().decodeToString())
                    assertEquals(listOf(origin.url("/page?q=1")), proxy.absoluteTargets)
                    assertTrue(proxy.connectTargets.isEmpty())
                    assertOnlyProxyReachedOrigin(proxy, origin)
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `cross origin redirect and robots fetches all go through the proxy`(path: ClientPath): Unit = runBounded {
        HttpsOrigin(path) { exchange ->
            if (exchange.requestURI.path == "/robots.txt") exchange.respond(404, "") else exchange.respond(200, "final")
        }.use { destination ->
            HttpsOrigin(path) { exchange ->
                if (exchange.requestURI.path == "/robots.txt") {
                    exchange.respond(404, "")
                } else {
                    exchange.responseHeaders.add("Location", destination.url("/final"))
                    exchange.respond(302, "")
                }
            }.use { source ->
                FixtureProxy().use { proxy ->
                    val destinations = RecordingDestination()
                    transport(
                        path,
                        proxy,
                        destination = destinations,
                        robots = CachingRobotsPolicy(),
                    ).use { transport ->
                        val response = transport.execute(CrawlRequest(source.url("/start")))

                        assertEquals("final", response.body.toByteArray().decodeToString())
                        assertEquals("1", response.attributes["trawler.web.redirect-count"])
                        assertEquals(listOf("/robots.txt", "/start"), source.requests.map { it.path })
                        assertEquals(listOf("/robots.txt", "/final"), destination.requests.map { it.path })
                        assertEquals(
                            setOf(source.authority, destination.authority),
                            proxy.connectTargets.toSet(),
                        )
                        assertOnlyProxyReachedOrigin(proxy, source)
                        assertOnlyProxyReachedOrigin(proxy, destination)
                        assertEquals(listOf(source.url("/start"), destination.url("/final")), destinations.targets)
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `plain HTTP redirect hop goes through the proxy`(path: ClientPath): Unit = runBounded {
        PlainOrigin { it.respond(200, "final") }.use { destination ->
            PlainOrigin { exchange ->
                exchange.responseHeaders.add("Location", destination.url("/final"))
                exchange.respond(302, "")
            }.use { source ->
                FixtureProxy().use { proxy ->
                    transport(path, proxy).use { transport ->
                        val response = transport.execute(CrawlRequest(source.url("/start")))

                        assertEquals("final", response.body.toByteArray().decodeToString())
                        assertEquals(listOf(source.url("/start"), destination.url("/final")), proxy.absoluteTargets)
                        assertOnlyProxyReachedOrigin(proxy, source)
                        assertOnlyProxyReachedOrigin(proxy, destination)
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `refused proxy fails typed and never reaches the origin`(path: ClientPath): Unit = runBounded {
        HttpsOrigin(path) { it.respond(200, "must-not-be-reached") }.use { origin ->
            PlainOrigin { it.respond(200, "must-not-be-reached") }.use { plain ->
                val port = unusedPort()
                transport(path, WebProxy("http://127.0.0.1:$port")).use { transport ->
                    val secure = assertFailsWith<WebProxyFailureException> {
                        transport.execute(CrawlRequest(origin.url("/page")))
                    }
                    assertNull(secure.connectStatus)
                    assertNotNull(secure.cause)
                    assertFailsWith<WebProxyFailureException> {
                        transport.execute(CrawlRequest(plain.url("/page")))
                    }
                    assertEquals(0, origin.connections.get())
                    assertEquals(0, plain.connections.get())
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `CONNECT rejection is a typed failure and never a response`(path: ClientPath): Unit = runBounded {
        for (status in listOf(403, 407, 502)) {
            HttpsOrigin(path) { it.respond(200, "must-not-be-reached") }.use { origin ->
                FixtureProxy(ProxyBehavior.Reject(status)).use { proxy ->
                    transport(path, proxy).use { transport ->
                        val failure = assertFailsWith<WebProxyFailureException>("status $status") {
                            transport.execute(CrawlRequest(origin.url("/page")))
                        }

                        val expected = if (path == ClientPath.JDK) status else null
                        assertEquals(expected, failure.connectStatus, "status $status")
                        assertEquals(listOf(origin.authority), proxy.connectTargets, "one attempt only")
                        assertEquals(0, origin.connections.get())
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `proxy answering 407 to a plain request is a typed failure`(path: ClientPath): Unit = runBounded {
        PlainOrigin { it.respond(200, "must-not-be-reached") }.use { origin ->
            FixtureProxy(ProxyBehavior.Reject(407)).use { proxy ->
                transport(path, proxy).use { transport ->
                    val failure = assertFailsWith<WebProxyFailureException> {
                        transport.execute(CrawlRequest(origin.url("/page")))
                    }

                    assertEquals(407, failure.connectStatus)
                    assertEquals(0, origin.connections.get())
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `proxy closing before it answers CONNECT is a typed failure`(path: ClientPath): Unit = runBounded {
        HttpsOrigin(path) { it.respond(200, "must-not-be-reached") }.use { origin ->
            FixtureProxy(ProxyBehavior.CloseOnConnect).use { proxy ->
                transport(path, proxy).use { transport ->
                    val failure = assertFailsWith<WebProxyFailureException> {
                        transport.execute(CrawlRequest(origin.url("/page")))
                    }

                    assertNull(failure.connectStatus)
                    assertEquals(0, origin.connections.get())
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `silent proxy is bounded by the configured timeouts`(path: ClientPath): Unit = runBounded {
        HttpsOrigin(path) { it.respond(200, "must-not-be-reached") }.use { origin ->
            FixtureProxy(ProxyBehavior.Silent).use { proxy ->
                val configuration = configuration(
                    path,
                    WebProxy(proxy.url),
                    connect = Duration.ofMillis(300),
                    socket = Duration.ofMillis(300),
                    request = Duration.ofSeconds(2),
                )
                transportWith(configuration).use { transport ->
                    val startedAt = System.nanoTime()
                    val failure = assertFailsWith<WebProxyFailureException> {
                        transport.execute(CrawlRequest(origin.url("/page")))
                    }
                    val elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000

                    assertNull(failure.connectStatus)
                    assertTrue(elapsedMillis < 3_000, "took $elapsedMillis ms")
                    assertEquals(0, origin.connections.get())
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `site stalling behind a working tunnel is not a proxy failure`(path: ClientPath): Unit = runBounded {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        HttpsOrigin(path) { exchange ->
            entered.countDown()
            release.await(8, TimeUnit.SECONDS)
            exchange.respond(200, "late")
        }.use { origin ->
            FixtureProxy().use { proxy ->
                val configuration = configuration(
                    path,
                    WebProxy(proxy.url),
                    connect = Duration.ofSeconds(3),
                    socket = Duration.ofMillis(1_500),
                    request = Duration.ofSeconds(3),
                )
                transportWith(configuration).use { transport ->
                    val failure = runCatching { transport.execute(CrawlRequest(origin.url("/page"))) }
                        .exceptionOrNull()
                    val reachedSite = entered.await(0, TimeUnit.SECONDS)
                    release.countDown()

                    assertTrue(reachedSite, "the site never received the request: $failure")
                    assertNotNull(failure)
                    assertFalse(failure is WebProxyFailureException, "misattributed: $failure")
                    assertEquals(listOf(origin.authority), proxy.connectTargets)
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `TLS stall behind a working tunnel counts as a proxy failure`(path: ClientPath): Unit = runBounded {
        FixtureProxy().use { proxy ->
            SilentOrigin().use { origin ->
                val configuration = configuration(
                    path,
                    WebProxy(proxy.url),
                    connect = Duration.ofMillis(300),
                    socket = Duration.ofMillis(300),
                    request = Duration.ofSeconds(2),
                )
                transportWith(configuration).use { transport ->
                    assertFailsWith<WebProxyFailureException> {
                        transport.execute(CrawlRequest("https://localhost:${origin.port}/page"))
                    }
                    assertEquals(listOf("localhost:${origin.port}"), proxy.connectTargets)
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `any spelling of the CONNECT method is refused before a connection exists`(path: ClientPath): Unit = runBounded {
        HttpsOrigin(path) { it.respond(200, "must-not-be-reached") }.use { origin ->
            FixtureProxy().use { proxy ->
                transport(path, proxy).use { transport ->
                    for (spelling in listOf("connect", "Connect", "CONNECT")) {
                        assertFailsWith<IllegalArgumentException>(spelling) {
                            transport.execute(CrawlRequest(origin.url("/page"), method = RequestMethod(spelling)))
                        }
                    }
                    assertEquals(0, origin.connections.get())
                    assertEquals(0, proxy.accepted.get())
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `user info in a plain target is refused before reaching the proxy`(path: ClientPath): Unit = runBounded {
        PlainOrigin { it.respond(200, "must-not-be-reached") }.use { origin ->
            FixtureProxy().use { proxy ->
                transport(path, proxy).use { transport ->
                    val target = origin.url("/page").replace("http://", "http://user:secret@")
                    assertFailsWith<IllegalArgumentException> { transport.execute(CrawlRequest(target)) }
                    assertEquals(0, proxy.accepted.get())
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `fragments are not sent to the proxy`(path: ClientPath): Unit = runBounded {
        PlainOrigin { it.respond(200, "plain") }.use { origin ->
            FixtureProxy().use { proxy ->
                transport(path, proxy).use { transport ->
                    val response = transport.execute(CrawlRequest(origin.url("/page#section")))

                    assertEquals(200, response.statusCode)
                    assertEquals(listOf(origin.url("/page")), proxy.absoluteTargets)
                    assertEquals("/page", origin.requests.single().path)
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `an origin answering 407 inside a tunnel stays an ordinary response`(path: ClientPath): Unit = runBounded {
        HttpsOrigin(path) { it.respond(407, "site says so") }.use { origin ->
            FixtureProxy().use { proxy ->
                transport(path, proxy).use { transport ->
                    val response = transport.execute(CrawlRequest(origin.url("/page")))

                    assertEquals(407, response.statusCode)
                    assertEquals("site says so", response.body.toByteArray().decodeToString())
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `an oversized plain 407 from the proxy is still a proxy failure`(path: ClientPath): Unit = runBounded {
        PlainOrigin { it.respond(200, "must-not-be-reached") }.use { origin ->
            FixtureProxy(ProxyBehavior.Reject(407, "x".repeat(4_096))).use { proxy ->
                val configuration = configuration(path, WebProxy(proxy.url)).copy(maxCompressedBodyBytes = 64)
                transportWith(configuration).use { transport ->
                    val failure = assertFailsWith<WebProxyFailureException> {
                        transport.execute(CrawlRequest(origin.url("/page")))
                    }
                    assertEquals(407, failure.connectStatus)
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `proxy resetting the connection during CONNECT is a typed failure`(path: ClientPath): Unit = runBounded {
        HttpsOrigin(path) { it.respond(200, "must-not-be-reached") }.use { origin ->
            FixtureProxy(ProxyBehavior.ResetOnConnect).use { proxy ->
                transport(path, proxy).use { transport ->
                    assertFailsWith<WebProxyFailureException> {
                        transport.execute(CrawlRequest(origin.url("/page")))
                    }
                    assertEquals(0, origin.connections.get())
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `proxy answering CONNECT with garbage is a typed failure`(path: ClientPath): Unit = runBounded {
        HttpsOrigin(path) { it.respond(200, "must-not-be-reached") }.use { origin ->
            FixtureProxy(ProxyBehavior.Garbage).use { proxy ->
                transport(path, proxy).use { transport ->
                    assertFailsWith<WebProxyFailureException> {
                        transport.execute(CrawlRequest(origin.url("/page")))
                    }
                    assertEquals(0, origin.connections.get())
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `proxy cutting off its CONNECT answer is a typed failure`(path: ClientPath): Unit = runBounded {
        for (partial in listOf("HTTP/1.", "HTTP/1.1 200 OK\r\nX-Cut: of")) {
            HttpsOrigin(path) { it.respond(200, "must-not-be-reached") }.use { origin ->
                FixtureProxy(ProxyBehavior.Raw(partial)).use { proxy ->
                    transport(path, proxy).use { transport ->
                        assertFailsWith<WebProxyFailureException>(partial) {
                            transport.execute(CrawlRequest(origin.url("/page")))
                        }
                        assertEquals(0, origin.connections.get())
                    }
                }
            }
        }
    }

    @Test
    fun `CIO reports an invalid CONNECT content length as a proxy failure`(): Unit = runBounded {
        HttpsOrigin(ClientPath.CIO) { it.respond(200, "must-not-be-reached") }.use { origin ->
            FixtureProxy(ProxyBehavior.Raw("HTTP/1.1 200 OK\r\nContent-Length: invalid\r\n\r\n")).use { proxy ->
                transport(ClientPath.CIO, proxy).use { transport ->
                    assertFailsWith<WebProxyFailureException> {
                        transport.execute(CrawlRequest(origin.url("/page")))
                    }
                    assertEquals(0, origin.connections.get())
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `total chain timeout during a silent proxy stays attributed to the proxy`(path: ClientPath): Unit = runBounded {
        HttpsOrigin(path) { it.respond(200, "must-not-be-reached") }.use { origin ->
            FixtureProxy(ProxyBehavior.Silent).use { proxy ->
                val configuration = configuration(path, WebProxy(proxy.url)).copy(
                    totalRequestTimeout = Duration.ofMillis(300),
                )
                transportWith(configuration).use { transport ->
                    val failure = assertFailsWith<WebProxyFailureException> {
                        transport.execute(CrawlRequest(origin.url("/page")))
                    }
                    assertIs<HttpRequestTimeoutException>(failure.cause)
                    assertEquals(0, origin.connections.get())
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `TLS failure behind a working tunnel is not a proxy failure`(path: ClientPath): Unit = runBounded {
        FixtureProxy().use { proxy ->
            ClosingOrigin().use { origin ->
                transport(path, proxy).use { transport ->
                    val failure = runCatching {
                        transport.execute(CrawlRequest("https://localhost:${origin.port}/page"))
                    }.exceptionOrNull()

                    assertNotNull(failure)
                    assertFalse(failure is WebProxyFailureException, "misattributed: $failure")
                    // CIO may reconnect by itself when TLS setup is cut off, always through the proxy.
                    assertEquals(setOf("localhost:${origin.port}"), proxy.connectTargets.toSet())
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `proxy failure on robots surfaces typed and is not cached`(path: ClientPath): Unit = runBounded {
        HttpsOrigin(path) { exchange ->
            if (exchange.requestURI.path == "/robots.txt") exchange.respond(404, "") else exchange.respond(200, "ok")
        }.use { origin ->
            FixtureProxy(ProxyBehavior.Reject(403)).use { proxy ->
                transport(path, proxy, robots = CachingRobotsPolicy()).use { transport ->
                    assertFailsWith<WebProxyFailureException> {
                        transport.execute(CrawlRequest(origin.url("/page")))
                    }
                    assertEquals(0, origin.connections.get())

                    proxy.behavior = ProxyBehavior.Forward
                    val response = transport.execute(CrawlRequest(origin.url("/page")))

                    assertEquals(200, response.statusCode)
                    assertEquals(listOf("/robots.txt", "/page"), origin.requests.map { it.path })
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `cancellation during the tunnel closes the proxy connection`(path: ClientPath): Unit = runBounded {
        HttpsOrigin(path) { it.respond(200, "must-not-be-reached") }.use { origin ->
            FixtureProxy(ProxyBehavior.Silent).use { proxy ->
                transport(path, proxy).use { transport ->
                    val call = async(Dispatchers.Default, start = CoroutineStart.UNDISPATCHED) {
                        transport.execute(CrawlRequest(origin.url("/page")))
                    }
                    assertTrue(proxy.awaitConnectSeen(), "CONNECT never arrived")

                    call.cancel()
                    assertFailsWith<CancellationException> { call.await() }

                    assertTrue(proxy.awaitClientClosed(), "proxy connection stayed open")
                    assertEquals(0, origin.connections.get())
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `a configured proxy never consults the JVM proxy selector`(path: ClientPath): Unit = runBounded {
        val previous = ProxySelector.getDefault()
        val selector = RecordingProxySelector()
        ProxySelector.setDefault(selector)
        try {
            HttpsOrigin(path) { it.respond(200, "ok") }.use { origin ->
                FixtureProxy().use { proxy ->
                    transport(path, proxy).use { transport ->
                        assertEquals(200, transport.execute(CrawlRequest(origin.url("/page"))).statusCode)
                        assertEquals(1, proxy.connectTargets.size)
                    }
                }
            }
            assertEquals(0, selector.selections.get())
        } finally {
            ProxySelector.setDefault(previous)
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `null proxy connects directly and makes no proxy connection`(path: ClientPath): Unit = runBounded {
        HttpsOrigin(path) { it.respond(200, "direct") }.use { origin ->
            FixtureProxy().use { proxy ->
                transportWith(KtorTransportConfig(tls13Hosts = path.tls13Hosts)).use { transport ->
                    val response = transport.execute(CrawlRequest(origin.url("/page")))

                    assertEquals("direct", response.body.toByteArray().decodeToString())
                }
                assertEquals(0, proxy.accepted.get())
                assertTrue(proxy.connectTargets.isEmpty())
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `per origin limit holds per origin and connections never mix origins`(path: ClientPath): Unit = runBounded {
        val bothOrigins = CyclicBarrier(2)
        val active = ConcurrentHashMap<String, AtomicInteger>()
        val highest = ConcurrentHashMap<String, AtomicInteger>()
        val passed = AtomicInteger()
        val handler: (HttpExchange, String) -> Unit = { exchange, name ->
            val current = active.computeIfAbsent(name) { AtomicInteger() }.incrementAndGet()
            highest.computeIfAbsent(name) { AtomicInteger() }.accumulateAndGet(current, ::maxOf)
            if (exchange.requestURI.path == "/first") {
                bothOrigins.await(8, TimeUnit.SECONDS)
                passed.incrementAndGet()
            }
            active.getValue(name).decrementAndGet()
            exchange.respond(200, name)
        }
        HttpsOrigin(path) { handler(it, "a") }.use { first ->
            HttpsOrigin(path) { handler(it, "b") }.use { second ->
                FixtureProxy().use { proxy ->
                    val configuration = configuration(path, WebProxy(proxy.url)).copy(
                        maxConnectionsPerOrigin = 1,
                        requestTimeout = Duration.ofSeconds(10),
                    )
                    transportWith(configuration).use { transport ->
                        val requests = listOf(first to "a", first to "a", second to "b", second to "b")
                        val paths = mutableMapOf<HttpsOrigin, Int>()
                        val responses = requests.map { (origin, name) ->
                            val count = paths.merge(origin, 1, Int::plus)
                            val page = if (count == 1) "/first" else "/second"
                            async(Dispatchers.Default) {
                                name to transport.execute(CrawlRequest(origin.url(page)))
                            }
                        }.awaitAll()

                        responses.forEach { (name, response) ->
                            assertEquals(name, response.body.toByteArray().decodeToString())
                        }
                        assertEquals(2, passed.get(), "origins were serialized behind the proxy")
                        assertEquals(1, highest.getValue("a").get())
                        assertEquals(1, highest.getValue("b").get())
                        assertEquals(setOf(first.authority, second.authority), proxy.connectTargets.toSet())
                        assertOnlyTunnelsToThisOrigin(proxy, first)
                        assertOnlyTunnelsToThisOrigin(proxy, second)
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ClientPath::class)
    fun `total connection limit holds across origins through the proxy`(path: ClientPath): Unit = runBounded {
        val release = CountDownLatch(1)
        val firstReached = CountDownLatch(1)
        val secondReached = CountDownLatch(1)
        HttpsOrigin(path) { exchange ->
            firstReached.countDown()
            release.await(8, TimeUnit.SECONDS)
            exchange.respond(200, "ok")
        }.use { first ->
            HttpsOrigin(path) { exchange ->
                secondReached.countDown()
                exchange.respond(200, "ok")
            }.use { second ->
                FixtureProxy().use { proxy ->
                    val configuration = configuration(path, WebProxy(proxy.url)).copy(
                        maxClientConnections = 1,
                        requestTimeout = Duration.ofSeconds(10),
                    )
                    transportWith(configuration).use { transport ->
                        val holder = async(Dispatchers.Default) {
                            transport.execute(CrawlRequest(first.url("/page")))
                        }
                        assertTrue(firstReached.await(5, TimeUnit.SECONDS), "first request never arrived")
                        val waiter = async(Dispatchers.Default) {
                            transport.execute(CrawlRequest(second.url("/page")))
                        }

                        assertFalse(secondReached.await(300, TimeUnit.MILLISECONDS), "limit was not held")
                        assertEquals(1, proxy.accepted.get())
                        release.countDown()
                        assertEquals(200, holder.await().statusCode)
                        assertEquals(200, waiter.await().statusCode)
                        assertEquals(2, proxy.accepted.get())
                    }
                }
            }
        }
    }

    private fun assertOnlyProxyReachedOrigin(proxy: FixtureProxy, origin: RecordingOrigin) {
        assertTrue(origin.peerPorts.isNotEmpty(), "origin saw no request")
        assertTrue(proxy.outboundPorts.keys.containsAll(origin.peerPorts), "a connection bypassed the proxy")
    }

    private fun assertOnlyTunnelsToThisOrigin(proxy: FixtureProxy, origin: RecordingOrigin) {
        origin.peerPorts.forEach { port ->
            assertEquals(origin.authority, proxy.outboundPorts[port], "connection mixed origins")
        }
    }

    private fun runBounded(block: suspend CoroutineScope.() -> Unit): Unit = runBlocking {
        withTimeout(TEST_BUDGET_MILLIS, block)
    }

    private fun restoreProperty(name: String, value: String?) {
        if (value == null) System.clearProperty(name) else System.setProperty(name, value)
    }

    private fun unusedPort(): Int =
        ServerSocket(0, 1, InetAddress.getLoopbackAddress()).use { it.localPort }

    private fun configuration(
        path: ClientPath,
        proxy: WebProxy,
        connect: Duration = Duration.ofSeconds(2),
        socket: Duration = Duration.ofSeconds(4),
        request: Duration = Duration.ofSeconds(6),
    ): KtorTransportConfig = KtorTransportConfig(
        connectTimeout = connect,
        socketTimeout = socket,
        requestTimeout = request,
        tls13Hosts = path.tls13Hosts,
        proxy = proxy,
    )

    private fun transport(
        path: ClientPath,
        proxy: FixtureProxy,
        destination: WebDestinationPolicy = AllowAllWebDestinationPolicy,
        robots: RobotsPolicy = NoRobotsPolicy,
    ): KtorTransport = transportWith(configuration(path, WebProxy(proxy.url)), destination, robots)

    private fun transport(path: ClientPath, proxy: WebProxy): KtorTransport =
        transportWith(configuration(path, proxy))

    private fun transportWith(
        configuration: KtorTransportConfig,
        destination: WebDestinationPolicy = AllowAllWebDestinationPolicy,
        robots: RobotsPolicy = NoRobotsPolicy,
    ): KtorTransport = KtorTransport.create(
        configuration = configuration,
        policies = WebTransportPolicies(
            destination = destination,
            robots = robots,
            concurrency = NoOriginConcurrencyPolicy,
            pacing = NoOriginPacingPolicy,
            cookies = NoCookieSession,
            failureClassifier = DefaultHttpFailureClassifier,
            retryAfter = DefaultRetryAfterPolicy,
            redirectHeaders = DefaultRedirectHeaderPolicy(),
            headerRedaction = DefaultHeaderRedactionPolicy(),
        ),
    )

    private fun HttpExchange.respond(status: Int, body: String) {
        val bytes = body.toByteArray()
        sendResponseHeaders(status, if (bytes.isEmpty()) -1 else bytes.size.toLong())
        if (bytes.isNotEmpty()) responseBody.write(bytes)
        close()
    }

    private class RecordingDestination : WebDestinationPolicy {
        override val id: String = "recording-destination-proxy"
        val targets = CopyOnWriteArrayList<String>()

        override suspend fun evaluate(request: CrawlRequest, history: RedirectHistory): DestinationDecision {
            targets += request.target
            return DestinationDecision.Allowed
        }
    }

    private class RecordingProxySelector : ProxySelector() {
        val selections = AtomicInteger()

        override fun select(uri: URI): List<Proxy> {
            selections.incrementAndGet()
            return listOf(Proxy.NO_PROXY)
        }

        override fun connectFailed(uri: URI, address: SocketAddress, failure: IOException) = Unit
    }

    private class RecordedRequest(
        val method: String,
        val path: String,
        val headers: Map<String, List<String>>,
        val body: ByteArray,
    )

    private abstract class RecordingOrigin(private val handler: (HttpExchange) -> Unit) : AutoCloseable {
        val requests = CopyOnWriteArrayList<RecordedRequest>()
        val peerPorts: MutableSet<Int> = ConcurrentHashMap.newKeySet()
        val tlsProtocols = CopyOnWriteArrayList<String>()
        val connections = AtomicInteger()
        private val executor = Executors.newFixedThreadPool(8)

        protected abstract val server: HttpServer
        protected abstract val scheme: String

        val authority: String get() = "localhost:${server.address.port}"

        fun url(path: String): String = "$scheme://$authority$path"

        protected fun start(target: HttpServer) {
            target.executor = executor
            target.createContext("/") { exchange ->
                try {
                    connections.incrementAndGet()
                    peerPorts += exchange.remoteAddress.port
                    (exchange as? com.sun.net.httpserver.HttpsExchange)?.let { tlsProtocols += it.sslSession.protocol }
                    requests += RecordedRequest(
                        method = exchange.requestMethod,
                        path = exchange.requestURI.path,
                        headers = exchange.requestHeaders.entries.associate { (name, values) ->
                            name.lowercase() to values.toList()
                        },
                        body = exchange.requestBody.readAllBytes(),
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

        override fun close() {
            server.stop(0)
            executor.shutdownNow()
        }
    }

    private inner class HttpsOrigin(
        path: ClientPath,
        handler: (HttpExchange) -> Unit,
    ) : RecordingOrigin(handler) {
        override val scheme: String = "https"
        override val server: HttpsServer =
            HttpsServer.create(InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0).apply {
                httpsConfigurator = object : HttpsConfigurator(serverContext) {
                    override fun configure(parameters: HttpsParameters) {
                        val ssl = serverContext.defaultSSLParameters
                        ssl.protocols = arrayOf(path.tlsProtocol)
                        parameters.setSSLParameters(ssl)
                    }
                }
            }

        init {
            start(server)
        }
    }

    private class PlainOrigin(handler: (HttpExchange) -> Unit) : RecordingOrigin(handler) {
        override val scheme: String = "http"
        override val server: HttpServer =
            HttpServer.create(InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0)

        init {
            start(server)
        }
    }

    /** Accepts TCP and closes at once, failing a TLS handshake behind a healthy tunnel. */
    private class ClosingOrigin : AutoCloseable {
        private val socket = ServerSocket(0, 8, InetAddress.getLoopbackAddress())
        private val acceptor = Thread {
            try {
                while (true) socket.accept().close()
            } catch (_: IOException) {
                // close() ends the accept loop.
            }
        }.apply { isDaemon = true; start() }

        val port: Int get() = socket.localPort

        override fun close() {
            socket.close()
            acceptor.join(CLOSE_MILLIS)
        }
    }

    /** Accepts TCP and never speaks, stalling a TLS handshake behind a healthy tunnel. */
    private class SilentOrigin : AutoCloseable {
        private val socket = ServerSocket(0, 8, InetAddress.getLoopbackAddress())
        private val accepted = CopyOnWriteArrayList<Socket>()
        private val acceptor = Thread {
            try {
                while (true) accepted += socket.accept()
            } catch (_: IOException) {
                // close() ends the accept loop.
            }
        }.apply { isDaemon = true; start() }

        val port: Int get() = socket.localPort

        override fun close() {
            socket.close()
            accepted.forEach { it.close() }
            acceptor.join(CLOSE_MILLIS)
        }
    }

    private sealed interface ProxyBehavior {
        data object Forward : ProxyBehavior
        data class Reject(val status: Int, val body: String = "denied") : ProxyBehavior
        data object Silent : ProxyBehavior
        data object CloseOnConnect : ProxyBehavior
        data object ResetOnConnect : ProxyBehavior
        data object Garbage : ProxyBehavior
        data class Raw(val answer: String) : ProxyBehavior
    }

    /**
     * Blocking forward proxy on a loopback socket. Each client connection
     * carries one CONNECT tunnel or one absolute-form request, which is
     * rewritten to origin form and answered with the connection closing.
     */
    private class FixtureProxy(behavior: ProxyBehavior = ProxyBehavior.Forward) : AutoCloseable {
        @Volatile
        var behavior: ProxyBehavior = behavior

        val connectTargets = CopyOnWriteArrayList<String>()
        val absoluteTargets = CopyOnWriteArrayList<String>()
        val accepted = AtomicInteger()

        /** Local port of each upstream socket, mapped to the authority it reaches. */
        val outboundPorts = ConcurrentHashMap<Int, String>()

        private val requestSeen = CountDownLatch(1)
        private val clientClosed = CountDownLatch(1)
        private val executor = Executors.newCachedThreadPool { task -> Thread(task).apply { isDaemon = true } }
        private val listener = ServerSocket(0, 64, InetAddress.getLoopbackAddress())
        private val openSockets: MutableSet<Socket> = ConcurrentHashMap.newKeySet()

        val url: String get() = "http://127.0.0.1:${listener.localPort}"

        init {
            executor.execute {
                try {
                    while (true) {
                        val client = listener.accept()
                        accepted.incrementAndGet()
                        openSockets += client
                        executor.execute { serve(client) }
                    }
                } catch (_: IOException) {
                    // close() ends the accept loop.
                }
            }
        }

        fun awaitConnectSeen(): Boolean = requestSeen.await(WAIT_SECONDS, TimeUnit.SECONDS)

        fun awaitClientClosed(): Boolean = clientClosed.await(WAIT_SECONDS, TimeUnit.SECONDS)

        private fun serve(client: Socket) {
            client.use {
                try {
                    val input = client.getInputStream()
                    val head = readHead(input) ?: return
                    val requestLine = head.substringBefore("\r\n")
                    val (method, target) = requestLine.split(' ')
                    if (method == "CONNECT") connectTargets += target else absoluteTargets += target
                    requestSeen.countDown()
                    when (val current = behavior) {
                        ProxyBehavior.Forward -> forward(client, input, head, method, target)
                        is ProxyBehavior.Reject -> respondWithStatus(client, current.status, current.body)
                        ProxyBehavior.CloseOnConnect -> Unit
                        ProxyBehavior.ResetOnConnect -> client.setSoLinger(true, 0)
                        is ProxyBehavior.Raw -> client.getOutputStream().apply {
                            write(current.answer.toByteArray())
                            flush()
                        }
                        ProxyBehavior.Garbage -> client.getOutputStream().apply {
                            write("not an http response\r\n\r\n".toByteArray())
                            flush()
                        }
                        ProxyBehavior.Silent -> awaitClientEnd(input)
                    }
                } catch (_: IOException) {
                    // Aborted connections are expected in failure tests.
                } finally {
                    openSockets -= client
                }
            }
        }

        private fun respondWithStatus(client: Socket, status: Int, body: String) {
            val head = "HTTP/1.1 $status Proxy Says No\r\nContent-Length: ${body.length}\r\n" +
                "Connection: close\r\n\r\n"
            client.getOutputStream().apply {
                write((head + body).toByteArray())
                flush()
            }
        }

        private fun awaitClientEnd(input: InputStream) {
            while (input.read() != -1) {
                // The fixture never answers, so only the client can end this connection.
            }
            clientClosed.countDown()
        }

        private fun forward(client: Socket, input: InputStream, head: String, method: String, target: String) {
            val authority = if (method == "CONNECT") target else URI(target).rawAuthority
            val (host, port) = authority.split(':').let { it[0] to it[1].toInt() }
            // Proxy.NO_PROXY keeps this fixture socket away from the JVM proxy selector under test.
            Socket(Proxy.NO_PROXY).use { upstream ->
                upstream.connect(InetSocketAddress(host, port))
                outboundPorts[upstream.localPort] = authority
                openSockets += upstream
                val upstreamOutput = upstream.getOutputStream()
                if (method == "CONNECT") {
                    client.getOutputStream().apply {
                        write("HTTP/1.1 200 Connection established\r\n\r\n".toByteArray())
                        flush()
                    }
                } else {
                    val uri = URI(target)
                    val path = uri.rawPath.ifEmpty { "/" } + (uri.rawQuery?.let { "?$it" } ?: "")
                    val rest = head.substringAfter("\r\n").substringBeforeLast("\r\n\r\n")
                    val rewritten = "$method $path HTTP/1.1\r\n$rest\r\nConnection: close\r\n\r\n"
                    upstreamOutput.write(rewritten.toByteArray())
                    upstreamOutput.flush()
                }
                executor.execute {
                    pump(input, upstreamOutput)
                    runCatching { upstream.shutdownOutput() }
                }
                pump(upstream.getInputStream(), client.getOutputStream())
                openSockets -= upstream
            }
        }

        private fun pump(from: InputStream, to: OutputStream) {
            val buffer = ByteArray(8 * 1024)
            try {
                while (true) {
                    val read = from.read(buffer)
                    if (read == -1) return
                    to.write(buffer, 0, read)
                    to.flush()
                }
            } catch (_: IOException) {
                // Either side closing ends the relay.
            }
        }

        private fun readHead(input: InputStream): String? {
            val head = ByteArrayOutputStream()
            while (head.size() < MAX_HEAD_BYTES) {
                val next = input.read()
                if (next == -1) return null
                head.write(next)
                val bytes = head.toByteArray()
                if (bytes.size >= 4 && String(bytes, bytes.size - 4, 4, Charsets.ISO_8859_1) == "\r\n\r\n") {
                    return String(bytes, Charsets.ISO_8859_1)
                }
            }
            throw IOException("Request head exceeds fixture bound")
        }

        override fun close() {
            listener.close()
            openSockets.forEach { runCatching { it.close() } }
            executor.shutdownNow()
        }
    }

    private companion object {
        const val STORE_PASSWORD = "throwaway-test-only"
        const val TEST_BUDGET_MILLIS = 30_000L
        const val CLOSE_MILLIS = 2_000L
        const val WAIT_SECONDS = 10L
        const val MAX_HEAD_BYTES = 64 * 1024
    }
}
