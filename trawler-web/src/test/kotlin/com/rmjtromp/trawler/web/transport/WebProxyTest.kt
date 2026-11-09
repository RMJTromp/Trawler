package com.rmjtromp.trawler.web.transport

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.policy.FailureContext
import com.rmjtromp.trawler.web.policy.AllowAllWebDestinationPolicy
import com.rmjtromp.trawler.web.policy.CachingRobotsPolicy
import com.rmjtromp.trawler.web.policy.DefaultHeaderRedactionPolicy
import com.rmjtromp.trawler.web.policy.DefaultHttpFailureClassifier
import com.rmjtromp.trawler.web.policy.DefaultRedirectHeaderPolicy
import com.rmjtromp.trawler.web.policy.DefaultRetryAfterPolicy
import com.rmjtromp.trawler.web.policy.DefaultWebFailureClassifier
import com.rmjtromp.trawler.web.policy.NoRobotsPolicy
import com.rmjtromp.trawler.web.policy.RobotsDecision
import com.rmjtromp.trawler.web.policy.RobotsFetchResult
import com.rmjtromp.trawler.web.policy.RobotsFetcher
import com.rmjtromp.trawler.web.policy.WebProxyFailureException
import com.rmjtromp.trawler.web.session.NoCookieSession
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.network.sockets.ConnectTimeoutException
import io.ktor.client.plugins.HttpRequestTimeoutException
import io.ktor.http.HttpStatusCode
import java.io.EOFException
import java.io.IOException
import java.net.ConnectException
import java.net.NoRouteToHostException
import java.net.ProtocolException
import java.net.SocketException
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.nio.channels.UnresolvedAddressException
import javax.net.ssl.SSLHandshakeException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking

class WebProxyTest {
    @Test
    fun `accepts host and IPv6 literal forms`() {
        val named = WebProxy("http://proxy.internal:8888")
        assertEquals("proxy.internal", named.host)
        assertEquals(8888, named.port)

        val literal = WebProxy("http://[fd00::1]:3128")
        assertEquals("fd00::1", literal.host)
        assertEquals(3128, literal.port)
        assertTrue(literal.socketAddress().isUnresolved)
        assertEquals("http://proxy.internal:8888", named.url)
    }

    @Test
    fun `rejects every malformed proxy URL`() {
        val invalid = listOf(
            "https://proxy:8888" to "scheme",
            "socks5://proxy:1080" to "scheme",
            "HTTP://proxy:8888" to "scheme",
            "proxy:8888" to "scheme",
            "http://proxy" to "port",
            "http://proxy:" to "port",
            "http://proxy:0" to "port",
            "http://proxy:65536" to "port",
            "http://tiny_proxy:8888" to "host",
            "http://:8888" to "host",
            "http:///path" to "host",
            "http://proxy:8888/" to "path",
            "http://proxy:8888/forward" to "path",
            "http://proxy:8888?a=b" to "query",
            "http://proxy:8888?" to "query",
            "http://proxy:8888#frag" to "fragment",
            "http://proxy:8888#" to "fragment",
            "not a url" to "valid",
            "" to "scheme",
        )
        invalid.forEach { (value, expected) ->
            val failure = assertFailsWith<IllegalArgumentException>(value) { WebProxy(value) }
            assertTrue(failure.message.orEmpty().contains(expected), "$value -> ${failure.message}")
        }
    }

    @Test
    fun `cannot carry credentials and never repeats a rejected value`() {
        val withCredentials = listOf(
            "http://user:s3cret-value@proxy:8888",
            "http://user@proxy:8888",
            "http://s3cret-value:@proxy:8888",
            "http://us_er:s3cret-value@proxy:8888",
        )
        withCredentials.forEach { value ->
            val failure = assertFailsWith<IllegalArgumentException>(value) { WebProxy(value) }
            assertEquals("Proxy URL must not carry credentials", failure.message)
            assertNull(failure.cause)
        }

        val unparseable = assertFailsWith<IllegalArgumentException> { WebProxy("http://user:s3cret value@proxy:1") }
        assertFalse(unparseable.message.orEmpty().contains("s3cret"))
        assertNull(unparseable.cause)
    }

    @Test
    fun `transport config defaults to no proxy and keeps the value it is given`() {
        assertNull(KtorTransportConfig().proxy)
        val proxy = WebProxy("http://127.0.0.1:8888")
        assertSame(proxy, KtorTransportConfig(proxy = proxy).proxy)
    }

    @Test
    fun `a caller owned client cannot be given a proxy`() {
        val client = HttpClient(MockEngine) { engine { addHandler { respond("", HttpStatusCode.OK) } } }
        client.use {
            val failure = assertFailsWith<IllegalArgumentException> {
                KtorTransport.fromClient(
                    client = client,
                    configuration = KtorTransportConfig(proxy = WebProxy("http://127.0.0.1:8888")),
                    policies = policies(),
                )
            }
            assertTrue(failure.message.orEmpty().contains("caller-owned"))
        }
    }

    @Test
    fun `classifier reports proxy failures as a distinct retryable category`() {
        val classification = DefaultWebFailureClassifier.classify(
            WebProxyFailureException(502, IOException("Tunnel failed, got: 502")),
            FAILURE_CONTEXT,
        )
        assertEquals("web-proxy-failure", classification.category)
        assertTrue(classification.retryableByDefault)
        assertEquals("502", classification.detail)

        val withoutStatus = DefaultWebFailureClassifier.classify(WebProxyFailureException(null, null), FAILURE_CONTEXT)
        assertEquals("web-proxy-failure", withoutStatus.category)
        assertNull(withoutStatus.detail)
    }

    @Test
    fun `failure keeps its status and original cause`() {
        val cause = IOException("Tunnel failed, got: 403")
        val failure = WebProxyFailureException(403, cause)
        assertEquals(403, failure.connectStatus)
        assertSame(cause, failure.cause)
        assertEquals("Forward proxy answered with status 403", failure.message)
        assertEquals("Forward proxy failed", WebProxyFailureException(null, null).message)
    }

    @Test
    fun `connection phase failures map to the proxy`() {
        val failures = listOf(
            ConnectException("Connection refused"),
            ConnectTimeoutException("timed out"),
            NoRouteToHostException("no route"),
            SocketException("Connection reset"),
            ProtocolException("Invalid status line"),
            NumberFormatException("For input string: \"invalid\""),
            IOException("parsing HTTP/1.1 status line, receiving [HTTP/1.]"),
            UnknownHostException("proxy.internal"),
            UnresolvedAddressException(),
            SocketTimeoutException("Socket timeout has expired"),
            HttpRequestTimeoutException("https://example.test/", 1_000L),
            IOException("HTTP/1.1 header parser received no bytes"),
            RuntimeException("wrapper", ConnectException("Connection refused")),
        )
        failures.forEach { failure ->
            val mapped = assertIs<WebProxyFailureException>(proxyFailureOf(failure, connectingHop()), "$failure")
            assertNull(mapped.connectStatus)
            assertSame(failure, mapped.cause)
        }
    }

    @Test
    fun `a rejected JDK tunnel keeps the proxy status`() {
        val rejected = IOException("Tunnel failed, got: 503")
        assertEquals(503, proxyFailureOf(rejected, connectingHop())?.connectStatus)
    }

    @Test
    fun `failures outside the connection phase or of another kind stay unmapped`() {
        val refused = ConnectException("Connection refused")
        assertNull(proxyFailureOf(refused, ProxyHop()), "queued for a permit")
        assertNull(proxyFailureOf(refused, connectingHop().apply { dispatched() }), "already dispatched")
        assertNull(proxyFailureOf(SSLHandshakeException("bad certificate"), connectingHop()))
        assertNull(proxyFailureOf(EOFException("Not enough data available"), connectingHop()))
        assertNull(proxyFailureOf(IllegalArgumentException("restricted header"), connectingHop()))
        assertNull(proxyFailureOf(IOException("Tunnel failed, got: nope"), connectingHop()))
    }

    @Test
    fun `407 on plain HTTP is the proxy and elsewhere is the site`() {
        val proxied = KtorTransportConfig(proxy = WebProxy("http://127.0.0.1:8888"))
        assertEquals(407, proxied.proxyRejection("http://example.test/", 407)?.connectStatus)
        assertNull(proxied.proxyRejection("https://example.test/", 407))
        assertNull(proxied.proxyRejection("http://example.test/", 403))
        assertNull(KtorTransportConfig().proxyRejection("http://example.test/", 407))
    }

    @Test
    fun `only a configured proxy removes the fragment and encodes non-ASCII targets`() {
        val target = "http://example.test/page?a=1#section"
        val proxied = KtorTransportConfig(proxy = WebProxy("http://1.2.3.4:8"))
        assertEquals("http://example.test/page?a=1", proxied.wireTarget(target))
        assertEquals(target, KtorTransportConfig().wireTarget(target))
        assertEquals("https://example.test/caf%C3%A9", proxied.wireTarget("https://example.test/caf\u00e9"))
        assertEquals("http://example.test:8080/", proxied.wireTarget("http://example.test:8080"))
        assertEquals("http://example.test/?x=1", proxied.wireTarget("http://example.test?x=1#top"))
    }

    @Test
    fun `robots policy rethrows proxy failures instead of caching them`(): Unit = runBlocking {
        val policy = CachingRobotsPolicy()
        var calls = 0
        val failing = object : RobotsFetcher {
            override suspend fun fetch(origin: WebOrigin, userAgent: String): RobotsFetchResult {
                calls++
                throw WebProxyFailureException(502, null)
            }
        }
        repeat(2) {
            assertFailsWith<WebProxyFailureException> { policy.evaluate("https://example.test/a", failing) }
        }
        assertEquals(2, calls)

        val available = object : RobotsFetcher {
            override suspend fun fetch(origin: WebOrigin, userAgent: String): RobotsFetchResult =
                RobotsFetchResult.Available(200, "", java.time.Instant.now())
        }
        assertIs<RobotsDecision.Allowed>(policy.evaluate("https://example.test/a", available))
    }

    private fun connectingHop(): ProxyHop = ProxyHop().apply { connecting() }

    private fun policies(): WebTransportPolicies = WebTransportPolicies(
        destination = AllowAllWebDestinationPolicy,
        robots = NoRobotsPolicy,
        concurrency = NoOriginConcurrencyPolicy,
        pacing = NoOriginPacingPolicy,
        cookies = NoCookieSession,
        failureClassifier = DefaultHttpFailureClassifier,
        retryAfter = DefaultRetryAfterPolicy,
        redirectHeaders = DefaultRedirectHeaderPolicy(),
        headerRedaction = DefaultHeaderRedactionPolicy(),
    )

    private companion object {
        val FAILURE_CONTEXT = FailureContext(CrawlerContext("engine", "run", "pipeline"), CrawlRequest("https://example.test/"))
    }
}
