package com.rmjtromp.trawler.flaresolverr

import com.rmjtromp.trawler.web.transport.KtorTransportConfig
import com.rmjtromp.trawler.web.transport.WebProxy
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class FlareSolverrConfigurationTest {
    @Test
    fun `service config takes the proxy defined on the transport config`() {
        val transport = KtorTransportConfig(proxy = WebProxy("http://10.8.0.1:8888"))

        val linked = serviceConfig().withProxyOf(transport)

        assertEquals("http://10.8.0.1:8888", linked.proxy?.url)
        assertFalse(checkNotNull(linked.proxy).hasCredentials)
        assertEquals(serviceConfig().endpoint, linked.endpoint)
    }

    @Test
    fun `a transport without a proxy leaves the browser without one`() {
        val configured = serviceConfig(proxy = FlareSolverrProxy("http://stale.test:1"))

        assertNull(configured.withProxyOf(KtorTransportConfig()).proxy)
    }

    @Test
    fun `proxy validates scheme host user-info and credential pairing`() {
        listOf("http://proxy.test:8080", "socks4://proxy.test:1080", "SOCKS5://proxy.test").forEach {
            FlareSolverrProxy(it)
        }
        assertTrue(FlareSolverrProxy("http://proxy.test", "user", "pass").hasCredentials)
        assertFalse(FlareSolverrProxy("http://proxy.test").hasCredentials)
        listOf(
            { FlareSolverrProxy(" ") },
            { FlareSolverrProxy("http://" + "a".repeat(FlareSolverrProxy.MAX_PROXY_URL_LENGTH)) },
            { FlareSolverrProxy("http://bad host") },
            { FlareSolverrProxy("https://proxy.test") },
            { FlareSolverrProxy("proxy.test:8080") },
            { FlareSolverrProxy("http:///nohost") },
            { FlareSolverrProxy("http://user:pass@proxy.test") },
            { FlareSolverrProxy("http://proxy.test", username = "user") },
            { FlareSolverrProxy("http://proxy.test", password = "pass") },
            { FlareSolverrProxy("http://proxy.test", username = "", password = "pass") },
        ).forEach { assertFailsWith<IllegalArgumentException> { it() } }
    }

    @Test
    fun `proxy toString never reveals credentials or path`() {
        val proxy = FlareSolverrProxy("http://proxy.test:8080/secret-path", "proxy-user", "proxy-pass")
        val text = proxy.toString()
        assertEquals("FlareSolverrProxy(url=http://proxy.test:8080, credentials=[REDACTED])", text)
        assertFalse(text.contains("proxy-user"))
        assertFalse(text.contains("proxy-pass"))
        assertEquals(
            "FlareSolverrProxy(url=socks5://proxy.test, credentials=none)",
            FlareSolverrProxy("socks5://proxy.test").toString(),
        )
        val service = serviceConfig(proxy = proxy).toString()
        assertFalse(service.contains("proxy-pass"))
        assertFalse(service.contains("proxy-user"))
    }

    @Test
    fun `service config has explicit endpoint and validates every bound`() {
        val config = FlareSolverrServiceConfig("https://flaresolverr.internal/v1")
        assertEquals(Duration.ofSeconds(60), config.maxTimeout)
        assertEquals(Duration.ofSeconds(90), config.solveCallTimeout)
        assertEquals(2, config.maxConcurrentCalls)
        assertNull(config.proxy)
        FlareSolverrServiceConfig(ENDPOINT, maxTimeout = Duration.ofSeconds(1), responseTimeoutMargin = Duration.ZERO)
        FlareSolverrServiceConfig(ENDPOINT, maxConcurrentCalls = FlareSolverrServiceConfig.MAX_CONCURRENT_CALLS)
        listOf(
            { FlareSolverrServiceConfig("") },
            { FlareSolverrServiceConfig("http://" + "a".repeat(FlareSolverrServiceConfig.MAX_ENDPOINT_LENGTH)) },
            { FlareSolverrServiceConfig("http://bad host/v1") },
            { FlareSolverrServiceConfig("ftp://flaresolverr.test/v1") },
            { FlareSolverrServiceConfig("localhost:8191/v1") },
            { FlareSolverrServiceConfig("http:///v1") },
            { FlareSolverrServiceConfig("http://user:pass@flaresolverr.test/v1") },
            { FlareSolverrServiceConfig("http://flaresolverr.test/v1#fragment") },
            { FlareSolverrServiceConfig(ENDPOINT, maxTimeout = Duration.ofMillis(999)) },
            { FlareSolverrServiceConfig(ENDPOINT, maxTimeout = Duration.ofMillis(Int.MAX_VALUE.toLong() + 1)) },
            { FlareSolverrServiceConfig(ENDPOINT, responseTimeoutMargin = Duration.ofMillis(-1)) },
            { FlareSolverrServiceConfig(ENDPOINT, sessionCommandTimeout = Duration.ZERO) },
            { FlareSolverrServiceConfig(ENDPOINT, connectTimeout = Duration.ZERO) },
            { FlareSolverrServiceConfig(ENDPOINT, maxServiceResponseBytes = 0) },
            { FlareSolverrServiceConfig(ENDPOINT, maxConcurrentCalls = 0) },
            { FlareSolverrServiceConfig(ENDPOINT, maxConcurrentCalls = FlareSolverrServiceConfig.MAX_CONCURRENT_CALLS + 1) },
        ).forEach { assertFailsWith<IllegalArgumentException> { it() } }
    }

    @Test
    fun `session config validates count and whole-minute ttl`() {
        assertEquals(1, FlareSolverrSessionConfig().count)
        FlareSolverrSessionConfig(count = FlareSolverrSessionConfig.MAX_SESSIONS, ttl = Duration.ofMinutes(1))
        listOf(
            { FlareSolverrSessionConfig(count = 0) },
            { FlareSolverrSessionConfig(count = FlareSolverrSessionConfig.MAX_SESSIONS + 1) },
            { FlareSolverrSessionConfig(ttl = Duration.ofSeconds(59)) },
            { FlareSolverrSessionConfig(ttl = Duration.ofMinutes(Int.MAX_VALUE.toLong() + 1)) },
        ).forEach { assertFailsWith<IllegalArgumentException> { it() } }
    }

    @Test
    fun `transport config validates bounds and proxy credential placement`() {
        val service = serviceConfig()
        val config = FlareSolverrTransportConfig(service)
        assertEquals("flaresolverr", config.id)
        assertNull(config.sessions)
        FlareSolverrTransportConfig(service, maxRequestCookies = 0, maxResponseHeaders = 0, maxResponseCookies = 0)
        FlareSolverrTransportConfig(
            serviceConfig(proxy = FlareSolverrProxy("http://proxy.test", "u", "p")),
            sessions = FlareSolverrSessionConfig(),
        )
        listOf(
            { FlareSolverrTransportConfig(service, id = " ") },
            { FlareSolverrTransportConfig(service, maxTargetLength = 0) },
            { FlareSolverrTransportConfig(service, maxRequestBodyBytes = 0) },
            { FlareSolverrTransportConfig(service, maxRequestCookies = -1) },
            { FlareSolverrTransportConfig(service, maxResponseBodyBytes = 0) },
            { FlareSolverrTransportConfig(service, maxResponseHeaders = -1) },
            { FlareSolverrTransportConfig(service, maxHeaderNameLength = 0) },
            { FlareSolverrTransportConfig(service, maxHeaderValueLength = 0) },
            { FlareSolverrTransportConfig(service, maxResponseCookies = -1) },
            { FlareSolverrTransportConfig(service, maxCookieValueLength = 0) },
            { FlareSolverrTransportConfig(service, maxUserAgentLength = 0) },
            { FlareSolverrTransportConfig(serviceConfig(proxy = FlareSolverrProxy("http://proxy.test", "u", "p"))) },
        ).forEach { assertFailsWith<IllegalArgumentException> { it() } }
    }

    @Test
    fun `resolver config validates bounds and rejects proxy credentials`() {
        val service = serviceConfig()
        assertEquals("flaresolverr-challenge-resolver", FlareSolverrResolverConfig(service).id)
        FlareSolverrResolverConfig(service, maxCookies = 1)
        FlareSolverrResolverConfig(service, maxCookies = 1_024)
        FlareSolverrResolverConfig(serviceConfig(proxy = FlareSolverrProxy("http://proxy.test")))
        listOf(
            { FlareSolverrResolverConfig(service, id = "") },
            { FlareSolverrResolverConfig(service, maxTargetLength = 0) },
            { FlareSolverrResolverConfig(service, maxCookies = 0) },
            { FlareSolverrResolverConfig(service, maxCookies = 1_025) },
            { FlareSolverrResolverConfig(service, maxCookieValueLength = 0) },
            { FlareSolverrResolverConfig(service, maxUserAgentLength = 0) },
            { FlareSolverrResolverConfig(serviceConfig(proxy = FlareSolverrProxy("http://proxy.test", "u", "p"))) },
        ).forEach { assertFailsWith<IllegalArgumentException> { it() } }
    }

    @Test
    fun `exception messages carry only reason codes and limits`() {
        assertEquals(
            "Request is not supported by FlareSolverr: method-not-supported",
            UnsupportedFlareSolverrRequestException("method-not-supported").message,
        )
        assertEquals(
            "FlareSolverr response exceeds the response body bytes limit of 10",
            FlareSolverrResponseLimitException("response body bytes", 10).message,
        )
        assertEquals("Unexpected FlareSolverr response: x", FlareSolverrProtocolException("x").message)
        assertEquals("FlareSolverr reported an error: boom", FlareSolverrErrorException("boom").message)
        val cause = java.io.IOException("refused")
        val unavailable = FlareSolverrUnavailableException(cause)
        assertEquals("FlareSolverr service is unavailable or timed out", unavailable.message)
        assertEquals(cause, unavailable.cause)
    }
}
