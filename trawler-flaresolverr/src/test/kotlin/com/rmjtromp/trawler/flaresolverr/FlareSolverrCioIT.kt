package com.rmjtromp.trawler.flaresolverr

import com.rmjtromp.trawler.flaresolverr.challenge.ChallengeKind
import com.rmjtromp.trawler.flaresolverr.challenge.ChallengeResolutionRequest
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.web.policy.AllowAllWebDestinationPolicy
import com.rmjtromp.trawler.web.transport.KtorTransportConfig
import com.rmjtromp.trawler.web.transport.WebOrigin
import com.rmjtromp.trawler.web.transport.WebProxy
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

/**
 * Real Ktor CIO client against a process-local loopback FlareSolverr fake
 * (JDK `HttpServer`). No Docker, no live FlareSolverr, no public network.
 */
class FlareSolverrCioIT {
    private class LoopbackFlareSolverr(
        private val handler: (JsonObject, HttpExchange) -> Unit,
    ) : AutoCloseable {
        val commands = CopyOnWriteArrayList<JsonObject>()
        private val executor = Executors.newFixedThreadPool(4)
        private val server: HttpServer = HttpServer.create(InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 16).apply {
            createContext("/v1") { exchange ->
                try {
                    val json = Json.parseToJsonElement(String(exchange.requestBody.readAllBytes(), Charsets.UTF_8)).jsonObject
                    commands += json
                    handler(json, exchange)
                } finally {
                    exchange.close()
                }
            }
            executor = this@LoopbackFlareSolverr.executor
            start()
        }

        val endpoint: String get() = "http://127.0.0.1:${server.address.port}/v1"

        override fun close() {
            server.stop(0)
            executor.shutdownNow()
            executor.awaitTermination(5, TimeUnit.SECONDS)
        }
    }

    private fun HttpExchange.json(body: JsonObject, status: Int = 200) {
        val bytes = body.toString().toByteArray()
        responseHeaders.add("Content-Type", "application/json")
        sendResponseHeaders(status, bytes.size.toLong())
        responseBody.write(bytes)
    }

    private fun service(endpoint: String, maxBytes: Int = 1024 * 1024, maxTimeout: Duration = Duration.ofSeconds(5), margin: Duration = Duration.ofSeconds(5)) =
        FlareSolverrServiceConfig(
            endpoint = endpoint,
            maxTimeout = maxTimeout,
            responseTimeoutMargin = margin,
            sessionCommandTimeout = Duration.ofSeconds(5),
            maxServiceResponseBytes = maxBytes,
        )

    @Test
    fun `CIO transport runs the session lifecycle and maps a solution`(): Unit = runBlocking {
        LoopbackFlareSolverr { command, exchange ->
            when ((command["cmd"] as JsonPrimitive).content) {
                "sessions.create" -> exchange.json(buildJsonObject { put("status", "ok"); put("session", "loop-1") })
                "sessions.destroy" -> exchange.json(buildJsonObject { put("status", "ok") })
                else -> exchange.json(okRoot(solution(url = "$SITE/final", response = "<p>cio</p>")))
            }
        }.use { server ->
            val transport = FlareSolverrTransport.create(
                FlareSolverrTransportConfig(service(server.endpoint), sessions = FlareSolverrSessionConfig()),
                AllowAllWebDestinationPolicy,
            )
            transport.start(START_CONTEXT)
            val response = transport.execute(CrawlRequest("$SITE/start"))
            transport.stop(STOP_CONTEXT)
            assertEquals(200, response.statusCode)
            assertEquals("$SITE/final", response.finalTarget)
            assertEquals("<p>cio</p>", String(response.body.toByteArray()))
            assertEquals(
                listOf("sessions.create", "request.get", "sessions.destroy"),
                server.commands.map { (it["cmd"] as JsonPrimitive).content },
            )
            assertEquals(JsonPrimitive("loop-1"), server.commands[1]["session"])
        }
    }

    @Test
    fun `CIO resolver returns clearance from a loopback service`(): Unit = runBlocking {
        LoopbackFlareSolverr { _, exchange ->
            exchange.json(okRoot(solution(response = null, cookies = cookies(cookie("cf_clearance", "loop-token")))))
        }.use { server ->
            val resolver = FlareSolverrChallengeResolver.create(
                FlareSolverrResolverConfig(service(server.endpoint)),
                AllowAllWebDestinationPolicy,
            )
            val clearance = resolver.resolve(
                ChallengeResolutionRequest(WebOrigin.fromTarget(SITE), "$SITE/", ChallengeKind.CLOUDFLARE),
            )
            resolver.close()
            assertEquals("loop-token", clearance.cookies.single().value)
            assertEquals(JsonPrimitive(true), server.commands.single()["returnOnlyCookies"])
        }
    }

    @Test
    fun `a shared proxy reaches the browser while the service connection stays direct`(): Unit = runBlocking {
        // Nothing listens on this port, so a service call routed through the proxy could not succeed.
        val closedPort = ServerSocket(0, 1, InetAddress.getLoopbackAddress()).use { it.localPort }
        val proxyUrl = "http://127.0.0.1:$closedPort"
        val transport = KtorTransportConfig(proxy = WebProxy(proxyUrl))
        LoopbackFlareSolverr { _, exchange ->
            exchange.json(okRoot(solution(response = null, cookies = cookies(cookie("cf_clearance", "loop-token")))))
        }.use { server ->
            val resolver = FlareSolverrChallengeResolver.create(
                FlareSolverrResolverConfig(service(server.endpoint).withProxyOf(transport)),
                AllowAllWebDestinationPolicy,
            )
            val clearance = resolver.resolve(
                ChallengeResolutionRequest(WebOrigin.fromTarget(SITE), "$SITE/", ChallengeKind.CLOUDFLARE),
            )
            resolver.close()

            assertEquals("loop-token", clearance.cookies.single().value)
            val command = server.commands.single()
            assertEquals(JsonPrimitive(proxyUrl), command["proxy"]?.jsonObject?.get("url"))
        }
    }

    @Test
    fun `HTTP 500 status error maps to a service error`(): Unit = runBlocking {
        LoopbackFlareSolverr { _, exchange -> exchange.json(errorRoot("Error: net::ERR_CONNECTION_REFUSED"), 500) }.use { server ->
            FlareSolverrTransport.create(FlareSolverrTransportConfig(service(server.endpoint)), AllowAllWebDestinationPolicy).use { transport ->
                val error = assertFailsWith<FlareSolverrErrorException> { transport.execute(CrawlRequest("$SITE/")) }
                assertEquals("Error: net::ERR_CONNECTION_REFUSED", error.serviceMessage)
            }
        }
    }

    @Test
    fun `declared and chunked oversized responses are rejected by the real client`(): Unit = runBlocking {
        val payload = ByteArray(4096) { ' '.code.toByte() }
        LoopbackFlareSolverr { _, exchange ->
            exchange.sendResponseHeaders(200, payload.size.toLong())
            exchange.responseBody.write(payload)
        }.use { server ->
            FlareSolverrTransport.create(FlareSolverrTransportConfig(service(server.endpoint, maxBytes = 1024)), AllowAllWebDestinationPolicy).use {
                assertFailsWith<FlareSolverrResponseLimitException> { it.execute(CrawlRequest("$SITE/")) }
            }
        }
        LoopbackFlareSolverr { _, exchange ->
            exchange.sendResponseHeaders(200, 0)
            repeat(4) { exchange.responseBody.write(payload) }
        }.use { server ->
            FlareSolverrTransport.create(FlareSolverrTransportConfig(service(server.endpoint, maxBytes = 1024)), AllowAllWebDestinationPolicy).use {
                assertFailsWith<FlareSolverrResponseLimitException> { it.execute(CrawlRequest("$SITE/")) }
            }
        }
    }

    @Test
    fun `a refused connection maps to unavailable`(): Unit = runBlocking {
        val port = ServerSocket(0, 1, InetAddress.getLoopbackAddress()).use { it.localPort }
        FlareSolverrTransport.create(
            FlareSolverrTransportConfig(service("http://127.0.0.1:$port/v1")),
            AllowAllWebDestinationPolicy,
        ).use { transport ->
            val error = assertFailsWith<FlareSolverrUnavailableException> { transport.execute(CrawlRequest("$SITE/")) }
            assertTrue(error.cause is java.io.IOException)
        }
    }

    @Test
    fun `a silent service is bounded by the call timeout and maps to unavailable`(): Unit = runBlocking {
        val release = CountDownLatch(1)
        LoopbackFlareSolverr { _, exchange ->
            release.await(30, TimeUnit.SECONDS)
            exchange.json(okRoot(solution()))
        }.use { server ->
            try {
                FlareSolverrTransport.create(
                    FlareSolverrTransportConfig(service(server.endpoint, maxTimeout = Duration.ofSeconds(1), margin = Duration.ZERO)),
                    AllowAllWebDestinationPolicy,
                ).use { transport ->
                    val error = assertFailsWith<FlareSolverrUnavailableException> { transport.execute(CrawlRequest("$SITE/")) }
                    assertIs<java.io.IOException>(error.cause)
                }
            } finally {
                release.countDown()
            }
        }
    }
}
