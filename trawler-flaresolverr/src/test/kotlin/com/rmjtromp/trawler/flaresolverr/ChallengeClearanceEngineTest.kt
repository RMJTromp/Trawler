package com.rmjtromp.trawler.flaresolverr

import com.rmjtromp.trawler.discovery.SeedDiscovery
import com.rmjtromp.trawler.event.CrawlerEvent
import com.rmjtromp.trawler.event.CrawlerEventType
import com.rmjtromp.trawler.event.Observer
import com.rmjtromp.trawler.event.ObserverResult
import com.rmjtromp.trawler.flaresolverr.challenge.ChallengeClearance
import com.rmjtromp.trawler.flaresolverr.challenge.ChallengeDetectedException
import com.rmjtromp.trawler.flaresolverr.challenge.ChallengeResolutionException
import com.rmjtromp.trawler.flaresolverr.challenge.ChallengeRetryPolicy
import com.rmjtromp.trawler.flaresolverr.challenge.ClearanceStore
import com.rmjtromp.trawler.flaresolverr.challenge.CloudflareChallengeDetector
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.ExtractionContext
import com.rmjtromp.trawler.output.CallbackOutput
import com.rmjtromp.trawler.pipeline.EngineConfigurationBuilder
import com.rmjtromp.trawler.pipeline.ExtractionResult
import com.rmjtromp.trawler.pipeline.Extractor
import com.rmjtromp.trawler.pipeline.PipelineBuilder
import com.rmjtromp.trawler.pipeline.PipelineDefaults
import com.rmjtromp.trawler.pipeline.RunResult
import com.rmjtromp.trawler.pipeline.RunStatus
import com.rmjtromp.trawler.pipeline.TrawlerEngine
import com.rmjtromp.trawler.policy.BuiltInPolicies
import com.rmjtromp.trawler.web.policy.AllowAllWebDestinationPolicy
import com.rmjtromp.trawler.web.policy.DefaultHeaderRedactionPolicy
import com.rmjtromp.trawler.web.policy.DefaultHttpFailureClassifier
import com.rmjtromp.trawler.web.policy.DefaultHttpRetryPolicy
import com.rmjtromp.trawler.web.policy.DefaultRedirectHeaderPolicy
import com.rmjtromp.trawler.web.policy.DefaultRetryAfterPolicy
import com.rmjtromp.trawler.web.policy.DefaultWebFailureClassifier
import com.rmjtromp.trawler.web.policy.NoRobotsPolicy
import com.rmjtromp.trawler.web.session.InMemoryCookieSession
import com.rmjtromp.trawler.web.transport.KtorTransport
import com.rmjtromp.trawler.web.transport.KtorTransportConfig
import com.rmjtromp.trawler.web.transport.NoOriginConcurrencyPolicy
import com.rmjtromp.trawler.web.transport.NoOriginPacingPolicy
import com.rmjtromp.trawler.web.transport.WebOrigin
import com.rmjtromp.trawler.web.transport.WebTransportPolicies
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.request.HttpRequestData
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

/**
 * Engine-level solve-then-reuse: a controlled target site (MockEngine behind
 * `KtorTransport`) challenges until it sees the clearance cookie, a jar
 * cookie, and the browser User-Agent; the fake FlareSolverr (MockEngine)
 * hands out that clearance. Every retry must be an engine attempt.
 */
class ChallengeClearanceEngineTest {
    private val token = "clearance-token-value"

    private class Harness(
        val engine: TrawlerEngine,
        val fake: FakeFlareSolverr,
        val siteRequests: CopyOnWriteArrayList<HttpRequestData>,
        val events: CopyOnWriteArrayList<CrawlerEvent>,
        val produced: CopyOnWriteArrayList<String>,
        val store: ClearanceStore,
    )

    private fun harness(
        seeds: Int,
        siteAccepts: Boolean = true,
        holdFirstRound: CompletableDeferred<Unit>? = null,
        fake: FakeFlareSolverr = FakeFlareSolverr {
            replyJson(okRoot(solution(response = null, cookies = cookies(cookie("cf_clearance", token)))))
        },
    ): Harness {
        val siteRequests = CopyOnWriteArrayList<HttpRequestData>()
        val challenged = AtomicInteger()
        val site = MockEngine { request ->
            siteRequests += request
            val cookie = request.headers[HttpHeaders.Cookie].orEmpty()
            val cleared = siteAccepts &&
                cookie.contains("cf_clearance=$token") &&
                cookie.contains("jar=1") &&
                request.headers[HttpHeaders.UserAgent] == BROWSER_UA
            if (cleared) {
                respond("cleared:${request.url.encodedPathAndQuery}", HttpStatusCode.OK)
            } else {
                if (holdFirstRound != null && challenged.incrementAndGet() <= seeds) {
                    if (challenged.get() == seeds) holdFirstRound.complete(Unit)
                    holdFirstRound.await()
                }
                respond(
                    "<html><head><title>Just a moment...</title></head></html>",
                    HttpStatusCode.Forbidden,
                    headersOf(
                        "cf-mitigated" to listOf("challenge"),
                        HttpHeaders.Server to listOf("cloudflare"),
                        HttpHeaders.SetCookie to listOf("jar=1; Path=/"),
                    ),
                )
            }
        }
        val store = ClearanceStore()
        val resolver = FlareSolverrChallengeResolver.createWithEngine(
            FlareSolverrResolverConfig(serviceConfig()),
            AllowAllWebDestinationPolicy,
            fake.engine,
        )
        val clearance = ChallengeClearance(resolver, CloudflareChallengeDetector(), store)
        val ktor = KtorTransport.fromClient(
            client = HttpClient(site),
            configuration = KtorTransportConfig(),
            policies = WebTransportPolicies(
                destination = AllowAllWebDestinationPolicy,
                robots = NoRobotsPolicy,
                concurrency = NoOriginConcurrencyPolicy,
                pacing = NoOriginPacingPolicy,
                cookies = clearance.cookieSession(InMemoryCookieSession()),
                failureClassifier = DefaultHttpFailureClassifier,
                retryAfter = DefaultRetryAfterPolicy,
                redirectHeaders = DefaultRedirectHeaderPolicy(),
                headerRedaction = DefaultHeaderRedactionPolicy(),
            ),
            closeClient = true,
        )
        val events = CopyOnWriteArrayList<CrawlerEvent>()
        val produced = CopyOnWriteArrayList<String>()
        val pipeline = PipelineBuilder<String>("protected")
            .transport(ktor)
            .discover(SeedDiscovery(requests = (1..seeds).map { CrawlRequest("$SITE/item?id=$it") }))
            .addRequestMiddleware(clearance.requestMiddleware)
            .addResponseMiddleware(clearance.responseMiddleware)
            .classifyFailures(FlareSolverrFailureClassifier(DefaultWebFailureClassifier))
            .retry(ChallengeRetryPolicy(DefaultHttpRetryPolicy()))
            .extract(object : Extractor<String> {
                override val id: String = "body"
                override suspend fun extract(response: CrawlResponse, context: ExtractionContext): ExtractionResult<String> =
                    ExtractionResult.Success(listOf(String(response.body.toByteArray())))
            })
            .output(CallbackOutput { produced += it.value })
            .build(PipelineDefaults(policies = BuiltInPolicies.ordinary(maxConcurrency = 4)))
        val engine = EngineConfigurationBuilder()
            .extension(ktor)
            .extension(clearance)
            .observe(object : Observer {
                override val id: String = "events"
                override suspend fun onEvent(event: CrawlerEvent): ObserverResult {
                    events += event
                    return ObserverResult.Accepted
                }
            })
            .register(pipeline)
            .build()
            .let(::TrawlerEngine)
        return Harness(engine, fake, siteRequests, events, produced, store)
    }

    private fun run(harness: Harness): RunResult = runBlocking {
        try {
            withTimeout(30_000) { harness.engine.runOnce() }
        } finally {
            harness.engine.shutdown()
        }
    }

    @Test
    fun `concurrent challenged work resolves once and succeeds through visible engine retries`() {
        val firstRound = CompletableDeferred<Unit>()
        val harness = harness(seeds = 3, holdFirstRound = firstRound)
        val result = run(harness)
        val pipeline = result.pipelineRuns.single()

        assertEquals(RunStatus.COMPLETED, result.status)
        assertEquals(6L, pipeline.requestsStarted, "3 challenged attempts + 3 cleared retries")
        assertEquals(3L, pipeline.deliveredValues)
        assertEquals(
            (1..3).map { "cleared:/item?id=$it" }.toSet(),
            harness.produced.toSet(),
        )
        assertEquals(1, harness.fake.commands("request.get").size, "exactly one FlareSolverr resolution")
        val solve = harness.fake.commands("request.get").single()
        assertEquals(kotlinx.serialization.json.JsonPrimitive(true), solve.json["returnOnlyCookies"])

        assertEquals(6, harness.siteRequests.size)
        val (challengedRound, clearedRound) = harness.siteRequests.partition {
            it.headers[HttpHeaders.UserAgent] != BROWSER_UA
        }
        assertEquals(3, challengedRound.size)
        assertTrue(challengedRound.all { !it.headers[HttpHeaders.Cookie].orEmpty().contains("cf_clearance") })
        assertEquals(3, clearedRound.size)
        assertTrue(clearedRound.all { it.headers[HttpHeaders.Cookie]!!.contains("cf_clearance=$token") })

        val retries = harness.events.filter { it.type == CrawlerEventType.RETRY_SCHEDULED }
        assertEquals(3, retries.size)
        assertTrue(retries.all { it.detail == "now" })
        assertEquals(3, pipeline.failures.size)
        assertTrue(pipeline.failures.all { it.error is ChallengeDetectedException })
        assertFalse(harness.events.any { it.toString().contains(token) }, "clearance values never reach events")
        assertFalse(pipeline.failures.any { it.error.toString().contains(token) })
        assertNull(harness.store.current(WebOrigin.fromTarget(SITE)), "engine shutdown clears clearance")
    }

    @Test
    fun `clearance rejected by the site is re-resolved and bounded by the attempt limit`() {
        val harness = harness(seeds = 1, siteAccepts = false)
        val result = run(harness)
        val pipeline = result.pipelineRuns.single()

        assertEquals(RunStatus.FAILED, result.status)
        assertEquals(3L, pipeline.requestsStarted)
        assertEquals(0L, pipeline.deliveredValues)
        assertEquals(2, harness.fake.commands("request.get").size, "attempts 2 and 3 each resolve once")
        assertEquals(2, harness.events.count { it.type == CrawlerEventType.RETRY_SCHEDULED })
        assertEquals(3, harness.siteRequests.size)
        assertTrue(pipeline.failures.all { it.error is ChallengeDetectedException })
    }

    @Test
    fun `a failed resolution is classified and deferred by the delegate retry policy`() {
        val failing = FakeFlareSolverr {
            replyJson(errorRoot("Error: Error solving the challenge. Timeout after 5.0 seconds."), HttpStatusCode.InternalServerError)
        }
        val harness = harness(seeds = 1, fake = failing)
        val result = run(harness)
        val pipeline = result.pipelineRuns.single()

        assertEquals(RunStatus.ABANDONED, result.status, "RetryAt defers retained work beyond this run")
        assertEquals(2L, pipeline.requestsStarted)
        assertEquals(1, failing.commands("request.get").size)
        assertEquals(1, harness.siteRequests.size, "the failed resolution sends no target request")
        val retries = harness.events.filter { it.type == CrawlerEventType.RETRY_SCHEDULED }
        assertEquals(listOf("now"), retries.take(1).map { it.detail })
        assertEquals(2, retries.size)
        val resolution = pipeline.failures.last().error
        assertIs<ChallengeResolutionException>(resolution)
        assertEquals("resolver-failed", resolution.reason)
        assertIs<FlareSolverrErrorException>(resolution.cause)
    }
}
