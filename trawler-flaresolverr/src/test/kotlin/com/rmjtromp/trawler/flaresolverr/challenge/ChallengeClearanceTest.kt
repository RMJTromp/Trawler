package com.rmjtromp.trawler.flaresolverr.challenge

import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.flaresolverr.BROWSER_UA
import com.rmjtromp.trawler.flaresolverr.FAILURE_CONTEXT
import com.rmjtromp.trawler.flaresolverr.FlareSolverrFailureCategories
import com.rmjtromp.trawler.flaresolverr.FlareSolverrProtocolException
import com.rmjtromp.trawler.flaresolverr.FlareSolverrUnavailableException
import com.rmjtromp.trawler.flaresolverr.MutableClock
import com.rmjtromp.trawler.flaresolverr.SITE
import com.rmjtromp.trawler.flaresolverr.START_CONTEXT
import com.rmjtromp.trawler.flaresolverr.STOP_CONTEXT
import com.rmjtromp.trawler.model.ContextAttribute
import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.Header
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.model.ResponseContext
import com.rmjtromp.trawler.policy.AttemptSummary
import com.rmjtromp.trawler.policy.FailureClassification
import com.rmjtromp.trawler.policy.FailureContext
import com.rmjtromp.trawler.policy.RetryDecision
import com.rmjtromp.trawler.policy.RetryPolicy
import com.rmjtromp.trawler.transport.RequestExecutor
import com.rmjtromp.trawler.transport.ResponseHandler
import com.rmjtromp.trawler.web.transport.WebOrigin
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import java.io.IOException
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

/** Controlled resolver that records calls and delegates to a scripted behavior. */
internal class ScriptedResolver(
    var behavior: suspend (ChallengeResolutionRequest) -> Clearance,
) : ChallengeResolver {
    override val id: String = "scripted-resolver"
    val calls = CopyOnWriteArrayList<ChallengeResolutionRequest>()

    override suspend fun resolve(request: ChallengeResolutionRequest): Clearance {
        calls += request
        return behavior(request)
    }
}

@OptIn(ExperimentalCoroutinesApi::class)
class ChallengeClearanceTest {
    private val clock = MutableClock(Instant.parse("2026-02-02T00:00:00Z"))
    private val origin = WebOrigin.fromTarget(SITE)
    private val crawler = CrawlerContext("engine", "run", "pipeline")

    private fun clearanceFor(origin: WebOrigin = this.origin, token: String = "clear-token") =
        Clearance(origin, listOf(ClearanceCookie("cf_clearance", token)), BROWSER_UA, clock.now())

    private val store = ClearanceStore(clock = clock)
    private val resolver = ScriptedResolver { request -> clearanceFor(request.origin) }
    private val clearance = ChallengeClearance(resolver, CloudflareChallengeDetector(), store)

    private val forwarded = CopyOnWriteArrayList<Pair<CrawlRequest, RequestContext>>()
    private val recordingNext = RequestExecutor { request, context ->
        forwarded += request to context
        okResponse(request)
    }

    private fun okResponse(request: CrawlRequest, status: Int = 200, finalTarget: String? = request.target, vararg headers: Header) =
        CrawlResponse(
            request = request,
            statusCode = status,
            headers = Headers.from(headers.toList()),
            body = ResponseBody.from("ok".toByteArray()),
            receivedAt = clock.now(),
            finalTarget = finalTarget,
        )

    private fun challengeResponse(request: CrawlRequest, finalTarget: String? = request.target) =
        okResponse(request, 403, finalTarget, Header("cf-mitigated", "challenge"))

    private suspend fun send(request: CrawlRequest): CrawlResponse =
        clearance.requestMiddleware.intercept(request, RequestContext(crawler, request), recordingNext)

    private suspend fun respond(request: CrawlRequest, response: CrawlResponse, next: ResponseHandler = ResponseHandler { r, _ -> r }) =
        clearance.responseMiddleware.intercept(response, ResponseContext(crawler, request, response), next)

    @Test
    fun `identifiers are derived from the coordinator id and validated`() {
        assertEquals("flaresolverr-challenge-clearance:request", clearance.requestMiddleware.id)
        assertEquals("flaresolverr-challenge-clearance:response", clearance.responseMiddleware.id)
        assertEquals("custom:request", ChallengeClearance(resolver, CloudflareChallengeDetector(), store, "custom").requestMiddleware.id)
        assertFailsWith<IllegalArgumentException> { ChallengeClearance(resolver, CloudflareChallengeDetector(), store, " ") }
        assertSame(store, clearance.store)
    }

    @Test
    fun `requests pass through untouched without a challenge marker or for non-web targets`(): Unit = runBlocking {
        val request = CrawlRequest("$SITE/page", headers = Headers.of(Header("User-Agent", "TrawlerBot/0.1")))
        send(request)
        assertSame(request, forwarded.single().first)
        val custom = CrawlRequest("custom-scheme:resource")
        store.recordChallenge(origin, ChallengeKind.CLOUDFLARE, null)
        send(custom)
        assertSame(custom, forwarded[1].first)
        assertTrue(resolver.calls.isEmpty())
    }

    @Test
    fun `an existing clearance replaces every user agent and tags the generation without resolving`(): Unit = runBlocking {
        val stored = store.store(clearanceFor())
        val request = CrawlRequest(
            "$SITE/page",
            headers = Headers.of(Header("user-agent", "a"), Header("User-Agent", "b"), Header("Accept", "text/html")),
            attributes = ContextAttributes.of(
                ContextAttribute(ChallengeClearance.GENERATION_ATTRIBUTE, "999"),
                ContextAttribute("keep", "me"),
            ),
        )
        send(request)
        val (sent, context) = forwarded.single()
        assertEquals(listOf(BROWSER_UA), sent.headers["User-Agent"])
        assertEquals(listOf("text/html"), sent.headers["Accept"])
        assertEquals(stored.generation.toString(), sent.attributes[ChallengeClearance.GENERATION_ATTRIBUTE])
        assertEquals(1, sent.attributes.entries.count { it.name == ChallengeClearance.GENERATION_ATTRIBUTE })
        assertEquals("me", sent.attributes["keep"])
        assertSame(sent, context.request)
        assertTrue(resolver.calls.isEmpty())
        assertFalse(sent.headers.entries.any { it.value.contains("clear-token") }, "cookies are added by the cookie session only")
    }

    @Test
    fun `a marked origin resolves once and applies the new clearance`(): Unit = runBlocking {
        store.recordChallenge(origin, ChallengeKind("custom-kind"), null)
        send(CrawlRequest("$SITE/page?id=1"))
        val call = resolver.calls.single()
        assertEquals(origin, call.origin)
        assertEquals("$SITE/page?id=1", call.target)
        assertEquals(ChallengeKind("custom-kind"), call.kind)
        assertNull(store.pendingChallenge(origin))
        assertEquals(BROWSER_UA, forwarded.single().first.headers["User-Agent"].single())
        send(CrawlRequest("$SITE/other"))
        assertEquals(1, resolver.calls.size)
    }

    @Test
    fun `N concurrent requests to a challenged origin share exactly one resolution`() = runTest {
        val gate = CompletableDeferred<Unit>()
        resolver.behavior = { request ->
            gate.await()
            clearanceFor(request.origin)
        }
        store.recordChallenge(origin, ChallengeKind.CLOUDFLARE, null)
        val requests = (1..8).map { async { send(CrawlRequest("$SITE/item/$it")) } }
        advanceUntilIdle()
        assertEquals(1, resolver.calls.size, "only the leader resolves")
        assertTrue(forwarded.isEmpty(), "waiters are suspended behind the leader")
        gate.complete(Unit)
        requests.awaitAll()
        assertEquals(1, resolver.calls.size)
        assertEquals(8, forwarded.size)
        assertTrue(forwarded.all { it.first.headers["User-Agent"] == listOf(BROWSER_UA) })
        assertEquals(setOf("1"), forwarded.map { it.first.attributes[ChallengeClearance.GENERATION_ATTRIBUTE] }.toSet())
    }

    @Test
    fun `different origins resolve independently`() = runTest {
        val other = WebOrigin.fromTarget("https://other.test/")
        store.recordChallenge(origin, ChallengeKind.CLOUDFLARE, null)
        store.recordChallenge(other, ChallengeKind.CLOUDFLARE, null)
        val gate = CompletableDeferred<Unit>()
        resolver.behavior = { request ->
            gate.await()
            clearanceFor(request.origin)
        }
        val a = async { send(CrawlRequest("$SITE/")) }
        val b = async { send(CrawlRequest("https://other.test/")) }
        advanceUntilIdle()
        assertEquals(setOf(origin, other), resolver.calls.map { it.origin }.toSet())
        gate.complete(Unit)
        a.await()
        b.await()
    }

    @Test
    fun `a cancelled leader hands resolution to the next waiter without a cooldown`() = runTest {
        val attempts = AtomicInteger()
        resolver.behavior = { request ->
            if (attempts.incrementAndGet() == 1) awaitCancellation()
            clearanceFor(request.origin)
        }
        store.recordChallenge(origin, ChallengeKind.CLOUDFLARE, null)
        val leader = async { send(CrawlRequest("$SITE/leader")) }
        advanceUntilIdle()
        val follower = async { send(CrawlRequest("$SITE/follower")) }
        advanceUntilIdle()
        assertEquals(1, resolver.calls.size)
        leader.cancel()
        advanceUntilIdle()
        assertTrue(leader.isCancelled)
        assertEquals(2, resolver.calls.size)
        assertEquals("$SITE/follower", follower.await().request.target)
        assertFalse(store.inResolutionCooldown(origin))
        assertNotNull(store.current(origin))
    }

    @Test
    fun `resolver failure starts a cooldown that fails concurrent and immediate retries fast`() = runTest {
        val gate = CompletableDeferred<Unit>()
        resolver.behavior = {
            gate.await()
            throw FlareSolverrUnavailableException(IOException("down"))
        }
        store.recordChallenge(origin, ChallengeKind.CLOUDFLARE, null)
        val results = (1..3).map { async { runCatching { send(CrawlRequest("$SITE/$it")) } } }
        advanceUntilIdle()
        gate.complete(Unit)
        val errors = results.awaitAll().map { assertIs<ChallengeResolutionException>(it.exceptionOrNull()) }
        assertEquals(1, resolver.calls.size)
        assertEquals(listOf("resolver-failed", "resolution-cooldown", "resolution-cooldown"), errors.map { it.reason })
        assertIs<FlareSolverrUnavailableException>(errors.first().cause)
        assertEquals(ChallengeKind.CLOUDFLARE, errors.first().kind)
        assertTrue(forwarded.isEmpty())

        assertEquals("resolution-cooldown", assertFailsWith<ChallengeResolutionException> { send(CrawlRequest("$SITE/")) }.reason)
        clock.advance(Duration.ofSeconds(30))
        resolver.behavior = { request -> clearanceFor(request.origin) }
        send(CrawlRequest("$SITE/after-cooldown"))
        assertEquals(2, resolver.calls.size)
        assertEquals(1, forwarded.size)
    }

    @Test
    fun `a waiter whose marker expired while queued proceeds without resolving`() = runTest {
        val gate = CompletableDeferred<Unit>()
        resolver.behavior = {
            gate.await()
            throw FlareSolverrUnavailableException(IOException("down"))
        }
        store.recordChallenge(origin, ChallengeKind.CLOUDFLARE, null)
        val leader = async { runCatching { send(CrawlRequest("$SITE/leader")) } }
        advanceUntilIdle()
        val waiter = async { send(CrawlRequest("$SITE/waiter")) }
        advanceUntilIdle()
        clock.advance(Duration.ofMinutes(10))
        clock.advance(Duration.ofSeconds(30))
        gate.complete(Unit)
        assertIs<ChallengeResolutionException>(leader.await().exceptionOrNull())
        assertEquals("$SITE/waiter", waiter.await().request.target)
        assertEquals(1, resolver.calls.size)
        assertNull(forwarded.single().first.attributes[ChallengeClearance.GENERATION_ATTRIBUTE])
    }

    @Test
    fun `non-HTTP origins are never resolved`(): Unit = runBlocking {
        val websocket = CrawlRequest("ws://example.test:80/socket")
        store.recordChallenge(WebOrigin("ws", "example.test", 80), ChallengeKind.CLOUDFLARE, null)
        send(websocket)
        assertSame(websocket, forwarded.single().first)
        assertTrue(resolver.calls.isEmpty())
    }

    @Test
    fun `clearance for another origin is rejected and cooled down`(): Unit = runBlocking {
        resolver.behavior = { clearanceFor(WebOrigin.fromTarget("https://evil.test/")) }
        store.recordChallenge(origin, ChallengeKind.CLOUDFLARE, null)
        val error = assertFailsWith<ChallengeResolutionException> { send(CrawlRequest("$SITE/")) }
        assertEquals("origin-mismatch", error.reason)
        assertNull(error.cause)
        assertTrue(store.inResolutionCooldown(origin))
        assertNull(store.current(origin))
        assertNull(store.current(WebOrigin.fromTarget("https://evil.test/")))
    }

    @Test
    fun `resolution exception messages contain only kind and reason`(): Unit = runBlocking {
        resolver.behavior = { throw FlareSolverrProtocolException("cookie=secret-value") }
        store.recordChallenge(origin, ChallengeKind.CLOUDFLARE, null)
        val error = assertFailsWith<ChallengeResolutionException> { send(CrawlRequest("$SITE/?token=query-secret")) }
        assertEquals("Challenge 'cloudflare' resolution failed: resolver-failed", error.message)
    }

    @Test
    fun `undetected responses continue down the response chain`(): Unit = runBlocking {
        val request = CrawlRequest("$SITE/")
        val response = okResponse(request)
        var handled: CrawlResponse? = null
        val result = respond(request, response, ResponseHandler { r, _ -> handled = r; r })
        assertSame(response, result)
        assertSame(response, handled)
        assertNull(store.pendingChallenge(origin))
    }

    @Test
    fun `a detected challenge marks the origin and fails the attempt without calling next`(): Unit = runBlocking {
        val request = CrawlRequest("$SITE/?secret=1")
        var called = false
        val error = assertFailsWith<ChallengeDetectedException> {
            respond(request, challengeResponse(request), ResponseHandler { r, _ -> called = true; r })
        }
        assertFalse(called)
        assertEquals(ChallengeKind.CLOUDFLARE, error.kind)
        assertEquals("cf-mitigated-header", error.signal)
        assertEquals(origin, error.origin)
        assertEquals("Anti-bot challenge 'cloudflare' detected (cf-mitigated-header)", error.message)
        assertNotNull(store.pendingChallenge(origin))
    }

    @Test
    fun `challenge on a redirected final origin marks that origin and ignores the sent generation`(): Unit = runBlocking {
        val final = WebOrigin.fromTarget("https://final.test/")
        val finalClearance = store.store(clearanceFor(final))
        val request = CrawlRequest(
            "$SITE/start",
            attributes = ContextAttributes.of(ContextAttribute(ChallengeClearance.GENERATION_ATTRIBUTE, finalClearance.generation.toString())),
        )
        val error = assertFailsWith<ChallengeDetectedException> {
            respond(request, challengeResponse(request, finalTarget = "https://final.test/landing"))
        }
        assertEquals(final, error.origin)
        assertSame(finalClearance, store.current(final), "a generation from another origin must not invalidate")
        assertNull(store.pendingChallenge(origin))
    }

    @Test
    fun `a challenge on a cross-origin redirect hop resolves that hop origin on the next attempt`(): Unit = runBlocking {
        val hop = WebOrigin.fromTarget("https://www.example.test/")
        val request = CrawlRequest("http://www.example.test/x.jpg")
        val error = assertFailsWith<ChallengeDetectedException> {
            respond(request, challengeResponse(request, finalTarget = "https://www.example.test/x.jpg"))
        }
        assertEquals(hop, error.origin)
        assertNotNull(store.pendingChallenge(hop))
        assertNull(store.pendingChallenge(WebOrigin.fromTarget(request.target)))
        assertTrue(resolver.calls.isEmpty(), "detection never resolves")

        send(request)
        assertEquals(
            listOf(ChallengeResolutionRequest(hop, "https://www.example.test/x.jpg", ChallengeKind.CLOUDFLARE)),
            resolver.calls,
        )
        assertNotNull(store.current(hop))
        assertSame(request, forwarded.single().first, "the hop receives clearance from the cookie session, not the request")

        send(request)
        assertEquals(1, resolver.calls.size, "the hop clearance is reused")
    }

    @Test
    fun `a request origin and a linked hop origin are each resolved with their own target`(): Unit = runBlocking {
        val hop = WebOrigin.fromTarget("https://hop.test/")
        val request = CrawlRequest("$SITE/start", headers = Headers.of(Header("User-Agent", "TrawlerBot/0.1")))
        assertFailsWith<ChallengeDetectedException> {
            respond(request, challengeResponse(request, finalTarget = "https://hop.test/landing"))
        }
        store.recordChallenge(origin, ChallengeKind.CLOUDFLARE, null)

        send(request)
        assertEquals(
            listOf(
                ChallengeResolutionRequest(origin, "$SITE/start", ChallengeKind.CLOUDFLARE),
                ChallengeResolutionRequest(hop, "https://hop.test/landing", ChallengeKind.CLOUDFLARE),
            ),
            resolver.calls,
        )
        val sent = forwarded.single().first
        assertEquals(listOf(BROWSER_UA), sent.headers["User-Agent"])
        assertEquals(store.current(origin)!!.generation.toString(), sent.attributes[ChallengeClearance.GENERATION_ATTRIBUTE])
        assertNotNull(store.current(hop))
    }

    @Test
    fun `a failed hop resolution fails the attempt before sending and cools the hop origin down`(): Unit = runBlocking {
        val hop = WebOrigin.fromTarget("https://hop.test/")
        val request = CrawlRequest("$SITE/start")
        assertFailsWith<ChallengeDetectedException> {
            respond(request, challengeResponse(request, finalTarget = "https://hop.test/landing"))
        }
        resolver.behavior = { throw IOException("down") }
        val failure = assertFailsWith<ChallengeResolutionException> { send(request) }
        assertEquals("resolver-failed", failure.reason)
        assertTrue(forwarded.isEmpty())
        assertTrue(store.inResolutionCooldown(hop))
        assertFalse(store.inResolutionCooldown(origin))
    }

    @Test
    fun `a hop challenge invalidates the hop clearance only when the reported hop carried its cookies`(): Unit = runBlocking {
        val hop = WebOrigin.fromTarget("https://hop.test/")
        val stored = store.store(clearanceFor(hop, token = "hop-token"))
        val request = CrawlRequest("$SITE/start")
        fun challengeVia(sent: CrawlRequest) = CrawlResponse(
            request = sent,
            statusCode = 403,
            headers = Headers.of(Header("cf-mitigated", "challenge")),
            body = ResponseBody.from("challenge".toByteArray()),
            receivedAt = clock.now(),
            finalTarget = "https://hop.test/landing",
        )
        fun hopRequest(vararg headers: Header) = CrawlRequest("https://hop.test/landing", headers = Headers.of(*headers))
        listOf(
            hopRequest(),
            hopRequest(Header("Cookie", "jar=1; cf_clearance=old-token"), Header("User-Agent", BROWSER_UA)),
            CrawlRequest("$SITE/start", headers = Headers.of(Header("Cookie", "cf_clearance=hop-token"))),
        ).forEach { sent ->
            assertFailsWith<ChallengeDetectedException> { respond(request, challengeVia(sent)) }
            assertSame(stored, store.current(hop), "a hop without the current clearance must not invalidate it")
            assertNull(store.pendingChallenge(hop))
        }

        assertFailsWith<ChallengeDetectedException> {
            respond(request, challengeVia(hopRequest(Header("cookie", "jar=1;cf_clearance=hop-token"))))
        }
        assertNull(store.current(hop))
        assertNotNull(store.pendingChallenge(hop))
        send(request)
        assertEquals("https://hop.test/landing", resolver.calls.single().target)
    }

    @Test
    fun `a cookie-less hop clearance counts as sent when the hop carried its user agent`(): Unit = runBlocking {
        val hop = WebOrigin.fromTarget("https://hop.test/")
        val stored = store.store(Clearance(hop, emptyList(), BROWSER_UA, clock.now()))
        val request = CrawlRequest("$SITE/start")
        fun challengeWith(userAgent: String): CrawlResponse {
            val sent = CrawlRequest("https://hop.test/landing", headers = Headers.of(Header("User-Agent", userAgent)))
            return challengeResponse(sent, finalTarget = sent.target)
        }
        assertFailsWith<ChallengeDetectedException> { respond(request, challengeWith("TrawlerBot/0.1")) }
        assertSame(stored, store.current(hop))
        assertFailsWith<ChallengeDetectedException> { respond(request, challengeWith(BROWSER_UA)) }
        assertNull(store.current(hop))
    }

    @Test
    fun `a secure-only clearance challenged on plain http fails as not applicable instead of retrying`(): Unit = runBlocking {
        val plain = WebOrigin.fromTarget("http://www.example.test/")
        val stored = store.store(Clearance(plain, listOf(ClearanceCookie("sucuri", "v", secure = true)), BROWSER_UA, clock.now()))
        val request = CrawlRequest("$SITE/")
        var called = false
        val error = assertFailsWith<ChallengeResolutionException> {
            respond(
                request,
                challengeResponse(request, finalTarget = "http://www.example.test/wp/"),
                ResponseHandler { r, _ -> called = true; r },
            )
        }
        assertFalse(called)
        assertEquals("clearance-not-applicable", error.reason)
        assertEquals(ChallengeKind.CLOUDFLARE, error.kind)
        assertEquals("Challenge 'cloudflare' resolution failed: clearance-not-applicable", error.message)
        assertSame(stored, store.current(plain))
        assertNull(store.pendingChallenge(plain))
        assertNull(store.redirectedChallenge(request.target))

        val direct = CrawlRequest("http://www.example.test/wp/")
        assertEquals(
            "clearance-not-applicable",
            assertFailsWith<ChallengeResolutionException> { respond(direct, challengeResponse(direct)) }.reason,
        )

        store.store(
            Clearance(
                plain,
                listOf(ClearanceCookie("sucuri", "v", secure = true), ClearanceCookie("plain", "p")),
                BROWSER_UA,
                clock.now(),
            ),
        )
        assertFailsWith<ChallengeDetectedException> { respond(direct, challengeResponse(direct)) }

        store.store(
            Clearance(
                plain,
                listOf(ClearanceCookie("sucuri", "v", secure = true, expiresAt = clock.now().plusSeconds(1))),
                BROWSER_UA,
                clock.now(),
            ),
        )
        clock.advance(Duration.ofSeconds(1))
        assertFailsWith<ChallengeDetectedException>("expired cookies are not blocked by the scheme") {
            respond(direct, challengeResponse(direct))
        }
    }

    @Test
    fun `challenges on responses without a web origin are passed through`(): Unit = runBlocking {
        val request = CrawlRequest("custom-scheme:resource")
        val response = challengeResponse(request, finalTarget = null)
        assertSame(response, respond(request, response))
    }

    @Test
    fun `a stale challenge does not invalidate a newer clearance but a current one does`(): Unit = runBlocking {
        store.store(clearanceFor(token = "first"))
        val newer = store.store(clearanceFor(token = "second"))
        fun tagged(generation: String) = CrawlRequest(
            "$SITE/",
            attributes = ContextAttributes.of(ContextAttribute(ChallengeClearance.GENERATION_ATTRIBUTE, generation)),
        )
        listOf(tagged("1"), tagged("not-a-number"), CrawlRequest("$SITE/")).forEach { stale ->
            assertFailsWith<ChallengeDetectedException> { respond(stale, challengeResponse(stale)) }
            assertSame(newer, store.current(origin))
        }
        assertTrue(resolver.calls.isEmpty())

        val current = tagged(newer.generation.toString())
        assertFailsWith<ChallengeDetectedException> { respond(current, challengeResponse(current)) }
        assertNull(store.current(origin))
        send(CrawlRequest("$SITE/again"))
        assertEquals(1, resolver.calls.size)
        assertEquals((newer.generation + 1).toString(), forwarded.single().first.attributes[ChallengeClearance.GENERATION_ATTRIBUTE])
    }

    @Test
    fun `lifecycle is forwarded to a lifecycle resolver and stop always clears the store`(): Unit = runBlocking {
        val events = CopyOnWriteArrayList<String>()
        val lifecycleResolver = object : ChallengeResolver, ExtensionLifecycle {
            override val id: String = "lifecycle-resolver"
            var failStop = false
            override suspend fun resolve(request: ChallengeResolutionRequest): Clearance = clearanceFor()
            override suspend fun start(context: ExtensionStartContext) { events += "start" }
            override suspend fun stop(context: ExtensionStopContext) {
                events += "stop:${context.reason}"
                if (failStop) throw IllegalStateException("stop failed")
            }
        }
        val owned = ChallengeClearance(lifecycleResolver, CloudflareChallengeDetector(), store)
        owned.start(START_CONTEXT)
        store.store(clearanceFor())
        owned.stop(STOP_CONTEXT)
        assertEquals(listOf("start", "stop:ENGINE_SHUTDOWN"), events)
        assertNull(store.current(origin))

        store.store(clearanceFor())
        lifecycleResolver.failStop = true
        assertFailsWith<IllegalStateException> { owned.stop(STOP_CONTEXT) }
        assertNull(store.current(origin))

        clearance.start(START_CONTEXT)
        store.store(clearanceFor())
        clearance.stop(STOP_CONTEXT)
        assertNull(store.current(origin))
    }

    @Test
    fun `challenge retry policy retries detected challenges visibly and delegates everything else`() {
        val delegated = CopyOnWriteArrayList<FailureClassification>()
        val delegate = object : RetryPolicy {
            override val id: String = "delegate"
            override fun decide(failure: FailureClassification, attempt: AttemptSummary, context: FailureContext): RetryDecision {
                delegated += failure
                return RetryDecision.Abandon("delegated")
            }
        }
        val policy = ChallengeRetryPolicy(delegate, maxChallengeAttempts = 2)
        assertEquals("challenge-retry+delegate", policy.id)
        val challenge = FailureClassification(FlareSolverrFailureCategories.CHALLENGE_DETECTED, true, "cloudflare")
        fun attempt(n: Int) = AttemptSummary(n, clock.now(), n - 1)
        assertEquals(RetryDecision.RetryNow, policy.decide(challenge, attempt(1), FAILURE_CONTEXT))
        assertEquals(RetryDecision.DoNotRetry, policy.decide(challenge, attempt(2), FAILURE_CONTEXT))
        assertEquals(RetryDecision.DoNotRetry, policy.decide(challenge, attempt(3), FAILURE_CONTEXT))
        assertTrue(delegated.isEmpty())
        val resolution = FailureClassification(FlareSolverrFailureCategories.CHALLENGE_RESOLUTION_FAILED, true, "resolver-failed")
        assertEquals(RetryDecision.Abandon("delegated"), policy.decide(resolution, attempt(1), FAILURE_CONTEXT))
        assertEquals(listOf(resolution), delegated)
        assertEquals(RetryDecision.RetryNow, ChallengeRetryPolicy(delegate).decide(challenge, attempt(2), FAILURE_CONTEXT))
        assertEquals(RetryDecision.DoNotRetry, ChallengeRetryPolicy(delegate).decide(challenge, attempt(3), FAILURE_CONTEXT))
        assertFailsWith<IllegalArgumentException> { ChallengeRetryPolicy(delegate, 0) }
    }
}
