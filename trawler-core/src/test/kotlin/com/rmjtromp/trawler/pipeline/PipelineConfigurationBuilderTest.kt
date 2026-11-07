package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.CursorHarvestStrategy
import com.rmjtromp.trawler.discovery.FixedOverlapBoundaryPolicy
import com.rmjtromp.trawler.discovery.StatelessHarvestStrategy
import com.rmjtromp.trawler.discovery.WatermarkHarvestStrategy
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.ExtractionContext
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.policy.ActiveWorkOnlyDeduplicationPolicy
import com.rmjtromp.trawler.policy.AllowAllAuthorizationPolicy
import com.rmjtromp.trawler.policy.AllowAllScopePolicy
import com.rmjtromp.trawler.policy.AlwaysEligiblePipelinePolicy
import com.rmjtromp.trawler.policy.AlwaysRecrawlPolicy
import com.rmjtromp.trawler.policy.AuthorizationPolicy
import com.rmjtromp.trawler.policy.BuiltInPolicies
import com.rmjtromp.trawler.policy.CircuitBreakerPolicy
import com.rmjtromp.trawler.policy.ConcurrencyPolicy
import com.rmjtromp.trawler.policy.Continuous
import com.rmjtromp.trawler.policy.CrawlBudgetPolicy
import com.rmjtromp.trawler.policy.CrawlScopePolicy
import com.rmjtromp.trawler.policy.DeduplicationPolicy
import com.rmjtromp.trawler.policy.DefaultFailureClassifier
import com.rmjtromp.trawler.policy.DigestRequestIdentityStrategy
import com.rmjtromp.trawler.policy.DiscoveryAdmissionPolicy
import com.rmjtromp.trawler.policy.FailureClassifier
import com.rmjtromp.trawler.policy.FifoWorkOrderingPolicy
import com.rmjtromp.trawler.policy.FixedConcurrencyPolicy
import com.rmjtromp.trawler.policy.IdentityRequestCanonicalizer
import com.rmjtromp.trawler.policy.ImmediatePacingPolicy
import com.rmjtromp.trawler.policy.NeverOpenCircuitBreakerPolicy
import com.rmjtromp.trawler.policy.NoOpPipelineHealthPolicy
import com.rmjtromp.trawler.policy.NoRetryPolicy
import com.rmjtromp.trawler.policy.PipelineEligibilityPolicy
import com.rmjtromp.trawler.policy.PipelineHealthPolicy
import com.rmjtromp.trawler.policy.RecrawlPolicy
import com.rmjtromp.trawler.policy.RequestCanonicalizer
import com.rmjtromp.trawler.policy.RequestIdentityStrategy
import com.rmjtromp.trawler.policy.RequestPacingPolicy
import com.rmjtromp.trawler.policy.RetryPolicy
import com.rmjtromp.trawler.policy.UnlimitedBudgetPolicy
import com.rmjtromp.trawler.policy.WorkOrderingPolicy
import com.rmjtromp.trawler.transport.RequestExecutor
import com.rmjtromp.trawler.transport.RequestMiddleware
import com.rmjtromp.trawler.transport.ResponseHandler
import com.rmjtromp.trawler.transport.ResponseMiddleware
import com.rmjtromp.trawler.transport.Transport
import com.rmjtromp.trawler.transport.TransportRouter
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

/**
 * Builder-level contracts for [PipelineBuilder]: single-cardinality policy
 * slots, explicit replacement, inheritance precedence, harvest selection,
 * and the immutable [Pipeline]/[PipelinePolicies] value surfaces.
 */
class PipelineConfigurationBuilderTest {
    private val base: PipelinePolicies = BuiltInPolicies.ordinary()

    @Test
    fun `every local policy setter overrides the inherited bundle slot without touching other slots`() {
        val local = LocalPolicies("local")
        val pipeline = local.applySetters(minimalBuilder("overrides")).build(PipelineDefaults(policies = base))

        assertEquals(local.ids(), pipeline.policies.slotIds())
        assertTrue(pipeline.policies.slotIds().values.none { it in base.slotIds().values })
    }

    @Test
    fun `unset policy slots keep the inherited bundle values`() {
        val retry = renamedRetry("local-retry")
        val pipeline = minimalBuilder("partial").retry(retry).build(PipelineDefaults(policies = base))

        assertSame(retry, pipeline.policies.retry)
        assertEquals(base.copy(retry = retry), pipeline.policies)
    }

    @Test
    fun `local policy overrides apply on top of a local bundle rather than the inherited one`() {
        val inherited = base.copy(recrawl = renamedRecrawl("inherited-recrawl"))
        val localBundle = base.copy(retry = renamedRetry("bundle-retry"))
        val pacing = renamedPacing("local-pacing")

        val pipeline = minimalBuilder("bundle")
            .policies(localBundle)
            .pacing(pacing)
            .build(PipelineDefaults(policies = inherited))

        assertEquals("bundle-retry", pipeline.policies.retry.id)
        assertSame(pacing, pipeline.policies.pacing)
        assertSame(AlwaysRecrawlPolicy, pipeline.policies.recrawl)
    }

    @Test
    fun `setting any policy slot twice reports a deterministic duplicate issue naming the real replace method`() {
        val first = LocalPolicies("first")
        val second = LocalPolicies("second")
        val builder = second.applySetters(first.applySetters(minimalBuilder("twice")))

        val issues = builder.validationIssues(PipelineDefaults(policies = base))

        val expectedMethods = mapOf(
            "requestCanonicalizer" to "replaceCanonicalizer",
            "requestIdentity" to "replaceIdentity",
            "deduplication" to "replaceDeduplication",
            "recrawl" to "replaceRecrawl",
            "failureClassifier" to "replaceFailureClassifier",
            "retry" to "replaceRetry",
            "pacing" to "replacePacing",
            "concurrency" to "replaceConcurrency",
            "scope" to "replaceScope",
            "authorization" to "replaceAuthorization",
            "ordering" to "replaceOrdering",
            "budget" to "replaceBudget",
            "admission" to "replaceAdmission",
            "eligibility" to "replaceEligibility",
            "health" to "replaceHealth",
            "circuitBreaker" to "replaceCircuitBreaker",
        )
        val expected = expectedMethods.map { (slot, method) ->
            ConfigurationIssue(
                path = "pipeline[twice].$slot",
                code = ConfigurationIssueCode.DUPLICATE_SINGLE_EXTENSION,
                message = "Multiple local registrations require an explicit $method call",
            )
        }.sortedBy { it.path }
        assertEquals(expected, issues)
        // Every named method is a real public builder method.
        val methods = PipelineBuilder::class.java.methods.map { it.name }.toSet()
        assertTrue(expectedMethods.values.all { it in methods }, "diagnostic named a missing method")

        val error = assertFailsWith<InvalidConfigurationException> { builder.build(PipelineDefaults(policies = base)) }
        assertEquals(expected, error.issues)
    }

    @Test
    fun `explicit replace methods replace an earlier local policy without issues`() {
        val first = LocalPolicies("first")
        val second = LocalPolicies("second")
        val builder = second.applyReplacements(first.applySetters(minimalBuilder("replaced")))

        assertEquals(emptyList(), builder.validationIssues(PipelineDefaults(policies = base)))
        assertEquals(second.ids(), builder.build(PipelineDefaults(policies = base)).policies.slotIds())
    }

    @Test
    fun `replace methods also work without a prior local registration`() {
        val local = LocalPolicies("only-replace")
        val pipeline = local.applyReplacements(minimalBuilder("replace-only")).build(PipelineDefaults(policies = base))

        assertEquals(local.ids(), pipeline.policies.slotIds())
    }

    @Test
    fun `blank policy identifiers are rejected per slot`() {
        val issues = minimalBuilder("blank")
            .retry(renamedRetry(" "))
            .validationIssues(PipelineDefaults(policies = base))

        assertEquals(
            listOf(
                ConfigurationIssue(
                    "pipeline[blank].policies.retry",
                    ConfigurationIssueCode.INVALID_IDENTIFIER,
                    "Extension identifier must not be blank",
                ),
            ),
            issues,
        )

        // Engine validation must agree with the engine build boundary for local slot overrides.
        val engine = EngineConfigurationBuilder()
            .register<Int>("blank") {
                transport(NamedTransport("transport"))
                extract(NamedExtractor("extractor"))
                retry(renamedRetry(""))
            }
        assertEquals(issues, engine.validate())
        assertEquals(issues, assertFailsWith<InvalidConfigurationException> { engine.build() }.issues)
    }

    @Test
    fun `harvest strategy defaults to stateless, is single-assignment, and supports explicit replacement`() {
        assertSame(StatelessHarvestStrategy, minimalBuilder("default-harvest").build(PipelineDefaults(policies = base)).harvestStrategy)

        val cursor = CursorHarvestStrategy("cursor-a")
        val watermark = WatermarkHarvestStrategy(FixedOverlapBoundaryPolicy(Duration.ofMinutes(1)), id = "watermark-b")
        val duplicate = minimalBuilder("duplicate-harvest").harvest(cursor).harvest(watermark)
        assertEquals(
            listOf(
                ConfigurationIssue(
                    "pipeline[duplicate-harvest].harvest",
                    ConfigurationIssueCode.DUPLICATE_SINGLE_EXTENSION,
                    "Multiple local registrations require an explicit replaceHarvest call",
                ),
            ),
            duplicate.validationIssues(PipelineDefaults(policies = base)),
        )

        val replaced = minimalBuilder("replaced-harvest").harvest(cursor).replaceHarvest(watermark)
            .build(PipelineDefaults(policies = base))
        assertSame(watermark, replaced.harvestStrategy)

        val replacedWithoutPrior = minimalBuilder("replace-only-harvest").replaceHarvest(cursor)
            .build(PipelineDefaults(policies = base))
        assertSame(cursor, replacedWithoutPrior.harvestStrategy)
    }

    @Test
    fun `blank harvest strategy identifiers are rejected`() {
        val blank = object : com.rmjtromp.trawler.discovery.HarvestStrategy by StatelessHarvestStrategy {
            override val id: String = ""
        }

        val issues = minimalBuilder("blank-harvest").harvest(blank).validationIssues(PipelineDefaults(policies = base))

        assertEquals(
            listOf(
                ConfigurationIssue(
                    "pipeline[blank-harvest].harvest",
                    ConfigurationIssueCode.INVALID_IDENTIFIER,
                    "Extension identifier must not be blank",
                ),
            ),
            issues,
        )
    }

    @Test
    fun `transport, extractor and policy bundle each require explicit replacement after a first local set`() {
        val issues = PipelineBuilder<Int>("singles")
            .transport(NamedTransport("t1"))
            .transport(NamedTransport("t2"))
            .extract(NamedExtractor("e1"))
            .extract(NamedExtractor("e2"))
            .policies(base)
            .policies(base)
            .validationIssues()

        assertEquals(
            listOf(
                "pipeline[singles].extractor" to "replaceExtractor",
                "pipeline[singles].policies" to "replacePolicies",
                "pipeline[singles].transport" to "replaceTransport",
            ),
            issues.map { it.path to it.message.removePrefix("Multiple local registrations require an explicit ").removeSuffix(" call") },
        )
        assertTrue(issues.all { it.code == ConfigurationIssueCode.DUPLICATE_SINGLE_EXTENSION })
    }

    @Test
    fun `fixed transport convenience exposes the transport ID and always selects that transport`() {
        val transport = NamedTransport("fixed-transport")
        val pipeline = PipelineBuilder<Int>("fixed")
            .transport(transport)
            .extract(NamedExtractor("extractor"))
            .build(PipelineDefaults(policies = base))

        assertEquals("fixed-transport", pipeline.transportRouter.id)
        val crawler = CrawlerContext("engine", "run", "fixed")
        listOf(CrawlRequest("a"), CrawlRequest("b")).forEach { request ->
            assertSame(transport, pipeline.transportRouter.select(request, RequestContext(crawler, request)))
        }
    }

    @Test
    fun `pre-built output bindings keep their requirement and declared order`() {
        val required = NamedOutput<Int>("required")
        val bestEffort = NamedOutput<Int>("best-effort")
        val pipeline = minimalBuilder("bindings")
            .output(OutputBinding(required, OutputRequirement.REQUIRED))
            .output(bestEffort, OutputRequirement.BEST_EFFORT)
            .build(PipelineDefaults(policies = base))

        assertEquals(listOf("required", "best-effort"), pipeline.outputs.map { it.output.id })
        assertEquals(
            listOf(OutputRequirement.REQUIRED, OutputRequirement.BEST_EFFORT),
            pipeline.outputs.map { it.requirement },
        )

        val duplicate = minimalBuilder("duplicate-bindings")
            .output(OutputBinding(required, OutputRequirement.REQUIRED))
            .output(required)
            .validationIssues(PipelineDefaults(policies = base))
        assertEquals(
            listOf("pipeline[duplicate-bindings].outputs" to ConfigurationIssueCode.DUPLICATE_ORDERED_EXTENSION),
            duplicate.map { it.path to it.code },
        )
    }

    @Test
    fun `response middleware replacement discards inherited response middleware before local additions`() {
        val inherited = NamedResponseMiddleware("inherited")
        val replacement = NamedResponseMiddleware("replacement")
        val local = NamedResponseMiddleware("local")
        val defaults = PipelineDefaults(
            transportRouter = NamedRouter("router"),
            policies = base,
            responseMiddlewares = listOf(inherited),
        )

        val pipeline = PipelineBuilder<Int>("response-replace")
            .extract(NamedExtractor("extractor"))
            .addResponseMiddleware(local)
            .replaceResponseMiddleware(listOf(replacement))
            .build(defaults)

        assertEquals(listOf("replacement", "local"), pipeline.responseMiddlewares.map { it.id })
        assertEquals(listOf("inherited"), defaults.responseMiddlewares.map { it.id })
    }

    @Test
    fun `middleware replacement copies the caller list and duplicate effective IDs are reported`() {
        val shared = NamedRequestMiddleware("shared")
        val replacements = mutableListOf<RequestMiddleware>(shared)
        val builder = minimalBuilder("copy")
            .replaceRequestMiddleware(replacements)
        replacements += NamedRequestMiddleware("late")

        assertEquals(listOf("shared"), builder.build(PipelineDefaults(policies = base)).requestMiddlewares.map { it.id })

        val issues = minimalBuilder("duplicate-middleware")
            .replaceResponseMiddleware(listOf(NamedResponseMiddleware("same")))
            .addResponseMiddleware(NamedResponseMiddleware("same"))
            .validationIssues(PipelineDefaults(policies = base))
        assertEquals(
            listOf("pipeline[duplicate-middleware].responseMiddleware" to ConfigurationIssueCode.DUPLICATE_ORDERED_EXTENSION),
            issues.map { it.path to it.code },
        )
    }

    @Test
    fun `pipeline defaults snapshot their middleware and none selects nothing`() {
        val none = PipelineDefaults.none()
        assertEquals(null, none.transportRouter)
        assertEquals(null, none.policies)
        assertEquals(emptyList(), none.requestMiddlewares)
        assertEquals(emptyList(), none.responseMiddlewares)

        val source = mutableListOf<RequestMiddleware>(NamedRequestMiddleware("a"))
        val defaults = PipelineDefaults(requestMiddlewares = source)
        source += NamedRequestMiddleware("b")
        assertEquals(listOf("a"), defaults.requestMiddlewares.map { it.id })
    }

    @Test
    fun `directly constructed pipelines use empty ordered lists, stateless harvest and immutable snapshots`() {
        val router = NamedRouter("router")
        val extractor = NamedExtractor<Int>("extractor")
        val pipeline = Pipeline(
            definition = PipelineDefinition("direct"),
            transportRouter = router,
            extractor = extractor,
            policies = base,
        )

        assertEquals("direct", pipeline.id)
        assertSame(router, pipeline.transportRouter)
        assertSame(extractor, pipeline.extractor)
        assertEquals(emptyList(), pipeline.discoveries)
        assertEquals(emptyList(), pipeline.requestMiddlewares)
        assertEquals(emptyList(), pipeline.responseMiddlewares)
        assertEquals(emptyList(), pipeline.transforms)
        assertEquals(emptyList(), pipeline.validators)
        assertEquals(emptyList(), pipeline.outputs)
        assertEquals(emptyList(), pipeline.extensions)
        assertSame(StatelessHarvestStrategy, pipeline.harvestStrategy)

        val transforms = mutableListOf<Transform<Int>>()
        val withTransforms = Pipeline(
            definition = PipelineDefinition("direct-copy"),
            transportRouter = router,
            extractor = extractor,
            transforms = transforms,
            policies = base,
        )
        transforms += object : Transform<Int> {
            override val id: String = "late"
            override suspend fun apply(value: Int, context: com.rmjtromp.trawler.model.TransformContext) =
                TransformDecision.Accept(value)
        }
        assertEquals(emptyList(), withTransforms.transforms)
    }

    @Test
    fun `policy bundle defaults select the documented no-op admission eligibility health and breaker policies`() {
        val policies = PipelinePolicies(
            requestCanonicalizer = IdentityRequestCanonicalizer,
            requestIdentity = DigestRequestIdentityStrategy,
            deduplication = ActiveWorkOnlyDeduplicationPolicy,
            recrawl = AlwaysRecrawlPolicy,
            failureClassifier = DefaultFailureClassifier,
            retry = NoRetryPolicy,
            pacing = ImmediatePacingPolicy,
            concurrency = FixedConcurrencyPolicy(1),
            scope = AllowAllScopePolicy,
            authorization = AllowAllAuthorizationPolicy,
            ordering = FifoWorkOrderingPolicy,
            budget = UnlimitedBudgetPolicy,
        )

        assertSame(Continuous, policies.admission)
        assertSame(AlwaysEligiblePipelinePolicy, policies.eligibility)
        assertSame(NoOpPipelineHealthPolicy, policies.health)
        assertSame(NeverOpenCircuitBreakerPolicy, policies.circuitBreaker)
    }

    @Test
    fun `top-level pipeline function supplies ordinary policies and still validates required slots`() {
        val built = pipeline<Int>("top-level") {
            transport(NamedTransport("transport"))
            extract(NamedExtractor("extractor"))
        }
        assertEquals(BuiltInPolicies.ordinary().slotIds(), built.policies.slotIds())

        val error = assertFailsWith<InvalidConfigurationException> {
            pipeline<Int>("top-level-missing") { transport(NamedTransport("transport")) }
        }
        assertEquals(listOf("pipeline[top-level-missing].extractor"), error.issues.map { it.path })
    }

    @Test
    fun `builder can be created from a definition and keeps it`() {
        val definition = PipelineDefinition("from-definition")
        val builder = PipelineBuilder<Int>(definition)

        assertSame(definition, builder.definition)
        assertSame(
            definition,
            builder.transport(NamedTransport("t")).extract(NamedExtractor("e")).build(PipelineDefaults(policies = base)).definition,
        )
    }

    @Test
    fun `issues with the same path and code are ordered by message`() {
        val issues = PipelineBuilder<Int>("tie")
            .discover(NamedDiscovery(""))
            .discover(NamedDiscovery("x"))
            .discover(NamedDiscovery("x"))
            .discover(NamedDiscovery(" "))
            .transport(NamedTransport("t"))
            .extract(NamedExtractor("e"))
            .validationIssues(PipelineDefaults(policies = base))

        assertEquals(
            listOf(
                ConfigurationIssueCode.DUPLICATE_ORDERED_EXTENSION to "Extension identifier 'x' is registered more than once in this ordered scope",
                ConfigurationIssueCode.INVALID_IDENTIFIER to "Extension identifier must not be blank",
                ConfigurationIssueCode.INVALID_IDENTIFIER to "Extension identifier must not be blank",
            ),
            issues.map { it.code to it.message },
        )
        val exception = InvalidConfigurationException(issues.reversed())
        assertEquals(issues, exception.issues)
        assertEquals(
            "Invalid Trawler configuration:\n" + issues.joinToString("\n") { "- ${it.path} [${it.code}]: ${it.message}" },
            exception.message,
        )
        val mixed = InvalidConfigurationException(
            listOf(
                ConfigurationIssue("p", ConfigurationIssueCode.INVALID_IDENTIFIER, "b"),
                ConfigurationIssue("p", ConfigurationIssueCode.INVALID_IDENTIFIER, "a"),
                ConfigurationIssue("p", ConfigurationIssueCode.DUPLICATE_PIPELINE_ID, "z"),
            ),
        )
        assertEquals(listOf("z", "a", "b"), mixed.issues.map { it.message })
    }

    private fun minimalBuilder(id: String): PipelineBuilder<Int> =
        PipelineBuilder<Int>(id)
            .transport(NamedRouter("router-$id"))
            .extract(NamedExtractor("extractor-$id"))

    /** One renamed delegate per policy slot so precedence is observable by ID. */
    private class LocalPolicies(prefix: String) {
        val canonicalizer = object : RequestCanonicalizer by IdentityRequestCanonicalizer {
            override val id = "$prefix-canonicalizer"
        }
        val identity = object : RequestIdentityStrategy by DigestRequestIdentityStrategy {
            override val id = "$prefix-identity"
        }
        val deduplication = object : DeduplicationPolicy by ActiveWorkOnlyDeduplicationPolicy {
            override val id = "$prefix-deduplication"
        }
        val recrawl = renamedRecrawl("$prefix-recrawl")
        val failureClassifier = object : FailureClassifier by DefaultFailureClassifier {
            override val id = "$prefix-failure"
        }
        val retry = renamedRetry("$prefix-retry")
        val pacing = renamedPacing("$prefix-pacing")
        val concurrency = object : ConcurrencyPolicy by FixedConcurrencyPolicy(2) {
            override val id = "$prefix-concurrency"
        }
        val scope = object : CrawlScopePolicy by AllowAllScopePolicy {
            override val id = "$prefix-scope"
        }
        val authorization = object : AuthorizationPolicy by AllowAllAuthorizationPolicy {
            override val id = "$prefix-authorization"
        }
        val ordering = object : WorkOrderingPolicy by FifoWorkOrderingPolicy {
            override val id = "$prefix-ordering"
        }
        val budget = object : CrawlBudgetPolicy by UnlimitedBudgetPolicy {
            override val id = "$prefix-budget"
        }
        val admission = object : DiscoveryAdmissionPolicy by Continuous {
            override val id = "$prefix-admission"
        }
        val eligibility = object : PipelineEligibilityPolicy by AlwaysEligiblePipelinePolicy {
            override val id = "$prefix-eligibility"
        }
        val health = object : PipelineHealthPolicy by NoOpPipelineHealthPolicy {
            override val id = "$prefix-health"
        }
        val circuitBreaker = object : CircuitBreakerPolicy by NeverOpenCircuitBreakerPolicy {
            override val id = "$prefix-breaker"
        }

        fun <T : Any> applySetters(builder: PipelineBuilder<T>): PipelineBuilder<T> = builder
            .canonicalize(canonicalizer)
            .identity(identity)
            .deduplicate(deduplication)
            .recrawl(recrawl)
            .classifyFailures(failureClassifier)
            .retry(retry)
            .pacing(pacing)
            .concurrency(concurrency)
            .scope(scope)
            .authorize(authorization)
            .ordering(ordering)
            .budget(budget)
            .admission(admission)
            .eligibility(eligibility)
            .health(health)
            .circuitBreaker(circuitBreaker)

        fun <T : Any> applyReplacements(builder: PipelineBuilder<T>): PipelineBuilder<T> = builder
            .replaceCanonicalizer(canonicalizer)
            .replaceIdentity(identity)
            .replaceDeduplication(deduplication)
            .replaceRecrawl(recrawl)
            .replaceFailureClassifier(failureClassifier)
            .replaceRetry(retry)
            .replacePacing(pacing)
            .replaceConcurrency(concurrency)
            .replaceScope(scope)
            .replaceAuthorization(authorization)
            .replaceOrdering(ordering)
            .replaceBudget(budget)
            .replaceAdmission(admission)
            .replaceEligibility(eligibility)
            .replaceHealth(health)
            .replaceCircuitBreaker(circuitBreaker)

        fun ids(): Map<String, String> = mapOf(
            "requestCanonicalizer" to canonicalizer.id,
            "requestIdentity" to identity.id,
            "deduplication" to deduplication.id,
            "recrawl" to recrawl.id,
            "failureClassifier" to failureClassifier.id,
            "retry" to retry.id,
            "pacing" to pacing.id,
            "concurrency" to concurrency.id,
            "scope" to scope.id,
            "authorization" to authorization.id,
            "ordering" to ordering.id,
            "budget" to budget.id,
            "admission" to admission.id,
            "eligibility" to eligibility.id,
            "health" to health.id,
            "circuitBreaker" to circuitBreaker.id,
        )
    }

    private class NamedRouter(override val id: String) : TransportRouter {
        private val transport = NamedTransport("transport-$id")

        override fun select(request: CrawlRequest, context: RequestContext): Transport = transport
    }

    private class NamedTransport(override val id: String) : Transport {
        override suspend fun execute(request: CrawlRequest): CrawlResponse = CrawlResponse(
            request = request,
            statusCode = 200,
            headers = Headers.empty(),
            body = ResponseBody.from(ByteArray(0)),
            receivedAt = Instant.EPOCH,
        )
    }

    private class NamedExtractor<T : Any>(override val id: String) : Extractor<T> {
        override suspend fun extract(response: CrawlResponse, context: ExtractionContext): ExtractionResult<T> =
            ExtractionResult.Ignore("test")
    }

    private class NamedDiscovery(override val id: String) : com.rmjtromp.trawler.discovery.Discovery {
        override fun discover(context: com.rmjtromp.trawler.discovery.DiscoveryContext) =
            kotlinx.coroutines.flow.emptyFlow<com.rmjtromp.trawler.discovery.DiscoveryEmission>()
    }

    private class NamedOutput<T : Any>(override val id: String) : Output<T> {
        override suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult =
            OutputResult.Accepted(items.size)
    }

    private class NamedRequestMiddleware(override val id: String) : RequestMiddleware {
        override suspend fun intercept(
            request: CrawlRequest,
            context: RequestContext,
            next: RequestExecutor,
        ): CrawlResponse = next.execute(request, context)
    }

    private class NamedResponseMiddleware(override val id: String) : ResponseMiddleware {
        override suspend fun intercept(
            response: CrawlResponse,
            context: com.rmjtromp.trawler.model.ResponseContext,
            next: ResponseHandler,
        ): CrawlResponse = next.handle(response, context)
    }

    private companion object {
        fun renamedRetry(name: String): RetryPolicy = object : RetryPolicy by NoRetryPolicy {
            override val id = name
        }

        fun renamedRecrawl(name: String): RecrawlPolicy = object : RecrawlPolicy by AlwaysRecrawlPolicy {
            override val id = name
        }

        fun renamedPacing(name: String): RequestPacingPolicy = object : RequestPacingPolicy by ImmediatePacingPolicy {
            override val id = name
        }

        fun PipelinePolicies.slotIds(): Map<String, String> = mapOf(
            "requestCanonicalizer" to requestCanonicalizer.id,
            "requestIdentity" to requestIdentity.id,
            "deduplication" to deduplication.id,
            "recrawl" to recrawl.id,
            "failureClassifier" to failureClassifier.id,
            "retry" to retry.id,
            "pacing" to pacing.id,
            "concurrency" to concurrency.id,
            "scope" to scope.id,
            "authorization" to authorization.id,
            "ordering" to ordering.id,
            "budget" to budget.id,
            "admission" to admission.id,
            "eligibility" to eligibility.id,
            "health" to health.id,
            "circuitBreaker" to circuitBreaker.id,
        )
    }
}
