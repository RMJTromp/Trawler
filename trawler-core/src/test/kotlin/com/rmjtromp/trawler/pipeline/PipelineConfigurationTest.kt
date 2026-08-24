package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.Discovery
import com.rmjtromp.trawler.discovery.DiscoveryContext
import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.event.Observer
import com.rmjtromp.trawler.event.ObserverResult
import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.ExtractionContext
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.model.TransformContext
import com.rmjtromp.trawler.model.ValidationContext
import com.rmjtromp.trawler.output.OutputLifecycle
import com.rmjtromp.trawler.policy.BuiltInPolicies
import com.rmjtromp.trawler.policy.NoRetryPolicy
import com.rmjtromp.trawler.response.LatestTargetResponseCachePolicy
import com.rmjtromp.trawler.response.ResponseLookup
import com.rmjtromp.trawler.response.ResponseStore
import com.rmjtromp.trawler.response.ResponseStoreCapability
import com.rmjtromp.trawler.response.StoredResponse
import com.rmjtromp.trawler.state.CrawlStateCapability
import com.rmjtromp.trawler.state.CrawlStateStore
import com.rmjtromp.trawler.transport.RequestExecutor
import com.rmjtromp.trawler.transport.RequestMiddleware
import com.rmjtromp.trawler.transport.ResponseHandler
import com.rmjtromp.trawler.transport.ResponseMiddleware
import com.rmjtromp.trawler.transport.Transport
import com.rmjtromp.trawler.transport.TransportRouter
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.junit.jupiter.api.Test

class PipelineConfigurationTest {
    @Test
    fun `pipeline builder inherits defaults and preserves ordered extensions`() {
        val inheritedRequest = NamedRequestMiddleware("inherited-request")
        val localRequest = NamedRequestMiddleware("local-request")
        val inheritedResponse = NamedResponseMiddleware("inherited-response")
        val localResponse = NamedResponseMiddleware("local-response")
        val defaults = defaults(
            requestMiddlewares = listOf(inheritedRequest),
            responseMiddlewares = listOf(inheritedResponse),
        )
        val discoveryOne = NamedDiscovery("discovery-one")
        val discoveryTwo = NamedDiscovery("discovery-two")
        val transformOne = NamedTransform<Int>("transform-one")
        val transformTwo = NamedTransform<Int>("transform-two")
        val validator = NamedValidator<Int>("validator")
        val output = NamedOutput<Int>("output")
        val extension = NamedExtension("extension")

        val pipeline = PipelineBuilder<Int>("typed")
            .extract(NamedExtractor("extractor"))
            .discover(discoveryOne)
            .discover(discoveryTwo)
            .addRequestMiddleware(localRequest)
            .addResponseMiddleware(localResponse)
            .transform(transformOne)
            .transform(transformTwo)
            .validator(validator)
            .output(output)
            .extension(extension)
            .build(defaults)

        assertSame(defaults.transportRouter, pipeline.transportRouter)
        assertEquals(listOf("inherited-request", "local-request"), pipeline.requestMiddlewares.map { it.id })
        assertEquals(listOf("inherited-response", "local-response"), pipeline.responseMiddlewares.map { it.id })
        assertEquals(listOf("discovery-one", "discovery-two"), pipeline.discoveries.map { it.id })
        assertEquals(listOf("transform-one", "transform-two"), pipeline.transforms.map { it.id })
        assertEquals(listOf("validator"), pipeline.validators.map { it.id })
        assertEquals(listOf("output"), pipeline.outputs.map { it.output.id })
        assertEquals(listOf("extension"), pipeline.extensions.map { it.id })
        @Suppress("UNCHECKED_CAST")
        val mutableDiscoveries = pipeline.discoveries as MutableList<Discovery>
        assertFailsWith<UnsupportedOperationException> { mutableDiscoveries += NamedDiscovery("forbidden") }
    }

    @Test
    fun `pipeline replacement is explicit and replaces inherited single values`() {
        val firstTransport = NamedRouter("first-transport")
        val secondTransport = NamedRouter("second-transport")
        val firstExtractor = NamedExtractor<Int>("first-extractor")
        val secondExtractor = NamedExtractor<Int>("second-extractor")
        val firstPolicies = BuiltInPolicies.ordinary()
        val secondPolicies = firstPolicies.copy(retry = NoRetryPolicy)
        val inherited = NamedRequestMiddleware("inherited")
        val replacement = NamedRequestMiddleware("replacement")

        val pipeline = PipelineBuilder<Int>("replaced")
            .transport(firstTransport)
            .replaceTransport(secondTransport)
            .extract(firstExtractor)
            .replaceExtractor(secondExtractor)
            .policies(firstPolicies)
            .replacePolicies(secondPolicies)
            .replaceRequestMiddleware(listOf(replacement))
            .build(
                PipelineDefaults(
                    transportRouter = firstTransport,
                    policies = firstPolicies,
                    requestMiddlewares = listOf(inherited),
                ),
            )

        assertSame(secondTransport, pipeline.transportRouter)
        assertSame(secondExtractor, pipeline.extractor)
        assertSame(NoRetryPolicy, pipeline.policies.retry)
        assertEquals(listOf("replacement"), pipeline.requestMiddlewares.map { it.id })
    }

    @Test
    fun `duplicate single registrations and duplicate ordered identifiers are deterministic`() {
        val firstRouter = NamedRouter("router-one")
        val secondRouter = NamedRouter("router-two")
        val builder = PipelineBuilder<Int>("duplicates")
            .transport(firstRouter)
            .transport(secondRouter)
            .extract(NamedExtractor("extractor-one"))
            .extract(NamedExtractor("extractor-two"))
            .retry(NoRetryPolicy)
            .retry(NoRetryPolicy)
            .discover(NamedDiscovery("same-discovery"))
            .discover(NamedDiscovery("same-discovery"))

        val issues = builder.validationIssues(defaults())
        assertTrue(issues.any { it.code == ConfigurationIssueCode.DUPLICATE_SINGLE_EXTENSION && it.path.endsWith(".transport") })
        assertTrue(issues.any { it.code == ConfigurationIssueCode.DUPLICATE_SINGLE_EXTENSION && it.path.endsWith(".extractor") })
        assertTrue(issues.any { it.code == ConfigurationIssueCode.DUPLICATE_SINGLE_EXTENSION && it.path.endsWith(".retry") })
        assertTrue(issues.any { it.code == ConfigurationIssueCode.DUPLICATE_ORDERED_EXTENSION && it.path.endsWith(".discoveries") })

        val replacement = PipelineBuilder<Int>("duplicates-fixed")
            .transport(firstRouter)
            .replaceTransport(secondRouter)
            .extract(NamedExtractor("extractor-one"))
            .replaceExtractor(NamedExtractor("extractor-two"))
            .retry(NoRetryPolicy)
            .replaceRetry(NoRetryPolicy)
            .build(defaults())
        assertEquals("router-two", replacement.transportRouter.id)
        assertEquals("extractor-two", replacement.extractor.id)
    }

    @Test
    fun `missing requirements and diagnostics are stable before runtime construction`() {
        val builder = PipelineBuilder<Int>("missing")
            .discover(NamedDiscovery(" "))
            .discover(NamedDiscovery("duplicate"))
            .discover(NamedDiscovery("duplicate"))

        val issues = builder.validationIssues()
        val sorted = issues.sortedWith(compareBy<ConfigurationIssue>({ it.path }, { it.code.name }, { it.message }))
        assertEquals(sorted, issues)
        assertTrue(issues.any { it.path.endsWith(".transport") && it.code == ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION })
        assertTrue(issues.any { it.path.endsWith(".extractor") && it.code == ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION })
        assertTrue(issues.any { it.path.endsWith(".policies") && it.code == ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION })
        assertTrue(issues.any { it.path.endsWith(".discoveries") && it.code == ConfigurationIssueCode.INVALID_IDENTIFIER })
        assertTrue(issues.any { it.path.endsWith(".discoveries") && it.code == ConfigurationIssueCode.DUPLICATE_ORDERED_EXTENSION })

        val exception = assertFailsWith<InvalidConfigurationException> { builder.build() }
        assertEquals(issues, exception.issues)
        assertTrue(exception.message!!.contains("pipeline[missing].transport"))
    }

    @Test
    fun `engine registry accepts unrelated typed pipelines and rejects duplicate IDs`() {
        val router = NamedRouter("router")
        val config = EngineConfigurationBuilder()
            .transportRouter(router)
            .policies(BuiltInPolicies.ordinary())
            .register<Int>("numbers") { extract(NamedExtractor("numbers-extractor")) }
            .register<String>("words") { extract(NamedExtractor("words-extractor")) }
            .build()

        assertEquals(listOf("numbers", "words"), config.pipelines.map { it.id })
        assertEquals(router.id, config.pipelines[0].transportRouter.id)

        val duplicate = EngineConfigurationBuilder()
            .transportRouter(router)
            .policies(BuiltInPolicies.ordinary())
            .register(validPipeline<Int>("same"))
            .register(validPipeline<String>("same"))
        val error = assertFailsWith<InvalidConfigurationException> { duplicate.build() }
        assertTrue(error.issues.any { it.code == ConfigurationIssueCode.DUPLICATE_PIPELINE_ID })
    }

    @Test
    fun `capability and interface mismatches fail before work`() {
        val stateError = assertFailsWith<InvalidConfigurationException> {
            EngineConfigurationBuilder()
                .stateStore(AdvertisedStateStore())
                .build()
        }
        assertTrue(stateError.issues.any {
            it.path == "engine.stateStore.runtime.WORK_QUEUE" &&
                it.code == ConfigurationIssueCode.MISSING_RUNTIME_CAPABILITY
        })

        val responseError = assertFailsWith<InvalidConfigurationException> {
            EngineConfigurationBuilder()
                .responseStore(AdvertisedResponseStore())
                .build()
        }
        assertTrue(responseError.issues.any {
            it.path == "engine.responseStore.capabilities.HISTORY" &&
                it.code == ConfigurationIssueCode.MISSING_RUNTIME_CAPABILITY
        })

        val requiredError = assertFailsWith<InvalidConfigurationException> {
            EngineConfigurationBuilder()
                .requireStateCapability(CrawlStateCapability.DURABLE)
                .build()
        }
        assertTrue(requiredError.issues.any {
            it.path == "engine.stateStore.capabilities.DURABLE" &&
                it.code == ConfigurationIssueCode.UNSUPPORTED_CAPABILITY
        })

        val cacheError = assertFailsWith<InvalidConfigurationException> {
            EngineConfigurationBuilder()
                .responseStore(AdvertisedResponseStore(setOf(ResponseStoreCapability.LOOKUP)))
                .responseCache(LatestTargetResponseCachePolicy())
                .build()
        }
        assertTrue(cacheError.issues.any {
            it.path == "engine.responseCache.capabilities.CACHE" &&
                it.code == ConfigurationIssueCode.UNSUPPORTED_CAPABILITY
        })
    }

    @Test
    fun `required response capture and lifecycle registration are validated`() {
        val requiredCaptureError = assertFailsWith<InvalidConfigurationException> {
            EngineConfigurationBuilder()
                .responsePolicy(com.rmjtromp.trawler.response.RequireResponseCapture)
                .build()
        }
        assertTrue(requiredCaptureError.issues.any {
            it.path == "engine.responsePolicy" &&
                it.code == ConfigurationIssueCode.MISSING_RUNTIME_CAPABILITY
        })

        val responseStore = LifecycleResponseStore()
        val missingRegistration = assertFailsWith<InvalidConfigurationException> {
            EngineConfigurationBuilder().responseStore(responseStore).build()
        }
        assertTrue(missingRegistration.issues.any { it.path == "engine.responseStore.registration" })

        val registered = EngineConfigurationBuilder()
            .responseStore(responseStore)
            .extension(responseStore)
            .build()
        assertEquals(listOf(responseStore), registered.extensions)

        val output = LifecycleOutput<Int>()
        val pipeline = validPipeline("output", output = output)
        val missingOutputRegistration = assertFailsWith<InvalidConfigurationException> {
            EngineConfigurationBuilder().register(pipeline).build()
        }
        assertTrue(missingOutputRegistration.issues.any {
            it.path == "pipeline[output].output[${output.id}].registration"
        })

        val outputRegistered = EngineConfigurationBuilder()
            .register(pipeline)
            .extension(output)
            .build()
        assertEquals(listOf(output), outputRegistered.extensions)

        val pipelineExtension = NamedExtension("pipeline-extension")
        val pipelineWithExtension = validPipeline<Int>("extension", extension = pipelineExtension)
        val duplicateLifecycle = assertFailsWith<InvalidConfigurationException> {
            EngineConfigurationBuilder()
                .register(pipelineWithExtension)
                .extension(pipelineExtension)
                .build()
        }
        assertTrue(duplicateLifecycle.issues.any {
            it.path == "pipeline[extension].extensions[pipeline-extension]" &&
                it.code == ConfigurationIssueCode.DUPLICATE_ORDERED_EXTENSION
        })
    }

    private fun defaults(
        requestMiddlewares: List<RequestMiddleware> = emptyList(),
        responseMiddlewares: List<ResponseMiddleware> = emptyList(),
    ): PipelineDefaults = PipelineDefaults(
        transportRouter = NamedRouter("default-router"),
        policies = BuiltInPolicies.ordinary(),
        requestMiddlewares = requestMiddlewares,
        responseMiddlewares = responseMiddlewares,
    )

    private fun <T : Any> validPipeline(
        id: String,
        output: Output<T>? = null,
        extension: ExtensionLifecycle? = null,
    ): Pipeline<T> {
        val builder = PipelineBuilder<T>(id)
            .transport(NamedRouter("router-$id"))
            .extract(NamedExtractor("extractor-$id"))
        if (output != null) builder.output(output)
        if (extension != null) builder.extension(extension)
        return builder.build(PipelineDefaults(policies = BuiltInPolicies.ordinary()))
    }

    private class NamedRouter(override val id: String) : TransportRouter {
        private val transport = NamedTransport("transport-$id")

        override fun select(request: CrawlRequest, context: RequestContext): Transport = transport
    }

    private class NamedTransport(override val id: String) : Transport {
        override suspend fun execute(request: CrawlRequest): CrawlResponse = error("not executed")
    }

    private class NamedExtractor<T : Any>(override val id: String) : Extractor<T> {
        override suspend fun extract(response: CrawlResponse, context: ExtractionContext): ExtractionResult<T> =
            ExtractionResult.Ignore("test")
    }

    private class NamedDiscovery(override val id: String) : Discovery {
        override fun discover(context: DiscoveryContext): Flow<DiscoveryEmission> = flowOf()
    }

    private class NamedTransform<T : Any>(override val id: String) : Transform<T> {
        override suspend fun apply(value: T, context: TransformContext): TransformDecision<T> =
            TransformDecision.Accept(value)
    }

    private class NamedValidator<T : Any>(override val id: String) : Validator<T> {
        override suspend fun validate(value: T, context: ValidationContext): ValidationResult =
            ValidationResult.Accepted
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

    private class NamedExtension(override val id: String) : ExtensionLifecycle {
        override suspend fun start(context: ExtensionStartContext) = Unit

        override suspend fun stop(context: ExtensionStopContext) = Unit
    }

    private class LifecycleOutput<T : Any> : Output<T>, OutputLifecycle, ExtensionLifecycle {
        override val id: String = "lifecycle-output"

        override suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult =
            OutputResult.Accepted(items.size)

        override suspend fun close() = Unit

        override suspend fun start(context: ExtensionStartContext) = Unit

        override suspend fun stop(context: ExtensionStopContext) = Unit
    }

    private class AdvertisedStateStore : CrawlStateStore {
        override val id: String = "invalid-state"
        override val capabilities: Set<CrawlStateCapability> = setOf(CrawlStateCapability.WORK_QUEUE)
    }

    private class AdvertisedResponseStore(
        override val capabilities: Set<ResponseStoreCapability> = setOf(ResponseStoreCapability.HISTORY),
    ) : ResponseStore {
        override val id: String = "invalid-response"

        override suspend fun put(response: CrawlResponse) = null

        override suspend fun find(query: ResponseLookup): StoredResponse? = null
    }

    private class LifecycleResponseStore : ResponseStore, ExtensionLifecycle {
        override val id: String = "lifecycle-response"
        override val capabilities: Set<ResponseStoreCapability> = emptySet()

        override suspend fun put(response: CrawlResponse) = null

        override suspend fun find(query: ResponseLookup): StoredResponse? = null

        override suspend fun start(context: ExtensionStartContext) = Unit

        override suspend fun stop(context: ExtensionStopContext) = Unit
    }
}
