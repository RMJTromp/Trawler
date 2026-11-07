package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.CursorHarvestStrategy
import com.rmjtromp.trawler.discovery.Discovery
import com.rmjtromp.trawler.discovery.DiscoveryCheckpointCodec
import com.rmjtromp.trawler.discovery.DiscoveryContext
import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.discovery.FixedOverlapBoundaryPolicy
import com.rmjtromp.trawler.discovery.HarvestStrategy
import com.rmjtromp.trawler.discovery.ResumableDiscovery
import com.rmjtromp.trawler.discovery.WatermarkHarvestStrategy
import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.extension.ExtensionStartContext
import com.rmjtromp.trawler.extension.ExtensionStopContext
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.ExtractionContext
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.policy.BuiltInPolicies
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.DefaultRandomSource
import com.rmjtromp.trawler.policy.FifoWorkOrderingPolicy
import com.rmjtromp.trawler.policy.NoRetryPolicy
import com.rmjtromp.trawler.policy.RandomSource
import com.rmjtromp.trawler.policy.SystemClock
import com.rmjtromp.trawler.policy.WorkOrderingPolicy
import com.rmjtromp.trawler.response.CaptureResponsesWhenConfigured
import com.rmjtromp.trawler.response.DoNotCaptureResponses
import com.rmjtromp.trawler.response.InMemoryResponseStore
import com.rmjtromp.trawler.response.LatestTargetResponseCachePolicy
import com.rmjtromp.trawler.response.NoResponseCachePolicy
import com.rmjtromp.trawler.response.ResponseCacheContext
import com.rmjtromp.trawler.response.ResponseCacheDecision
import com.rmjtromp.trawler.response.ResponseCachePolicy
import com.rmjtromp.trawler.response.ResponseLookup
import com.rmjtromp.trawler.response.ResponseStore
import com.rmjtromp.trawler.response.ResponseStoreCapability
import com.rmjtromp.trawler.response.StoredResponse
import com.rmjtromp.trawler.retention.RetentionCategory
import com.rmjtromp.trawler.retention.RetentionSummary
import com.rmjtromp.trawler.state.AtomicEnqueueAndCheckpointStore
import com.rmjtromp.trawler.state.AttemptStore
import com.rmjtromp.trawler.state.CheckpointQuarantineStore
import com.rmjtromp.trawler.state.CrawlStateCapability
import com.rmjtromp.trawler.state.CrawlStateRetentionRequest
import com.rmjtromp.trawler.state.CrawlStateRetentionStore
import com.rmjtromp.trawler.state.CrawlStateStore
import com.rmjtromp.trawler.state.DiscoveryCheckpointCodecDescriptor
import com.rmjtromp.trawler.state.DurableRunStatus
import com.rmjtromp.trawler.state.HarvestStateQuarantineStore
import com.rmjtromp.trawler.state.HarvestStateStore
import com.rmjtromp.trawler.state.InMemoryCrawlStateStore
import com.rmjtromp.trawler.state.LeaseStore
import com.rmjtromp.trawler.state.PipelineEligibilityOverrideStore
import com.rmjtromp.trawler.state.PipelineHealthStore
import com.rmjtromp.trawler.state.PipelineScopedWorkQueueStore
import com.rmjtromp.trawler.state.ResourceHistoryStore
import com.rmjtromp.trawler.state.RunHistoryStore
import com.rmjtromp.trawler.state.WorkCancellationStore
import com.rmjtromp.trawler.state.WorkOrderingStore
import com.rmjtromp.trawler.state.WorkQueueMetricsStore
import com.rmjtromp.trawler.state.WorkQueueStatus
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
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import org.junit.jupiter.api.Test

/**
 * Engine-level configuration contracts: single-cardinality engine slots,
 * inherited defaults, capability and runtime-surface validation, and the
 * direct [EngineConfiguration] validation boundary. Every case is decided
 * before any runtime work, so the state/response fakes here are
 * validation-only and never receive calls.
 */
class PipelineConfigurationEngineBuilderTest {
    @Test
    fun `engine single slots reject a second registration and name the explicit replace method`() {
        val builder = EngineConfigurationBuilder()
            .stateStore(InMemoryCrawlStateStore()).stateStore(InMemoryCrawlStateStore())
            .responseStore(InMemoryResponseStore()).responseStore(InMemoryResponseStore())
            .responsePolicy(CaptureResponsesWhenConfigured).responsePolicy(CaptureResponsesWhenConfigured)
            .responseCache(NoResponseCachePolicy).responseCache(NoResponseCachePolicy)
            .transportRouter(NamedRouter("r1")).transportRouter(NamedRouter("r2"))
            .policies(BuiltInPolicies.ordinary()).policies(BuiltInPolicies.ordinary())
            .runtime(EngineRuntimeOptions()).runtime(EngineRuntimeOptions())
            .clock(SystemClock).clock(SystemClock)
            .randomSource(DefaultRandomSource).randomSource(DefaultRandomSource)

        val expected = listOf(
            "clock" to "replaceClock",
            "policies" to "replacePolicies",
            "randomSource" to "replaceRandomSource",
            "responseCache" to "replaceResponseCache",
            "responsePolicy" to "replaceResponsePolicy",
            "responseStore" to "replaceResponseStore",
            "runtime" to "replaceRuntime",
            "stateStore" to "replaceStateStore",
            "transportRouter" to "replaceTransportRouter",
        ).map { (slot, method) ->
            ConfigurationIssue(
                path = "engine.$slot",
                code = ConfigurationIssueCode.DUPLICATE_SINGLE_EXTENSION,
                message = "Multiple registrations require an explicit $method call",
            )
        }
        assertEquals(expected, builder.validate())
        val methods = EngineConfigurationBuilder::class.java.methods.map { it.name }.toSet()
        assertTrue(expected.all { it.message.split(' ')[5] in methods })
        assertEquals(expected, assertFailsWith<InvalidConfigurationException> { builder.build() }.issues)
    }

    @Test
    fun `engine replace methods select the replacement value after or without an earlier registration`() {
        val state = InMemoryCrawlStateStore(id = "replacement-state")
        val responses = InMemoryResponseStore(id = "replacement-responses")
        val cache = LatestTargetResponseCachePolicy()
        val router = NamedRouter("replacement-router")
        val policies = BuiltInPolicies.ordinary().copy(retry = NoRetryPolicy)
        val options = EngineRuntimeOptions(workerCount = 2)
        val clock = FixedClock(Instant.parse("2026-01-01T00:00:00Z"))
        val random = FixedRandom()

        val afterFirst = EngineConfigurationBuilder()
            .stateStore(InMemoryCrawlStateStore()).replaceStateStore(state)
            .responseStore(InMemoryResponseStore()).replaceResponseStore(responses)
            .responsePolicy(CaptureResponsesWhenConfigured).replaceResponsePolicy(DoNotCaptureResponses)
            .responseCache(NoResponseCachePolicy).replaceResponseCache(cache)
            .transportRouter(NamedRouter("first-router")).replaceTransportRouter(router)
            .policies(BuiltInPolicies.ordinary()).replacePolicies(policies)
            .runtime(EngineRuntimeOptions()).replaceRuntime(options)
            .clock(SystemClock).replaceClock(clock)
            .randomSource(DefaultRandomSource).replaceRandomSource(random)
        val withoutFirst = EngineConfigurationBuilder()
            .replaceStateStore(state)
            .replaceResponseStore(responses)
            .replaceResponsePolicy(DoNotCaptureResponses)
            .replaceResponseCache(cache)
            .replaceTransportRouter(router)
            .replacePolicies(policies)
            .replaceRuntime(options)
            .replaceClock(clock)
            .replaceRandomSource(random)

        listOf(afterFirst, withoutFirst).forEach { builder ->
            assertEquals(emptyList(), builder.validate())
            val config = builder.build()
            assertSame(state, config.stateStore)
            assertSame(responses, config.responseStore)
            assertSame(DoNotCaptureResponses, config.responseCapturePolicy)
            assertSame(cache, config.responseCachePolicy)
            assertSame(router, config.defaultTransportRouter)
            assertSame(policies, config.defaultPolicies)
            assertSame(options, config.runtimeOptions)
            assertSame(clock, config.clock)
            assertSame(random, config.randomSource)
        }
    }

    @Test
    fun `an empty builder selects bounded in-memory state and ordinary policies derived from runtime options`() {
        val options = EngineRuntimeOptions(workerCount = 7, maxQueueSize = 16)
        val config = EngineConfigurationBuilder().runtime(options).build()

        val state = assertIs<InMemoryCrawlStateStore>(config.stateStore)
        assertEquals(16, state.maxQueueSize)
        assertEquals(options.leaseDuration, state.leaseDuration)
        assertNull(config.responseStore)
        assertSame(CaptureResponsesWhenConfigured, config.responseCapturePolicy)
        assertSame(NoResponseCachePolicy, config.responseCachePolicy)
        assertNull(config.defaultTransportRouter)
        assertEquals("concurrency-7", config.defaultPolicies?.concurrency?.id)
        assertSame(SystemClock, config.clock)
        assertSame(DefaultRandomSource, config.randomSource)
        assertEquals(emptyList(), config.pipelines)
        assertEquals(emptyList(), config.observers)
        assertEquals(emptyList(), config.extensions)
    }

    @Test
    fun `engine middleware replacement is inherited by pipelines ahead of local middleware`() {
        val config = EngineConfigurationBuilder()
            .transportRouter(NamedRouter("router"))
            .addRequestMiddleware(NamedRequestMiddleware("engine-request"))
            .replaceRequestMiddleware(listOf(NamedRequestMiddleware("replacement-request")))
            .addResponseMiddleware(NamedResponseMiddleware("engine-response"))
            .replaceResponseMiddleware(listOf(NamedResponseMiddleware("replacement-response")))
            .register<Int>("inherits") {
                extract(NamedExtractor("extractor"))
                addRequestMiddleware(NamedRequestMiddleware("local-request"))
                addResponseMiddleware(NamedResponseMiddleware("local-response"))
            }
            .build()

        assertEquals(listOf("replacement-request"), config.defaultRequestMiddlewares.map { it.id })
        assertEquals(listOf("replacement-response"), config.defaultResponseMiddlewares.map { it.id })
        val pipeline = config.pipelines.single()
        assertEquals(listOf("replacement-request", "local-request"), pipeline.requestMiddlewares.map { it.id })
        assertEquals(listOf("replacement-response", "local-response"), pipeline.responseMiddlewares.map { it.id })
    }

    @Test
    fun `engine-wide ordered extension identifiers must be non-blank and unique`() {
        val issues = EngineConfigurationBuilder()
            .addRequestMiddleware(NamedRequestMiddleware("same"))
            .addRequestMiddleware(NamedRequestMiddleware("same"))
            .addResponseMiddleware(NamedResponseMiddleware(" "))
            .observe(NamedObserver("observer"))
            .observe(NamedObserver("observer"))
            .extension(NamedExtension("extension"))
            .extension(NamedExtension("extension"))
            .validate()

        assertEquals(
            listOf(
                "engine.extensions" to ConfigurationIssueCode.DUPLICATE_ORDERED_EXTENSION,
                "engine.observers" to ConfigurationIssueCode.DUPLICATE_ORDERED_EXTENSION,
                "engine.requestMiddleware" to ConfigurationIssueCode.DUPLICATE_ORDERED_EXTENSION,
                "engine.responseMiddleware" to ConfigurationIssueCode.INVALID_IDENTIFIER,
            ),
            issues.map { it.path to it.code },
        )
    }

    @Test
    fun `invalid registrations are reported and never built into the registry`() {
        val existing = Pipeline(
            definition = PipelineDefinition("shared"),
            transportRouter = NamedRouter("router"),
            extractor = NamedExtractor<Int>(" "),
            policies = BuiltInPolicies.ordinary(),
        )
        val builder = EngineConfigurationBuilder()
            .transportRouter(NamedRouter("router"))
            .register(existing)
            .register<String>("shared") { /* missing extractor */ }

        val issues = builder.validate()

        // Neither registration was built, so no duplicate-ID issue is reported.
        assertEquals(
            listOf(
                ConfigurationIssue(
                    "pipeline[shared].extractor",
                    ConfigurationIssueCode.INVALID_IDENTIFIER,
                    "Extension identifier must not be blank",
                ),
                ConfigurationIssue(
                    "pipeline[shared].extractor",
                    ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION,
                    "Exactly one typed extractor is required",
                ),
            ),
            issues,
        )
        assertEquals(issues, assertFailsWith<InvalidConfigurationException> { builder.build() }.issues)
        assertEquals(emptyList(), EngineConfigurationBuilder().validate())
    }

    @Test
    fun `direct engine configuration applies documented defaults and is itself the validation boundary`() {
        val pipeline = validPipeline<Int>("direct")
        val config = EngineConfiguration(pipelines = listOf(pipeline))

        assertEquals(listOf(pipeline), config.pipelines)
        val state = assertIs<InMemoryCrawlStateStore>(config.stateStore)
        assertEquals(EngineRuntimeOptions().maxQueueSize, state.maxQueueSize)
        assertNull(config.responseStore)
        assertSame(CaptureResponsesWhenConfigured, config.responseCapturePolicy)
        assertSame(NoResponseCachePolicy, config.responseCachePolicy)
        assertSame(SystemClock, config.clock)
        assertSame(DefaultRandomSource, config.randomSource)
        assertEquals(emptySet(), config.requiredStateCapabilities)
        assertEquals(emptySet(), config.requiredResponseCapabilities)
        assertNull(config.defaultTransportRouter)
        assertNull(config.defaultPolicies)
        assertEquals(emptyList(), config.defaultRequestMiddlewares)
        assertEquals(emptyList(), config.defaultResponseMiddlewares)

        val duplicate = assertFailsWith<InvalidConfigurationException> {
            EngineConfiguration(pipelines = listOf(validPipeline<Int>("same"), validPipeline<String>("same")))
        }
        assertEquals(
            listOf("engine.pipelines[same]" to ConfigurationIssueCode.DUPLICATE_PIPELINE_ID),
            duplicate.issues.map { it.path to it.code },
        )

        val blankDiscovery = Pipeline(
            definition = PipelineDefinition("blank"),
            transportRouter = NamedRouter("router"),
            extractor = NamedExtractor<Int>("extractor"),
            discoveries = listOf(NamedDiscovery("")),
            policies = BuiltInPolicies.ordinary(),
        )
        val invalidPipeline = assertFailsWith<InvalidConfigurationException> {
            EngineConfiguration(pipelines = listOf(blankDiscovery))
        }
        assertEquals(
            listOf("pipeline[blank].discoveries" to ConfigurationIssueCode.INVALID_IDENTIFIER),
            invalidPipeline.issues.map { it.path to it.code },
        )
    }

    @Test
    fun `direct engine configuration snapshots its collections`() {
        val requirements = linkedSetOf(CrawlStateCapability.WORK_QUEUE)
        val observers = mutableListOf<com.rmjtromp.trawler.event.Observer>(NamedObserver("o"))
        val config = EngineConfiguration(
            pipelines = emptyList(),
            observers = observers,
            requiredStateCapabilities = requirements,
        )
        requirements += CrawlStateCapability.DURABLE
        observers += NamedObserver("late")

        assertEquals(setOf(CrawlStateCapability.WORK_QUEUE), config.requiredStateCapabilities)
        assertEquals(listOf("o"), config.observers.map { it.id })
        @Suppress("UNCHECKED_CAST")
        val mutable = config.requiredStateCapabilities as MutableSet<CrawlStateCapability>
        assertFailsWith<UnsupportedOperationException> { mutable += CrawlStateCapability.DURABLE }
    }

    @Test
    fun `required response capabilities need an installed store that advertises and implements them`() {
        val noStore = EngineConfigurationBuilder()
            .requireResponseCapability(ResponseStoreCapability.REPLAY)
            .requireResponseCapability(ResponseStoreCapability.LOOKUP)
            .validate()
        assertEquals(
            listOf(
                ConfigurationIssue(
                    "engine.responseStore.capabilities.LOOKUP",
                    ConfigurationIssueCode.UNSUPPORTED_CAPABILITY,
                    "Response capability LOOKUP requires an installed response store",
                ),
                ConfigurationIssue(
                    "engine.responseStore.capabilities.REPLAY",
                    ConfigurationIssueCode.UNSUPPORTED_CAPABILITY,
                    "Response capability REPLAY requires an installed response store",
                ),
            ),
            noStore,
        )

        val unadvertised = EngineConfigurationBuilder()
            .responseStore(AdvertisedResponseStore(emptySet()))
            .requireResponseCapability(ResponseStoreCapability.REPLAY)
            .validate()
        assertEquals(
            listOf(
                ConfigurationIssue(
                    "engine.responseStore.capabilities.REPLAY",
                    ConfigurationIssueCode.UNSUPPORTED_CAPABILITY,
                    "Response store 'advertised-response' does not advertise required capability REPLAY",
                ),
            ),
            unadvertised,
        )

        val advertisedOnly = EngineConfigurationBuilder()
            .responseStore(
                AdvertisedResponseStore(
                    setOf(
                        ResponseStoreCapability.HISTORY,
                        ResponseStoreCapability.REPLAY,
                        ResponseStoreCapability.TTL_EVICTION,
                    ),
                ),
            )
            .validate()
        assertEquals(
            listOf(
                "engine.responseStore.capabilities.HISTORY",
                "engine.responseStore.capabilities.REPLAY",
                "engine.responseStore.capabilities.TTL_EVICTION",
            ),
            advertisedOnly.map { it.path },
        )
        assertTrue(advertisedOnly.all { it.code == ConfigurationIssueCode.MISSING_RUNTIME_CAPABILITY })

        val memory = InMemoryResponseStore()
        val satisfied = EngineConfigurationBuilder().responseStore(memory)
        memory.capabilities.forEach(satisfied::requireResponseCapability)
        val config = satisfied.build()
        assertEquals(memory.capabilities, config.requiredResponseCapabilities)
    }

    @Test
    fun `a non-default response cache requires an installed response store`() {
        val cache = NamedCachePolicy("custom-cache")
        val missing = EngineConfigurationBuilder().responseCache(cache).validate()
        assertEquals(
            listOf(
                ConfigurationIssue(
                    "engine.responseCache",
                    ConfigurationIssueCode.MISSING_RUNTIME_CAPABILITY,
                    "Response cache policy 'custom-cache' requires an installed response store",
                ),
            ),
            missing,
        )

        val config = EngineConfigurationBuilder()
            .responseStore(AdvertisedResponseStore(emptySet()))
            .responseCache(cache)
            .build()
        assertSame(cache, config.responseCachePolicy)
    }

    @Test
    fun `required state capabilities are satisfied by the advertised set`() {
        val config = EngineConfigurationBuilder()
            .requireStateCapability(CrawlStateCapability.WORK_QUEUE)
            .requireStateCapability(CrawlStateCapability.DISCOVERY_CHECKPOINTS)
            .build()

        assertEquals(
            setOf(CrawlStateCapability.WORK_QUEUE, CrawlStateCapability.DISCOVERY_CHECKPOINTS),
            config.requiredStateCapabilities,
        )
    }

    @Test
    fun `a lifecycle-owning state store must be registered as an engine extension`() {
        val store = LifecycleStateStore()
        val missing = EngineConfigurationBuilder().stateStore(store).extension(NamedExtension("other")).validate()
        assertEquals(
            listOf(
                ConfigurationIssue(
                    "engine.stateStore.registration",
                    ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION,
                    "Lifecycle 'lifecycle-state' must be registered as an engine extension",
                ),
            ),
            missing,
        )

        val config = EngineConfigurationBuilder().stateStore(store).extension(store).build()
        assertSame(store, config.stateStore)
        assertEquals(listOf<ExtensionLifecycle>(store), config.extensions)
    }

    @Test
    fun `advertised state capabilities without their focused interfaces are rejected per runtime capability`() {
        val issues = EngineConfigurationBuilder()
            .stateStore(AdvertiseOnlyStateStore(CrawlStateCapability.entries.toSet()))
            .validate()

        assertEquals(
            RUNTIME_CAPABILITIES.sorted().map { "engine.stateStore.runtime.$it" },
            issues.map { it.path },
        )
        assertTrue(issues.all { it.code == ConfigurationIssueCode.MISSING_RUNTIME_CAPABILITY })
        assertEquals(
            "State store 'advertise-only' must implement and advertise runtime capability WORK_QUEUE",
            issues.single { it.path.endsWith(".WORK_QUEUE") }.message,
        )
    }

    @Test
    fun `implemented runtime interfaces must also be advertised while optional ones may stay silent`() {
        val silent = EngineConfigurationBuilder()
            .stateStore(FullSurfaceStateStore(emptySet()))
            .validate()
        assertEquals(
            listOf(
                "ATTEMPT_HISTORY",
                "CANCELLATION",
                "LEASE_FENCING",
                "PIPELINE_SCOPED_WORK",
                "RESOURCE_HISTORY",
                "WORK_ORDERING",
                "WORK_QUEUE",
                "WORK_STATUS",
            ).map { "engine.stateStore.runtime.$it" },
            silent.map { it.path },
        )

        val full = FullSurfaceStateStore(CrawlStateCapability.entries.toSet())
        val config = EngineConfigurationBuilder()
            .stateStore(full)
            .apply { CrawlStateCapability.entries.forEach(::requireStateCapability) }
            .build()
        assertSame(full, config.stateStore)
    }

    @Test
    fun `ordering policies the state store cannot apply are rejected per pipeline`() {
        val unsupported = object : WorkOrderingPolicy by FifoWorkOrderingPolicy {
            override val id: String = "unsupported-ordering"
        }
        val issues = EngineConfigurationBuilder()
            .stateStore(FullSurfaceStateStore(CrawlStateCapability.entries.toSet(), unsupportedOrderingId = unsupported.id))
            .transportRouter(NamedRouter("router"))
            .register<Int>("custom") {
                extract(NamedExtractor("extractor"))
                ordering(unsupported)
            }
            .register<Int>("fifo") { extract(NamedExtractor("extractor")) }
            .validate()

        assertEquals(
            listOf(
                ConfigurationIssue(
                    "pipeline[custom].policies.ordering",
                    ConfigurationIssueCode.UNSUPPORTED_CAPABILITY,
                    "State store 'full-surface' does not support ordering 'unsupported-ordering' without substitution",
                ),
            ),
            issues,
        )
    }

    @Test
    fun `resumable discoveries require a store that implements and advertises discovery checkpoints`() {
        val checkpointIssue = ConfigurationIssue(
            "pipeline[resumable].discoveries.checkpoint",
            ConfigurationIssueCode.MISSING_RUNTIME_CAPABILITY,
            "Resumable discoveries require a checkpoint-capable state store",
        )

        assertEquals(listOf(checkpointIssue), resumableIssues(MinimalRuntimeStateStore()))
        assertEquals(
            listOf(checkpointIssue),
            resumableIssues(FullSurfaceStateStore(CrawlStateCapability.entries.toSet() - CrawlStateCapability.DISCOVERY_CHECKPOINTS)),
        )
        assertEquals(emptyList(), resumableIssues(InMemoryCrawlStateStore()))
        assertEquals(emptyList(), resumableIssues(CheckpointOnlyStateStore()))
    }

    @Test
    fun `non-stateless harvest needs a resumable discovery and codec-backed harvest needs harvest state`() {
        val cursorWithoutResumable = validationFor(
            InMemoryCrawlStateStore(),
            harvest = CursorHarvestStrategy(),
            discovery = NamedDiscovery("plain"),
        )
        assertEquals(
            listOf(
                ConfigurationIssue(
                    "pipeline[resumable].harvest",
                    ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION,
                    "Non-stateless harvest strategies require at least one resumable discovery",
                ),
            ),
            cursorWithoutResumable,
        )
        assertEquals(emptyList(), validationFor(CheckpointOnlyStateStore(), harvest = CursorHarvestStrategy()))

        val watermark = WatermarkHarvestStrategy(FixedOverlapBoundaryPolicy(Duration.ZERO))
        val harvestStateIssue = ConfigurationIssue(
            "pipeline[resumable].harvest",
            ConfigurationIssueCode.MISSING_RUNTIME_CAPABILITY,
            "Non-stateless harvest strategies require harvest state storage",
        )
        assertEquals(listOf(harvestStateIssue), validationFor(CheckpointOnlyStateStore(), harvest = watermark))
        assertEquals(
            listOf(harvestStateIssue),
            validationFor(
                FullSurfaceStateStore(CrawlStateCapability.entries.toSet() - CrawlStateCapability.HARVEST_STATE),
                harvest = watermark,
            ),
        )
        assertEquals(emptyList(), validationFor(InMemoryCrawlStateStore(), harvest = watermark))
    }

    private fun resumableIssues(store: CrawlStateStore): List<ConfigurationIssue> =
        validationFor(store, harvest = null)

    private fun validationFor(
        store: CrawlStateStore,
        harvest: HarvestStrategy?,
        discovery: Discovery = NamedResumableDiscovery("resumable-discovery"),
    ): List<ConfigurationIssue> = EngineConfigurationBuilder()
        .stateStore(store)
        .transportRouter(NamedRouter("router"))
        .register<Int>("resumable") {
            extract(NamedExtractor("extractor"))
            discover(discovery)
            if (harvest != null) harvest(harvest)
        }
        .validate()

    private fun <T : Any> validPipeline(id: String): Pipeline<T> =
        PipelineBuilder<T>(id)
            .transport(NamedRouter("router-$id"))
            .extract(NamedExtractor("extractor-$id"))
            .build(PipelineDefaults(policies = BuiltInPolicies.ordinary()))

    private class FixedClock(private val now: Instant) : Clock {
        override fun now(): Instant = now
    }

    private class FixedRandom : RandomSource {
        override fun nextInt(boundExclusive: Int): Int = 0

        override fun nextLong(boundExclusive: Long): Long = 0

        override fun nextDouble(): Double = 0.5
    }

    private class NamedRouter(override val id: String) : TransportRouter {
        override fun select(request: CrawlRequest, context: RequestContext): Transport = error("validation only")
    }

    private class NamedExtractor<T : Any>(override val id: String) : Extractor<T> {
        override suspend fun extract(response: CrawlResponse, context: ExtractionContext): ExtractionResult<T> =
            ExtractionResult.Ignore("test")
    }

    private open class NamedDiscovery(override val id: String) : Discovery {
        override fun discover(context: DiscoveryContext): Flow<DiscoveryEmission> = emptyFlow()
    }

    private class NamedResumableDiscovery(id: String) : NamedDiscovery(id), ResumableDiscovery {
        override val checkpointCodec: DiscoveryCheckpointCodec = object : DiscoveryCheckpointCodec {
            override val descriptor = DiscoveryCheckpointCodecDescriptor("codec", 1)
            override val strategyId: String = "strategy"
        }
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

    private class NamedObserver(override val id: String) : com.rmjtromp.trawler.event.Observer {
        override suspend fun onEvent(event: com.rmjtromp.trawler.event.CrawlerEvent) =
            com.rmjtromp.trawler.event.ObserverResult.Accepted
    }

    private class NamedExtension(override val id: String) : ExtensionLifecycle {
        override suspend fun start(context: ExtensionStartContext) = Unit

        override suspend fun stop(context: ExtensionStopContext) = Unit
    }

    private class NamedCachePolicy(override val id: String) : ResponseCachePolicy {
        override val requiredCapabilities: Set<ResponseStoreCapability> = emptySet()

        override suspend fun lookup(context: ResponseCacheContext, store: ResponseStore): ResponseCacheDecision =
            error("validation only")
    }

    private class AdvertisedResponseStore(
        override val capabilities: Set<ResponseStoreCapability>,
    ) : ResponseStore {
        override val id: String = "advertised-response"

        override suspend fun put(response: CrawlResponse) = null

        override suspend fun find(query: ResponseLookup): StoredResponse? = null
    }

    /** Advertises capabilities but implements none of the focused runtime interfaces. */
    private class AdvertiseOnlyStateStore(
        override val capabilities: Set<CrawlStateCapability>,
    ) : CrawlStateStore {
        override val id: String = "advertise-only"
    }

    /** Implements and advertises exactly the eight mandatory runtime surfaces. */
    private open class MinimalRuntimeStateStore(
        private val delegate: InMemoryCrawlStateStore = InMemoryCrawlStateStore(),
        override val id: String = "minimal-runtime",
    ) : CrawlStateStore,
        PipelineScopedWorkQueueStore by delegate,
        LeaseStore by delegate,
        AttemptStore by delegate,
        ResourceHistoryStore by delegate,
        WorkCancellationStore by delegate,
        WorkOrderingStore by delegate,
        WorkQueueStatus by delegate {
        override val capabilities: Set<CrawlStateCapability> = MANDATORY_CAPABILITIES
    }

    private class LifecycleStateStore : MinimalRuntimeStateStore(id = "lifecycle-state"), ExtensionLifecycle {
        override suspend fun start(context: ExtensionStartContext) = Unit

        override suspend fun stop(context: ExtensionStopContext) = Unit
    }

    /** Mandatory surfaces plus discovery checkpoints, but no harvest state. */
    private class CheckpointOnlyStateStore(
        private val delegate: InMemoryCrawlStateStore = InMemoryCrawlStateStore(),
    ) : CrawlStateStore,
        PipelineScopedWorkQueueStore by delegate,
        LeaseStore by delegate,
        AttemptStore by delegate,
        ResourceHistoryStore by delegate,
        WorkCancellationStore by delegate,
        WorkOrderingStore by delegate,
        WorkQueueStatus by delegate,
        AtomicEnqueueAndCheckpointStore by delegate {
        override val id: String = "checkpoint-only"
        override val capabilities: Set<CrawlStateCapability> = MANDATORY_CAPABILITIES + setOf(
            CrawlStateCapability.DISCOVERY_CHECKPOINTS,
            CrawlStateCapability.ATOMIC_ENQUEUE_AND_CHECKPOINT,
        )
    }

    /** Implements every focused state interface and advertises a caller-selected set. */
    private class FullSurfaceStateStore(
        override val capabilities: Set<CrawlStateCapability>,
        private val unsupportedOrderingId: String? = null,
        private val delegate: InMemoryCrawlStateStore = InMemoryCrawlStateStore(),
    ) : CrawlStateStore,
        PipelineScopedWorkQueueStore by delegate,
        LeaseStore by delegate,
        AttemptStore by delegate,
        ResourceHistoryStore by delegate,
        WorkCancellationStore by delegate,
        WorkOrderingStore by delegate,
        WorkQueueStatus by delegate,
        AtomicEnqueueAndCheckpointStore by delegate,
        CheckpointQuarantineStore by delegate,
        HarvestStateStore by delegate,
        HarvestStateQuarantineStore by delegate,
        PipelineEligibilityOverrideStore by delegate,
        PipelineHealthStore by delegate,
        WorkQueueMetricsStore by delegate,
        RunHistoryStore,
        CrawlStateRetentionStore {
        override val id: String = "full-surface"
        override val retentionCategories: Set<RetentionCategory> = emptySet()

        override fun supports(policy: WorkOrderingPolicy): Boolean = policy.id != unsupportedOrderingId

        override suspend fun prune(request: CrawlStateRetentionRequest): RetentionSummary = error("validation only")

        override suspend fun recoverStaleRuns(now: Instant, staleAfter: Duration): Int = error("validation only")

        override suspend fun startEngineRun(engineRunId: String, startedAt: Instant, reason: String?) =
            error("validation only")

        override suspend fun startPipelineRun(
            engineRunId: String,
            pipelineRunId: String,
            pipelineId: String,
            startedAt: Instant,
        ) = error("validation only")

        override suspend fun completePipelineRun(
            pipelineRunId: String,
            status: DurableRunStatus,
            completedAt: Instant,
            failureCount: Long,
        ) = error("validation only")

        override suspend fun completeEngineRun(
            engineRunId: String,
            status: DurableRunStatus,
            completedAt: Instant,
            failureCount: Long,
        ) = error("validation only")
    }

    private companion object {
        val MANDATORY_CAPABILITIES: Set<CrawlStateCapability> = setOf(
            CrawlStateCapability.WORK_QUEUE,
            CrawlStateCapability.PIPELINE_SCOPED_WORK,
            CrawlStateCapability.LEASE_FENCING,
            CrawlStateCapability.RESOURCE_HISTORY,
            CrawlStateCapability.ATTEMPT_HISTORY,
            CrawlStateCapability.CANCELLATION,
            CrawlStateCapability.WORK_ORDERING,
            CrawlStateCapability.WORK_STATUS,
        )

        val RUNTIME_CAPABILITIES: List<String> = listOf(
            "WORK_QUEUE",
            "PIPELINE_SCOPED_WORK",
            "LEASE_FENCING",
            "RESOURCE_HISTORY",
            "ATTEMPT_HISTORY",
            "CANCELLATION",
            "WORK_ORDERING",
            "WORK_STATUS",
            "RUN_HISTORY",
            "RUN_RECOVERY",
            "DISCOVERY_CHECKPOINTS",
            "ATOMIC_ENQUEUE_AND_CHECKPOINT",
            "CHECKPOINT_QUARANTINE",
            "HARVEST_STATE",
            "HARVEST_STATE_QUARANTINE",
            "QUEUE_WATERMARKS",
            "PIPELINE_HEALTH",
            "PIPELINE_ELIGIBILITY_OVERRIDES",
            "STATE_RETENTION",
        )
    }
}
