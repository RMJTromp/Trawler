package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.Discovery
import com.rmjtromp.trawler.event.Observer
import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.model.immutableSnapshot
import com.rmjtromp.trawler.policy.AuthorizationPolicy
import com.rmjtromp.trawler.policy.BuiltInPolicies
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.ConcurrencyPolicy
import com.rmjtromp.trawler.policy.CrawlBudgetPolicy
import com.rmjtromp.trawler.policy.CrawlScopePolicy
import com.rmjtromp.trawler.policy.DeduplicationPolicy
import com.rmjtromp.trawler.policy.CircuitBreakerPolicy
import com.rmjtromp.trawler.policy.DefaultRandomSource
import com.rmjtromp.trawler.policy.DiscoveryAdmissionPolicy
import com.rmjtromp.trawler.policy.FailureClassifier
import com.rmjtromp.trawler.policy.RandomSource
import com.rmjtromp.trawler.policy.RecrawlPolicy
import com.rmjtromp.trawler.policy.RequestCanonicalizer
import com.rmjtromp.trawler.policy.RequestIdentityStrategy
import com.rmjtromp.trawler.policy.RequestPacingPolicy
import com.rmjtromp.trawler.policy.RetryPolicy
import com.rmjtromp.trawler.policy.PipelineEligibilityPolicy
import com.rmjtromp.trawler.policy.PipelineHealthPolicy
import com.rmjtromp.trawler.policy.SystemClock
import com.rmjtromp.trawler.policy.WorkOrderingPolicy
import com.rmjtromp.trawler.discovery.HarvestStrategy
import com.rmjtromp.trawler.discovery.ResumableDiscovery
import com.rmjtromp.trawler.discovery.StatelessHarvestStrategy
import com.rmjtromp.trawler.response.ResponseStore
import com.rmjtromp.trawler.response.ResponseStoreCapability
import com.rmjtromp.trawler.response.ResponseHistoryStore
import com.rmjtromp.trawler.response.ResponseReplayStore
import com.rmjtromp.trawler.response.ResponseRetentionStore
import com.rmjtromp.trawler.response.ResponseCapturePolicy
import com.rmjtromp.trawler.response.CaptureResponsesWhenConfigured
import com.rmjtromp.trawler.response.ResponseCachePolicy
import com.rmjtromp.trawler.response.NoResponseCachePolicy
import com.rmjtromp.trawler.state.CrawlStateCapability
import com.rmjtromp.trawler.state.CrawlStateStore
import com.rmjtromp.trawler.state.AttemptStore
import com.rmjtromp.trawler.state.InMemoryCrawlStateStore
import com.rmjtromp.trawler.state.PipelineScopedWorkQueueStore
import com.rmjtromp.trawler.state.ResourceHistoryStore
import com.rmjtromp.trawler.state.RunHistoryStore
import com.rmjtromp.trawler.state.WorkCancellationStore
import com.rmjtromp.trawler.state.WorkQueueStore
import com.rmjtromp.trawler.state.LeaseStore
import com.rmjtromp.trawler.state.WorkOrderingStore
import com.rmjtromp.trawler.state.WorkQueueStatus
import com.rmjtromp.trawler.transport.RequestMiddleware
import com.rmjtromp.trawler.transport.ResponseMiddleware
import com.rmjtromp.trawler.transport.Transport
import com.rmjtromp.trawler.transport.TransportRouter
import java.util.Collections

/**
 * The complete policy selection for one typed pipeline.
 *
 * Every field is one effective policy after engine-default inheritance has
 * been resolved. The bundle contains policy contracts only; it does not run
 * workers or choose a web/database/serialization implementation. Replacing a
 * pipeline policy is explicit in [PipelineBuilder] and never silently stacks
 * single-cardinality policies.
 */
public data class PipelinePolicies(
    public val requestCanonicalizer: RequestCanonicalizer,
    public val requestIdentity: RequestIdentityStrategy,
    public val deduplication: DeduplicationPolicy,
    public val recrawl: RecrawlPolicy,
    public val failureClassifier: FailureClassifier,
    public val retry: RetryPolicy,
    public val pacing: RequestPacingPolicy,
    public val concurrency: ConcurrencyPolicy,
    public val scope: CrawlScopePolicy,
    public val authorization: AuthorizationPolicy,
    public val ordering: WorkOrderingPolicy,
    public val budget: CrawlBudgetPolicy,
    public val admission: DiscoveryAdmissionPolicy = com.rmjtromp.trawler.policy.Continuous,
    public val eligibility: PipelineEligibilityPolicy =
        com.rmjtromp.trawler.policy.AlwaysEligiblePipelinePolicy,
    public val health: PipelineHealthPolicy = com.rmjtromp.trawler.policy.NoOpPipelineHealthPolicy,
    public val circuitBreaker: CircuitBreakerPolicy =
        com.rmjtromp.trawler.policy.NeverOpenCircuitBreakerPolicy,
)

/**
 * Immutable defaults inherited by pipeline builders registered with an
 * engine.
 *
 * A missing default is intentional: configuration validation reports it
 * before runtime work instead of silently installing an unsafe algorithm.
 * Middleware defaults are prepended in registration order; pipeline-local
 * additions follow them. A pipeline-local replacement discards inherited
 * middleware for that chain.
 */
public class PipelineDefaults(
    public val transportRouter: TransportRouter? = null,
    public val policies: PipelinePolicies? = null,
    requestMiddlewares: List<RequestMiddleware> = emptyList(),
    responseMiddlewares: List<ResponseMiddleware> = emptyList(),
) {
    public val requestMiddlewares: List<RequestMiddleware> = immutableSnapshot(requestMiddlewares)
    public val responseMiddlewares: List<ResponseMiddleware> = immutableSnapshot(responseMiddlewares)

    public companion object {
        /** Returns defaults with no transport or policy silently selected. */
        public fun none(): PipelineDefaults = PipelineDefaults()
    }
}

/**
 * Immutable typed pipeline definition.
 *
 * A pipeline retains `T` from [extractor] through [transforms], [validators],
 * and [outputs]. Lists are immutable snapshots and preserve declared order.
 * The object is a composition description only; it does not start coroutines,
 * claim work, invoke transports, or own extension shutdown.
 */
public class Pipeline<T : Any>(
    public val definition: PipelineDefinition,
    public val transportRouter: TransportRouter,
    public val extractor: Extractor<T>,
    discoveries: List<Discovery> = emptyList(),
    requestMiddlewares: List<RequestMiddleware> = emptyList(),
    responseMiddlewares: List<ResponseMiddleware> = emptyList(),
    transforms: List<Transform<T>> = emptyList(),
    validators: List<Validator<T>> = emptyList(),
    outputs: List<OutputBinding<T>> = emptyList(),
    public val policies: PipelinePolicies,
    public val harvestStrategy: HarvestStrategy = StatelessHarvestStrategy,
    extensions: List<ExtensionLifecycle> = emptyList(),
) {
    public val id: String
        get() = definition.id

    public val discoveries: List<Discovery> = immutableSnapshot(discoveries)
    public val requestMiddlewares: List<RequestMiddleware> = immutableSnapshot(requestMiddlewares)
    public val responseMiddlewares: List<ResponseMiddleware> = immutableSnapshot(responseMiddlewares)
    public val transforms: List<Transform<T>> = immutableSnapshot(transforms)
    public val validators: List<Validator<T>> = immutableSnapshot(validators)
    public val outputs: List<OutputBinding<T>> = immutableSnapshot(outputs)
    public val extensions: List<ExtensionLifecycle> = immutableSnapshot(extensions)

    /**
     * Captures this pipeline's type-sensitive execution owner at the internal
     * heterogeneous registration boundary. The owner retains `T`; the
     * registry never accepts or stores a produced value.
     */
    internal fun runtimeRegistration(): PipelineRegistration = TypedPipelineRegistration(this)
}

/**
 * Type-safe DSL builder for one [Pipeline] value type.
 *
 * Ordered extensions are appended. Single-cardinality values are inherited
 * from [PipelineDefaults] and may be set once locally; a second local set is
 * rejected unless the corresponding `replace...` method is used. The builder
 * is configuration-thread-confined and must be built before any engine scope
 * starts. Build failures are deterministic and occur before runtime work.
 */
public class PipelineBuilder<T : Any> public constructor(
    public val definition: PipelineDefinition,
) {
    public constructor(id: String) : this(PipelineDefinition(id))

    private var transportRouter: TransportRouter? = null
    private var transportConfigured: Boolean = false
    private var extractor: Extractor<T>? = null
    private var extractorConfigured: Boolean = false
    private var policySet: PipelinePolicies? = null
    private var policySetConfigured: Boolean = false

    private var requestCanonicalizer: RequestCanonicalizer? = null
    private var requestIdentity: RequestIdentityStrategy? = null
    private var deduplication: DeduplicationPolicy? = null
    private var recrawl: RecrawlPolicy? = null
    private var failureClassifier: FailureClassifier? = null
    private var retry: RetryPolicy? = null
    private var pacing: RequestPacingPolicy? = null
    private var concurrency: ConcurrencyPolicy? = null
    private var scope: CrawlScopePolicy? = null
    private var authorization: AuthorizationPolicy? = null
    private var ordering: WorkOrderingPolicy? = null
    private var budget: CrawlBudgetPolicy? = null
    private var admission: DiscoveryAdmissionPolicy? = null
    private var eligibility: PipelineEligibilityPolicy? = null
    private var health: PipelineHealthPolicy? = null
    private var circuitBreaker: CircuitBreakerPolicy? = null
    private var harvestStrategy: HarvestStrategy? = null

    private var requestMiddlewareReplacement: List<RequestMiddleware>? = null
    private var responseMiddlewareReplacement: List<ResponseMiddleware>? = null

    private val discoveries = mutableListOf<Discovery>()
    private val requestMiddlewares = mutableListOf<RequestMiddleware>()
    private val responseMiddlewares = mutableListOf<ResponseMiddleware>()
    private val transforms = mutableListOf<Transform<T>>()
    private val validators = mutableListOf<Validator<T>>()
    private val outputs = mutableListOf<OutputBinding<T>>()
    private val extensions = mutableListOf<ExtensionLifecycle>()
    private val builderIssues = mutableListOf<ConfigurationIssue>()

    /** Adds one discovery to the end of the ordered discovery list. */
    public fun discover(discovery: Discovery): PipelineBuilder<T> {
        discoveries += discovery
        return this
    }

    /** Sets the local fixed transport, replacing an inherited router. */
    public fun transport(transport: Transport): PipelineBuilder<T> =
        transport(FixedTransportRouter(transport))

    /** Sets the local router once; use [replaceTransport] for an explicit replacement. */
    public fun transport(router: TransportRouter): PipelineBuilder<T> {
        recordSingleAssignment("transport", transportConfigured)
        transportRouter = router
        transportConfigured = true
        return this
    }

    /** Explicitly replaces the local or inherited transport router. */
    public fun replaceTransport(router: TransportRouter): PipelineBuilder<T> {
        transportRouter = router
        transportConfigured = true
        return this
    }

    /** Sets the required extractor once; use [replaceExtractor] to replace it explicitly. */
    public fun extract(extractor: Extractor<T>): PipelineBuilder<T> {
        recordSingleAssignment("extractor", extractorConfigured)
        this.extractor = extractor
        extractorConfigured = true
        return this
    }

    /** Explicitly replaces the typed extractor. */
    public fun replaceExtractor(extractor: Extractor<T>): PipelineBuilder<T> {
        this.extractor = extractor
        extractorConfigured = true
        return this
    }

    /** Adds a same-type transform after all previously added transforms. */
    public fun transform(transform: Transform<T>): PipelineBuilder<T> {
        transforms += transform
        return this
    }

    /** Adds a same-type validator after all previously added validators. */
    public fun validator(validator: Validator<T>): PipelineBuilder<T> {
        validators += validator
        return this
    }

    /** Adds an output binding to the ordered output fan-out. */
    public fun output(
        output: Output<T>,
        requirement: OutputRequirement = OutputRequirement.REQUIRED,
    ): PipelineBuilder<T> {
        outputs += OutputBinding(output, requirement)
        return this
    }

    /** Adds a pre-built output binding to the ordered output fan-out. */
    public fun output(binding: OutputBinding<T>): PipelineBuilder<T> {
        outputs += binding
        return this
    }

    /** Appends request middleware after inherited request middleware. */
    public fun addRequestMiddleware(middleware: RequestMiddleware): PipelineBuilder<T> {
        requestMiddlewares += middleware
        return this
    }

    /** Appends response middleware after inherited response middleware. */
    public fun addResponseMiddleware(middleware: ResponseMiddleware): PipelineBuilder<T> {
        responseMiddlewares += middleware
        return this
    }

    /** Replaces inherited request middleware before local additions are appended. */
    public fun replaceRequestMiddleware(middleware: List<RequestMiddleware>): PipelineBuilder<T> {
        requestMiddlewareReplacement = middleware.toList()
        return this
    }

    /** Replaces inherited response middleware before local additions are appended. */
    public fun replaceResponseMiddleware(middleware: List<ResponseMiddleware>): PipelineBuilder<T> {
        responseMiddlewareReplacement = middleware.toList()
        return this
    }

    /** Sets the complete local policy bundle once, replacing inherited defaults. */
    public fun policies(policies: PipelinePolicies): PipelineBuilder<T> {
        recordSingleAssignment("policies", policySetConfigured)
        policySet = policies
        policySetConfigured = true
        return this
    }

    /** Explicitly replaces the complete local policy bundle. */
    public fun replacePolicies(policies: PipelinePolicies): PipelineBuilder<T> {
        policySet = policies
        policySetConfigured = true
        return this
    }

    /** Replaces the inherited request canonicalizer once. */
    public fun canonicalize(policy: RequestCanonicalizer): PipelineBuilder<T> {
        requestCanonicalizer = assignPolicy("requestCanonicalizer", requestCanonicalizer, policy, "replaceCanonicalizer")
        return this
    }

    /** Explicitly replaces the request canonicalizer. */
    public fun replaceCanonicalizer(policy: RequestCanonicalizer): PipelineBuilder<T> {
        requestCanonicalizer = policy
        return this
    }

    /** Replaces the inherited request identity strategy once. */
    public fun identity(policy: RequestIdentityStrategy): PipelineBuilder<T> {
        requestIdentity = assignPolicy("requestIdentity", requestIdentity, policy, "replaceIdentity")
        return this
    }

    /** Explicitly replaces the request identity strategy. */
    public fun replaceIdentity(policy: RequestIdentityStrategy): PipelineBuilder<T> {
        requestIdentity = policy
        return this
    }

    /** Replaces the inherited active-work deduplication policy once. */
    public fun deduplicate(policy: DeduplicationPolicy): PipelineBuilder<T> {
        deduplication = assignPolicy("deduplication", deduplication, policy)
        return this
    }

    /** Explicitly replaces the active-work deduplication policy. */
    public fun replaceDeduplication(policy: DeduplicationPolicy): PipelineBuilder<T> {
        deduplication = policy
        return this
    }

    /** Replaces the inherited recrawl policy once. */
    public fun recrawl(policy: RecrawlPolicy): PipelineBuilder<T> {
        recrawl = assignPolicy("recrawl", recrawl, policy)
        return this
    }

    /** Explicitly replaces the recrawl policy. */
    public fun replaceRecrawl(policy: RecrawlPolicy): PipelineBuilder<T> {
        recrawl = policy
        return this
    }

    /** Replaces the inherited failure classifier once. */
    public fun classifyFailures(policy: FailureClassifier): PipelineBuilder<T> {
        failureClassifier = assignPolicy("failureClassifier", failureClassifier, policy)
        return this
    }

    /** Explicitly replaces the failure classifier. */
    public fun replaceFailureClassifier(policy: FailureClassifier): PipelineBuilder<T> {
        failureClassifier = policy
        return this
    }

    /** Replaces the inherited retry policy once. */
    public fun retry(policy: RetryPolicy): PipelineBuilder<T> {
        retry = assignPolicy("retry", retry, policy)
        return this
    }

    /** Explicitly replaces the retry policy. */
    public fun replaceRetry(policy: RetryPolicy): PipelineBuilder<T> {
        retry = policy
        return this
    }

    /** Replaces the inherited pacing policy once. */
    public fun pacing(policy: RequestPacingPolicy): PipelineBuilder<T> {
        pacing = assignPolicy("pacing", pacing, policy)
        return this
    }

    /** Explicitly replaces the pacing policy. */
    public fun replacePacing(policy: RequestPacingPolicy): PipelineBuilder<T> {
        pacing = policy
        return this
    }

    /** Replaces the inherited concurrency policy once. */
    public fun concurrency(policy: ConcurrencyPolicy): PipelineBuilder<T> {
        concurrency = assignPolicy("concurrency", concurrency, policy)
        return this
    }

    /** Explicitly replaces the concurrency policy. */
    public fun replaceConcurrency(policy: ConcurrencyPolicy): PipelineBuilder<T> {
        concurrency = policy
        return this
    }

    /** Replaces the inherited crawl-scope policy once. */
    public fun scope(policy: CrawlScopePolicy): PipelineBuilder<T> {
        scope = assignPolicy("scope", scope, policy)
        return this
    }

    /** Explicitly replaces the crawl-scope policy. */
    public fun replaceScope(policy: CrawlScopePolicy): PipelineBuilder<T> {
        scope = policy
        return this
    }

    /** Replaces the inherited authorization policy once. */
    public fun authorize(policy: AuthorizationPolicy): PipelineBuilder<T> {
        authorization = assignPolicy("authorization", authorization, policy)
        return this
    }

    /** Explicitly replaces the authorization policy. */
    public fun replaceAuthorization(policy: AuthorizationPolicy): PipelineBuilder<T> {
        authorization = policy
        return this
    }

    /** Replaces the inherited work-ordering policy once. */
    public fun ordering(policy: WorkOrderingPolicy): PipelineBuilder<T> {
        ordering = assignPolicy("ordering", ordering, policy)
        return this
    }

    /** Explicitly replaces the work-ordering policy. */
    public fun replaceOrdering(policy: WorkOrderingPolicy): PipelineBuilder<T> {
        ordering = policy
        return this
    }

    /** Replaces the inherited crawl-budget policy once. */
    public fun budget(policy: CrawlBudgetPolicy): PipelineBuilder<T> {
        budget = assignPolicy("budget", budget, policy)
        return this
    }

    /** Explicitly replaces the crawl-budget policy. */
    public fun replaceBudget(policy: CrawlBudgetPolicy): PipelineBuilder<T> {
        budget = policy
        return this
    }

    /** Replaces the inherited discovery-admission policy once. */
    public fun admission(policy: DiscoveryAdmissionPolicy): PipelineBuilder<T> {
        admission = assignPolicy("admission", admission, policy)
        return this
    }

    /** Explicitly replaces the discovery-admission policy. */
    public fun replaceAdmission(policy: DiscoveryAdmissionPolicy): PipelineBuilder<T> {
        admission = policy
        return this
    }

    /** Replaces the inherited pipeline eligibility policy once. */
    public fun eligibility(policy: PipelineEligibilityPolicy): PipelineBuilder<T> {
        eligibility = assignPolicy("eligibility", eligibility, policy)
        return this
    }

    /** Explicitly replaces the pipeline eligibility policy. */
    public fun replaceEligibility(policy: PipelineEligibilityPolicy): PipelineBuilder<T> {
        eligibility = policy
        return this
    }

    /** Replaces the inherited health policy once. */
    public fun health(policy: PipelineHealthPolicy): PipelineBuilder<T> {
        health = assignPolicy("health", health, policy)
        return this
    }

    /** Explicitly replaces the health policy. */
    public fun replaceHealth(policy: PipelineHealthPolicy): PipelineBuilder<T> {
        health = policy
        return this
    }

    /** Replaces the inherited circuit-breaker policy once. */
    public fun circuitBreaker(policy: CircuitBreakerPolicy): PipelineBuilder<T> {
        circuitBreaker = assignPolicy("circuitBreaker", circuitBreaker, policy)
        return this
    }

    /** Explicitly replaces the circuit-breaker policy. */
    public fun replaceCircuitBreaker(policy: CircuitBreakerPolicy): PipelineBuilder<T> {
        circuitBreaker = policy
        return this
    }

    /** Installs one explicit recurring-harvest strategy for this pipeline. */
    public fun harvest(strategy: HarvestStrategy): PipelineBuilder<T> {
        recordSingleAssignment("harvest", harvestStrategy != null)
        harvestStrategy = strategy
        return this
    }

    /** Explicitly replaces the recurring-harvest strategy. */
    public fun replaceHarvest(strategy: HarvestStrategy): PipelineBuilder<T> {
        harvestStrategy = strategy
        return this
    }

    /** Appends a pipeline-owned lifecycle extension after engine extensions. */
    public fun extension(extension: ExtensionLifecycle): PipelineBuilder<T> {
        extensions += extension
        return this
    }

    /** Returns deterministic build issues after applying [defaults]. */
    public fun validationIssues(defaults: PipelineDefaults = PipelineDefaults.none()): List<ConfigurationIssue> {
        val issues = builderIssues.toMutableList()
        val effectiveTransport = transportRouter ?: defaults.transportRouter
        val effectivePolicies = policySet ?: defaults.policies
        val effectiveRequestMiddleware =
            (requestMiddlewareReplacement ?: defaults.requestMiddlewares) + requestMiddlewares
        val effectiveResponseMiddleware =
            (responseMiddlewareReplacement ?: defaults.responseMiddlewares) + responseMiddlewares

        if (effectiveTransport == null) {
            issues += ConfigurationIssue(
                path = "pipeline[${definition.id}].transport",
                code = ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION,
                message = "Exactly one effective transport router is required",
            )
        }
        if (extractor == null) {
            issues += ConfigurationIssue(
                path = "pipeline[${definition.id}].extractor",
                code = ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION,
                message = "Exactly one typed extractor is required",
            )
        }
        if (effectivePolicies == null) {
            issues += ConfigurationIssue(
                path = "pipeline[${definition.id}].policies",
                code = ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION,
                message = "Exactly one effective policy bundle is required",
            )
        }

        addIdentifierIssues(issues, "pipeline[${definition.id}].discoveries", discoveries.map { it.id })
        addIdentifierIssues(
            issues,
            "pipeline[${definition.id}].requestMiddleware",
            effectiveRequestMiddleware.map { it.id },
        )
        addIdentifierIssues(
            issues,
            "pipeline[${definition.id}].responseMiddleware",
            effectiveResponseMiddleware.map { it.id },
        )
        addIdentifierIssues(issues, "pipeline[${definition.id}].transforms", transforms.map { it.id })
        addIdentifierIssues(issues, "pipeline[${definition.id}].validators", validators.map { it.id })
        addIdentifierIssues(issues, "pipeline[${definition.id}].outputs", outputs.map { it.output.id })
        addIdentifierIssues(issues, "pipeline[${definition.id}].extensions", extensions.map { it.id })
        harvestStrategy?.let {
            addIdentifierIssues(issues, "pipeline[${definition.id}].harvest", listOf(it.id))
        }

        effectiveTransport?.let {
            addIdentifierIssues(issues, "pipeline[${definition.id}].transport", listOf(it.id))
        }
        extractor?.let {
            addIdentifierIssues(issues, "pipeline[${definition.id}].extractor", listOf(it.id))
        }
        effectivePolicies?.let { addPolicyIdentifierIssues(issues, definition.id, resolvePolicies(defaults)) }

        return sortedIssues(issues)
    }

    /** Builds an immutable pipeline or throws deterministic configuration issues. */
    public fun build(defaults: PipelineDefaults = PipelineDefaults.none()): Pipeline<T> {
        val issues = validationIssues(defaults)
        if (issues.isNotEmpty()) {
            throw InvalidConfigurationException(issues)
        }

        val effectivePolicies = resolvePolicies(defaults)
        val effectiveTransport = transportRouter ?: defaults.transportRouter
        val effectiveRequestMiddleware =
            (requestMiddlewareReplacement ?: defaults.requestMiddlewares) + requestMiddlewares
        val effectiveResponseMiddleware =
            (responseMiddlewareReplacement ?: defaults.responseMiddlewares) + responseMiddlewares

        return Pipeline(
            definition = definition,
            transportRouter = requireNotNull(effectiveTransport),
            extractor = requireNotNull(extractor),
            discoveries = discoveries,
            requestMiddlewares = effectiveRequestMiddleware,
            responseMiddlewares = effectiveResponseMiddleware,
            transforms = transforms,
            validators = validators,
            outputs = outputs,
            policies = effectivePolicies,
            harvestStrategy = harvestStrategy ?: StatelessHarvestStrategy,
            extensions = extensions,
        )
    }

    private fun resolvePolicies(defaults: PipelineDefaults): PipelinePolicies {
        val base = requireNotNull(policySet ?: defaults.policies)
        return base.copy(
            requestCanonicalizer = requestCanonicalizer ?: base.requestCanonicalizer,
            requestIdentity = requestIdentity ?: base.requestIdentity,
            deduplication = deduplication ?: base.deduplication,
            recrawl = recrawl ?: base.recrawl,
            failureClassifier = failureClassifier ?: base.failureClassifier,
            retry = retry ?: base.retry,
            pacing = pacing ?: base.pacing,
            concurrency = concurrency ?: base.concurrency,
            scope = scope ?: base.scope,
            authorization = authorization ?: base.authorization,
            ordering = ordering ?: base.ordering,
            budget = budget ?: base.budget,
            admission = admission ?: base.admission,
            eligibility = eligibility ?: base.eligibility,
            health = health ?: base.health,
            circuitBreaker = circuitBreaker ?: base.circuitBreaker,
        )
    }

    private fun recordSingleAssignment(
        slot: String,
        alreadyConfigured: Boolean,
        replaceMethod: String = defaultReplaceMethod(slot),
    ) {
        if (alreadyConfigured) {
            builderIssues += ConfigurationIssue(
                path = "pipeline[${definition.id}].$slot",
                code = ConfigurationIssueCode.DUPLICATE_SINGLE_EXTENSION,
                message = "Multiple local registrations require an explicit $replaceMethod call",
            )
        }
    }

    private fun <P> assignPolicy(
        slot: String,
        current: P?,
        next: P,
        replaceMethod: String = defaultReplaceMethod(slot),
    ): P {
        recordSingleAssignment(slot, current != null, replaceMethod)
        return next
    }

    /** Slots are non-empty literals, so the first character always exists. */
    private fun defaultReplaceMethod(slot: String): String = "replace${slot[0].uppercaseChar()}${slot.substring(1)}"
}

/** Creates and builds a typed pipeline outside an engine DSL. */
public fun <T : Any> pipeline(
    id: String,
    configure: PipelineBuilder<T>.() -> Unit,
): Pipeline<T> = PipelineBuilder<T>(id).apply(configure).build(
    PipelineDefaults(policies = BuiltInPolicies.ordinary()),
)

/**
 * Immutable engine configuration containing heterogeneous typed pipelines.
 *
 * The engine itself is intentionally non-generic. Each pipeline is built by
 * a typed builder before registration, and the internal registry stores an
 * owner object for each typed pipeline rather than a user value, JSON node,
 * map, record envelope, or unchecked cast. Configuration construction is the
 * validation boundary; no runtime worker or coroutine is started here.
 */
public class EngineConfiguration(
    pipelines: List<Pipeline<*>>,
    stateStore: CrawlStateStore? = null,
    responseStore: ResponseStore? = null,
    responseCapturePolicy: ResponseCapturePolicy = CaptureResponsesWhenConfigured,
    responseCachePolicy: ResponseCachePolicy = NoResponseCachePolicy,
    observers: List<Observer> = emptyList(),
    extensions: List<ExtensionLifecycle> = emptyList(),
    clock: Clock = SystemClock,
    randomSource: RandomSource = DefaultRandomSource,
    requiredStateCapabilities: Set<CrawlStateCapability> = emptySet(),
    requiredResponseCapabilities: Set<ResponseStoreCapability> = emptySet(),
    defaultTransportRouter: TransportRouter? = null,
    defaultPolicies: PipelinePolicies? = null,
    defaultRequestMiddlewares: List<RequestMiddleware> = emptyList(),
    defaultResponseMiddlewares: List<ResponseMiddleware> = emptyList(),
    runtimeOptions: EngineRuntimeOptions = EngineRuntimeOptions(),
) {
    private val registry: HeterogeneousPipelineRegistry = HeterogeneousPipelineRegistry.from(pipelines)

    public val stateStore: CrawlStateStore = stateStore ?: InMemoryCrawlStateStore(
        maxQueueSize = runtimeOptions.maxQueueSize,
        maxRetainedJobs = runtimeOptions.maxRetainedJobs,
        maxRetainedResources = runtimeOptions.maxRetainedResources,
        maxRetainedDiscoveryState = runtimeOptions.maxRetainedDiscoveryState,
        maxCheckpointPayloadBytes = runtimeOptions.maxCheckpointPayloadBytes,
        leaseDuration = runtimeOptions.leaseDuration,
        clock = clock,
    )
    public val responseStore: ResponseStore? = responseStore
    public val responseCapturePolicy: ResponseCapturePolicy = responseCapturePolicy
    public val responseCachePolicy: ResponseCachePolicy = responseCachePolicy
    public val observers: List<Observer> = immutableSnapshot(observers)
    public val extensions: List<ExtensionLifecycle> = immutableSnapshot(extensions)
    public val clock: Clock = clock
    public val randomSource: RandomSource = randomSource
    public val requiredStateCapabilities: Set<CrawlStateCapability> = immutableSet(requiredStateCapabilities)
    public val requiredResponseCapabilities: Set<ResponseStoreCapability> = immutableSet(requiredResponseCapabilities)
    public val defaultTransportRouter: TransportRouter? = defaultTransportRouter
    public val defaultPolicies: PipelinePolicies? = defaultPolicies
    public val runtimeOptions: EngineRuntimeOptions = runtimeOptions

    /** Executable typed owners retained only for the internal runtime boundary. */
    internal val pipelineRegistrations: List<PipelineRegistration>
        get() = registry.registrations

    public val pipelines: List<Pipeline<*>>
        get() = registry.pipelines

    public val defaultRequestMiddlewares: List<RequestMiddleware> =
        immutableSnapshot(defaultRequestMiddlewares)
    public val defaultResponseMiddlewares: List<ResponseMiddleware> =
        immutableSnapshot(defaultResponseMiddlewares)

    init {
        val issues = ConfigurationValidator.validate(this)
        if (issues.isNotEmpty()) {
            throw InvalidConfigurationException(issues)
        }
    }
}

/**
 * Mutable, configuration-thread-confined DSL for [EngineConfiguration].
 *
 * Registration order is preserved for pipelines, observers, lifecycle
 * extensions, and middleware. Engine middleware/policies/router are inherited
 * by each registered pipeline unless that pipeline explicitly replaces them.
 * A single-cardinality engine registration may be set once or replaced only
 * through a `replace...` method. [build] validates capabilities and all
 * conflicts before constructing the immutable configuration.
 */
public class EngineConfigurationBuilder public constructor() {
    private val registrations = mutableListOf<PipelineRegistrationSpec>()
    private val observers = mutableListOf<Observer>()
    private val extensions = mutableListOf<ExtensionLifecycle>()
    private val requestMiddlewares = mutableListOf<RequestMiddleware>()
    private val responseMiddlewares = mutableListOf<ResponseMiddleware>()
    private val requiredStateCapabilities = linkedSetOf<CrawlStateCapability>()
    private val requiredResponseCapabilities = linkedSetOf<ResponseStoreCapability>()
    private val builderIssues = mutableListOf<ConfigurationIssue>()

    private var stateStore: CrawlStateStore? = null
    private var stateStoreConfigured = false
    private var responseStore: ResponseStore? = null
    private var responseStoreConfigured = false
    private var responseCapturePolicy: ResponseCapturePolicy = CaptureResponsesWhenConfigured
    private var responseCaptureConfigured = false
    private var responseCachePolicy: ResponseCachePolicy = NoResponseCachePolicy
    private var responseCacheConfigured = false
    private var defaultTransportRouter: TransportRouter? = null
    private var defaultTransportConfigured = false
    private var defaultPolicies: PipelinePolicies? = null
    private var defaultPoliciesConfigured = false
    private var clock: Clock = SystemClock
    private var clockConfigured = false
    private var randomSource: RandomSource = DefaultRandomSource
    private var randomConfigured = false
    private var requestMiddlewareReplacement: List<RequestMiddleware>? = null
    private var responseMiddlewareReplacement: List<ResponseMiddleware>? = null
    private var runtimeOptions: EngineRuntimeOptions = EngineRuntimeOptions()
    private var runtimeConfigured = false

    /** Sets the single effective state store; use [replaceStateStore] explicitly thereafter. */
    public fun stateStore(store: CrawlStateStore): EngineConfigurationBuilder {
        recordEngineSingle("stateStore", stateStoreConfigured)
        stateStore = store
        stateStoreConfigured = true
        return this
    }

    /** Explicitly replaces the configured state store. */
    public fun replaceStateStore(store: CrawlStateStore): EngineConfigurationBuilder {
        stateStore = store
        stateStoreConfigured = true
        return this
    }

    /** Installs the optional response store once. */
    public fun responseStore(store: ResponseStore): EngineConfigurationBuilder {
        recordEngineSingle("responseStore", responseStoreConfigured)
        responseStore = store
        responseStoreConfigured = true
        return this
    }

    /** Explicitly replaces the optional response store. */
    public fun replaceResponseStore(store: ResponseStore): EngineConfigurationBuilder {
        responseStore = store
        responseStoreConfigured = true
        return this
    }

    /** Sets the response-capture policy once; use [replaceResponsePolicy] to replace it. */
    public fun responsePolicy(policy: ResponseCapturePolicy): EngineConfigurationBuilder {
        recordEngineSingle("responsePolicy", responseCaptureConfigured)
        responseCapturePolicy = policy
        responseCaptureConfigured = true
        return this
    }

    /** Explicitly replaces the response-capture policy. */
    public fun replaceResponsePolicy(policy: ResponseCapturePolicy): EngineConfigurationBuilder {
        responseCapturePolicy = policy
        responseCaptureConfigured = true
        return this
    }

    /** Sets the response-cache policy once; use [replaceResponseCache] to replace it. */
    public fun responseCache(policy: ResponseCachePolicy): EngineConfigurationBuilder {
        recordEngineSingle("responseCache", responseCacheConfigured)
        responseCachePolicy = policy
        responseCacheConfigured = true
        return this
    }

    /** Explicitly replaces the response-cache policy. */
    public fun replaceResponseCache(policy: ResponseCachePolicy): EngineConfigurationBuilder {
        responseCachePolicy = policy
        responseCacheConfigured = true
        return this
    }

    /** Sets the inherited default transport router once. */
    public fun transportRouter(router: TransportRouter): EngineConfigurationBuilder {
        recordEngineSingle("transportRouter", defaultTransportConfigured)
        defaultTransportRouter = router
        defaultTransportConfigured = true
        return this
    }

    /** Explicitly replaces the inherited default transport router. */
    public fun replaceTransportRouter(router: TransportRouter): EngineConfigurationBuilder {
        defaultTransportRouter = router
        defaultTransportConfigured = true
        return this
    }

    /** Sets the inherited default policy bundle once. */
    public fun policies(policies: PipelinePolicies): EngineConfigurationBuilder {
        recordEngineSingle("policies", defaultPoliciesConfigured)
        defaultPolicies = policies
        defaultPoliciesConfigured = true
        return this
    }

    /** Explicitly replaces the inherited default policy bundle. */
    public fun replacePolicies(policies: PipelinePolicies): EngineConfigurationBuilder {
        defaultPolicies = policies
        defaultPoliciesConfigured = true
        return this
    }

    /** Appends one inherited request middleware. */
    public fun addRequestMiddleware(middleware: RequestMiddleware): EngineConfigurationBuilder {
        requestMiddlewares += middleware
        return this
    }

    /** Appends one inherited response middleware. */
    public fun addResponseMiddleware(middleware: ResponseMiddleware): EngineConfigurationBuilder {
        responseMiddlewares += middleware
        return this
    }

    /** Replaces the inherited request middleware chain explicitly. */
    public fun replaceRequestMiddleware(middleware: List<RequestMiddleware>): EngineConfigurationBuilder {
        requestMiddlewareReplacement = middleware.toList()
        return this
    }

    /** Replaces the inherited response middleware chain explicitly. */
    public fun replaceResponseMiddleware(middleware: List<ResponseMiddleware>): EngineConfigurationBuilder {
        responseMiddlewareReplacement = middleware.toList()
        return this
    }

    /** Sets bounded runtime options once; use [replaceRuntime] to replace them. */
    public fun runtime(options: EngineRuntimeOptions): EngineConfigurationBuilder {
        recordEngineSingle("runtime", runtimeConfigured)
        runtimeOptions = options
        runtimeConfigured = true
        return this
    }

    /** Explicitly replaces bounded runtime options. */
    public fun replaceRuntime(options: EngineRuntimeOptions): EngineConfigurationBuilder {
        runtimeOptions = options
        runtimeConfigured = true
        return this
    }

    /** Sets the clock once; use [replaceClock] for explicit replacement. */
    public fun clock(clock: Clock): EngineConfigurationBuilder {
        recordEngineSingle("clock", clockConfigured)
        this.clock = clock
        clockConfigured = true
        return this
    }

    /** Explicitly replaces the configured clock. */
    public fun replaceClock(clock: Clock): EngineConfigurationBuilder {
        this.clock = clock
        clockConfigured = true
        return this
    }

    /** Sets the randomness source once; use [replaceRandomSource] for replacement. */
    public fun randomSource(randomSource: RandomSource): EngineConfigurationBuilder {
        recordEngineSingle("randomSource", randomConfigured)
        this.randomSource = randomSource
        randomConfigured = true
        return this
    }

    /** Explicitly replaces the configured randomness source. */
    public fun replaceRandomSource(randomSource: RandomSource): EngineConfigurationBuilder {
        this.randomSource = randomSource
        randomConfigured = true
        return this
    }

    /** Adds an observer to the ordered observer list. */
    public fun observe(observer: Observer): EngineConfigurationBuilder {
        observers += observer
        return this
    }

    /** Adds a lifecycle extension after previously registered extensions. */
    public fun extension(extension: ExtensionLifecycle): EngineConfigurationBuilder {
        extensions += extension
        return this
    }

    /** Requires one state capability during configuration validation. */
    public fun requireStateCapability(capability: CrawlStateCapability): EngineConfigurationBuilder {
        requiredStateCapabilities += capability
        return this
    }

    /** Requires one response-store capability during configuration validation. */
    public fun requireResponseCapability(capability: ResponseStoreCapability): EngineConfigurationBuilder {
        requiredResponseCapabilities += capability
        return this
    }

    /** Registers an already-built typed pipeline without erasing its value type. */
    public fun <T : Any> register(pipeline: Pipeline<T>): EngineConfigurationBuilder {
        registrations += ExistingPipelineRegistration(pipeline)
        return this
    }

    /** Builds a typed pipeline registration at engine configuration time. */
    public fun <T : Any> register(
        id: String,
        configure: PipelineBuilder<T>.() -> Unit,
    ): EngineConfigurationBuilder {
        val builder = PipelineBuilder<T>(id).apply(configure)
        registrations += TypedPipelineBuilderRegistration(builder)
        return this
    }

    /** Returns deterministic issues without starting runtime work. */
    public fun validate(): List<ConfigurationIssue> = prepare().issues

    /** Builds the immutable configuration or throws [InvalidConfigurationException]. */
    public fun build(): EngineConfiguration {
        val prepared = prepare()
        if (prepared.issues.isNotEmpty()) {
            throw InvalidConfigurationException(prepared.issues)
        }
        return EngineConfiguration(
            pipelines = prepared.registry.pipelines,
            stateStore = prepared.stateStore,
            responseStore = responseStore,
            responseCapturePolicy = responseCapturePolicy,
            responseCachePolicy = responseCachePolicy,
            observers = immutableSnapshot(observers),
            extensions = immutableSnapshot(extensions),
            clock = clock,
            randomSource = randomSource,
            requiredStateCapabilities = immutableSet(requiredStateCapabilities),
            requiredResponseCapabilities = immutableSet(requiredResponseCapabilities),
            defaultTransportRouter = defaultTransportRouter,
            defaultPolicies = prepared.defaults.policies,
            defaultRequestMiddlewares = prepared.defaults.requestMiddlewares,
            defaultResponseMiddlewares = prepared.defaults.responseMiddlewares,
            runtimeOptions = runtimeOptions,
        )
    }

    private fun prepare(): PreparedConfiguration {
        val defaults = PipelineDefaults(
            transportRouter = defaultTransportRouter,
            policies = defaultPolicies ?: BuiltInPolicies.ordinary(runtimeOptions.workerCount, clock),
            requestMiddlewares = requestMiddlewareReplacement ?: requestMiddlewares,
            responseMiddlewares = responseMiddlewareReplacement ?: responseMiddlewares,
        )
        val issues = builderIssues.toMutableList()
        val effectiveStateStore = stateStore ?: InMemoryCrawlStateStore(
            maxQueueSize = runtimeOptions.maxQueueSize,
            maxRetainedJobs = runtimeOptions.maxRetainedJobs,
            maxRetainedResources = runtimeOptions.maxRetainedResources,
            maxRetainedDiscoveryState = runtimeOptions.maxRetainedDiscoveryState,
            maxCheckpointPayloadBytes = runtimeOptions.maxCheckpointPayloadBytes,
            leaseDuration = runtimeOptions.leaseDuration,
            clock = clock,
        )

        val builtRegistrations = mutableListOf<PipelineRegistration>()
        registrations.forEach { registration ->
            val registrationIssues = registration.validationIssues(defaults)
            issues += registrationIssues
            if (registrationIssues.isEmpty()) {
                builtRegistrations += registration.build(defaults)
            }
        }

        issues += ConfigurationValidator.validateEngineSelections(
            stateStore = effectiveStateStore,
            responseStore = responseStore,
            responseCapturePolicy = responseCapturePolicy,
            responseCachePolicy = responseCachePolicy,
            observers = observers,
            extensions = extensions,
            requiredStateCapabilities = requiredStateCapabilities,
            requiredResponseCapabilities = requiredResponseCapabilities,
            requestMiddlewares = defaults.requestMiddlewares,
            responseMiddlewares = defaults.responseMiddlewares,
            pipelines = builtRegistrations.map { it.asPipeline() },
        )
        return PreparedConfiguration(
            defaults = defaults,
            registry = HeterogeneousPipelineRegistry(builtRegistrations),
            stateStore = effectiveStateStore,
            issues = sortedIssues(issues),
        )
    }

    private fun recordEngineSingle(slot: String, alreadyConfigured: Boolean) {
        if (alreadyConfigured) {
            builderIssues += ConfigurationIssue(
                path = "engine.$slot",
                code = ConfigurationIssueCode.DUPLICATE_SINGLE_EXTENSION,
                message = "Multiple registrations require an explicit " +
                    "replace${slot.replaceFirstChar { it.uppercase() }} call",
            )
        }
    }
}

/** Non-generic engine facade over an immutable configuration. */
public class TrawlerEngine public constructor(
    public val configuration: EngineConfiguration,
) {
    private val runtime: EngineRuntime = EngineRuntime(configuration)

    /** Runs selected pipelines once and returns an aggregate lifecycle result. */
    public suspend fun runOnce(request: RunRequest = RunRequest()): RunResult = runtime.runOnce(request)

    /** Replays stored responses through a registered typed pipeline without network or crawl jobs. */
    public suspend fun <T : Any> replay(selection: ReplaySelection<T>): ReplayResult =
        runtime.replay(selection)

    /**
     * Waits for all finite engine runs currently owned by this engine to
     * settle. The default timeout is bounded by [EngineRuntimeOptions].
     * Returns `false` when the bound expires.
     */
    public suspend fun awaitIdle(timeout: java.time.Duration = configuration.runtimeOptions.awaitIdleTimeout): Boolean =
        runtime.awaitIdle(timeout)

    /** Starts the selected built-in coroutine scheduler without activating one during construction. */
    public suspend fun startDaemon(config: DaemonConfig = DaemonConfig()): EngineHandle =
        runtime.startDaemon(config, com.rmjtromp.trawler.schedule.CoroutineScheduler())

    /** Starts a caller-selected scheduler as a structured daemon child. */
    public suspend fun startDaemon(
        scheduler: com.rmjtromp.trawler.schedule.Scheduler,
        config: DaemonConfig = DaemonConfig(),
    ): EngineHandle = runtime.startDaemon(config, scheduler)

    /** Cooperatively stops daemon triggers, active runs, extensions, and owned resources. */
    public suspend fun shutdown(): Unit = runtime.shutdown()

    /** Builds a non-generic engine from the configuration DSL. */
    public companion object {
        public operator fun invoke(configure: EngineConfigurationBuilder.() -> Unit): TrawlerEngine =
            TrawlerEngine(EngineConfigurationBuilder().apply(configure).build())
    }
}

/** Stable issue code used by deterministic configuration validation. */
public enum class ConfigurationIssueCode {
    INVALID_IDENTIFIER,
    DUPLICATE_PIPELINE_ID,
    DUPLICATE_SINGLE_EXTENSION,
    DUPLICATE_ORDERED_EXTENSION,
    MISSING_REQUIRED_EXTENSION,
    MISSING_RUNTIME_CAPABILITY,
    UNSUPPORTED_CAPABILITY,
}

/** One deterministic, redacted configuration diagnostic. */
public data class ConfigurationIssue(
    public val path: String,
    public val code: ConfigurationIssueCode,
    public val message: String,
)

/**
 * Thrown when configuration cannot be made into a valid immutable engine
 * graph. The sorted issue list is stable across runs and contains no request
 * bodies, response bodies, credentials, or produced values.
 */
public class InvalidConfigurationException(
    issues: List<ConfigurationIssue>,
) : IllegalArgumentException(formatIssues(issues)) {
    public val issues: List<ConfigurationIssue> = immutableSnapshot(sortedIssues(issues))
}

internal interface PipelineRegistration {
    val pipelineId: String
    fun asPipeline(): Pipeline<*>
    suspend fun execute(context: PipelineExecutionContext): PipelineRunResult
    suspend fun executeReplay(
        context: ReplayExecutionContext,
        responses: List<com.rmjtromp.trawler.response.StoredResponse>,
    ): ReplayResult
}

/**
 * Type-preserving registration owner. The generic value type is captured when
 * [EngineConfigurationBuilder.register] receives a [Pipeline<T>]. This
 * boundary stores no produced value and performs no unchecked cast.
 */
internal class TypedPipelineRegistration<T : Any>(
    private val typedPipeline: Pipeline<T>,
) : PipelineRegistration {
    override val pipelineId: String = typedPipeline.id

    override fun asPipeline(): Pipeline<*> = typedPipeline

    override suspend fun execute(context: PipelineExecutionContext): PipelineRunResult =
        executeTypedPipeline(typedPipeline, context)

    override suspend fun executeReplay(
        context: ReplayExecutionContext,
        responses: List<com.rmjtromp.trawler.response.StoredResponse>,
    ): ReplayResult = executeTypedReplay(typedPipeline, context, responses)
}

private interface PipelineRegistrationSpec {
    fun validationIssues(defaults: PipelineDefaults): List<ConfigurationIssue>
    fun build(defaults: PipelineDefaults): PipelineRegistration
}

private class ExistingPipelineRegistration<T : Any>(
    private val pipeline: Pipeline<T>,
) : PipelineRegistrationSpec {
    override fun validationIssues(defaults: PipelineDefaults): List<ConfigurationIssue> =
        ConfigurationValidator.validatePipeline(pipeline)

    override fun build(defaults: PipelineDefaults): PipelineRegistration =
        TypedPipelineRegistration(pipeline)
}

private class TypedPipelineBuilderRegistration<T : Any>(
    private val builder: PipelineBuilder<T>,
) : PipelineRegistrationSpec {
    override fun validationIssues(defaults: PipelineDefaults): List<ConfigurationIssue> =
        builder.validationIssues(defaults)

    override fun build(defaults: PipelineDefaults): PipelineRegistration =
        TypedPipelineRegistration(builder.build(defaults))
}

/**
 * Isolated heterogeneous registry. Each entry was built through a typed
 * registration owner; the registry exposes only star-projected definitions
 * for inspection and never executes or casts a produced value.
 */
internal class HeterogeneousPipelineRegistry(
    registrations: List<PipelineRegistration>,
) {
    internal val registrations: List<PipelineRegistration> = immutableSnapshot(registrations)
    val pipelines: List<Pipeline<*>> = immutableSnapshot(registrations.map { it.asPipeline() })

    companion object {
        fun from(pipelines: List<Pipeline<*>>): HeterogeneousPipelineRegistry =
            HeterogeneousPipelineRegistry(pipelines.map { it.runtimeRegistration() })
    }
}

private data class PreparedConfiguration(
    val defaults: PipelineDefaults,
    val registry: HeterogeneousPipelineRegistry,
    val stateStore: CrawlStateStore,
    val issues: List<ConfigurationIssue>,
)

internal object ConfigurationValidator {
    fun validate(configuration: EngineConfiguration): List<ConfigurationIssue> =
        validateEngineSelections(
            stateStore = configuration.stateStore,
            responseStore = configuration.responseStore,
            responseCapturePolicy = configuration.responseCapturePolicy,
            responseCachePolicy = configuration.responseCachePolicy,
            observers = configuration.observers,
            extensions = configuration.extensions,
            requiredStateCapabilities = configuration.requiredStateCapabilities,
            requiredResponseCapabilities = configuration.requiredResponseCapabilities,
            requestMiddlewares = configuration.defaultRequestMiddlewares,
            responseMiddlewares = configuration.defaultResponseMiddlewares,
            pipelines = configuration.pipelines,
        ) + configuration.pipelines.flatMap(::validatePipeline)

    fun validatePipeline(pipeline: Pipeline<*>): List<ConfigurationIssue> {
        val issues = mutableListOf<ConfigurationIssue>()
        val path = "pipeline[${pipeline.id}]"
        addIdentifierIssues(issues, "$path.transport", listOf(pipeline.transportRouter.id))
        addIdentifierIssues(issues, "$path.extractor", listOf(pipeline.extractor.id))
        addIdentifierIssues(issues, "$path.discoveries", pipeline.discoveries.map { it.id })
        addIdentifierIssues(issues, "$path.requestMiddleware", pipeline.requestMiddlewares.map { it.id })
        addIdentifierIssues(issues, "$path.responseMiddleware", pipeline.responseMiddlewares.map { it.id })
        addIdentifierIssues(issues, "$path.transforms", pipeline.transforms.map { it.id })
        addIdentifierIssues(issues, "$path.validators", pipeline.validators.map { it.id })
        addIdentifierIssues(issues, "$path.outputs", pipeline.outputs.map { it.output.id })
        addIdentifierIssues(issues, "$path.extensions", pipeline.extensions.map { it.id })
        addIdentifierIssues(issues, "$path.harvest", listOf(pipeline.harvestStrategy.id))
        addPolicyIdentifierIssues(issues, pipeline.id, pipeline.policies)
        return sortedIssues(issues)
    }

    fun validateEngineSelections(
        stateStore: CrawlStateStore?,
        responseStore: ResponseStore?,
        responseCapturePolicy: ResponseCapturePolicy,
        responseCachePolicy: ResponseCachePolicy,
        observers: List<Observer>,
        extensions: List<ExtensionLifecycle>,
        requiredStateCapabilities: Set<CrawlStateCapability>,
        requiredResponseCapabilities: Set<ResponseStoreCapability>,
        requestMiddlewares: List<RequestMiddleware>,
        responseMiddlewares: List<ResponseMiddleware>,
        pipelines: List<Pipeline<*>>,
    ): List<ConfigurationIssue> {
        val issues = mutableListOf<ConfigurationIssue>()
        stateStore?.let {
            addIdentifierIssues(issues, "engine.stateStore", listOf(it.id))
            requiredStateCapabilities
                .filterNot { capability -> it.capabilities.contains(capability) }
                .forEach { capability ->
                    issues += ConfigurationIssue(
                        path = "engine.stateStore.capabilities.${capability.name}",
                        code = ConfigurationIssueCode.UNSUPPORTED_CAPABILITY,
                        message = "State store '${it.id}' does not advertise required capability ${capability.name}",
                    )
                }
            validateRuntimeStateCapabilities(issues, it)
        }
        if (responseStore == null && requiredResponseCapabilities.isNotEmpty()) {
            requiredResponseCapabilities.forEach { capability ->
                issues += ConfigurationIssue(
                    path = "engine.responseStore.capabilities.${capability.name}",
                    code = ConfigurationIssueCode.UNSUPPORTED_CAPABILITY,
                    message = "Response capability ${capability.name} requires an installed response store",
                )
            }
        }
        responseStore?.let {
            addIdentifierIssues(issues, "engine.responseStore", listOf(it.id))
            requiredResponseCapabilities
                .filterNot { capability -> it.capabilities.contains(capability) }
                .forEach { capability ->
                    issues += ConfigurationIssue(
                        path = "engine.responseStore.capabilities.${capability.name}",
                        code = ConfigurationIssueCode.UNSUPPORTED_CAPABILITY,
                        message = "Response store '${it.id}' does not advertise required capability ${capability.name}",
                    )
                }
            validateResponseCapabilities(issues, it)
        }
        addIdentifierIssues(issues, "engine.responsePolicy", listOf(responseCapturePolicy.id))
        if (responseCapturePolicy.requiresStore && responseStore == null) {
            issues += ConfigurationIssue(
                path = "engine.responsePolicy",
                code = ConfigurationIssueCode.MISSING_RUNTIME_CAPABILITY,
                message = "Response policy '${responseCapturePolicy.id}' requires an installed response store",
            )
        }
        addIdentifierIssues(issues, "engine.responseCache", listOf(responseCachePolicy.id))
        if (responseCachePolicy !== NoResponseCachePolicy && responseStore == null) {
            issues += ConfigurationIssue(
                path = "engine.responseCache",
                code = ConfigurationIssueCode.MISSING_RUNTIME_CAPABILITY,
                message = "Response cache policy '${responseCachePolicy.id}' requires an installed response store",
            )
        }
        responseStore?.let { store ->
            responseCachePolicy.requiredCapabilities
                .filterNot(store.capabilities::contains)
                .forEach { capability ->
                    issues += ConfigurationIssue(
                        path = "engine.responseCache.capabilities.${capability.name}",
                        code = ConfigurationIssueCode.UNSUPPORTED_CAPABILITY,
                        message = "Response cache policy '${responseCachePolicy.id}' requires capability ${capability.name}",
                    )
                }
        }
        addIdentifierIssues(issues, "engine.observers", observers.map { it.id })
        addIdentifierIssues(issues, "engine.extensions", extensions.map { it.id })
        addIdentifierIssues(issues, "engine.requestMiddleware", requestMiddlewares.map { it.id })
        addIdentifierIssues(issues, "engine.responseMiddleware", responseMiddlewares.map { it.id })
        fun requireRegisteredLifecycle(path: String, extension: ExtensionLifecycle) {
            if (extensions.none { it === extension }) {
                issues += ConfigurationIssue(
                    path = "$path.registration",
                    code = ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION,
                    message = "Lifecycle '${extension.id}' must be registered as an engine extension",
                )
            }
        }
        if (stateStore is ExtensionLifecycle) {
            requireRegisteredLifecycle("engine.stateStore", stateStore)
        }
        if (responseStore is ExtensionLifecycle) {
            requireRegisteredLifecycle("engine.responseStore", responseStore)
        }
        pipelines.forEach { pipeline ->
            pipeline.extensions
                .filter { pipelineExtension -> extensions.any { engineExtension -> engineExtension === pipelineExtension } }
                .forEach { extension ->
                    issues += ConfigurationIssue(
                        path = "pipeline[${pipeline.id}].extensions[${extension.id}]",
                        code = ConfigurationIssueCode.DUPLICATE_ORDERED_EXTENSION,
                        message = "Pipeline lifecycle '${extension.id}' cannot also be registered as an engine extension",
                    )
                }
            pipeline.outputs.forEach { binding ->
                if (binding.output is ExtensionLifecycle) {
                    requireRegisteredLifecycle("pipeline[${pipeline.id}].output[${binding.output.id}]", binding.output)
                }
            }
        }

        val pipelineIds = pipelines.map { it.id }
        val seenPipelineIds = mutableSetOf<String>()
        pipelineIds.forEach { id ->
            if (!seenPipelineIds.add(id)) {
                issues += ConfigurationIssue(
                    path = "engine.pipelines[$id]",
                    code = ConfigurationIssueCode.DUPLICATE_PIPELINE_ID,
                    message = "Pipeline IDs must be unique within one engine",
                )
            }
        }
        if (stateStore is WorkOrderingStore) {
            pipelines.filterNot { stateStore.supports(it.policies.ordering) }.forEach { pipeline ->
                issues += ConfigurationIssue(
                    path = "pipeline[${pipeline.id}].policies.ordering",
                    code = ConfigurationIssueCode.UNSUPPORTED_CAPABILITY,
                    message = "State store '${stateStore.id}' does not support ordering " +
                        "'${pipeline.policies.ordering.id}' without substitution",
                )
            }
        }
        pipelines.forEach { pipeline ->
            val resumable = pipeline.discoveries.filterIsInstance<ResumableDiscovery>()
            if (resumable.isNotEmpty() &&
                (stateStore !is com.rmjtromp.trawler.state.DiscoveryCheckpointStore ||
                    !stateStore.capabilities.contains(CrawlStateCapability.DISCOVERY_CHECKPOINTS))
            ) {
                issues += ConfigurationIssue(
                    path = "pipeline[${pipeline.id}].discoveries.checkpoint",
                    code = ConfigurationIssueCode.MISSING_RUNTIME_CAPABILITY,
                    message = "Resumable discoveries require a checkpoint-capable state store",
                )
            }
            if (pipeline.harvestStrategy.mode != com.rmjtromp.trawler.discovery.HarvestMode.STATELESS &&
                pipeline.discoveries.none { it is ResumableDiscovery }
            ) {
                issues += ConfigurationIssue(
                    path = "pipeline[${pipeline.id}].harvest",
                    code = ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION,
                    message = "Non-stateless harvest strategies require at least one resumable discovery",
                )
            }
            if (pipeline.harvestStrategy.stateCodec != null &&
                (stateStore !is com.rmjtromp.trawler.state.HarvestStateStore ||
                    !stateStore.capabilities.contains(CrawlStateCapability.HARVEST_STATE))
            ) {
                issues += ConfigurationIssue(
                    path = "pipeline[${pipeline.id}].harvest",
                    code = ConfigurationIssueCode.MISSING_RUNTIME_CAPABILITY,
                    message = "Non-stateless harvest strategies require harvest state storage",
                )
            }
        }
        return sortedIssues(issues)
    }

    private fun validateResponseCapabilities(
        issues: MutableList<ConfigurationIssue>,
        store: ResponseStore,
    ) {
        val requirements = listOf(
            ResponseStoreCapability.HISTORY to (store is ResponseHistoryStore),
            ResponseStoreCapability.REPLAY to (store is ResponseReplayStore),
            ResponseStoreCapability.TTL_EVICTION to (store is ResponseRetentionStore),
        )
        requirements
            .filter { (capability, implemented) -> capability in store.capabilities && !implemented }
            .forEach { (capability, _) ->
                issues += ConfigurationIssue(
                    path = "engine.responseStore.capabilities.${capability.name}",
                    code = ConfigurationIssueCode.MISSING_RUNTIME_CAPABILITY,
                    message = "Response store '${store.id}' advertises ${capability.name} but does not implement its focused capability",
                )
            }
    }

    private fun validateRuntimeStateCapabilities(
        issues: MutableList<ConfigurationIssue>,
        store: CrawlStateStore,
    ) {
        val requirements = listOf(
            "WORK_QUEUE" to (
                store is WorkQueueStore && store.capabilities.contains(CrawlStateCapability.WORK_QUEUE)
                ),
            "PIPELINE_SCOPED_WORK" to (
                store is PipelineScopedWorkQueueStore &&
                    store.capabilities.contains(CrawlStateCapability.PIPELINE_SCOPED_WORK)
                ),
            "LEASE_FENCING" to (
                store is LeaseStore && store.capabilities.contains(CrawlStateCapability.LEASE_FENCING)
                ),
            "RESOURCE_HISTORY" to (
                store is ResourceHistoryStore &&
                    store.capabilities.contains(CrawlStateCapability.RESOURCE_HISTORY)
                ),
            "ATTEMPT_HISTORY" to (
                store is AttemptStore &&
                    store.capabilities.contains(CrawlStateCapability.ATTEMPT_HISTORY)
                ),
            "CANCELLATION" to (
                store is WorkCancellationStore &&
                    store.capabilities.contains(CrawlStateCapability.CANCELLATION)
                ),
            "WORK_ORDERING" to (
                store is WorkOrderingStore &&
                    store.capabilities.contains(CrawlStateCapability.WORK_ORDERING)
                ),
            "WORK_STATUS" to (
                store is WorkQueueStatus &&
                    store.capabilities.contains(CrawlStateCapability.WORK_STATUS)
                ),
            "RUN_HISTORY" to (
                !store.capabilities.contains(CrawlStateCapability.RUN_HISTORY) ||
                    store is RunHistoryStore
                ),
            "RUN_RECOVERY" to (
                !store.capabilities.contains(CrawlStateCapability.RUN_RECOVERY) ||
                    store is RunHistoryStore
                ),
            "DISCOVERY_CHECKPOINTS" to (
                !store.capabilities.contains(CrawlStateCapability.DISCOVERY_CHECKPOINTS) ||
                    store is com.rmjtromp.trawler.state.DiscoveryCheckpointStore
                ),
            "ATOMIC_ENQUEUE_AND_CHECKPOINT" to (
                !store.capabilities.contains(CrawlStateCapability.ATOMIC_ENQUEUE_AND_CHECKPOINT) ||
                    store is com.rmjtromp.trawler.state.AtomicEnqueueAndCheckpointStore
                ),
            "CHECKPOINT_QUARANTINE" to (
                !store.capabilities.contains(CrawlStateCapability.CHECKPOINT_QUARANTINE) ||
                    store is com.rmjtromp.trawler.state.CheckpointQuarantineStore
                ),
            "HARVEST_STATE" to (
                !store.capabilities.contains(CrawlStateCapability.HARVEST_STATE) ||
                    store is com.rmjtromp.trawler.state.HarvestStateStore
                ),
            "HARVEST_STATE_QUARANTINE" to (
                !store.capabilities.contains(CrawlStateCapability.HARVEST_STATE_QUARANTINE) ||
                    store is com.rmjtromp.trawler.state.HarvestStateQuarantineStore
                ),
            "QUEUE_WATERMARKS" to (
                !store.capabilities.contains(CrawlStateCapability.QUEUE_WATERMARKS) ||
                    store is com.rmjtromp.trawler.state.WorkQueueMetricsStore
                ),
            "PIPELINE_HEALTH" to (
                !store.capabilities.contains(CrawlStateCapability.PIPELINE_HEALTH) ||
                    store is com.rmjtromp.trawler.state.PipelineHealthStore
                ),
            "PIPELINE_ELIGIBILITY_OVERRIDES" to (
                !store.capabilities.contains(CrawlStateCapability.PIPELINE_ELIGIBILITY_OVERRIDES) ||
                    store is com.rmjtromp.trawler.state.PipelineEligibilityOverrideStore
                ),
            "STATE_RETENTION" to (
                !store.capabilities.contains(CrawlStateCapability.STATE_RETENTION) ||
                    store is com.rmjtromp.trawler.state.CrawlStateRetentionStore
                ),
        )
        requirements.filterNot { it.second }.forEach { (capability, _) ->
            issues += ConfigurationIssue(
                path = "engine.stateStore.runtime.$capability",
                code = ConfigurationIssueCode.MISSING_RUNTIME_CAPABILITY,
                message = "State store '${store.id}' must implement and advertise runtime capability $capability",
            )
        }
    }
}

private fun addPolicyIdentifierIssues(
    issues: MutableList<ConfigurationIssue>,
    pipelineId: String,
    policies: PipelinePolicies,
) {
    val entries = listOf(
        "requestCanonicalizer" to policies.requestCanonicalizer.id,
        "requestIdentity" to policies.requestIdentity.id,
        "deduplication" to policies.deduplication.id,
        "recrawl" to policies.recrawl.id,
        "failureClassifier" to policies.failureClassifier.id,
        "retry" to policies.retry.id,
        "pacing" to policies.pacing.id,
        "concurrency" to policies.concurrency.id,
        "scope" to policies.scope.id,
        "authorization" to policies.authorization.id,
        "ordering" to policies.ordering.id,
        "budget" to policies.budget.id,
        "admission" to policies.admission.id,
        "eligibility" to policies.eligibility.id,
        "health" to policies.health.id,
        "circuitBreaker" to policies.circuitBreaker.id,
    )
    entries.forEach { (slot, id) ->
        addIdentifierIssues(issues, "pipeline[$pipelineId].policies.$slot", listOf(id))
    }
}

private fun addIdentifierIssues(
    issues: MutableList<ConfigurationIssue>,
    path: String,
    identifiers: List<String>,
) {
    val seen = mutableSetOf<String>()
    identifiers.forEach { id ->
        if (id.isBlank()) {
            issues += ConfigurationIssue(
                path = path,
                code = ConfigurationIssueCode.INVALID_IDENTIFIER,
                message = "Extension identifier must not be blank",
            )
        } else if (!seen.add(id)) {
            issues += ConfigurationIssue(
                path = path,
                code = ConfigurationIssueCode.DUPLICATE_ORDERED_EXTENSION,
                message = "Extension identifier '$id' is registered more than once in this ordered scope",
            )
        }
    }
}

private fun sortedIssues(issues: List<ConfigurationIssue>): List<ConfigurationIssue> =
    immutableSnapshot(issues.sortedWith(compareBy({ it.path }, { it.code.name }, { it.message })))

private fun <T> immutableSet(values: Set<T>): Set<T> =
    Collections.unmodifiableSet(values.toSet())

private fun formatIssues(issues: List<ConfigurationIssue>): String =
    sortedIssues(issues).joinToString(
        prefix = "Invalid Trawler configuration:\n",
        separator = "\n",
    ) { issue -> "- ${issue.path} [${issue.code}]: ${issue.message}" }

private class FixedTransportRouter(
    private val transport: Transport,
) : TransportRouter {
    override val id: String
        get() = transport.id

    override fun select(request: CrawlRequest, context: RequestContext): Transport =
        transport
}
