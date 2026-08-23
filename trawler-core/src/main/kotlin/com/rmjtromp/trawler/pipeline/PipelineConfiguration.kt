package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.discovery.Discovery
import com.rmjtromp.trawler.event.Observer
import com.rmjtromp.trawler.extension.ExtensionLifecycle
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.model.immutableSnapshot
import com.rmjtromp.trawler.policy.AuthorizationPolicy
import com.rmjtromp.trawler.policy.Clock
import com.rmjtromp.trawler.policy.ConcurrencyPolicy
import com.rmjtromp.trawler.policy.CrawlBudgetPolicy
import com.rmjtromp.trawler.policy.CrawlScopePolicy
import com.rmjtromp.trawler.policy.DeduplicationPolicy
import com.rmjtromp.trawler.policy.DefaultRandomSource
import com.rmjtromp.trawler.policy.FailureClassifier
import com.rmjtromp.trawler.policy.RandomSource
import com.rmjtromp.trawler.policy.RecrawlPolicy
import com.rmjtromp.trawler.policy.RequestCanonicalizer
import com.rmjtromp.trawler.policy.RequestIdentityStrategy
import com.rmjtromp.trawler.policy.RequestPacingPolicy
import com.rmjtromp.trawler.policy.RetryPolicy
import com.rmjtromp.trawler.policy.SystemClock
import com.rmjtromp.trawler.policy.WorkOrderingPolicy
import com.rmjtromp.trawler.response.ResponseStore
import com.rmjtromp.trawler.response.ResponseStoreCapability
import com.rmjtromp.trawler.state.CrawlStateCapability
import com.rmjtromp.trawler.state.CrawlStateStore
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
        recordSingleAssignment("transport", transportConfigured, replacement = false)
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
        recordSingleAssignment("extractor", extractorConfigured, replacement = false)
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
        recordSingleAssignment("policies", policySetConfigured, replacement = false)
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
        requestCanonicalizer = assignPolicy("requestCanonicalizer", requestCanonicalizer, policy)
        return this
    }

    /** Explicitly replaces the request canonicalizer. */
    public fun replaceCanonicalizer(policy: RequestCanonicalizer): PipelineBuilder<T> {
        requestCanonicalizer = policy
        return this
    }

    /** Replaces the inherited request identity strategy once. */
    public fun identity(policy: RequestIdentityStrategy): PipelineBuilder<T> {
        requestIdentity = assignPolicy("requestIdentity", requestIdentity, policy)
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

        effectiveTransport?.let {
            addIdentifierIssues(issues, "pipeline[${definition.id}].transport", listOf(it.id))
        }
        extractor?.let {
            addIdentifierIssues(issues, "pipeline[${definition.id}].extractor", listOf(it.id))
        }
        effectivePolicies?.let { addPolicyIdentifierIssues(issues, definition.id, it) }

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
        )
    }

    private fun recordSingleAssignment(slot: String, alreadyConfigured: Boolean, replacement: Boolean) {
        if (alreadyConfigured && !replacement) {
            builderIssues += ConfigurationIssue(
                path = "pipeline[${definition.id}].$slot",
                code = ConfigurationIssueCode.DUPLICATE_SINGLE_EXTENSION,
                message = "Multiple local registrations require an explicit " +
                    "replace${slot.replaceFirstChar { it.uppercase() }} call",
            )
        }
    }

    private fun <P> assignPolicy(slot: String, current: P?, next: P): P {
        recordSingleAssignment(slot, current != null, replacement = false)
        return next
    }
}

/** Creates and builds a typed pipeline outside an engine DSL. */
public fun <T : Any> pipeline(
    id: String,
    configure: PipelineBuilder<T>.() -> Unit,
): Pipeline<T> = PipelineBuilder<T>(id).apply(configure).build()

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
    stateStore: CrawlStateStore,
    responseStore: ResponseStore? = null,
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
) {
    private val registry: HeterogeneousPipelineRegistry = HeterogeneousPipelineRegistry.from(pipelines)

    public val stateStore: CrawlStateStore = stateStore
    public val responseStore: ResponseStore? = responseStore
    public val observers: List<Observer> = immutableSnapshot(observers)
    public val extensions: List<ExtensionLifecycle> = immutableSnapshot(extensions)
    public val clock: Clock = clock
    public val randomSource: RandomSource = randomSource
    public val requiredStateCapabilities: Set<CrawlStateCapability> = immutableSet(requiredStateCapabilities)
    public val requiredResponseCapabilities: Set<ResponseStoreCapability> = immutableSet(requiredResponseCapabilities)
    public val defaultTransportRouter: TransportRouter? = defaultTransportRouter
    public val defaultPolicies: PipelinePolicies? = defaultPolicies

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
            stateStore = requireNotNull(stateStore),
            responseStore = responseStore,
            observers = immutableSnapshot(observers),
            extensions = immutableSnapshot(extensions),
            clock = clock,
            randomSource = randomSource,
            requiredStateCapabilities = immutableSet(requiredStateCapabilities),
            requiredResponseCapabilities = immutableSet(requiredResponseCapabilities),
            defaultTransportRouter = defaultTransportRouter,
            defaultPolicies = defaultPolicies,
            defaultRequestMiddlewares = prepared.defaults.requestMiddlewares,
            defaultResponseMiddlewares = prepared.defaults.responseMiddlewares,
        )
    }

    private fun prepare(): PreparedConfiguration {
        val defaults = PipelineDefaults(
            transportRouter = defaultTransportRouter,
            policies = defaultPolicies,
            requestMiddlewares = requestMiddlewareReplacement ?: requestMiddlewares,
            responseMiddlewares = responseMiddlewareReplacement ?: responseMiddlewares,
        )
        val issues = builderIssues.toMutableList()
        if (stateStore == null) {
            issues += ConfigurationIssue(
                path = "engine.stateStore",
                code = ConfigurationIssueCode.MISSING_REQUIRED_EXTENSION,
                message = "Exactly one effective crawl state store is required",
            )
        }

        val builtRegistrations = mutableListOf<PipelineRegistration>()
        registrations.forEach { registration ->
            val registrationIssues = registration.validationIssues(defaults)
            issues += registrationIssues
            if (registrationIssues.isEmpty()) {
                builtRegistrations += registration.build(defaults)
            }
        }

        issues += ConfigurationValidator.validateEngineSelections(
            stateStore = stateStore,
            responseStore = responseStore,
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
}

/** Safe star-projection wrapper used only by the direct immutable constructor. */
private class ExistingStarPipelineRegistration(
    private val pipeline: Pipeline<*>,
) : PipelineRegistration {
    override val pipelineId: String = pipeline.id

    override fun asPipeline(): Pipeline<*> = pipeline
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
    private val registrations: List<PipelineRegistration> = immutableSnapshot(registrations)
    val pipelines: List<Pipeline<*>> = immutableSnapshot(registrations.map { it.asPipeline() })

    companion object {
        fun from(pipelines: List<Pipeline<*>>): HeterogeneousPipelineRegistry =
            HeterogeneousPipelineRegistry(pipelines.map(::ExistingStarPipelineRegistration))
    }
}

private data class PreparedConfiguration(
    val defaults: PipelineDefaults,
    val registry: HeterogeneousPipelineRegistry,
    val issues: List<ConfigurationIssue>,
)

internal object ConfigurationValidator {
    fun validate(configuration: EngineConfiguration): List<ConfigurationIssue> =
        validateEngineSelections(
            stateStore = configuration.stateStore,
            responseStore = configuration.responseStore,
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
        addPolicyIdentifierIssues(issues, pipeline.id, pipeline.policies)
        return sortedIssues(issues)
    }

    fun validateEngineSelections(
        stateStore: CrawlStateStore?,
        responseStore: ResponseStore?,
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
        }
        addIdentifierIssues(issues, "engine.observers", observers.map { it.id })
        addIdentifierIssues(issues, "engine.extensions", extensions.map { it.id })
        addIdentifierIssues(issues, "engine.requestMiddleware", requestMiddlewares.map { it.id })
        addIdentifierIssues(issues, "engine.responseMiddleware", responseMiddlewares.map { it.id })

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
        return sortedIssues(issues)
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
