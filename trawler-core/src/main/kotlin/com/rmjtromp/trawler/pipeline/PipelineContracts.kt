package com.rmjtromp.trawler.pipeline

import com.rmjtromp.trawler.model.ContextAttributes
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.ExtractionContext
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.model.ResponseReference
import com.rmjtromp.trawler.model.TransformContext
import com.rmjtromp.trawler.model.ValidationContext
import com.rmjtromp.trawler.policy.RetryHint
import java.time.Instant

/**
 * A caller-owned value enriched with crawler provenance.
 *
 * The type parameter is preserved from [Extractor] through transforms,
 * validators, and [Output]. Core owns only [context]; it never serializes,
 * stores, logs, or converts [value] to an envelope. Outputs must acknowledge
 * or report failures from their suspending call before the owning pipeline
 * scope is allowed to finish.
 */
public data class Produced<T : Any>(
    public val value: T,
    public val context: ProductionContext,
)

/**
 * Immutable provenance metadata attached to a produced value.
 *
 * The fields identify crawler activity rather than application records. A
 * response reference is optional because response storage is optional. The
 * context is safe to pass across suspension points; the engine owns its
 * lifetime and extensions must not retain it beyond their lifecycle.
 */
public data class ProductionContext(
    public val pipelineId: String,
    public val engineRunId: String,
    public val pipelineRunId: String,
    public val requestId: String?,
    public val responseReference: ResponseReference?,
    public val originalTarget: String?,
    public val finalTarget: String?,
    public val producedAt: Instant,
    public val attributes: ContextAttributes = ContextAttributes.empty(),
) {
    init {
        require(pipelineId.isNotBlank()) { "Pipeline ID must not be blank" }
        require(engineRunId.isNotBlank()) { "Engine run ID must not be blank" }
        require(pipelineRunId.isNotBlank()) { "Pipeline run ID must not be blank" }
        require(requestId == null || requestId.isNotBlank()) { "Request ID must not be blank when present" }
        require(originalTarget == null || originalTarget.isNotBlank()) {
            "Original target must not be blank when present"
        }
        require(finalTarget == null || finalTarget.isNotBlank()) {
            "Final target must not be blank when present"
        }
    }
}

/**
 * Extracts one or more caller-owned values from a transport response.
 *
 * Extraction belongs to the pipeline/host, not to core. The suspending call
 * runs in an engine-owned structured scope; cancellation propagates to it and
 * an implementation must close any parser or response resources it owns. A
 * thrown exception is an infrastructure/programmer failure; expected
 * extraction outcomes should be returned as [ExtractionResult].
 */
public interface Extractor<T : Any> {
    /** Stable identifier used for diagnostics and deterministic validation. */
    public val id: String

    /** Extracts typed values without changing the response store or crawl state. */
    public suspend fun extract(
        response: CrawlResponse,
        context: ExtractionContext,
    ): ExtractionResult<T>
}

/** Expected outcomes of typed extraction. */
public sealed interface ExtractionResult<out T : Any> {
    /** Values were extracted; the extractor must keep values and follow-ups caller-bounded. */
    public data class Success<T : Any>(
        public val values: List<T>,
        public val followUps: List<CrawlRequest> = emptyList(),
    ) : ExtractionResult<T>

    /** The response was intentionally not useful to this extractor. */
    public data class Ignore(
        public val reason: String? = null,
    ) : ExtractionResult<Nothing>

    /** Extraction failed and preserved the original cause for policy handling. */
    public data class Failure(
        public val error: Throwable,
        public val retryHint: RetryHint? = null,
    ) : ExtractionResult<Nothing>
}

/**
 * Applies a same-type transformation to a caller-owned value.
 *
 * A transform cannot change `T`; use a future explicit mapper contract for a
 * type-changing boundary. The suspending call is cancellation-cooperative,
 * and expected drops are returned rather than thrown. The transform does not
 * own persistence or output acknowledgement.
 */
public interface Transform<T : Any> {
    /** Stable identifier used for ordering diagnostics and validation. */
    public val id: String

    /** Applies the transform and returns an explicit accept/drop/mutate decision. */
    public suspend fun apply(value: T, context: TransformContext): TransformDecision<T>
}

/** A same-type transform outcome. */
public sealed interface TransformDecision<out T : Any> {
    /** Keep the current value unchanged. */
    public data class Accept<T : Any>(
        public val value: T,
    ) : TransformDecision<T>

    /** Replace the current value with another value of the same type `T`. */
    public data class Mutate<T : Any>(
        public val value: T,
    ) : TransformDecision<T>

    /** Drop the current value as an expected pipeline decision. */
    public data class Drop(
        public val reason: String? = null,
    ) : TransformDecision<Nothing>
}

/** Compatibility name for callers that prefer a result-oriented term. */
public typealias TransformResult<T> = TransformDecision<T>

/**
 * Validates a transformed caller-owned value without changing its type.
 *
 * Validation belongs to the host/pipeline. The suspending call is owned by
 * the engine scope, cancellation propagates, and implementations must return
 * expected rejection as [ValidationResult.Rejected] rather than hiding it in
 * a thrown exception. A thrown exception remains an unexpected failure.
 */
public interface Validator<T : Any> {
    /** Stable identifier used for ordered validation diagnostics. */
    public val id: String

    /** Validates one value and preserves the original value type. */
    public suspend fun validate(value: T, context: ValidationContext): ValidationResult
}

/** Expected validation outcomes. */
public sealed interface ValidationResult {
    /** The value passed this validator. */
    public data object Accepted : ValidationResult

    /** The value was intentionally rejected by this validator. */
    public data class Rejected(
        public val reason: String,
    ) : ValidationResult {
        init {
            require(reason.isNotBlank()) { "Validation rejection reason must not be blank" }
        }
    }

    /** Validation could not complete and preserved its original cause. */
    public data class Failed(
        public val error: Throwable,
    ) : ValidationResult
}

/** Whether an output failure is allowed to leave the pipeline successful. */
public enum class OutputRequirement {
    /** Failure must be surfaced to the pipeline run. */
    REQUIRED,

    /** Failure is observable but does not by itself fail the pipeline run. */
    BEST_EFFORT,
}

/**
 * Consumes a bounded batch of typed produced values.
 *
 * Output owns the destination, encoding, transaction, retention, and
 * idempotency behavior. Core never converts `T` to a serialized or database
 * representation. The caller/engine owns the batch bound and must not pass an
 * unbounded list. The suspending call must honor cancellation and close
 * adapter-owned resources; a non-transactional output may observe duplicates
 * after a crash, so it must document its guarantee instead of claiming
 * exactly-once delivery.
 */
public interface Output<T : Any> {
    /** Stable identifier used for output ordering and deterministic validation. */
    public val id: String

    /** Writes [items] and reports an explicit acknowledgement or failure. */
    public suspend fun write(items: List<Produced<T>>, context: OutputContext): OutputResult
}

/** Typed output plus its explicit delivery requirement. */
public data class OutputBinding<T : Any>(
    public val output: Output<T>,
    public val requirement: OutputRequirement = OutputRequirement.REQUIRED,
)

/** Expected outcomes of an output acknowledgement. */
public sealed interface OutputResult {
    /** The output acknowledged the batch. */
    public data class Accepted(
        public val itemCount: Int,
    ) : OutputResult {
        init {
            require(itemCount >= 0) { "Accepted output item count must not be negative" }
        }
    }

    /** The output intentionally skipped the batch. */
    public data class Skipped(
        public val reason: String? = null,
    ) : OutputResult

    /** The output failed and preserved the original cause for policy handling. */
    public data class Failed(
        public val error: Throwable,
        public val retryHint: RetryHint? = null,
    ) : OutputResult
}

/**
 * Human-readable immutable identity for a pipeline definition.
 *
 * The identifier is configuration metadata, not a user-data key. It is
 * validated before an engine can be constructed.
 */
public data class PipelineDefinition(
    public val id: String,
    public val displayName: String? = null,
) {
    init {
        require(id.isNotBlank()) { "Pipeline ID must not be blank" }
        require(displayName == null || displayName.isNotBlank()) {
            "Pipeline display name must not be blank when present"
        }
    }
}
