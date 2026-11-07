package com.rmjtromp.trawler.flaresolverr

/**
 * Base type for failures raised by the FlareSolverr adapter.
 *
 * Messages never contain proxy credentials, session IDs, cookie values, or
 * request bodies. Original causes are preserved for diagnosis; callers that
 * persist or emit causes remain responsible for their own redaction.
 * [FlareSolverrFailureClassifier] maps every subtype to an extensible core
 * failure category.
 */
public sealed class FlareSolverrException(
    message: String,
    cause: Throwable? = null,
) : IllegalStateException(message, cause)

/** The FlareSolverr service could not be reached or did not answer in time. */
public class FlareSolverrUnavailableException(
    cause: Throwable,
) : FlareSolverrException("FlareSolverr service is unavailable or timed out", cause)

/**
 * FlareSolverr answered with `status: error`, for example when its browser
 * could not solve a challenge within `maxTimeout`.
 *
 * [serviceMessage] is the bounded, control-character-free FlareSolverr
 * message with configured proxy credentials removed. It may still mention the
 * target URL that FlareSolverr reported.
 */
public class FlareSolverrErrorException(
    public val serviceMessage: String,
) : FlareSolverrException("FlareSolverr reported an error: $serviceMessage")

/** The service response did not match the FlareSolverr v3 `/v1` shape. */
public class FlareSolverrProtocolException(
    public val reason: String,
    cause: Throwable? = null,
) : FlareSolverrException("Unexpected FlareSolverr response: $reason", cause)

/** A FlareSolverr response exceeded one of the configured adapter bounds. */
public class FlareSolverrResponseLimitException(
    public val limitKind: String,
    public val limit: Long,
) : FlareSolverrException("FlareSolverr response exceeds the $limitKind limit of $limit")

/**
 * A request cannot be expressed through FlareSolverr's command set.
 *
 * FlareSolverr v3 supports only `GET` and form-urlencoded `POST`; other
 * methods, bodies, and malformed request cookies are rejected before any
 * service call.
 */
public class UnsupportedFlareSolverrRequestException(
    public val reason: String,
) : FlareSolverrException("Request is not supported by FlareSolverr: $reason")
