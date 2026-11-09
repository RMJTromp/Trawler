package com.rmjtromp.trawler.web.transport

import com.rmjtromp.trawler.web.policy.WebProxyFailureException
import io.ktor.client.network.sockets.ConnectTimeoutException
import io.ktor.client.plugins.HttpRequestTimeoutException
import java.net.ProtocolException
import java.net.SocketException
import java.net.SocketTimeoutException
import java.net.URI
import java.net.UnknownHostException
import java.nio.channels.UnresolvedAddressException
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/**
 * Tracks how far one hop has progressed through a configured proxy.
 *
 * The JDK client does not report when a tunnel is ready, yet a stalled proxy and
 * a stalled site fail with the same timeout exceptions. It does read the request
 * body's length only once the connection, including its TLS handshake, is
 * usable, so that read separates connection setup from waiting on the site.
 */
internal class ProxyHop {
    @Volatile
    private var phase = Phase.QUEUED

    val isConnecting: Boolean get() = phase == Phase.CONNECTING

    /** Called once the hop owns its connection permits and starts connecting. */
    fun connecting() {
        phase = Phase.CONNECTING
    }

    /** Called when the client starts writing the request to a usable connection. */
    fun dispatched() {
        phase = Phase.DISPATCHED
    }

    private enum class Phase { QUEUED, CONNECTING, DISPATCHED }
}

/**
 * Remembers the active hop of one redirect chain, so that a chain timeout, which
 * reaches the hop only as cancellation, can still be attributed to the proxy.
 */
internal class ProxyChain : AbstractCoroutineContextElement(ProxyChain) {
    companion object Key : CoroutineContext.Key<ProxyChain>

    @Volatile
    var currentHop: ProxyHop? = null
}

/**
 * Absolute-form requests would carry a fragment to the proxy, the JDK client
 * would write each non-ASCII character of an absolute-form target as `?`, and it
 * would send an empty path as nothing instead of `/`.
 */
internal fun KtorTransportConfig.wireTarget(target: String): String {
    if (proxy == null) return target
    val uri = URI(URI(target.substringBefore('#')).toASCIIString())
    if (!uri.rawPath.isNullOrEmpty()) return uri.toString()
    return "${uri.scheme}://${uri.rawAuthority}/" + (uri.rawQuery?.let { "?$it" } ?: "")
}

/**
 * A 407 on plain HTTP can only be the proxy's own answer. Behind a tunnel it is
 * the site's response, so it stays an ordinary response there.
 */
internal fun KtorTransportConfig.proxyRejection(target: String, status: Int): WebProxyFailureException? =
    if (proxy != null && status == PROXY_AUTHENTICATION_REQUIRED && WebOrigin.fromTarget(target).scheme == "http") {
        WebProxyFailureException(status, null)
    } else {
        null
    }

/**
 * Returns the proxy failure [error] represents, or `null` when it belongs to the
 * site or transport. Only the connection phase can be the proxy's fault: the
 * client opens sockets solely to the proxy, so a connect failure there is the
 * proxy's, as is everything the client reports about the CONNECT exchange. A
 * reset or stall during the TLS handshake cannot be told apart from one during
 * CONNECT and counts as the proxy's too. The exception types and messages below
 * are the ones the JDK client was observed to throw; `KtorTransportProxyIT` pins
 * them against each supported version.
 */
internal fun proxyFailureOf(error: Throwable, hop: ProxyHop): WebProxyFailureException? {
    if (!hop.isConnecting) return null
    val chain = generateSequence(error) { it.cause }.take(MAX_CAUSE_DEPTH).toList()
    val status = chain.firstNotNullOfOrNull { failure ->
        failure.message?.let { JDK_TUNNEL_REJECTED.matchEntire(it) }?.groupValues?.get(1)?.toInt()
    }
    return when {
        status != null -> WebProxyFailureException(status, error)
        chain.any(::isProxyConnectionFailure) -> WebProxyFailureException(null, error)
        else -> null
    }
}

private fun isProxyConnectionFailure(failure: Throwable): Boolean =
    failure is SocketException ||
        failure is ProtocolException ||
        failure is NumberFormatException ||
        failure is ConnectTimeoutException ||
        failure is UnknownHostException ||
        failure is UnresolvedAddressException ||
        failure is SocketTimeoutException ||
        failure is HttpRequestTimeoutException ||
        failure.message?.let { message -> TUNNEL_FAILURE_MESSAGES.any { message.startsWith(it) } } == true

internal const val PROXY_AUTHENTICATION_REQUIRED = 407
private const val MAX_CAUSE_DEPTH = 8
private val JDK_TUNNEL_REJECTED = Regex("Tunnel failed, got: (\\d{3})")
private val TUNNEL_FAILURE_MESSAGES = listOf(
    "HTTP/1.1 header parser received no bytes",
    "parsing HTTP/1.1 ",
)
