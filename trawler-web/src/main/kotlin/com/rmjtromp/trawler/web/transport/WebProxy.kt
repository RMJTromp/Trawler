package com.rmjtromp.trawler.web.transport

import java.net.InetSocketAddress
import java.net.URI
import java.net.URISyntaxException

/**
 * Immutable address of the forward proxy every transport request leaves through.
 *
 * [url] must be exactly `http://host:port`: scheme `http`, a host, an explicit
 * port from 1 to 65535, and no user-info, path, query, or fragment. Validation
 * runs at construction, so an invalid value can never reach a client, and its
 * messages never repeat [url] because a rejected value may contain credentials.
 * The proxy is operator configuration: the destination policy does not check it.
 * Proxy authentication is not supported, so a value cannot carry credentials.
 */
public data class WebProxy(public val url: String) {
    /** Bare hostname or IPv6 literal without brackets. */
    internal val host: String

    internal val port: Int

    init {
        val parsed = try {
            URI(url)
        } catch (_: URISyntaxException) {
            throw IllegalArgumentException("Proxy URL must be a valid http://host:port URL")
        }
        require(parsed.rawUserInfo == null && parsed.rawAuthority?.contains('@') != true) {
            "Proxy URL must not carry credentials"
        }
        require(parsed.scheme == "http") { "Proxy URL scheme must be http" }
        val parsedHost = parsed.host
        require(!parsedHost.isNullOrEmpty()) { "Proxy URL requires a host valid in a URL, which excludes underscores" }
        require(parsed.port in 1..MAX_PORT) { "Proxy URL requires an explicit port from 1 to $MAX_PORT" }
        require(parsed.rawPath.isNullOrEmpty()) { "Proxy URL must not have a path" }
        require(parsed.rawQuery == null) { "Proxy URL must not have a query" }
        require(parsed.rawFragment == null) { "Proxy URL must not have a fragment" }
        host = parsedHost.removePrefix("[").removeSuffix("]")
        port = parsed.port
    }

    /**
     * The address is left unresolved so that the client performs no DNS or
     * reverse lookup when it is built; each connection resolves [host] itself.
     */
    internal fun socketAddress(): InetSocketAddress = InetSocketAddress.createUnresolved(host, port)

    private companion object {
        const val MAX_PORT = 65_535
    }
}
