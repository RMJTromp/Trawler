package com.rmjtromp.trawler.web.policy

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.web.transport.DestinationDecision
import com.rmjtromp.trawler.web.transport.RedirectHistory
import com.rmjtromp.trawler.web.transport.NetworkAddressResolver
import com.rmjtromp.trawler.web.transport.WebDestinationPolicy
import java.net.InetAddress
import java.net.URI
import java.util.Locale

/** Configuration for the opt-in public-web destination safety preset. */
public data class PublicWebSafetyConfig(
    public val allowedSchemes: Set<String> = setOf("http", "https"),
    public val blockPrivateAddresses: Boolean = true,
    public val blockLoopbackAddresses: Boolean = true,
    public val blockLinkLocalAddresses: Boolean = true,
    public val blockMulticastAddresses: Boolean = true,
    public val blockMetadataAddresses: Boolean = true,
    public val blockCarrierGradeNatAddresses: Boolean = true,
    public val blockBenchmarkAddresses: Boolean = true,
    public val maxTargetLength: Int = 16_384,
) {
    init {
        require(allowedSchemes.isNotEmpty()) { "At least one web scheme must be allowed" }
        require(allowedSchemes.all { it.isNotBlank() }) { "Allowed schemes must not be blank" }
        require(maxTargetLength > 0) { "Web target length bound must be positive" }
    }

    internal val normalizedSchemes: Set<String> =
        allowedSchemes.map { it.lowercase(Locale.ROOT) }.toSet()
}

/**
 * Fail-closed HTTP/HTTPS policy for public-network crawling.
 *
 * The policy validates every target and resolves its host before allowing a
 * request. The transport calls it again for every redirect hop. Resolution is
 * revalidated but is not socket-address pinning; callers requiring a stronger
 * DNS-to-connection guarantee must provide a transport/policy pair that owns
 * that capability.
 */
public class PublicWebSafetyPolicy(
    private val resolver: NetworkAddressResolver,
    private val configuration: PublicWebSafetyConfig = PublicWebSafetyConfig(),
) : WebDestinationPolicy {
    override val id: String = "public-web-safety"

    override suspend fun evaluate(request: CrawlRequest, history: RedirectHistory): DestinationDecision {
        if (request.target.length > configuration.maxTargetLength) {
            return DestinationDecision.Rejected("target-length-bound")
        }
        if (request.target.any { it.isISOControl() || it.isWhitespace() }) {
            return DestinationDecision.Rejected("target-contains-control-or-whitespace")
        }
        val uri = try {
            URI(request.target)
        } catch (_: IllegalArgumentException) {
            return DestinationDecision.Rejected("target-is-not-a-uri")
        }
        val scheme = uri.scheme?.lowercase(Locale.ROOT)
            ?: return DestinationDecision.Rejected("scheme-required")
        if (scheme !in configuration.normalizedSchemes) {
            return DestinationDecision.Rejected("scheme-not-allowed")
        }
        val host = uri.host?.lowercase(Locale.ROOT)
            ?: return DestinationDecision.Rejected("host-required")
        if (uri.userInfo != null) {
            return DestinationDecision.Rejected("userinfo-is-not-allowed")
        }
        if (uri.port == 0 || uri.port < -1 || uri.port > 65_535) {
            return DestinationDecision.Rejected("invalid-port")
        }
        val addresses = try {
            resolver.resolve(host)
        } catch (error: Exception) {
            if (error is kotlinx.coroutines.CancellationException) throw error
            throw WebAddressResolutionException(host, error)
        }
        if (addresses.isEmpty()) {
            return DestinationDecision.Rejected("host-resolved-to-no-address")
        }
        addresses.firstOrNull(::isBlocked)?.let { address ->
            return DestinationDecision.Rejected("destination-address-blocked:${address.hostAddress}")
        }
        return DestinationDecision.Allowed
    }

    private fun isBlocked(address: InetAddress): Boolean {
        if (configuration.blockLoopbackAddresses && address.isLoopbackAddress) return true
        if (configuration.blockLinkLocalAddresses && address.isLinkLocalAddress) return true
        if (configuration.blockMulticastAddresses && address.isMulticastAddress) return true
        if (configuration.blockPrivateAddresses && address.isSiteLocalAddress) return true
        if (configuration.blockPrivateAddresses && address.isAnyLocalAddress) return true

        val bytes = address.address
        val ipv4 = when {
            bytes.size == 4 -> bytes
            bytes.size == 16 && bytes.take(10).all { it == 0.toByte() } &&
                bytes[10] == 0xff.toByte() && bytes[11] == 0xff.toByte() -> bytes.copyOfRange(12, 16)
            else -> null
        }
        if (ipv4 != null) {
            val first = ipv4[0].toInt() and 0xff
            val second = ipv4[1].toInt() and 0xff
            val third = ipv4[2].toInt() and 0xff
            val fourth = ipv4[3].toInt() and 0xff
            if (configuration.blockPrivateAddresses &&
                ((first == 10) ||
                    (first == 172 && second in 16..31) ||
                    (first == 192 && second == 168) ||
                    (first == 0) ||
                    (first == 127))
            ) return true
            if (configuration.blockCarrierGradeNatAddresses && first == 100 && second in 64..127) return true
            if (configuration.blockBenchmarkAddresses && first == 198 && second in 18..19) return true
            if (configuration.blockMetadataAddresses &&
                ((first == 169 && second == 254 && third == 169 && fourth == 254) ||
                    (first == 100 && second == 100 && third == 100 && fourth == 200))
            ) return true
            if (configuration.blockMulticastAddresses && first in 224..239) return true
            if (configuration.blockPrivateAddresses && first == 255 && second == 255 && third == 255 && fourth == 255) {
                return true
            }
        } else if (bytes.size == 16) {
            val first = bytes[0].toInt() and 0xff
            val second = bytes[1].toInt() and 0xff
            if (configuration.blockPrivateAddresses && first in 0xfc..0xfd) return true
            if (configuration.blockLinkLocalAddresses && first == 0xfe && second in 0x80..0xbf) return true
            if (configuration.blockMulticastAddresses && first == 0xff) return true
            if (configuration.blockMetadataAddresses) {
                val text = address.hostAddress.substringBefore('%').lowercase(Locale.ROOT)
                if (text == "fd00:ec2::254" || text == "fe80::a9fe:a9fe") return true
            }
        }
        return false
    }
}

/** Explicit permissive policy for trusted/internal networks or custom safety. */
public object AllowAllWebDestinationPolicy : WebDestinationPolicy {
    override val id: String = "allow-all-web-destinations"

    override suspend fun evaluate(request: CrawlRequest, history: RedirectHistory): DestinationDecision =
        DestinationDecision.Allowed
}

/** Preserves the original DNS failure while failing closed at the transport boundary. */
public class WebAddressResolutionException(
    public val host: String,
    cause: Throwable,
) : IllegalStateException("Unable to resolve web destination host '$host'", cause)
