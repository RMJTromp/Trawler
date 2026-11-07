package com.rmjtromp.trawler.web.policy

import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.policy.FailureContext
import com.rmjtromp.trawler.web.transport.DestinationDecision
import com.rmjtromp.trawler.web.transport.NetworkAddressLimitExceededException
import com.rmjtromp.trawler.web.transport.NetworkAddressResolver
import com.rmjtromp.trawler.web.transport.RedirectHistory
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress
import java.net.UnknownHostException
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

/**
 * Exhaustive address-class coverage for the public-web destination preset.
 *
 * Every address is supplied through a handwritten [NetworkAddressResolver];
 * no test performs DNS. IPv6 values are built from raw bytes with
 * [Inet6Address.getByAddress] so IPv4-mapped forms stay `Inet6Address`
 * instances, exactly as a custom resolver may return them.
 */
class PublicWebSafetyPolicyTest {
    private enum class Flag { PRIVATE, LOOPBACK, LINK_LOCAL, MULTICAST, METADATA, CGNAT, BENCHMARK }

    private data class AddressCase(val label: String, val address: InetAddress, val blockedBy: Set<Flag>)

    private val cases: List<AddressCase> = listOf(
        // IPv4 private (RFC 1918), with both edges of each range and the first address outside it.
        AddressCase("10/8 low", v4(10, 0, 0, 0), setOf(Flag.PRIVATE)),
        AddressCase("10/8 high", v4(10, 255, 255, 255), setOf(Flag.PRIVATE)),
        AddressCase("below 10/8", v4(9, 255, 255, 255), emptySet()),
        AddressCase("above 10/8", v4(11, 0, 0, 0), emptySet()),
        AddressCase("172.16/12 low", v4(172, 16, 0, 0), setOf(Flag.PRIVATE)),
        AddressCase("172.16/12 high", v4(172, 31, 255, 255), setOf(Flag.PRIVATE)),
        AddressCase("below 172.16/12", v4(172, 15, 255, 255), emptySet()),
        AddressCase("above 172.16/12", v4(172, 32, 0, 0), emptySet()),
        AddressCase("192.168/16", v4(192, 168, 0, 1), setOf(Flag.PRIVATE)),
        AddressCase("below 192.168/16", v4(192, 167, 255, 255), emptySet()),
        AddressCase("above 192.168/16", v4(192, 169, 0, 0), emptySet()),
        // IPv4 "this network", unspecified, broadcast, and loopback.
        AddressCase("IPv4 unspecified", v4(0, 0, 0, 0), setOf(Flag.PRIVATE)),
        AddressCase("0/8 this-network", v4(0, 1, 2, 3), setOf(Flag.PRIVATE)),
        AddressCase("IPv4 limited broadcast", v4(255, 255, 255, 255), setOf(Flag.PRIVATE)),
        AddressCase("127/8 low", v4(127, 0, 0, 1), setOf(Flag.LOOPBACK, Flag.PRIVATE)),
        AddressCase("127/8 high", v4(127, 255, 255, 254), setOf(Flag.LOOPBACK, Flag.PRIVATE)),
        AddressCase("below 127/8", v4(126, 255, 255, 255), emptySet()),
        AddressCase("above 127/8", v4(128, 0, 0, 0), emptySet()),
        // IPv4 link-local and cloud metadata endpoints.
        AddressCase("169.254/16", v4(169, 254, 1, 1), setOf(Flag.LINK_LOCAL)),
        AddressCase("below 169.254/16", v4(169, 253, 255, 255), emptySet()),
        AddressCase("above 169.254/16", v4(169, 255, 0, 0), emptySet()),
        AddressCase("metadata 169.254.169.254", v4(169, 254, 169, 254), setOf(Flag.LINK_LOCAL, Flag.METADATA)),
        AddressCase("metadata neighbour", v4(169, 254, 169, 253), setOf(Flag.LINK_LOCAL)),
        AddressCase("metadata 100.100.100.200", v4(100, 100, 100, 200), setOf(Flag.CGNAT, Flag.METADATA)),
        AddressCase("alternate metadata neighbour", v4(100, 100, 100, 201), setOf(Flag.CGNAT)),
        AddressCase("metadata third-octet near miss", v4(169, 254, 168, 254), setOf(Flag.LINK_LOCAL)),
        AddressCase("metadata fourth-octet near miss", v4(169, 254, 169, 255), setOf(Flag.LINK_LOCAL)),
        AddressCase("alternate metadata third-octet near miss", v4(100, 100, 99, 200), setOf(Flag.CGNAT)),
        AddressCase("alternate metadata second-octet near miss", v4(100, 99, 100, 200), setOf(Flag.CGNAT)),
        // Carrier-grade NAT 100.64/10 and benchmark 198.18/15.
        AddressCase("CGNAT low", v4(100, 64, 0, 0), setOf(Flag.CGNAT)),
        AddressCase("CGNAT high", v4(100, 127, 255, 255), setOf(Flag.CGNAT)),
        AddressCase("below CGNAT", v4(100, 63, 255, 255), emptySet()),
        AddressCase("above CGNAT", v4(100, 128, 0, 0), emptySet()),
        AddressCase("benchmark low", v4(198, 18, 0, 0), setOf(Flag.BENCHMARK)),
        AddressCase("benchmark high", v4(198, 19, 255, 255), setOf(Flag.BENCHMARK)),
        AddressCase("below benchmark", v4(198, 17, 255, 255), emptySet()),
        AddressCase("above benchmark", v4(198, 20, 0, 0), emptySet()),
        // IPv4 multicast 224/4.
        AddressCase("multicast low", v4(224, 0, 0, 1), setOf(Flag.MULTICAST)),
        AddressCase("multicast high", v4(239, 255, 255, 255), setOf(Flag.MULTICAST)),
        AddressCase("below multicast", v4(223, 255, 255, 255), emptySet()),
        // Public IPv4.
        AddressCase("public 8.8.8.8", v4(8, 8, 8, 8), emptySet()),
        AddressCase("public 1.1.1.1", v4(1, 1, 1, 1), emptySet()),
        // IPv6 loopback, unspecified, unique-local, link-local, site-local, multicast.
        AddressCase("IPv6 loopback", v6(0, 0, 0, 0, 0, 0, 0, 1), setOf(Flag.LOOPBACK)),
        AddressCase("IPv6 unspecified", v6(0, 0, 0, 0, 0, 0, 0, 0), setOf(Flag.PRIVATE)),
        AddressCase("unique-local fc00::/7 low", v6(0xfc00, 0, 0, 0, 0, 0, 0, 1), setOf(Flag.PRIVATE)),
        AddressCase(
            "unique-local fc00::/7 high",
            v6(0xfdff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff),
            setOf(Flag.PRIVATE),
        ),
        AddressCase("below unique-local", v6(0xfbff, 0xffff, 0, 0, 0, 0, 0, 1), emptySet()),
        AddressCase("between unique-local and link-local", v6(0xfe00, 0, 0, 0, 0, 0, 0, 1), emptySet()),
        AddressCase("link-local fe80::/10 low", v6(0xfe80, 0, 0, 0, 0, 0, 0, 1), setOf(Flag.LINK_LOCAL)),
        AddressCase("link-local fe80::/10 high", v6(0xfebf, 0xffff, 0, 0, 0, 0, 0, 1), setOf(Flag.LINK_LOCAL)),
        AddressCase("deprecated site-local fec0::/10", v6(0xfec0, 0, 0, 0, 0, 0, 0, 1), setOf(Flag.PRIVATE)),
        AddressCase("multicast ff02::1", v6(0xff02, 0, 0, 0, 0, 0, 0, 1), setOf(Flag.MULTICAST)),
        AddressCase("multicast ff0e::1", v6(0xff0e, 0, 0, 0, 0, 0, 0, 1), setOf(Flag.MULTICAST)),
        AddressCase("AWS IPv6 metadata fd00:ec2::254", v6(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x0254), setOf(Flag.PRIVATE, Flag.METADATA)),
        AddressCase("link-local metadata fe80::a9fe:a9fe", v6(0xfe80, 0, 0, 0, 0, 0, 0xa9fe, 0xa9fe), setOf(Flag.LINK_LOCAL, Flag.METADATA)),
        AddressCase("public IPv6 2001:4860:4860::8888", v6(0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888), emptySet()),
        AddressCase("public IPv6 2606:4700:4700::1111", v6(0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111), emptySet()),
        // IPv4-mapped IPv6 (::ffff:a.b.c.d) kept as Inet6Address: every IPv4 rule must apply.
        AddressCase("mapped private", mapped(10, 0, 0, 1), setOf(Flag.PRIVATE)),
        AddressCase("mapped loopback", mapped(127, 0, 0, 1), setOf(Flag.LOOPBACK, Flag.PRIVATE)),
        AddressCase("mapped link-local", mapped(169, 254, 1, 1), setOf(Flag.LINK_LOCAL)),
        AddressCase("mapped metadata", mapped(169, 254, 169, 254), setOf(Flag.LINK_LOCAL, Flag.METADATA)),
        AddressCase("mapped CGNAT", mapped(100, 64, 0, 1), setOf(Flag.CGNAT)),
        AddressCase("mapped benchmark", mapped(198, 18, 0, 1), setOf(Flag.BENCHMARK)),
        AddressCase("mapped multicast", mapped(224, 0, 0, 1), setOf(Flag.MULTICAST)),
        AddressCase("mapped unspecified", mapped(0, 0, 0, 0), setOf(Flag.PRIVATE)),
        AddressCase("mapped public", mapped(8, 8, 8, 8), emptySet()),
        // Deprecated IPv4-compatible IPv6 (::a.b.c.d).
        AddressCase("compatible loopback", compatible(127, 0, 0, 1), setOf(Flag.LOOPBACK, Flag.PRIVATE)),
        AddressCase("compatible private", compatible(10, 0, 0, 1), setOf(Flag.PRIVATE)),
        AddressCase("compatible metadata", compatible(169, 254, 169, 254), setOf(Flag.LINK_LOCAL, Flag.METADATA)),
        AddressCase("compatible public", compatible(8, 8, 8, 8), emptySet()),
        // NAT64 well-known prefix 64:ff9b::/96 (RFC 6052) translates to the embedded IPv4 destination.
        AddressCase("NAT64 loopback", nat64(127, 0, 0, 1), setOf(Flag.LOOPBACK, Flag.PRIVATE)),
        AddressCase("NAT64 private", nat64(10, 0, 0, 1), setOf(Flag.PRIVATE)),
        AddressCase("NAT64 metadata", nat64(169, 254, 169, 254), setOf(Flag.LINK_LOCAL, Flag.METADATA)),
        AddressCase("NAT64 CGNAT", nat64(100, 64, 0, 1), setOf(Flag.CGNAT)),
        AddressCase("NAT64 public", nat64(8, 8, 8, 8), emptySet()),
    )

    @Test
    fun `test fixtures keep IPv4-in-IPv6 answers as Inet6Address instances`() {
        cases.filter { it.label.startsWith("mapped") || it.label.startsWith("compatible") || it.label.startsWith("NAT64") }
            .forEach { assertIs<Inet6Address>(it.address, it.label) }
        cases.filter { it.label.startsWith("IPv4") || it.label.contains("/8") }
            .forEach { assertIs<Inet4Address>(it.address, it.label) }
    }

    @Test
    fun `default configuration blocks every non-public address class and allows public neighbours`() = runTest {
        cases.forEach { case ->
            val decision = evaluateWith(PublicWebSafetyConfig(), case.address)
            if (case.blockedBy.isEmpty()) {
                assertEquals(DestinationDecision.Allowed, decision, case.label)
            } else {
                assertEquals(
                    DestinationDecision.Rejected("destination-address-blocked:${case.address.hostAddress}"),
                    decision,
                    case.label,
                )
            }
        }
    }

    @Test
    fun `each address class is blocked exactly by its own configuration flag`() = runTest {
        Flag.entries.forEach { enabled ->
            val configuration = configWith(setOf(enabled))
            cases.forEach { case ->
                val decision = evaluateWith(configuration, case.address)
                if (enabled in case.blockedBy) {
                    assertIs<DestinationDecision.Rejected>(decision, "${case.label} with only $enabled")
                } else {
                    assertEquals(DestinationDecision.Allowed, decision, "${case.label} with only $enabled")
                }
            }
        }
    }

    @Test
    fun `disabling one flag leaves every other overlapping class blocked`() = runTest {
        Flag.entries.forEach { disabled ->
            val configuration = configWith(Flag.entries.toSet() - disabled)
            cases.forEach { case ->
                val decision = evaluateWith(configuration, case.address)
                if ((case.blockedBy - disabled).isNotEmpty()) {
                    assertIs<DestinationDecision.Rejected>(decision, "${case.label} without $disabled")
                } else {
                    assertEquals(DestinationDecision.Allowed, decision, "${case.label} without $disabled")
                }
            }
        }
    }

    @Test
    fun `fully permissive address configuration still validates the target`() = runTest {
        val permissive = configWith(emptySet())
        cases.forEach { case ->
            assertEquals(DestinationDecision.Allowed, evaluateWith(permissive, case.address), case.label)
        }
        val resolver = RecordingResolver { listOf(v4(127, 0, 0, 1)) }
        assertEquals(
            DestinationDecision.Rejected("userinfo-is-not-allowed"),
            PublicWebSafetyPolicy(resolver, permissive).evaluate(
                CrawlRequest("https://user@example.test/"),
                RedirectHistory.empty(0),
            ),
        )
        assertTrue(resolver.hosts.isEmpty())
    }

    @Test
    fun `one blocked answer in a mixed DNS response rejects the whole destination`() = runTest {
        val answers = listOf(
            v4(8, 8, 8, 8),
            v6(0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888),
            mapped(192, 168, 0, 10),
            v4(10, 0, 0, 1),
        )
        val decision = PublicWebSafetyPolicy(RecordingResolver { answers })
            .evaluate(CrawlRequest("https://rebinding.test/"), RedirectHistory.empty(1))
        assertEquals(
            DestinationDecision.Rejected("destination-address-blocked:${answers[2].hostAddress}"),
            decision,
        )
    }

    @Test
    fun `target syntax is rejected before DNS with stable reasons`() = runTest {
        val resolver = RecordingResolver { listOf(v4(8, 8, 8, 8)) }
        val policy = PublicWebSafetyPolicy(resolver)
        listOf(
            "//example.test/scheme-relative" to "scheme-required",
            "/relative/path" to "scheme-required",
            "mailto:someone@example.test" to "scheme-not-allowed",
            "javascript:alert(1)" to "scheme-not-allowed",
            "file:///etc/passwd" to "scheme-not-allowed",
            "gopher://example.test:70/" to "scheme-not-allowed",
            "https://example.test:99999/" to "invalid-port",
            "https://example.test:0/" to "invalid-port",
            "https://exa_mple.test/" to "host-required",
            "https://user@example.test/" to "userinfo-is-not-allowed",
            "https://:@example.test/" to "userinfo-is-not-allowed",
            "https://example.test/\u0000" to "target-contains-control-or-whitespace",
            "https://example.test/\t" to "target-contains-control-or-whitespace",
            "https://example.test/\n" to "target-contains-control-or-whitespace",
            "https://example.test/ " to "target-contains-control-or-whitespace",
            "https://example.test/\u007f" to "target-contains-control-or-whitespace",
            "https://example.test/%zz" to "target-is-not-a-uri",
        ).forEach { (target, reason) ->
            assertEquals(
                DestinationDecision.Rejected(reason),
                policy.evaluate(CrawlRequest(target), RedirectHistory.empty(0)),
                target,
            )
        }
        assertTrue(resolver.hosts.isEmpty(), "no syntactic rejection may reach DNS")
    }

    @Test
    fun `target length bound is inclusive and scheme matching is case-insensitive in both directions`() = runTest {
        val target = "https://example.test/abc"
        val resolver = RecordingResolver { listOf(v4(8, 8, 8, 8)) }
        val exact = PublicWebSafetyPolicy(resolver, PublicWebSafetyConfig(maxTargetLength = target.length))
        assertEquals(DestinationDecision.Allowed, exact.evaluate(CrawlRequest(target), RedirectHistory.empty(0)))
        assertEquals(
            DestinationDecision.Rejected("target-length-bound"),
            exact.evaluate(CrawlRequest("${target}d"), RedirectHistory.empty(0)),
        )

        val upperConfigured = PublicWebSafetyPolicy(resolver, PublicWebSafetyConfig(allowedSchemes = setOf("HTTPS")))
        assertEquals("public-web-safety", upperConfigured.id)
        assertEquals(DestinationDecision.Allowed, upperConfigured.evaluate(CrawlRequest("hTtPs://Example.TEST/"), RedirectHistory.empty(0)))
        assertEquals(
            DestinationDecision.Rejected("scheme-not-allowed"),
            upperConfigured.evaluate(CrawlRequest("http://example.test/"), RedirectHistory.empty(0)),
        )
        assertEquals(listOf("example.test", "example.test"), resolver.hosts)
    }

    @Test
    fun `explicit ports and bracketed IPv6 literals reach the resolver normalized`() = runTest {
        val resolver = RecordingResolver { listOf(v4(8, 8, 8, 8)) }
        val policy = PublicWebSafetyPolicy(resolver)
        listOf(
            "https://example.test:1/",
            "https://example.test:65535/",
            "http://[2001:DB8::1]:8080/",
        ).forEach { target ->
            assertEquals(DestinationDecision.Allowed, policy.evaluate(CrawlRequest(target), RedirectHistory.empty(0)), target)
        }
        assertEquals(listOf("example.test", "example.test", "2001:db8::1"), resolver.hosts)
    }

    @Test
    fun `DNS failures fail closed preserve causes and keep cancellation`() = runTest {
        val unknown = UnknownHostException("no such host")
        val failure = assertFailsWith<WebAddressResolutionException> {
            PublicWebSafetyPolicy(RecordingResolver { throw unknown })
                .evaluate(CrawlRequest("https://missing.test/"), RedirectHistory.empty(0))
        }
        assertEquals("missing.test", failure.host)
        assertSame(unknown, failure.cause)
        assertTrue(failure.message.orEmpty().contains("missing.test"))

        val cancellation = CancellationException("stop")
        val cancelled = assertFailsWith<CancellationException> {
            PublicWebSafetyPolicy(RecordingResolver { throw cancellation })
                .evaluate(CrawlRequest("https://slow.test/"), RedirectHistory.empty(0))
        }
        assertSame(cancellation, cancelled)

        assertEquals(
            DestinationDecision.Rejected("host-resolved-to-no-address"),
            PublicWebSafetyPolicy(RecordingResolver { emptyList() })
                .evaluate(CrawlRequest("https://empty.test/"), RedirectHistory.empty(0)),
        )
    }

    @Test
    fun `an oversized DNS answer fails closed and is classified as a non-retryable answer bound`() = runTest {
        val limit = NetworkAddressLimitExceededException("wide.test", 16)
        val failure = assertFailsWith<WebAddressResolutionException> {
            PublicWebSafetyPolicy(RecordingResolver { throw limit })
                .evaluate(CrawlRequest("https://wide.test/"), RedirectHistory.empty(0))
        }
        assertSame(limit, failure.cause)

        val classification = DefaultWebFailureClassifier.classify(
            failure,
            FailureContext(CrawlerContext("engine", "run", "pipeline"), CrawlRequest("https://wide.test/")),
        )
        assertEquals("web-dns-answer-limit", classification.category)
        assertFalse(classification.retryableByDefault)
    }

    @Test
    fun `allow-all policy is an explicit opt-out that admits internal targets`() = runTest {
        assertEquals("allow-all-web-destinations", AllowAllWebDestinationPolicy.id)
        assertEquals(
            DestinationDecision.Allowed,
            AllowAllWebDestinationPolicy.evaluate(CrawlRequest("http://127.0.0.1/"), RedirectHistory.empty(0)),
        )
    }

    private suspend fun evaluateWith(configuration: PublicWebSafetyConfig, address: InetAddress): DestinationDecision =
        PublicWebSafetyPolicy(RecordingResolver { listOf(address) }, configuration)
            .evaluate(CrawlRequest("https://answer.test/"), RedirectHistory.empty(0))

    private fun configWith(flags: Set<Flag>): PublicWebSafetyConfig = PublicWebSafetyConfig(
        blockPrivateAddresses = Flag.PRIVATE in flags,
        blockLoopbackAddresses = Flag.LOOPBACK in flags,
        blockLinkLocalAddresses = Flag.LINK_LOCAL in flags,
        blockMulticastAddresses = Flag.MULTICAST in flags,
        blockMetadataAddresses = Flag.METADATA in flags,
        blockCarrierGradeNatAddresses = Flag.CGNAT in flags,
        blockBenchmarkAddresses = Flag.BENCHMARK in flags,
    )

    private class RecordingResolver(
        private val answer: suspend (String) -> List<InetAddress>,
    ) : NetworkAddressResolver {
        override val id: String = "recording-resolver"
        val hosts = CopyOnWriteArrayList<String>()

        override suspend fun resolve(host: String): List<InetAddress> {
            hosts += host
            return answer(host)
        }
    }

    private companion object {
        fun v4(a: Int, b: Int, c: Int, d: Int): InetAddress =
            InetAddress.getByAddress("answer", byteArrayOf(a.toByte(), b.toByte(), c.toByte(), d.toByte()))

        fun v6(vararg words: Int): InetAddress {
            require(words.size == 8)
            val bytes = ByteArray(16)
            words.forEachIndexed { index, word ->
                bytes[index * 2] = (word ushr 8).toByte()
                bytes[index * 2 + 1] = word.toByte()
            }
            return Inet6Address.getByAddress("answer", bytes, -1)
        }

        fun mapped(a: Int, b: Int, c: Int, d: Int): InetAddress =
            v6(0, 0, 0, 0, 0, 0xffff, (a shl 8) or b, (c shl 8) or d)

        fun compatible(a: Int, b: Int, c: Int, d: Int): InetAddress =
            v6(0, 0, 0, 0, 0, 0, (a shl 8) or b, (c shl 8) or d)

        fun nat64(a: Int, b: Int, c: Int, d: Int): InetAddress =
            v6(0x0064, 0xff9b, 0, 0, 0, 0, (a shl 8) or b, (c shl 8) or d)
    }
}
