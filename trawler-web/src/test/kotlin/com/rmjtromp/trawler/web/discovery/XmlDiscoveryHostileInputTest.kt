package com.rmjtromp.trawler.web.discovery

import com.rmjtromp.trawler.discovery.DiscoveryContext
import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.state.DiscoveryCheckpoint
import com.rmjtromp.trawler.transport.RequestExecutor
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.zip.GZIPOutputStream
import javax.xml.stream.XMLStreamException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

/** Malformed, hostile, and boundary XML inputs for the bounded discovery adapters. */
class XmlDiscoveryHostileInputTest {
    private val feed = "https://example.test/feed"
    private val map = "https://example.test/map.xml"

    @Test
    fun `constructors reject blank targets blank ids and non-URI targets`() {
        listOf<() -> Unit>(
            { RssDiscovery(" ") },
            { RssDiscovery(feed, id = " ") },
            { RssDiscovery("https://exa mple.test/feed") },
            { AtomDiscovery(" ") },
            { AtomDiscovery(feed, id = "") },
            { AtomDiscovery("https://example.test/%zz") },
            { SitemapDiscovery("") },
            { SitemapDiscovery(map, id = " ") },
            { SitemapDiscovery("mailto:someone@example.test") },
            { SitemapDiscovery("https://exa mple.test/map.xml") },
        ).forEach { invalid -> assertFailsWith<IllegalArgumentException> { invalid() } }
        assertEquals("rss", RssDiscovery(feed).id)
        assertEquals("atom", AtomDiscovery(feed).id)
        assertEquals("sitemap", SitemapDiscovery(map).id)
        assertEquals(feed, RssDiscovery(feed).target)
        assertEquals(XmlDiscoveryBounds(), SitemapDiscovery(map).bounds)
    }

    @Test
    fun `DTDs internal entity expansion and external entities are rejected without network`() = runTest {
        val billionLaughs = """
            <?xml version="1.0"?>
            <!DOCTYPE lolz [
              <!ENTITY lol "lol">
              <!ENTITY lol2 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">
              <!ENTITY lol3 "&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;">
            ]>
            <urlset><url><loc>https://example.test/&lol3;</loc></url></urlset>
        """.trimIndent()
        val externalDtd = """<?xml version="1.0"?><!DOCTYPE urlset SYSTEM "http://attacker.test/evil.dtd"><urlset/>"""
        val xxe = """<!DOCTYPE urlset [<!ENTITY x SYSTEM "file:///etc/passwd">]><urlset><url><loc>&x;</loc></url></urlset>"""
        listOf(billionLaughs, externalDtd, xxe).forEach { document ->
            val executor = executorFor(map to ok(document))
            assertFailsWith<XmlDiscoveryFormatException> { SitemapDiscovery(map).discover(context(executor)).toList() }
            assertEquals(listOf(map), executor.requests.map { it.target }, "only the engine-executed document request")
        }
        assertFailsWith<XmlDiscoveryFormatException> {
            AtomDiscovery(feed).discover(context(executorFor(feed to ok(xxe.replace("urlset", "feed"))))).toList()
        }
    }

    @Test
    fun `undeclared entity references are rejected in feeds and sitemaps while predefined ones decode`() = runTest {
        assertFailsWith<XmlDiscoveryFormatException> {
            SitemapDiscovery(map).discover(context(executorFor(map to ok("<urlset><url><loc>/a&undeclared;</loc></url></urlset>")))).toList()
        }
        assertFailsWith<XmlDiscoveryFormatException> {
            RssDiscovery(feed).discover(context(executorFor(feed to ok("<rss><item><link>/a&undeclared;</link></item></rss>")))).toList()
        }
        val decoded = SitemapDiscovery(map).discover(
            context(executorFor(map to ok("<urlset><url><loc>/p?a=1&amp;b=2&#38;c=3</loc></url></urlset>"))),
        ).toList()
        assertEquals(listOf("https://example.test/p?a=1&b=2&c=3"), targets(decoded))
    }

    @Test
    fun `malformed and truncated documents fail visibly`() = runTest {
        assertFailsWith<XMLStreamException> {
            SitemapDiscovery(map).discover(context(executorFor(map to ok("<urlset><url><loc>/a</url></urlset>")))).toList()
        }
        assertFailsWith<XMLStreamException> {
            SitemapDiscovery(map).discover(context(executorFor(map to ok("")))).toList()
        }
        assertFailsWith<XMLStreamException> {
            AtomDiscovery(feed).discover(context(executorFor(feed to ok("not xml")))).toList()
        }
        val truncatedGzip = gzip("<urlset><url><loc>/a</loc></url></urlset>".toByteArray()).let { it.copyOf(it.size / 2) }
        assertFailsWith<IOException> {
            SitemapDiscovery(map).discover(context(executorFor(map to ok(truncatedGzip)))).toList()
        }
        assertFailsWith<XMLStreamException> {
            SitemapDiscovery(map).discover(context(executorFor(map to ok(byteArrayOf(0x1f))))).toList()
        }
        assertFailsWith<XMLStreamException> {
            SitemapDiscovery(map).discover(context(executorFor(map to ok(byteArrayOf(0x1f, 0x00, 0x3c))))).toList()
        }
    }

    @Test
    fun `gzip bombs stop at the decompressed document bound`() = runTest {
        val bomb = gzip(ByteArray(8 * 1024 * 1024) { ' '.code.toByte() })
        val bounds = XmlDiscoveryBounds(maxDocumentBytes = 64 * 1024)
        assertTrue(bomb.size <= bounds.maxDocumentBytes, "compressed bomb fits the compressed bound")
        assertFailsWith<XmlDiscoveryLimitException> {
            SitemapDiscovery(map, bounds = bounds).discover(context(executorFor(map to ok(bomb)))).toList()
        }
        val exact = "<urlset><url><loc>/x</loc></url></urlset>".toByteArray()
        val exactBounds = XmlDiscoveryBounds(maxDocumentBytes = maxOf(exact.size, gzip(exact).size))
        assertEquals(
            listOf("https://example.test/x"),
            targets(SitemapDiscovery(map, bounds = exactBounds).discover(context(executorFor(map to ok(gzip(exact))))).toList()),
        )
    }

    @Test
    fun `discovery requires a 2xx status at both range edges`() = runTest {
        listOf(199, 300, 304, 500).forEach { status ->
            assertFailsWith<IllegalArgumentException>(status.toString()) {
                SitemapDiscovery(map).discover(context(executorFor(map to response(status, "<urlset/>".toByteArray())))).toList()
            }
        }
        listOf(200, 299).forEach { status ->
            assertEquals(
                emptyList(),
                SitemapDiscovery(map).discover(context(executorFor(map to response(status, "<urlset/>".toByteArray())))).toList(),
            )
        }
    }

    @Test
    fun `RSS ignores channel links blank links and nested items fail closed`() = runTest {
        val document = """
            <rss><channel>
              <link>/channel-home</link>
              <item><title>no link</title></item>
              <item><link>   </link></item>
              <item><link><![CDATA[/cdata]]></link></item>
              <item><link>/split</link><guid>ignored</guid></item>
            </channel></rss>
        """.trimIndent()
        assertEquals(
            listOf("https://example.test/cdata", "https://example.test/split"),
            targets(RssDiscovery(feed).discover(context(executorFor(feed to ok(document)))).toList()),
        )
        assertFailsWith<XmlDiscoveryLimitException> {
            RssDiscovery(feed).discover(
                context(executorFor(feed to ok("<rss><item><item><link>/x</link></item></item></rss>"))),
            ).toList()
        }
        val cdataBound = XmlDiscoveryBounds(maxLocationLength = 10)
        assertFailsWith<XmlDiscoveryLimitException> {
            RssDiscovery(feed, bounds = cdataBound).discover(
                context(executorFor(feed to ok("<rss><item><link>/12345<![CDATA[67890]]></link></item></rss>"))),
            ).toList()
        }
    }

    @Test
    fun `Atom uses entry href attributes ignores feed-level and hrefless links and bounds href length`() = runTest {
        val document = """
            <feed xmlns="http://www.w3.org/2005/Atom">
              <link href="/feed-home"/>
              <entry><link rel="alternate"/></entry>
              <entry><title>One</title><id>urn:one</id><link href=" /one "/></entry>
              <entry><link href="/two"/></entry>
            </feed>
        """.trimIndent()
        assertEquals(
            listOf("https://example.test/one", "https://example.test/two"),
            targets(AtomDiscovery(feed).discover(context(executorFor(feed to ok(document)))).toList()),
        )
        assertFailsWith<XmlDiscoveryLimitException> {
            AtomDiscovery(feed, bounds = XmlDiscoveryBounds(maxLocationLength = 8)).discover(
                context(executorFor(feed to ok("<feed><entry><link href=\"/123456789\"/></entry></feed>"))),
            ).toList()
        }
        assertFailsWith<XmlDiscoveryLimitException> {
            AtomDiscovery(feed).discover(
                context(executorFor(feed to ok("<feed><entry><entry><link href=\"/x\"/></entry></entry></feed>"))),
            ).toList()
        }
    }

    @Test
    fun `Atom resume skips already accepted entries and continues the cursor`() = runTest {
        val document = "<feed>" + (1..3).joinToString("") { "<entry><link href=\"/$it\"/></entry>" } + "</feed>"
        val discovery = AtomDiscovery(feed)
        val codec = discovery.checkpointCodec as XmlCursorCheckpointCodec
        val resumed = discovery.discover(
            context(executorFor(feed to ok(document))).copy(
                checkpoint = DiscoveryCheckpoint(
                    revision = 1,
                    codecId = codec.descriptor.id,
                    codecVersion = codec.descriptor.version,
                    payload = codec.encodeCursor(2),
                    strategyId = codec.strategyId,
                ),
            ),
        ).toList()
        assertEquals(listOf("https://example.test/3"), targets(resumed))
        assertEquals(3L, codec.decodeCursor(resumed.single().checkpoint?.payload))
    }

    @Test
    fun `sitemap locations are bounded per element per document and across documents`() = runTest {
        assertFailsWith<XmlDiscoveryLimitException> {
            SitemapDiscovery(map, bounds = XmlDiscoveryBounds(maxLocationLength = 8)).discover(
                context(executorFor(map to ok("<urlset><url><loc>/12345<![CDATA[6789]]></loc></url></urlset>"))),
            ).toList()
        }
        assertFailsWith<XmlDiscoveryLimitException> {
            SitemapDiscovery(map, bounds = XmlDiscoveryBounds(maxItems = 1)).discover(
                context(executorFor(map to ok("<urlset><url><loc>/a</loc></url><url><loc>/b</loc></url></urlset>"))),
            ).toList()
        }

        val index = "<sitemapindex><sitemap><loc>/one.xml</loc></sitemap><sitemap><loc>/two.xml</loc></sitemap></sitemapindex>"
        val child = { prefix: String -> "<urlset><url><loc>/$prefix-1</loc></url><url><loc>/$prefix-2</loc></url></urlset>" }
        val executor = executorFor(
            map to ok(index),
            "https://example.test/one.xml" to ok(child("one")),
            "https://example.test/two.xml" to ok(child("two")),
        )
        val emitted = mutableListOf<DiscoveryEmission>()
        val failure = assertFailsWith<XmlDiscoveryLimitException> {
            SitemapDiscovery(map, bounds = XmlDiscoveryBounds(maxItems = 3)).discover(context(executor)).collect { emitted += it }
        }
        assertEquals("Sitemap item bound", failure.message)
        assertEquals(
            listOf("https://example.test/one-1", "https://example.test/one-2", "https://example.test/two-1"),
            targets(emitted),
        )
    }

    @Test
    fun `sitemap ignores blank and nested locations and accepts mixed-case roots`() = runTest {
        val document = "<URLSET><url><loc>  </loc></url><url><loc>/outer<loc>/inner</loc></loc></url><url><LOC>/upper</LOC></url></URLSET>"
        assertEquals(
            listOf("https://example.test/inner", "https://example.test/upper"),
            targets(SitemapDiscovery(map).discover(context(executorFor(map to ok(document)))).toList()),
        )
    }

    @Test
    fun `invalid relative and over-long resolved locations are rejected`() = runTest {
        assertFailsWith<XmlDiscoveryFormatException> {
            SitemapDiscovery(map).discover(context(executorFor(map to ok("<urlset><url><loc>https://exa mple.test/</loc></url></urlset>")))).toList()
        }
        assertFailsWith<IllegalArgumentException> {
            SitemapDiscovery(map).discover(context(executorFor(map to ok("<urlset><url><loc>mailto:x@example.test</loc></url></urlset>")))).toList()
        }
        val bounds = XmlDiscoveryBounds(maxLocationLength = map.length)
        val failure = assertFailsWith<IllegalArgumentException> {
            SitemapDiscovery(map, bounds = bounds).discover(
                context(executorFor(map to ok("<urlset><url><loc>/a-longer-relative-path</loc></url></urlset>"))),
            ).toList()
        }
        assertEquals("Resolved discovery location exceeds its configured bound", failure.message)
    }

    @Test
    fun `self-referencing and deep sitemap indexes stop at the configured depth`() = runTest {
        val loop = executorFor(map to ok("<sitemapindex><sitemap><loc>/map.xml</loc></sitemap></sitemapindex>"))
        assertFailsWith<SitemapDepthExceededException> {
            SitemapDiscovery(map, bounds = XmlDiscoveryBounds(maxSitemapDepth = 2)).discover(context(loop)).toList()
        }
        assertEquals(3, loop.requests.size, "depths 0, 1, and 2 are fetched before the bound is reported")

        val nested = executorFor(
            map to ok("<sitemapindex><sitemap><loc>/child.xml</loc></sitemap></sitemapindex>"),
            "https://example.test/child.xml" to ok("<urlset><url><loc>/leaf</loc></url></urlset>"),
        )
        assertEquals(
            listOf("https://example.test/leaf"),
            targets(SitemapDiscovery(map, bounds = XmlDiscoveryBounds(maxSitemapDepth = 1)).discover(context(nested)).toList()),
        )
    }

    private fun context(executor: RequestExecutor): DiscoveryContext =
        DiscoveryContext(CrawlerContext("engine", "run", "pipeline"), executor)

    private fun ok(body: String): CrawlResponse = response(200, body.toByteArray())

    private fun ok(body: ByteArray): CrawlResponse = response(200, body)

    private fun response(status: Int?, body: ByteArray): CrawlResponse = CrawlResponse(
        request = CrawlRequest("https://example.test/response"),
        statusCode = status,
        headers = Headers.empty(),
        body = ResponseBody.from(body),
        receivedAt = Instant.EPOCH,
    )

    private fun targets(emissions: List<DiscoveryEmission>): List<String> =
        emissions.map { (it as DiscoveryEmission.Request).request.target }

    private fun gzip(bytes: ByteArray): ByteArray = ByteArrayOutputStream().use { output ->
        GZIPOutputStream(output).use { it.write(bytes) }
        output.toByteArray()
    }

    private fun executorFor(vararg responses: Pair<String, CrawlResponse>): RecordingExecutor = RecordingExecutor(responses.toMap())

    private class RecordingExecutor(private val responses: Map<String, CrawlResponse>) : RequestExecutor {
        val requests = CopyOnWriteArrayList<CrawlRequest>()

        override suspend fun execute(request: CrawlRequest, context: RequestContext): CrawlResponse {
            requests += request
            return responses[request.target] ?: error("unexpected request ${request.target}")
        }
    }
}
