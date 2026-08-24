package com.rmjtromp.trawler.web.discovery

import com.rmjtromp.trawler.discovery.DiscoveryContext
import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.CrawlerContext
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.state.CheckpointPayload
import com.rmjtromp.trawler.state.CheckpointResolution
import com.rmjtromp.trawler.state.DiscoveryCheckpoint
import com.rmjtromp.trawler.state.DiscoveryCheckpointCodecDescriptor
import com.rmjtromp.trawler.transport.RequestExecutor
import java.io.ByteArrayOutputStream
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.zip.GZIPOutputStream
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class XmlDiscoveryTest {
    @Test
    fun `cursor codec owns exact bytes and rejects incompatible payloads`() {
        val codec = XmlCursorCheckpointCodec(
            DiscoveryCheckpointCodecDescriptor("xml", 2),
            "feed",
        )
        val encoded = codec.encodeCursor(42)
        assertEquals(42, codec.decodeCursor(encoded))
        val bytes = encoded.toByteArray()
        bytes[7] = 0
        assertEquals(42, codec.decodeCursor(encoded))
        assertEquals(0, codec.decodeCursor(null))
        assertEquals(0, codec.decodeCursor(CheckpointPayload.from(ByteArray(0))))
        assertFailsWith<IllegalArgumentException> { codec.encodeCursor(-1) }
        assertFailsWith<IllegalArgumentException> {
            codec.decodeCursor(CheckpointPayload.from(byteArrayOf(1)))
        }
        assertFailsWith<IllegalArgumentException> {
            codec.decodeCursor(CheckpointPayload.from(java.nio.ByteBuffer.allocate(8).putLong(-1).array()))
        }
        assertIs<CheckpointResolution.Use>(codec.resolve(compatibleCheckpoint(codec)))
        assertIs<CheckpointResolution.Quarantine>(codec.resolve(compatibleCheckpoint(codec).copy(codecVersion = 1)))
        assertFailsWith<IllegalArgumentException> {
            XmlCursorCheckpointCodec(DiscoveryCheckpointCodecDescriptor("xml", 1), " ")
        }
    }

    @Test
    fun `RSS and Atom discoveries resolve links emit checkpoints and resume`() = runTest {
        val rss = """
            <rss version="2.0"><channel>
              <item><link>/first</link></item>
              <item><link>https://other.test/second</link></item>
              <item><title>no link</title></item>
            </channel></rss>
        """.trimIndent()
        val atom = """
            <feed xmlns="http://www.w3.org/2005/Atom">
              <entry><link href="/atom-first" /></entry>
              <entry><link href="https://other.test/atom-second" /></entry>
            </feed>
        """.trimIndent()
        val executor = RecordingExecutor(
            mapOf(
                "https://example.test/rss.xml" to response(200, rss.toByteArray()),
                "https://example.test/atom.xml" to response(200, atom.toByteArray()),
            ),
        )
        val context = context(executor)
        val rssEmissions = RssDiscovery("https://example.test/rss.xml").discover(context).toList()
        assertEquals(
            listOf("https://example.test/first", "https://other.test/second"),
            rssEmissions.map { (it as DiscoveryEmission.Request).request.target },
        )
        assertEquals(listOf(1L, 2L), rssEmissions.map { cursor(it) })

        val rssResume = RssDiscovery("https://example.test/rss.xml").discover(
            context.copy(checkpoint = checkpoint("trawler-web-rss", 1, 1L, "rss-items")),
        ).toList()
        assertEquals(listOf("https://other.test/second"), rssResume.map { (it as DiscoveryEmission.Request).request.target })

        val atomEmissions = AtomDiscovery("https://example.test/atom.xml").discover(context).toList()
        assertEquals(
            listOf("https://example.test/atom-first", "https://other.test/atom-second"),
            atomEmissions.map { (it as DiscoveryEmission.Request).request.target },
        )
        assertEquals(
            listOf("https://example.test/rss.xml", "https://example.test/rss.xml", "https://example.test/atom.xml"),
            executor.requests.map { it.target },
        )
    }

    @Test
    fun `RSS and Atom enforce status XML shape item location and compressed bounds`() = runTest {
        val valid = "<rss><channel><item><link>/one</link></item></channel></rss>"
        val executor = RecordingExecutor(mapOf("https://example.test/feed" to response(200, valid.toByteArray())))
        assertEquals(1, RssDiscovery("https://example.test/feed").discover(context(executor)).toList().size)
        assertFailsWith<IllegalArgumentException> {
            RssDiscovery("https://example.test/feed").discover(
                context(RecordingExecutor(mapOf("https://example.test/feed" to response(404, ByteArray(0))))),
            ).toList()
        }
        assertFailsWith<IllegalArgumentException> {
            RssDiscovery("https://example.test/feed").discover(
                context(RecordingExecutor(mapOf("https://example.test/feed" to response(null, ByteArray(0))))),
            ).toList()
        }
        assertFailsWith<IllegalArgumentException> { RssDiscovery("relative") }
        assertFailsWith<IllegalArgumentException> { AtomDiscovery("ftp://example.test/feed") }

        val three = "<rss><channel>" + (1..3).joinToString("") { "<item><link>/$it</link></item>" } + "</channel></rss>"
        assertEquals(
            3,
            RssDiscovery(
                "https://example.test/feed",
                bounds = XmlDiscoveryBounds(maxItems = 3),
            ).discover(context(RecordingExecutor(mapOf("https://example.test/feed" to response(200, three.toByteArray()))))).toList().size,
        )
        assertFailsWith<XmlDiscoveryLimitException> {
            RssDiscovery(
                "https://example.test/feed",
                bounds = XmlDiscoveryBounds(maxItems = 2),
            ).discover(context(RecordingExecutor(mapOf("https://example.test/feed" to response(200, three.toByteArray()))))).toList()
        }

        val compressed = gzip(valid.toByteArray())
        val gzipExecutor = RecordingExecutor(mapOf("https://example.test/feed" to response(200, compressed)))
        val compressedBounds = XmlDiscoveryBounds(maxDocumentBytes = compressed.size)
        assertEquals(1, RssDiscovery("https://example.test/feed", bounds = compressedBounds).discover(context(gzipExecutor)).toList().size)
        assertFailsWith<IllegalArgumentException> {
            RssDiscovery(
                "https://example.test/feed",
                bounds = XmlDiscoveryBounds(maxDocumentBytes = compressed.size - 1),
            ).discover(context(gzipExecutor)).toList()
        }
        assertFailsWith<XmlDiscoveryLimitException> {
            RssDiscovery(
                "https://example.test/feed",
                bounds = XmlDiscoveryBounds(maxDocumentBytes = compressed.size),
            ).discover(
                context(RecordingExecutor(mapOf("https://example.test/feed" to response(200, gzip("x".repeat(1000).toByteArray()))))),
            ).toList()
        }
        assertFailsWith<XmlDiscoveryLimitException> {
            RssDiscovery(
                "https://example.test/feed",
                bounds = XmlDiscoveryBounds(maxLocationLength = "https://example.test/one".length),
            ).discover(
                context(RecordingExecutor(mapOf("https://example.test/feed" to response(200, "<rss><channel><item><link>/${"x".repeat(40)}</link></item></channel></rss>".toByteArray())))),
            ).toList()
        }
    }

    @Test
    fun `XML parser rejects malformed documents and DTD external entities`() = runTest {
        val malformed = "<rss><channel><item><link>/one</link></channel></rss>"
        assertFailsWith<Exception> {
            RssDiscovery("https://example.test/feed").discover(
                context(RecordingExecutor(mapOf("https://example.test/feed" to response(200, malformed.toByteArray())))),
            ).toList()
        }
        val dtd = """
            <!DOCTYPE rss [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
            <rss><channel><item><link>&xxe;</link></item></channel></rss>
        """.trimIndent()
        assertFailsWith<Exception> {
            RssDiscovery("https://example.test/feed").discover(
                context(RecordingExecutor(mapOf("https://example.test/feed" to response(200, dtd.toByteArray())))),
            ).toList()
        }
        assertFailsWith<XmlDiscoveryFormatException> {
            SitemapDiscovery("https://example.test/map.xml").discover(
                context(RecordingExecutor(mapOf("https://example.test/map.xml" to response(200, "<other/>".toByteArray())))),
            ).toList()
        }
    }

    @Test
    fun `sitemap traversal is breadth first and resolves relative locations`() = runTest {
        val root = """
            <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <sitemap><loc>/one-index.xml</loc></sitemap>
              <sitemap><loc>https://example.test/two.xml</loc></sitemap>
            </sitemapindex>
        """.trimIndent()
        val one = """
            <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <sitemap><loc>/three.xml</loc></sitemap>
            </sitemapindex>
        """.trimIndent()
        val two = """
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <url><loc>/page-two</loc></url>
            </urlset>
        """.trimIndent()
        val three = """
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <url><loc>/page-three</loc></url>
            </urlset>
        """.trimIndent()
        val executor = RecordingExecutor(
            mapOf(
                "https://example.test/root.xml" to response(200, root.toByteArray()),
                "https://example.test/one-index.xml" to response(200, one.toByteArray()),
                "https://example.test/two.xml" to response(200, two.toByteArray()),
                "https://example.test/three.xml" to response(200, three.toByteArray()),
            ),
        )
        val emissions = SitemapDiscovery("https://example.test/root.xml").discover(context(executor)).toList()
        assertEquals(
            listOf("https://example.test/page-two", "https://example.test/page-three"),
            emissions.map { (it as DiscoveryEmission.Request).request.target },
        )
        assertEquals(
            listOf(
                "https://example.test/root.xml",
                "https://example.test/one-index.xml",
                "https://example.test/two.xml",
                "https://example.test/three.xml",
            ),
            executor.requests.map { it.target },
        )
    }

    @Test
    fun `sitemap resume and exact item document depth and location bounds are deterministic`() = runTest {
        val urlset = """
            <urlset><url><loc>/one</loc></url><url><loc>/two</loc></url><url><loc>/three</loc></url>
            </urlset>
        """.trimIndent()
        val target = "https://example.test/root.xml"
        val executor = RecordingExecutor(mapOf(target to response(200, urlset.toByteArray())))
        val discovery = SitemapDiscovery(target, bounds = XmlDiscoveryBounds(maxItems = 3))
        val resumed = discovery.discover(
            context(executor).copy(checkpoint = checkpoint("trawler-web-sitemap", 1, 1L, "sitemap-locations")),
        ).toList()
        assertEquals(listOf("https://example.test/two", "https://example.test/three"), resumed.map { (it as DiscoveryEmission.Request).request.target })
        assertEquals(listOf(2L, 3L), resumed.map { cursor(it) })

        assertEquals(
            3,
            SitemapDiscovery(target, bounds = XmlDiscoveryBounds(maxItems = 3))
                .discover(context(RecordingExecutor(mapOf(target to response(200, urlset.toByteArray()))))).toList().size,
        )
        assertFailsWith<XmlDiscoveryLimitException> {
            SitemapDiscovery(target, bounds = XmlDiscoveryBounds(maxItems = 2))
                .discover(context(RecordingExecutor(mapOf(target to response(200, urlset.toByteArray()))))).toList()
        }

        val index = "<sitemapindex><sitemap><loc>/child.xml</loc></sitemap></sitemapindex>"
        val child = "<urlset><url><loc>/page</loc></url></urlset>"
        val indexed = mapOf(
            target to response(200, index.toByteArray()),
            "https://example.test/child.xml" to response(200, child.toByteArray()),
        )
        assertFailsWith<SitemapDepthExceededException> {
            SitemapDiscovery(target, bounds = XmlDiscoveryBounds(maxSitemapDepth = 0))
                .discover(context(RecordingExecutor(indexed))).toList()
        }
        assertFailsWith<SitemapDocumentLimitExceededException> {
            SitemapDiscovery(target, bounds = XmlDiscoveryBounds(maxSitemapDocuments = 1))
                .discover(context(RecordingExecutor(indexed))).toList()
        }
        assertFailsWith<IllegalArgumentException> {
            SitemapDiscovery(target).discover(
                context(RecordingExecutor(mapOf(target to response(200, "<urlset><url><loc>ftp://bad.test/x</loc></url></urlset>".toByteArray())))),
            ).toList()
        }
        assertFailsWith<IllegalArgumentException> { XmlDiscoveryBounds(maxDocumentBytes = 0) }
        assertFailsWith<IllegalArgumentException> { XmlDiscoveryBounds(maxItems = 0) }
        assertFailsWith<IllegalArgumentException> { XmlDiscoveryBounds(maxLocationLength = 0) }
        assertFailsWith<IllegalArgumentException> { XmlDiscoveryBounds(maxSitemapDepth = -1) }
        assertFailsWith<IllegalArgumentException> { XmlDiscoveryBounds(maxSitemapDocuments = 0) }
    }

    @Test
    fun `sitemap discovery propagates cancellation while waiting for engine executor`() = runTest {
        val started = CompletableDeferred<Unit>()
        val cancelled = CompletableDeferred<Unit>()
        val executor = RequestExecutor { request, _ ->
            started.complete(Unit)
            try {
                awaitCancellation()
            } finally {
                cancelled.complete(Unit)
            }
        }
        val job = launch(start = CoroutineStart.UNDISPATCHED) {
            SitemapDiscovery("https://example.test/root.xml").discover(context(executor)).toList()
        }
        started.await()
        job.cancelAndJoin()
        assertTrue(cancelled.isCompleted)
    }

    private fun context(executor: RequestExecutor): DiscoveryContext = DiscoveryContext(
        crawler = CrawlerContext("engine", "run", "pipeline"),
        executor = executor,
    )

    private fun response(status: Int?, body: ByteArray): CrawlResponse = CrawlResponse(
        request = CrawlRequest("https://example.test/response"),
        statusCode = status,
        headers = Headers.empty(),
        body = ResponseBody.from(body),
        receivedAt = Instant.EPOCH,
    )

    private fun gzip(bytes: ByteArray): ByteArray = ByteArrayOutputStream().use { output ->
        GZIPOutputStream(output).use { it.write(bytes) }
        output.toByteArray()
    }

    private fun cursor(emission: DiscoveryEmission): Long = when (val candidate = emission.checkpoint) {
        null -> error("emission did not include a checkpoint")
        else -> XmlCursorCheckpointCodec(
            candidate.descriptor,
            candidate.strategyId,
        ).decodeCursor(candidate.payload)
    }

    private fun checkpoint(codecId: String, version: Int, cursor: Long, strategyId: String): DiscoveryCheckpoint =
        DiscoveryCheckpoint(
            revision = 1,
            codecId = codecId,
            codecVersion = version,
            payload = XmlCursorCheckpointCodec(
                DiscoveryCheckpointCodecDescriptor(codecId, version),
                strategyId,
            ).encodeCursor(cursor),
            strategyId = strategyId,
        )

    private fun compatibleCheckpoint(codec: XmlCursorCheckpointCodec): DiscoveryCheckpoint = DiscoveryCheckpoint(
        revision = 0,
        codecId = codec.descriptor.id,
        codecVersion = codec.descriptor.version,
        payload = codec.encodeCursor(0),
        strategyId = codec.strategyId,
    )

    private class RecordingExecutor(
        private val responses: Map<String, CrawlResponse>,
    ) : RequestExecutor {
        val requests = CopyOnWriteArrayList<CrawlRequest>()

        override suspend fun execute(request: CrawlRequest, context: RequestContext): CrawlResponse {
            requests += request
            return responses[request.target] ?: error("unexpected request ${request.target}")
        }
    }
}
