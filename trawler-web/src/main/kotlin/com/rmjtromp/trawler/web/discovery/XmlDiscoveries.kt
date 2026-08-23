package com.rmjtromp.trawler.web.discovery

import com.rmjtromp.trawler.discovery.DiscoveryCheckpointCandidate
import com.rmjtromp.trawler.discovery.DiscoveryContext
import com.rmjtromp.trawler.discovery.DiscoveryEmission
import com.rmjtromp.trawler.discovery.DiscoveryCheckpointCodec
import com.rmjtromp.trawler.discovery.ResumableDiscovery
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.RequestMethod
import com.rmjtromp.trawler.model.RequestContext
import com.rmjtromp.trawler.state.CheckpointPayload
import com.rmjtromp.trawler.state.DiscoveryCheckpointCodecDescriptor
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.net.URI
import java.nio.ByteBuffer
import java.util.ArrayDeque
import java.util.zip.GZIPInputStream
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants
import javax.xml.stream.XMLStreamReader

/** Immutable bounds shared by the XML discovery implementations. */
public data class XmlDiscoveryBounds(
    public val maxDocumentBytes: Int = 2 * 1024 * 1024,
    public val maxItems: Int = 10_000,
    public val maxLocationLength: Int = 16 * 1024,
    public val maxSitemapDepth: Int = 4,
    public val maxSitemapDocuments: Int = 256,
) {
    init {
        require(maxDocumentBytes > 0) { "XML document bound must be positive" }
        require(maxItems > 0) { "XML item bound must be positive" }
        require(maxLocationLength > 0) { "XML location bound must be positive" }
        require(maxSitemapDepth >= 0) { "Sitemap recursion depth must not be negative" }
        require(maxSitemapDocuments > 0) { "Sitemap document bound must be positive" }
    }
}

/** Exact cursor codec used by the bounded RSS/Atom discovery adapters. */
public class XmlCursorCheckpointCodec(
    override val descriptor: DiscoveryCheckpointCodecDescriptor,
    override val strategyId: String,
) : DiscoveryCheckpointCodec {
    init {
        require(strategyId.isNotBlank()) { "XML checkpoint strategy ID must not be blank" }
    }

    /** Encodes a non-negative item cursor without a serializer dependency. */
    public fun encodeCursor(cursor: Long): CheckpointPayload {
        require(cursor >= 0) { "XML discovery cursor must not be negative" }
        return CheckpointPayload.from(ByteBuffer.allocate(8).putLong(cursor).array())
    }

    /** Decodes a cursor; an empty reset payload starts at zero. */
    public fun decodeCursor(payload: CheckpointPayload?): Long {
        if (payload == null || payload.size == 0) return 0
        require(payload.size == 8) { "XML discovery checkpoint payload has an unsupported size" }
        return ByteBuffer.wrap(payload.toByteArray()).long.also {
            require(it >= 0) { "XML discovery cursor must not be negative" }
        }
    }
}

/** RSS 2.x item-link discovery through the engine-provided RequestExecutor. */
public class RssDiscovery(
    public val target: String,
    override val id: String = "rss",
    public val bounds: XmlDiscoveryBounds = XmlDiscoveryBounds(),
) : ResumableDiscovery {
    private val cursorCodec = XmlCursorCheckpointCodec(
        DiscoveryCheckpointCodecDescriptor("trawler-web-rss", 1),
        "rss-items",
    )
    override val checkpointCodec: DiscoveryCheckpointCodec = cursorCodec

    init {
        require(target.isNotBlank()) { "RSS target must not be blank" }
        require(id.isNotBlank()) { "RSS discovery ID must not be blank" }
        requireWebTarget(target, "RSS target")
    }

    override fun discover(context: DiscoveryContext): Flow<DiscoveryEmission> = flow {
        val request = CrawlRequest(target = target, method = RequestMethod.GET)
        val response = context.executor.execute(request, RequestContext(context.crawler, request))
        ensureSuccessful(response.statusCode, "RSS")
        val links = parseFeedLinks(response.body.toByteArray(), FeedFormat.RSS, bounds, target)
        var cursor = cursorCodec.decodeCursor(context.checkpoint?.payload)
        links.forEachIndexed { index, link ->
            if (cursor > 0) {
                cursor--
            } else {
                val next = cursorCodec.encodeCursor(index.toLong() + 1L)
                emit(DiscoveryEmission.Request(CrawlRequest(link), checkpoint(next)))
            }
        }
    }

    private fun checkpoint(payload: CheckpointPayload): DiscoveryCheckpointCandidate =
        DiscoveryCheckpointCandidate(
            descriptor = checkpointCodec.descriptor,
            strategyId = checkpointCodec.strategyId,
            payload = payload,
        )
}

/** Atom entry-link discovery through the engine-provided RequestExecutor. */
public class AtomDiscovery(
    public val target: String,
    override val id: String = "atom",
    public val bounds: XmlDiscoveryBounds = XmlDiscoveryBounds(),
) : ResumableDiscovery {
    private val cursorCodec = XmlCursorCheckpointCodec(
        DiscoveryCheckpointCodecDescriptor("trawler-web-atom", 1),
        "atom-entries",
    )
    override val checkpointCodec: DiscoveryCheckpointCodec = cursorCodec

    init {
        require(target.isNotBlank()) { "Atom target must not be blank" }
        require(id.isNotBlank()) { "Atom discovery ID must not be blank" }
        requireWebTarget(target, "Atom target")
    }

    override fun discover(context: DiscoveryContext): Flow<DiscoveryEmission> = flow {
        val request = CrawlRequest(target = target, method = RequestMethod.GET)
        val response = context.executor.execute(request, RequestContext(context.crawler, request))
        ensureSuccessful(response.statusCode, "Atom")
        val links = parseFeedLinks(response.body.toByteArray(), FeedFormat.ATOM, bounds, target)
        var skipped = cursorCodec.decodeCursor(context.checkpoint?.payload)
        links.forEachIndexed { index, link ->
            if (skipped > 0) {
                skipped--
            } else {
                emit(
                    DiscoveryEmission.Request(
                        CrawlRequest(link),
                        DiscoveryCheckpointCandidate(
                            descriptor = checkpointCodec.descriptor,
                            strategyId = checkpointCodec.strategyId,
                            payload = cursorCodec.encodeCursor(index.toLong() + 1L),
                        ),
                    ),
                )
            }
        }
    }
}

/**
 * Bounded sitemap and sitemap-index discovery. Recursion is breadth-first,
 * depth-limited, document-limited, and always uses the normal engine
 * transport path; this class never creates an HTTP client.
 */
public class SitemapDiscovery(
    public val target: String,
    override val id: String = "sitemap",
    public val bounds: XmlDiscoveryBounds = XmlDiscoveryBounds(),
) : ResumableDiscovery {
    private val cursorCodec = XmlCursorCheckpointCodec(
        DiscoveryCheckpointCodecDescriptor("trawler-web-sitemap", 1),
        "sitemap-locations",
    )
    override val checkpointCodec: DiscoveryCheckpointCodec = cursorCodec

    init {
        require(target.isNotBlank()) { "Sitemap target must not be blank" }
        require(id.isNotBlank()) { "Sitemap discovery ID must not be blank" }
        requireWebTarget(target, "Sitemap target")
    }

    override fun discover(context: DiscoveryContext): Flow<DiscoveryEmission> = flow {
        var skipped = cursorCodec.decodeCursor(context.checkpoint?.payload)
        val pending = ArrayDeque<SitemapDocument>()
        pending.addLast(SitemapDocument(target, 0))
        var documents = 0
        var visitedLocations = skipped
        var emittedLocations = 0
        while (pending.isNotEmpty() && documents < bounds.maxSitemapDocuments) {
            val document = pending.removeFirst()
            val request = CrawlRequest(target = document.target, method = RequestMethod.GET)
            val response = context.executor.execute(request, RequestContext(context.crawler, request))
            ensureSuccessful(response.statusCode, "Sitemap")
            val parsed = parseSitemap(response.body.toByteArray(), bounds)
            documents++
            if (parsed.kind == SitemapKind.INDEX) {
                if (document.depth >= bounds.maxSitemapDepth) {
                    throw SitemapDepthExceededException(bounds.maxSitemapDepth)
                }
                parsed.locations.forEach { location ->
                    pending.addLast(
                        SitemapDocument(
                            resolveLocation(document.target, location, bounds),
                            document.depth + 1,
                        ),
                    )
                }
            } else {
                for (location in parsed.locations) {
                    if (visitedLocations > 0) {
                        visitedLocations--
                        continue
                    }
                    if (emittedLocations >= bounds.maxItems) {
                        throw XmlDiscoveryLimitException("Sitemap item bound")
                    }
                    skipped++
                    emittedLocations++
                    emit(
                        DiscoveryEmission.Request(
                            CrawlRequest(resolveLocation(document.target, location, bounds)),
                            DiscoveryCheckpointCandidate(
                                descriptor = checkpointCodec.descriptor,
                                strategyId = checkpointCodec.strategyId,
                                payload = cursorCodec.encodeCursor(skipped),
                            ),
                        ),
                    )
                }
            }
        }
        if (pending.isNotEmpty()) throw SitemapDocumentLimitExceededException(bounds.maxSitemapDocuments)
    }
}

private enum class FeedFormat { RSS, ATOM }

private enum class SitemapKind { INDEX, URLSET }

private data class SitemapDocument(val target: String, val depth: Int)

private data class ParsedSitemap(val kind: SitemapKind, val locations: List<String>)

private fun ensureSuccessful(statusCode: Int?, kind: String) {
    require(statusCode != null && statusCode in 200..299) {
        "$kind discovery requires a successful response; received ${statusCode ?: "no-status"}"
    }
}

private fun parseFeedLinks(
    bytes: ByteArray,
    format: FeedFormat,
    bounds: XmlDiscoveryBounds,
    baseTarget: String,
): List<String> {
    val reader = newReader(boundedXmlBytes(bytes, bounds.maxDocumentBytes))
    val links = ArrayList<String>(minOf(bounds.maxItems, 64))
    var inItem = false
    var currentLink: String? = null
    var inLinkText = false
    try {
        while (reader.hasNext()) {
            when (reader.next()) {
                XMLStreamConstants.START_ELEMENT -> {
                    val name = reader.localName.lowercase()
                    if ((format == FeedFormat.RSS && name == "item") ||
                        (format == FeedFormat.ATOM && name == "entry")
                    ) {
                        if (inItem) throw XmlDiscoveryLimitException("Nested feed item")
                        inItem = true
                        currentLink = null
                    } else if (inItem && format == FeedFormat.ATOM && name == "link") {
                        currentLink = reader.getAttributeValue(null, "href")?.trim()?.also {
                            if (it.length > bounds.maxLocationLength) {
                                throw XmlDiscoveryLimitException("Feed link bound")
                            }
                        }
                    } else if (inItem && format == FeedFormat.RSS && name == "link") {
                        inLinkText = true
                    }
                }

                XMLStreamConstants.CHARACTERS,
                XMLStreamConstants.CDATA,
                -> if (inItem && inLinkText) {
                    val next = currentLink.orEmpty() + reader.text
                    if (next.length > bounds.maxLocationLength) {
                        throw XmlDiscoveryLimitException("Feed link bound")
                    }
                    currentLink = next.trim()
                }

                XMLStreamConstants.END_ELEMENT -> {
                    val name = reader.localName.lowercase()
                    if (inItem && format == FeedFormat.RSS && name == "link") inLinkText = false
                    if ((format == FeedFormat.RSS && name == "item") ||
                        (format == FeedFormat.ATOM && name == "entry")
                    ) {
                        currentLink?.takeIf(String::isNotBlank)?.let {
                            if (links.size >= bounds.maxItems) throw XmlDiscoveryLimitException("Feed item bound")
                            links += resolveLocation(baseTarget, it, bounds)
                        }
                        inItem = false
                        currentLink = null
                    }
                }
            }
        }
    } finally {
        reader.close()
    }
    return links
}

private fun parseSitemap(bytes: ByteArray, bounds: XmlDiscoveryBounds): ParsedSitemap {
    val reader = newReader(boundedXmlBytes(bytes, bounds.maxDocumentBytes))
    var root: String? = null
    var inLoc = false
    var text = StringBuilder()
    val locations = ArrayList<String>(minOf(bounds.maxItems, 64))
    try {
        while (reader.hasNext()) {
            when (reader.next()) {
                XMLStreamConstants.START_ELEMENT -> {
                    val name = reader.localName.lowercase()
                    if (root == null && name != "urlset" && name != "sitemapindex") {
                        root = name
                    } else if (root == null) {
                        root = name
                    }
                    if (name == "loc") {
                        inLoc = true
                        text = StringBuilder()
                    }
                }

                XMLStreamConstants.CHARACTERS,
                XMLStreamConstants.CDATA,
                -> if (inLoc) {
                    if (text.length + reader.text.length > bounds.maxLocationLength) {
                        throw XmlDiscoveryLimitException("Sitemap location bound")
                    }
                    text.append(reader.text)
                }

                XMLStreamConstants.END_ELEMENT -> if (reader.localName.equals("loc", true) && inLoc) {
                    val location = text.toString().trim()
                    if (location.isNotBlank()) {
                        if (locations.size >= bounds.maxItems) throw XmlDiscoveryLimitException("Sitemap URL bound")
                        locations += location
                    }
                    inLoc = false
                }
            }
        }
    } finally {
        reader.close()
    }
    val kind = when (root?.lowercase()) {
        "sitemapindex" -> SitemapKind.INDEX
        "urlset" -> SitemapKind.URLSET
        else -> throw XmlDiscoveryFormatException("Unsupported sitemap root element")
    }
    return ParsedSitemap(kind, locations)
}

private fun newReader(bytes: ByteArray): XMLStreamReader {
    val factory = XMLInputFactory.newFactory().apply {
        setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
        setProperty(XMLInputFactory.SUPPORT_DTD, false)
        setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, false)
    }
    return factory.createXMLStreamReader(ByteArrayInputStream(bytes))
}

private fun requireWebTarget(target: String, label: String) {
    val uri = runCatching { URI(target) }
        .getOrElse { throw IllegalArgumentException("$label must be an absolute HTTP(S) URI") }
    require(uri.isAbsolute && uri.scheme.lowercase() in setOf("http", "https")) {
        "$label must be an absolute HTTP(S) URI"
    }
}

private fun resolveLocation(baseTarget: String, location: String, bounds: XmlDiscoveryBounds): String {
    require(location.length <= bounds.maxLocationLength) { "Discovery location exceeds its configured bound" }
    val resolved = runCatching { URI(baseTarget).resolve(location) }
        .getOrElse { throw XmlDiscoveryFormatException("Invalid discovery location") }
    require(resolved.isAbsolute && resolved.scheme.lowercase() in setOf("http", "https")) {
        "Discovery location must resolve to an absolute HTTP(S) URI"
    }
    val value = resolved.toString()
    require(value.length <= bounds.maxLocationLength) {
        "Resolved discovery location exceeds its configured bound"
    }
    return value
}

private fun boundedXmlBytes(bytes: ByteArray, maximum: Int): ByteArray {
    require(bytes.size <= maximum) { "XML discovery document exceeds the configured compressed bound" }
    if (bytes.size < 2 || bytes[0] != 0x1f.toByte() || bytes[1] != 0x8b.toByte()) {
        return bytes
    }
    return GZIPInputStream(ByteArrayInputStream(bytes)).use { input ->
        val output = ByteArrayOutputStream(minOf(maximum, 8 * 1024))
        val buffer = ByteArray(minOf(maximum, 8 * 1024).coerceAtLeast(1))
        while (true) {
            val read = input.read(buffer)
            if (read == -1) break
            if (output.size() > maximum - read) {
                throw XmlDiscoveryLimitException("XML discovery decompressed bound")
            }
            output.write(buffer, 0, read)
        }
        output.toByteArray()
    }
}

/** Raised when an XML document exceeds a discovery bound. */
public class XmlDiscoveryLimitException(message: String) : IllegalStateException(message)

/** Raised when a feed or sitemap does not have an expected XML shape. */
public class XmlDiscoveryFormatException(message: String) : IllegalArgumentException(message)

/** Raised when sitemap recursion exceeds the configured depth. */
public class SitemapDepthExceededException(depth: Int) : IllegalStateException(
    "Sitemap recursion depth exceeds configured bound of $depth",
)

/** Raised when a sitemap index exceeds its configured document bound. */
public class SitemapDocumentLimitExceededException(limit: Int) : IllegalStateException(
    "Sitemap document count exceeds configured bound of $limit",
)
