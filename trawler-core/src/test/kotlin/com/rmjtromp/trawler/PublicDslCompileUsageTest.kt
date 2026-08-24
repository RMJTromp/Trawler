package com.rmjtromp.trawler

import com.rmjtromp.trawler.discovery.SeedDiscovery
import com.rmjtromp.trawler.model.CrawlRequest
import com.rmjtromp.trawler.model.CrawlResponse
import com.rmjtromp.trawler.model.ExtractionContext
import com.rmjtromp.trawler.model.Headers
import com.rmjtromp.trawler.model.OutputContext
import com.rmjtromp.trawler.model.ResponseBody
import com.rmjtromp.trawler.pipeline.EngineConfigurationBuilder
import com.rmjtromp.trawler.pipeline.ExtractionResult
import com.rmjtromp.trawler.pipeline.Extractor
import com.rmjtromp.trawler.pipeline.Output
import com.rmjtromp.trawler.pipeline.OutputResult
import com.rmjtromp.trawler.pipeline.Pipeline
import com.rmjtromp.trawler.pipeline.Produced
import com.rmjtromp.trawler.pipeline.RunRequest
import com.rmjtromp.trawler.pipeline.TrawlerEngine
import com.rmjtromp.trawler.pipeline.pipeline
import com.rmjtromp.trawler.pipeline.replay
import com.rmjtromp.trawler.response.ResponseReplayQuery
import com.rmjtromp.trawler.transport.Transport
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test

/** Compile-usage coverage for the documented public typed DSL. */
class PublicDslCompileUsageTest {
    @Test
    fun `typed pipeline and engine DSL compose without erased values`() {
        val transport = StaticTransport()
        val built: Pipeline<Int> = pipeline<Int>("built") {
            transport(transport)
            discover(SeedDiscovery(requests = listOf(CrawlRequest("https://example.test/built"))))
            extract(IntExtractor())
            output(IntOutput())
        }
        val engine = EngineConfigurationBuilder()
            .register(built)
            .register<String>("inline") {
                transport(transport)
                discover(SeedDiscovery(requests = listOf(CrawlRequest("https://example.test/inline"))))
                extract(StringExtractor())
            }
            .build()
            .let(::TrawlerEngine)

        val ids = engine.configuration.pipelines.map { it.id }
        assertEquals(listOf("built", "inline"), ids)
        val selected = RunRequest(pipelineIds = setOf("built"), reason = "compile-usage")
        assertEquals(setOf("built"), selected.selectedPipelineIds)
        val replaySelection = built.replay(ResponseReplayQuery.ByPipelineId("built"), limit = 2)
        assertEquals("built", replaySelection.pipeline.id)
        assertEquals(2, replaySelection.limit)
    }

    private class StaticTransport : Transport {
        override val id: String = "compile-usage-transport"

        override suspend fun execute(request: CrawlRequest): CrawlResponse = CrawlResponse(
            request = request,
            statusCode = 200,
            headers = Headers.empty(),
            body = ResponseBody.from("1".toByteArray()),
            receivedAt = java.time.Instant.EPOCH,
        )
    }

    private class IntExtractor : Extractor<Int> {
        override val id: String = "compile-int-extractor"

        override suspend fun extract(response: CrawlResponse, context: ExtractionContext): ExtractionResult<Int> =
            ExtractionResult.Success(listOf(response.body.toByteArray().decodeToString().toInt()))
    }

    private class StringExtractor : Extractor<String> {
        override val id: String = "compile-string-extractor"

        override suspend fun extract(response: CrawlResponse, context: ExtractionContext): ExtractionResult<String> =
            ExtractionResult.Success(listOf(response.body.toByteArray().decodeToString()))
    }

    private class IntOutput : Output<Int> {
        override val id: String = "compile-int-output"

        override suspend fun write(items: List<Produced<Int>>, context: OutputContext): OutputResult =
            OutputResult.Accepted(items.size)
    }
}
