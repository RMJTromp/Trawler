package com.rmjtromp.trawler

import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

/** Static guards for the published module dependency direction. */
class ArchitectureGuardTest {
    @Test
    fun `core source and POM do not acquire adapter technology`() {
        val root = repositoryRoot()
        val forbidden = listOf(
            "ktor",
            "jdbc",
            "postgres",
            "flyway",
            "hikari",
            "jackson",
            "gson",
            "serialization",
            "protobuf",
            "jsoup",
            "slf4j",
            "logback",
            "micrometer",
            "opentelemetry",
        )
        val pom = Files.readString(root.resolve("trawler-core/pom.xml")).lowercase()
        forbidden.forEach { token ->
            assertFalse(pom.contains(token), "core POM contains forbidden technology '$token'")
        }

        Files.walk(root.resolve("trawler-core/src/main/kotlin")).use { paths ->
            paths.filter { Files.isRegularFile(it) && it.toString().endsWith(".kt") }
                .forEach { source ->
                    val text = Files.readString(source).lowercase()
                    forbidden.forEach { token ->
                        assertFalse(
                            text.contains("import $token") || text.contains("import io.$token") ||
                                text.contains("import org.$token") || text.contains("import java.sql"),
                            "core source ${root.relativize(source)} imports forbidden technology '$token'",
                        )
                    }
                }
        }
    }

    @Test
    fun `adapter POMs depend on core but never on each other`() {
        val root = repositoryRoot()
        val webPom = Files.readString(root.resolve("trawler-web/pom.xml")).lowercase()
        val postgresPom = Files.readString(root.resolve("trawler-postgres/pom.xml")).lowercase()
        assertTrue(webPom.contains("trawler-core"))
        assertTrue(postgresPom.contains("trawler-core"))
        assertFalse(webPom.contains("trawler-postgres"), "web must not depend on PostgreSQL")
        assertFalse(postgresPom.contains("trawler-web"), "PostgreSQL must not depend on web")
    }

    private fun repositoryRoot(): Path {
        var current = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize()
        while (current.parent != null) {
            val pom = current.resolve("pom.xml")
            if (Files.exists(pom) && Files.exists(current.resolve("trawler-core/pom.xml")) &&
                Files.exists(current.resolve("trawler-web/pom.xml")) &&
                Files.exists(current.resolve("trawler-postgres/pom.xml"))
            ) {
                return current
            }
            current = current.parent
        }
        error("Unable to locate Trawler repository root from ${System.getProperty("user.dir")}")
    }
}
