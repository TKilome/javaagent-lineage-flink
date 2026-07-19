package io.github.tkilome.flink.lineage.reporter.http;

import io.github.tkilome.flink.lineage.exception.LineageReportingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HttpReporterConfigTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty("lineage.reporter.http.url");
        System.clearProperty("lineage.reporter.http.token");
        System.clearProperty("lineage.reporter.http.connectTimeoutMs");
        System.clearProperty("lineage.reporter.http.readTimeoutMs");
        System.clearProperty("lineage.reporter.http.retries");
    }

    @Test
    void readsSystemPropertiesWithDefaults() {
        System.setProperty("lineage.reporter.http.url", "http://localhost:8080/lineage");
        System.setProperty("lineage.reporter.http.token", "secret");

        HttpReporterConfig config = HttpReporterConfig.fromSystem();

        assertEquals("http://localhost:8080/lineage", config.getUrl().toString());
        assertEquals("secret", config.getToken());
        assertEquals(3000, config.getConnectTimeoutMs());
        assertEquals(5000, config.getReadTimeoutMs());
        assertEquals(0, config.getRetries());
    }

    @Test
    void readsTimeoutAndRetryProperties() {
        System.setProperty("lineage.reporter.http.url", "http://localhost:8080/lineage");
        System.setProperty("lineage.reporter.http.connectTimeoutMs", "100");
        System.setProperty("lineage.reporter.http.readTimeoutMs", "200");
        System.setProperty("lineage.reporter.http.retries", "2");

        HttpReporterConfig config = HttpReporterConfig.fromSystem();

        assertEquals(100, config.getConnectTimeoutMs());
        assertEquals(200, config.getReadTimeoutMs());
        assertEquals(2, config.getRetries());
    }

    @Test
    void failsWhenUrlIsMissing() {
        assertThrows(LineageReportingException.class, HttpReporterConfig::fromSystem);
    }

    @Test
    void failsWhenTimeoutIsInvalid() {
        System.setProperty("lineage.reporter.http.url", "http://localhost:8080/lineage");
        System.setProperty("lineage.reporter.http.connectTimeoutMs", "0");

        assertThrows(LineageReportingException.class, HttpReporterConfig::fromSystem);
    }

    @Test
    void failsWhenRetriesIsTooLarge() {
        System.setProperty("lineage.reporter.http.url", "http://localhost:8080/lineage");
        System.setProperty("lineage.reporter.http.retries", "4");

        assertThrows(LineageReportingException.class, HttpReporterConfig::fromSystem);
    }
}
