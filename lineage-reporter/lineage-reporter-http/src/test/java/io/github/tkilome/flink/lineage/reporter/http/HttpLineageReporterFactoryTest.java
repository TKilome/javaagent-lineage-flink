package io.github.tkilome.flink.lineage.reporter.http;

import io.github.tkilome.flink.lineage.api.factory.LineageComponentType;
import io.github.tkilome.flink.lineage.api.reporter.LineageReporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class HttpLineageReporterFactoryTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty("lineage.reporter.http.url");
    }

    @Test
    void createsHttpReporter() {
        System.setProperty("lineage.reporter.http.url", "http://localhost:8080/lineage");
        HttpLineageReporterFactory factory = new HttpLineageReporterFactory();

        LineageReporter reporter = factory.create(null);

        assertEquals(LineageComponentType.REPORTER, factory.componentType());
        assertEquals("http-reporter", factory.factoryName());
        assertNotNull(reporter);
    }
}
