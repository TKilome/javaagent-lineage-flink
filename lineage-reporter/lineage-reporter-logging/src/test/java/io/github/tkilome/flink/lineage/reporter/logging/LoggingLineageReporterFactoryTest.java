package io.github.tkilome.flink.lineage.reporter.logging;

import io.github.tkilome.flink.lineage.api.factory.LineageComponentType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class LoggingLineageReporterFactoryTest {

    @Test
    void createsLoggingReporterFactory() {
        LoggingLineageReporterFactory factory = new LoggingLineageReporterFactory();

        assertEquals("logging-reporter", factory.factoryName());
        assertEquals(LineageComponentType.REPORTER, factory.componentType());
        assertNotNull(factory.create(null));
    }
}
