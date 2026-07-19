package io.github.tkilome.flink.lineage.runtime;

import io.github.tkilome.flink.lineage.context.InstrumentationContext;
import io.github.tkilome.flink.lineage.context.ReporterContext;
import io.github.tkilome.flink.lineage.exception.LineageAgentInitializationException;
import io.github.tkilome.flink.lineage.api.factory.LineageFactory;
import io.github.tkilome.flink.lineage.api.instrumentation.LineageInstrumentation;
import io.github.tkilome.flink.lineage.api.factory.LineageInstrumentationFactory;
import io.github.tkilome.flink.lineage.api.reporter.LineageReporter;
import io.github.tkilome.flink.lineage.api.factory.LineageReporterFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LineageRuntimeTest {

    @AfterEach
    void tearDown() {
        LineageRuntime.resetForTest();
    }

    @Test
    void rejectsProcessBeforeInitialization() {
        assertThrows(
                LineageAgentInitializationException.class,
                () -> LineageRuntime.processLineage("job-id", "job", Collections.emptyList()));
    }

    @Test
    void initializesRuntimeAndInstallsInstrumentation() {
        CountingInstrumentationFactory instrumentationFactory = new CountingInstrumentationFactory();

        LineageRuntime.initialize(Arrays.asList(instrumentationFactory, reporter("logging")), null);

        assertEquals(1, instrumentationFactory.createCount);
        assertEquals(1, instrumentationFactory.installCount);
    }

    private static LineageFactory reporter(final String name) {
        return new LineageReporterFactory() {
            @Override
            public String factoryName() {
                return name;
            }

            @Override
            public LineageReporter create(ReporterContext context) {
                return event -> {};
            }
        };
    }

    private static final class CountingInstrumentationFactory implements LineageInstrumentationFactory {
        private int createCount;
        private int installCount;

        @Override
        public String factoryName() {
            return "flink";
        }

        @Override
        public LineageInstrumentation create(InstrumentationContext context) {
            createCount++;
            return instrumentation -> installCount++;
        }
    }
}
