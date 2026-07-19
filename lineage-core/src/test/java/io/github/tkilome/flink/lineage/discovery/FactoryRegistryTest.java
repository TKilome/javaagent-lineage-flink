package io.github.tkilome.flink.lineage.discovery;

import io.github.tkilome.flink.lineage.api.instrumentation.LineageInstrumentation;
import io.github.tkilome.flink.lineage.context.InstrumentationContext;
import io.github.tkilome.flink.lineage.context.ReporterContext;
import io.github.tkilome.flink.lineage.exception.LineageAgentInitializationException;
import io.github.tkilome.flink.lineage.api.factory.LineageInstrumentationFactory;
import io.github.tkilome.flink.lineage.api.factory.LineageFactory;
import io.github.tkilome.flink.lineage.api.factory.LineageReporterFactory;
import io.github.tkilome.flink.lineage.api.reporter.LineageReporter;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FactoryRegistryTest {

    @Test
    void acceptsExactlyOneInstrumentationAndAtLeastOneReporter() {
        FactoryRegistry registry =
                FactoryRegistry.create(
                        Arrays.asList(engine("flink-1.19"), reporter("logging")));

        assertEquals("flink-1.19", registry.getInstrumentationFactory().factoryName());
        assertEquals(1, registry.getReporterFactories().size());
    }

    @Test
    void rejectsMissingInstrumentation() {
        assertThrows(
                LineageAgentInitializationException.class,
                () -> FactoryRegistry.create(Collections.singletonList(reporter("logging"))));
    }

    @Test
    void rejectsMultipleInstrumentations() {
        assertThrows(
                LineageAgentInitializationException.class,
                () ->
                        FactoryRegistry.create(
                                Arrays.asList(
                                        engine("flink-1.19"),
                                        engine("flink-1.20"),
                                        reporter("logging"))));
    }

    @Test
    void acceptsDuplicateFactoryNamesBecauseTheyAreOnlyDiagnosticLabels() {
        FactoryRegistry registry =
                FactoryRegistry.create(Arrays.asList(engine("same"), reporter("same")));

        assertEquals("same", registry.getInstrumentationFactory().factoryName());
        assertEquals("same", registry.getReporterFactories().get(0).factoryName());
    }

    private static LineageFactory engine(final String identifier) {
        return new LineageInstrumentationFactory() {
            @Override
            public String factoryName() {
                return identifier;
            }

            @Override
            public LineageInstrumentation create(InstrumentationContext context) {
                return instrumentation -> {};
            }
        };
    }

    private static LineageFactory reporter(final String identifier) {
        return new LineageReporterFactory() {
            @Override
            public String factoryName() {
                return identifier;
            }

            @Override
            public LineageReporter create(ReporterContext context) {
                return event -> {};
            }
        };
    }
}
