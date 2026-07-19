package io.github.tkilome.flink.lineage.discovery;

import io.github.tkilome.flink.lineage.api.instrumentation.LineageInstrumentation;
import io.github.tkilome.flink.lineage.context.InstrumentationContext;
import io.github.tkilome.flink.lineage.context.ReporterContext;
import io.github.tkilome.flink.lineage.api.factory.LineageInstrumentationFactory;
import io.github.tkilome.flink.lineage.api.factory.LineageReporterFactory;
import io.github.tkilome.flink.lineage.api.reporter.LineageReporter;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FactoryDiscoveryTest {

    @Test
    void discoversFactoriesFromTheProvidedRuntimeClassLoader() {
        ClassLoader runtimeClassLoader = Thread.currentThread().getContextClassLoader();

        List<?> factories = FactoryDiscovery.discover(runtimeClassLoader);

        assertEquals(2, factories.size());
    }

    public static final class TestInstrumentationFactory implements LineageInstrumentationFactory {
        public static int installCount;

        @Override
        public String factoryName() {
            return "test-engine";
        }

        @Override
        public LineageInstrumentation create(InstrumentationContext context) {
            return instrumentation -> installCount++;
        }
    }

    public static final class TestReporterFactory implements LineageReporterFactory {
        @Override
        public String factoryName() {
            return "test-reporter";
        }

        @Override
        public LineageReporter create(ReporterContext context) {
            return event -> {};
        }
    }
}
