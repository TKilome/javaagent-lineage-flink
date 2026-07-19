package io.github.tkilome.flink.lineage.runtime;

import io.github.tkilome.flink.lineage.context.InstrumentationContext;
import io.github.tkilome.flink.lineage.context.LineageNode;
import io.github.tkilome.flink.lineage.discovery.FactoryRegistry;
import io.github.tkilome.flink.lineage.exception.LineageAgentInitializationException;
import io.github.tkilome.flink.lineage.model.LineageEvent;
import io.github.tkilome.flink.lineage.processor.LineageParserRegistry;
import io.github.tkilome.flink.lineage.processor.LineageProcessor;
import io.github.tkilome.flink.lineage.processor.ReporterRegistry;
import io.github.tkilome.flink.lineage.api.factory.LineageFactory;
import io.github.tkilome.flink.lineage.api.instrumentation.LineageInstrumentation;

import java.lang.instrument.Instrumentation;
import java.util.List;

/**
 * Global runtime bridge between bytecode instrumentation and the core lineage processing pipeline.
 *
 * <p>Instrumentation code runs inside Flink classes and should call {@link #processLineage(String,
 * String, List)} after it has collected job metadata and graph nodes. The runtime must be
 * initialized once from {@link io.github.tkilome.flink.lineage.agent.LineageAgent} before any lineage is
 * processed.
 */
public final class LineageRuntime {

    private static volatile LineageProcessor processor;

    private LineageRuntime() {}

    /**
     * Initializes the runtime and installs the selected instrumentation.
     *
     * <p>The factory list is expected to come from {@link java.util.ServiceLoader}. Exactly one
     * instrumentation factory and at least one reporter factory must be present; otherwise this
     * method throws and blocks job submission.
     *
     * @param factories discovered lineage factories
     * @param instrumentation JVM instrumentation handle
     */
    public static synchronized void initialize(
            List<? extends LineageFactory> factories, Instrumentation instrumentation) {
        FactoryRegistry registry = FactoryRegistry.create(factories);
        processor =
                new LineageProcessor(
                        new LineageParserRegistry(registry.getParserFactories()),
                        new ReporterRegistry(registry.getReporterFactories()));

        LineageInstrumentation lineageInstrumentation =
                registry.getInstrumentationFactory().create(new InstrumentationContext() {});
        lineageInstrumentation.installInstrumentation(instrumentation);
    }

    /**
     * Processes lineage for a generated job graph.
     *
     * <p>This method parses source/sink datasets from collected nodes, validates coverage, reports
     * the result, and returns the produced event. Any parsing, validation, or reporting exception is
     * propagated to fail fast during job submission.
     *
     * @param jobId Flink job id
     * @param jobName Flink job name, may be {@code null}
     * @param nodes version-neutral nodes collected by instrumentation
     * @return completed lineage event
     */
    public static LineageEvent processLineage(
            String jobId, String jobName, List<? extends LineageNode> nodes) {
        LineageProcessor current = processor;
        if (current == null) {
            throw new LineageAgentInitializationException("Lineage runtime has not been initialized");
        }
        return current.process(jobId, jobName, nodes);
    }

    /**
     * Clears runtime state for unit tests.
     */
    public static synchronized void resetForTest() {
        processor = null;
    }
}
