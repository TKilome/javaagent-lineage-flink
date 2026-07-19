package io.github.tkilome.flink.lineage.discovery;

import io.github.tkilome.flink.lineage.exception.LineageAgentInitializationException;
import io.github.tkilome.flink.lineage.api.factory.LineageComponentType;
import io.github.tkilome.flink.lineage.api.factory.LineageParserFactory;
import io.github.tkilome.flink.lineage.api.factory.LineageInstrumentationFactory;
import io.github.tkilome.flink.lineage.api.factory.LineageFactory;
import io.github.tkilome.flink.lineage.api.factory.LineageReporterFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Validated registry of discovered lineage factories.
 *
 * <p>The registry separates factories by component type. It does not perform version matching or
 * choose between multiple instrumentation implementations: the user must put exactly one matching
 * instrumentation jar on the classpath.
 */
public final class FactoryRegistry {

    private final LineageInstrumentationFactory instrumentationFactory;
    private final List<LineageParserFactory> parserFactories;
    private final List<LineageReporterFactory> reporterFactories;

    private FactoryRegistry(
            LineageInstrumentationFactory instrumentationFactory,
            List<LineageParserFactory> parserFactories,
            List<LineageReporterFactory> reporterFactories) {
        this.instrumentationFactory = instrumentationFactory;
        this.parserFactories = Collections.unmodifiableList(parserFactories);
        this.reporterFactories = Collections.unmodifiableList(reporterFactories);
    }

    /**
     * Builds a validated registry from discovered factories.
     *
     * @param factories discovered factories
     * @return validated factory registry
     */
    public static FactoryRegistry create(List<? extends LineageFactory> factories) {
        if (factories == null) {
            throw new LineageAgentInitializationException("factories must not be null");
        }

        List<LineageInstrumentationFactory> instrumentations = new ArrayList<>();
        List<LineageParserFactory> parsers = new ArrayList<>();
        List<LineageReporterFactory> reporters = new ArrayList<>();

        for (LineageFactory factory : factories) {
            if (factory == null) {
                throw new LineageAgentInitializationException("factory must not be null");
            }
            String factoryName = normalizeFactoryName(factory.factoryName());

            if (factory.componentType() == LineageComponentType.INSTRUMENTATION) {
                instrumentations.add((LineageInstrumentationFactory) factory);
            } else if (factory.componentType() == LineageComponentType.PARSER) {
                parsers.add((LineageParserFactory) factory);
            } else if (factory.componentType() == LineageComponentType.REPORTER) {
                reporters.add((LineageReporterFactory) factory);
            } else {
                throw new LineageAgentInitializationException(
                        "Unknown lineage component type for factory: " + factoryName);
            }
        }

        if (instrumentations.size() != 1) {
            throw new LineageAgentInitializationException(
                    "Expected exactly one instrumentation factory, but found: " + instrumentations.size());
        }
        if (reporters.isEmpty()) {
            throw new LineageAgentInitializationException(
                    "Expected at least one lineage reporter factory");
        }

        return new FactoryRegistry(instrumentations.get(0), parsers, reporters);
    }

    /**
     * Returns the single instrumentation factory selected by classpath composition.
     *
     * @return instrumentation factory
     */
    public LineageInstrumentationFactory getInstrumentationFactory() {
        return instrumentationFactory;
    }

    /**
     * Returns all parser factories.
     *
     * @return parser factories
     */
    public List<LineageParserFactory> getParserFactories() {
        return parserFactories;
    }

    /**
     * Returns all reporter factories.
     *
     * @return reporter factories
     */
    public List<LineageReporterFactory> getReporterFactories() {
        return reporterFactories;
    }

    private static String normalizeFactoryName(String factoryName) {
        if (factoryName == null || factoryName.trim().isEmpty()) {
            throw new LineageAgentInitializationException(
                    "Factory name must not be blank");
        }
        return factoryName.trim();
    }
}
