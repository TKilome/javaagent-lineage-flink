package io.github.tkilome.flink.lineage.reporter.logging;

import io.github.tkilome.flink.lineage.context.ReporterContext;
import io.github.tkilome.flink.lineage.api.reporter.LineageReporter;
import io.github.tkilome.flink.lineage.api.factory.LineageReporterFactory;

/**
 * ServiceLoader factory for the phase-1 logging reporter.
 */
public final class LoggingLineageReporterFactory implements LineageReporterFactory {

    @Override
    public String factoryName() {
        return "logging-reporter";
    }

    @Override
    public LineageReporter create(ReporterContext context) {
        return new LoggingLineageReporter(System.out);
    }
}
