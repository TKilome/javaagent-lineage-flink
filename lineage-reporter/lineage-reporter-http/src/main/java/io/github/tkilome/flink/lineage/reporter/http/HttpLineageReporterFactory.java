package io.github.tkilome.flink.lineage.reporter.http;

import io.github.tkilome.flink.lineage.api.factory.LineageReporterFactory;
import io.github.tkilome.flink.lineage.api.reporter.LineageReporter;
import io.github.tkilome.flink.lineage.context.ReporterContext;

/** ServiceLoader factory for the HTTP lineage reporter. */
public final class HttpLineageReporterFactory implements LineageReporterFactory {

    @Override
    public String factoryName() {
        return "http-reporter";
    }

    @Override
    public LineageReporter create(ReporterContext context) {
        return new HttpLineageReporter(HttpReporterConfig.fromSystem());
    }
}
