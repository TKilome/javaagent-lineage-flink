package io.github.tkilome.flink.lineage.api.factory;

import io.github.tkilome.flink.lineage.api.parser.LineageParser;
import io.github.tkilome.flink.lineage.api.parser.LineageParseResult;
import io.github.tkilome.flink.lineage.api.instrumentation.LineageInstrumentation;
import io.github.tkilome.flink.lineage.context.ParserContext;
import io.github.tkilome.flink.lineage.context.InstrumentationContext;
import io.github.tkilome.flink.lineage.context.ReporterContext;
import io.github.tkilome.flink.lineage.api.factory.LineageComponentType;
import io.github.tkilome.flink.lineage.api.factory.LineageParserFactory;
import io.github.tkilome.flink.lineage.api.factory.LineageInstrumentationFactory;
import io.github.tkilome.flink.lineage.api.factory.LineageReporterFactory;
import io.github.tkilome.flink.lineage.api.reporter.LineageReporter;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ApiContractsTest {

    @Test
    void specializedFactoriesExposeTheirLineageComponentTypes() {
        LineageInstrumentationFactory engineFactory =
                new LineageInstrumentationFactory() {
                    @Override
                    public String factoryName() {
                        return "engine";
                    }

                    @Override
                    public LineageInstrumentation create(InstrumentationContext context) {
                        return instrumentation -> {};
                    }
                };
        LineageParserFactory connectorFactory =
                new LineageParserFactory() {
                    @Override
                    public String factoryName() {
                        return "connector";
                    }

                    @Override
                    public LineageParser create(ParserContext context) {
                        return node -> LineageParseResult.matched(Collections.emptyList());
                    }
                };
        LineageReporterFactory reporterFactory =
                new LineageReporterFactory() {
                    @Override
                    public String factoryName() {
                        return "reporter";
                    }

                    @Override
                    public LineageReporter create(ReporterContext context) {
                        return event -> {};
                    }
                };

        assertEquals(LineageComponentType.INSTRUMENTATION, engineFactory.componentType());
        assertEquals(LineageComponentType.PARSER, connectorFactory.componentType());
        assertEquals(LineageComponentType.REPORTER, reporterFactory.componentType());
    }
}
