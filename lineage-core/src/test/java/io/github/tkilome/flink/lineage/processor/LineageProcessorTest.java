package io.github.tkilome.flink.lineage.processor;

import io.github.tkilome.flink.lineage.context.LineageNode;
import io.github.tkilome.flink.lineage.context.ParserContext;
import io.github.tkilome.flink.lineage.context.ReporterContext;
import io.github.tkilome.flink.lineage.exception.LineageReportingException;
import io.github.tkilome.flink.lineage.exception.LineageValidationException;
import io.github.tkilome.flink.lineage.model.EngineType;
import io.github.tkilome.flink.lineage.model.LineageDataset;
import io.github.tkilome.flink.lineage.model.LineageDirection;
import io.github.tkilome.flink.lineage.model.LineageEvent;
import io.github.tkilome.flink.lineage.api.parser.LineageParser;
import io.github.tkilome.flink.lineage.api.parser.LineageParseResult;
import io.github.tkilome.flink.lineage.api.factory.LineageParserFactory;
import io.github.tkilome.flink.lineage.api.reporter.LineageReporter;
import io.github.tkilome.flink.lineage.api.factory.LineageReporterFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LineageProcessorTest {

    @Test
    void parsesDeduplicatesValidatesAndReportsLineageEvent() {
        CapturingReporterFactory reporterFactory = new CapturingReporterFactory();
        LineageProcessor processor =
                new LineageProcessor(
                        new LineageParserRegistry(
                                Collections.singletonList(
                                        parserFactory(
                                                Arrays.asList(
                                                        dataset(LineageDirection.SOURCE, "orders"),
                                                        dataset(LineageDirection.SOURCE, "orders"),
                                                        dataset(LineageDirection.SINK, "orders-out"))))),
                        new ReporterRegistry(Collections.singletonList(reporterFactory)));

        LineageEvent event =
                processor.process(
                        "job-id-1",
                        "kafka-to-kafka",
                        Collections.singletonList(LineageNode.empty("node-1")));

        assertEquals("job-id-1", event.getJobId());
        assertEquals(EngineType.FLINK, event.getEngineType());
        assertEquals("kafka-to-kafka", event.getJobName());
        assertEquals(1, event.getSources().size());
        assertEquals("orders", event.getSources().get(0).getName());
        assertEquals(1, event.getSinks().size());
        assertEquals("orders-out", event.getSinks().get(0).getName());
        assertEquals(Collections.singletonList(event), reporterFactory.events);
    }

    @Test
    void rejectsLineageWithoutSource() {
        LineageProcessor processor =
                processorWithDatasets(Collections.singletonList(dataset(LineageDirection.SINK, "out")));

        LineageValidationException error =
                assertThrows(
                        LineageValidationException.class,
                        () ->
                                processor.process(
                                        "job-id-1",
                                        "job",
                                        Collections.singletonList(LineageNode.empty("node-1"))));

        assertTrue(error.getMessage().contains("source"));
    }

    @Test
    void rejectsLineageWithoutSink() {
        LineageProcessor processor =
                processorWithDatasets(Collections.singletonList(dataset(LineageDirection.SOURCE, "in")));

        LineageValidationException error =
                assertThrows(
                        LineageValidationException.class,
                        () ->
                                processor.process(
                                        "job-id-1",
                                        "job",
                                        Collections.singletonList(LineageNode.empty("node-1"))));

        assertTrue(error.getMessage().contains("sink"));
    }

    @Test
    void propagatesReporterFailures() {
        LineageProcessor processor =
                new LineageProcessor(
                        new LineageParserRegistry(
                                Collections.singletonList(
                                        parserFactory(
                                                Arrays.asList(
                                                        dataset(LineageDirection.SOURCE, "in"),
                                                        dataset(LineageDirection.SINK, "out"))))),
                        new ReporterRegistry(
                                Collections.singletonList(
                                        new LineageReporterFactory() {
                                            @Override
                                            public String factoryName() {
                                                return "failing";
                                            }

                                            @Override
                                            public LineageReporter create(ReporterContext context) {
                                                return event -> {
                                                    throw new IllegalStateException("boom");
                                                };
                                            }
                                        })));

        LineageReportingException error =
                assertThrows(
                        LineageReportingException.class,
                        () ->
                                processor.process(
                                        "job-id-1",
                                        "job",
                                        Collections.singletonList(LineageNode.empty("node-1"))));

        assertTrue(error.getMessage().contains("failing"));
    }

    private static LineageProcessor processorWithDatasets(List<LineageDataset> datasets) {
        return new LineageProcessor(
                new LineageParserRegistry(Collections.singletonList(parserFactory(datasets))),
                new ReporterRegistry(Collections.singletonList(new CapturingReporterFactory())));
    }

    private static LineageParserFactory parserFactory(final List<LineageDataset> datasets) {
        return new LineageParserFactory() {
            @Override
            public String factoryName() {
                return "test-parser";
            }

            @Override
            public LineageParser create(ParserContext context) {
                return node -> LineageParseResult.matched(datasets);
            }
        };
    }

    private static LineageDataset dataset(LineageDirection direction, String name) {
        return new LineageDataset(direction, "kafka", null, name, Collections.emptyMap());
    }

    private static final class CapturingReporterFactory implements LineageReporterFactory {
        private final List<LineageEvent> events = new ArrayList<>();

        @Override
        public String factoryName() {
            return "capturing";
        }

        @Override
        public LineageReporter create(ReporterContext context) {
            return events::add;
        }
    }
}
