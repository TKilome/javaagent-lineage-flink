package io.github.tkilome.flink.lineage.reporter.logging;

import io.github.tkilome.flink.lineage.model.LineageDataset;
import io.github.tkilome.flink.lineage.model.LineageDirection;
import io.github.tkilome.flink.lineage.model.LineageEvent;
import io.github.tkilome.flink.lineage.model.EngineType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LoggingLineageReporterTest {

    @Test
    void writesSingleLineJsonEvent() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        LoggingLineageReporter reporter = new LoggingLineageReporter(new PrintStream(output));

        reporter.report(
                new LineageEvent(
                        EngineType.FLINK,
                        "job-1",
                        "kafka-job",
                        123L,
                        Collections.singletonList(dataset(LineageDirection.SOURCE, "orders")),
                        Arrays.asList(dataset(LineageDirection.SINK, "orders-out"))));

        String json = new String(output.toByteArray());
        assertTrue(json.contains("\"jobId\":\"job-1\""));
        assertTrue(json.contains("\"engineType\":\"flink\""));
        assertTrue(json.contains("\"jobName\":\"kafka-job\""));
        assertTrue(json.contains("\"sources\""));
        assertTrue(json.contains("\"sinks\""));
        assertTrue(json.endsWith(System.lineSeparator()));
    }

    private static LineageDataset dataset(LineageDirection direction, String name) {
        return new LineageDataset(direction, "kafka", null, name, Collections.emptyMap());
    }
}
