package io.github.tkilome.flink.lineage.reporter.logging;

import io.github.tkilome.flink.lineage.exception.LineageReportingException;
import io.github.tkilome.flink.lineage.model.LineageDataset;
import io.github.tkilome.flink.lineage.model.LineageEvent;
import io.github.tkilome.flink.lineage.api.reporter.LineageReporter;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

/**
 * Reporter that writes one JSON lineage event per line to a {@link PrintStream}.
 *
 * <p>This reporter intentionally avoids a logging framework binding so it can run inside arbitrary
 * Flink clusters without changing their logging setup.
 */
final class LoggingLineageReporter implements LineageReporter {

    private final PrintStream output;

    /**
     * Creates a logging reporter.
     *
     * @param output destination stream
     */
    LoggingLineageReporter(PrintStream output) {
        if (output == null) {
            throw new IllegalArgumentException("output must not be null");
        }
        this.output = output;
    }

    /**
     * Serializes and writes the event.
     *
     * @param event completed lineage event
     */
    @Override
    public void report(LineageEvent event) {
        try {
            output.println(toJson(event));
            if (output.checkError()) {
                throw new LineageReportingException("Failed to write lineage event to output", null);
            }
        } catch (LineageReportingException error) {
            throw error;
        } catch (RuntimeException error) {
            throw new LineageReportingException("Failed to serialize lineage event", error);
        }
    }

    private static String toJson(LineageEvent event) {
        return "{"
                + "\"engineType\":\""
                + event.getEngineType().value()
                + "\",\"jobId\":\""
                + escape(event.getJobId())
                + "\",\"jobName\":"
                + nullableString(event.getJobName())
                + ",\"timestamp\":"
                + event.getTimestamp()
                + ",\"sources\":"
                + datasets(event.getSources())
                + ",\"sinks\":"
                + datasets(event.getSinks())
                + "}";
    }

    private static String datasets(List<LineageDataset> datasets) {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < datasets.size(); i++) {
            if (i > 0) {
                builder.append(',');
            }
            LineageDataset dataset = datasets.get(i);
            builder.append('{')
                    .append("\"connector\":\"")
                    .append(escape(dataset.getConnector()))
                    .append("\",\"namespace\":")
                    .append(nullableString(dataset.getNamespace()))
                    .append(",\"name\":\"")
                    .append(escape(dataset.getName()))
                    .append("\",\"properties\":")
                    .append(properties(dataset.getProperties()))
                    .append('}');
        }
        return builder.append(']').toString();
    }

    private static String properties(Map<String, String> properties) {
        StringBuilder builder = new StringBuilder("{");
        int index = 0;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (index++ > 0) {
                builder.append(',');
            }
            builder.append('"')
                    .append(escape(entry.getKey()))
                    .append("\":")
                    .append(nullableString(entry.getValue()));
        }
        return builder.append('}').toString();
    }

    private static String nullableString(String value) {
        return value == null ? "null" : "\"" + escape(value) + "\"";
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
