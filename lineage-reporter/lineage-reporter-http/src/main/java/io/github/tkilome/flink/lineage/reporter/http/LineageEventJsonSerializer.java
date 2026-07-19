package io.github.tkilome.flink.lineage.reporter.http;

import io.github.tkilome.flink.lineage.model.LineageDataset;
import io.github.tkilome.flink.lineage.model.LineageEvent;

import java.util.List;
import java.util.Map;

/** Serializes {@link LineageEvent} to the JSON payload used by the HTTP reporter. */
final class LineageEventJsonSerializer {

    private LineageEventJsonSerializer() {}

    static String toJson(LineageEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
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
