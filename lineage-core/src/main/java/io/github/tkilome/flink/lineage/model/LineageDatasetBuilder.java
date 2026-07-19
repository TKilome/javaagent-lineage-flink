package io.github.tkilome.flink.lineage.model;

import java.util.LinkedHashMap;
import java.util.Map;

/** Builder for the stable lineage dataset output contract. */
public final class LineageDatasetBuilder {

    private final LineageDirection direction;
    private final String connector;
    private final String name;
    private final Map<String, String> properties = new LinkedHashMap<>();
    private String namespace;

    private LineageDatasetBuilder(LineageDirection direction, String connector, String name) {
        this.direction = direction;
        this.connector = connector;
        this.name = name;
    }

    public static LineageDatasetBuilder dataset(
            LineageDirection direction, String connector, String name) {
        return new LineageDatasetBuilder(direction, connector, name);
    }

    public LineageDatasetBuilder namespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public LineageDatasetBuilder property(String key, String value) {
        properties.put(requireText(key, "property key"), value);
        return this;
    }

    public LineageDatasetBuilder optionalProperty(String key, String value) {
        if (trimToNull(value) != null) {
            property(key, value.trim());
        }
        return this;
    }

    public LineageDatasetBuilder properties(Map<String, String> values) {
        if (values == null) {
            return this;
        }
        for (Map.Entry<String, String> entry : values.entrySet()) {
            optionalProperty(entry.getKey(), entry.getValue());
        }
        return this;
    }

    public LineageDataset build() {
        return new LineageDataset(direction, connector, namespace, name, properties);
    }

    private static String requireText(String value, String name) {
        String normalized = trimToNull(value);
        if (normalized == null) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return normalized;
    }

    private static String trimToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }
}
