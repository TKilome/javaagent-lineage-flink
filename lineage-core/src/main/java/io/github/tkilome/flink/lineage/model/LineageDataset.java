package io.github.tkilome.flink.lineage.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A source or sink dataset discovered from a job graph.
 *
 * <p>The dataset identity is defined by direction, connector, namespace, and name. Properties are
 * descriptive metadata and do not participate in deduplication.
 */
public final class LineageDataset {

    private static final char KEY_SEPARATOR = '\u0000';

    private final LineageDirection direction;
    private final String connector;
    private final String namespace;
    private final String name;
    private final Map<String, String> properties;

    /**
     * Creates a lineage dataset.
     *
     * @param direction source or sink
     * @param connector connector type, for example {@code kafka}
     * @param namespace optional namespace, for example cluster, catalog, or database
     * @param name dataset name, for example Kafka topic or table name
     * @param properties additional metadata for reporting
     */
    public LineageDataset(
            LineageDirection direction,
            String connector,
            String namespace,
            String name,
            Map<String, String> properties) {
        this.direction = requireNonNull(direction, "direction");
        this.connector = requireText(connector, "connector");
        this.namespace = normalizeNullable(namespace);
        this.name = requireText(name, "name");
        this.properties =
                Collections.unmodifiableMap(
                        new LinkedHashMap<>(
                                properties == null ? Collections.emptyMap() : properties));
    }

    /**
     * Returns the dataset direction.
     *
     * @return source or sink
     */
    public LineageDirection getDirection() {
        return direction;
    }

    /**
     * Returns the connector type.
     *
     * @return connector name
     */
    public String getConnector() {
        return connector;
    }

    /**
     * Returns the optional namespace.
     *
     * @return namespace, or {@code null}
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Returns the dataset name.
     *
     * @return dataset name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns immutable metadata properties.
     *
     * @return dataset properties
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Returns the stable key used for deduplication.
     *
     * @return identity key
     */
    public String identityKey() {
        return direction.name()
                + KEY_SEPARATOR
                + connector
                + KEY_SEPARATOR
                + (namespace == null ? "" : namespace)
                + KEY_SEPARATOR
                + name;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static String normalizeNullable(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof LineageDataset)) {
            return false;
        }
        LineageDataset that = (LineageDataset) other;
        return direction == that.direction
                && connector.equals(that.connector)
                && Objects.equals(namespace, that.namespace)
                && name.equals(that.name)
                && properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(direction, connector, namespace, name, properties);
    }
}
