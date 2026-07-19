package io.github.tkilome.flink.lineage.model;

/** Compute engine that produced a lineage event. */
public enum EngineType {
    FLINK("flink");

    private final String value;

    EngineType(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}
