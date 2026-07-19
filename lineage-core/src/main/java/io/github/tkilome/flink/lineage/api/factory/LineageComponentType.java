package io.github.tkilome.flink.lineage.api.factory;

/**
 * Type of a lineage extension component discovered by the agent.
 */
public enum LineageComponentType {
    /** Component that installs bytecode instrumentation. */
    INSTRUMENTATION,

    /** Component that parses lineage datasets from collected runtime nodes. */
    PARSER,

    /** Component that reports the final lineage event to an external destination. */
    REPORTER
}
