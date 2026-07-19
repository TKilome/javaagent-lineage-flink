package io.github.tkilome.flink.lineage.model;

/**
 * Direction of a lineage dataset relative to the Flink job.
 */
public enum LineageDirection {
    /** Dataset read by the job. */
    SOURCE,

    /** Dataset written by the job. */
    SINK
}
