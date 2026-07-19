package io.github.tkilome.flink.lineage.api.reporter;

import io.github.tkilome.flink.lineage.model.LineageEvent;

/**
 * Reports a completed lineage event.
 *
 * <p>Reporter implementations should throw an exception when reporting fails. The core runtime does
 * not swallow reporter failures.
 */
@FunctionalInterface
public interface LineageReporter {
    /**
     * Reports the lineage event.
     *
     * @param event completed lineage event containing job metadata, sources, and sinks
     */
    void report(LineageEvent event);
}
