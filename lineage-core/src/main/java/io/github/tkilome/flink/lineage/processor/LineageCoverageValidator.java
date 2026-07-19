package io.github.tkilome.flink.lineage.processor;

import io.github.tkilome.flink.lineage.exception.LineageValidationException;
import io.github.tkilome.flink.lineage.model.LineageDataset;
import io.github.tkilome.flink.lineage.model.LineageDirection;

import java.util.List;

/**
 * Validates that parsed lineage contains the minimum required job-level coverage.
 *
 * <p>Phase 1 only cares about job sources and sinks, so at least one source and one sink must be
 * present before reporting.
 */
final class LineageCoverageValidator {

    private LineageCoverageValidator() {}

    /**
     * Validates parsed datasets.
     *
     * @param datasets merged datasets
     */
    static void validate(List<LineageDataset> datasets) {
        boolean hasSource = false;
        boolean hasSink = false;
        for (LineageDataset dataset : datasets) {
            if (dataset.getDirection() == LineageDirection.SOURCE) {
                hasSource = true;
            } else if (dataset.getDirection() == LineageDirection.SINK) {
                hasSink = true;
            }
        }
        if (!hasSource) {
            throw new LineageValidationException("Lineage parse result must contain at least one source");
        }
        if (!hasSink) {
            throw new LineageValidationException("Lineage parse result must contain at least one sink");
        }
    }
}
