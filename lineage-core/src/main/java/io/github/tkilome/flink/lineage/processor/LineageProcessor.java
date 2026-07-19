package io.github.tkilome.flink.lineage.processor;

import io.github.tkilome.flink.lineage.context.LineageNode;
import io.github.tkilome.flink.lineage.model.EngineType;
import io.github.tkilome.flink.lineage.model.LineageDataset;
import io.github.tkilome.flink.lineage.model.LineageDirection;
import io.github.tkilome.flink.lineage.model.LineageEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Coordinates the full lineage processing chain.
 *
 * <p>The processor parses datasets from all collected nodes, merges duplicate datasets, validates
 * that both source and sink are present, builds the final event, and reports it. Failures are
 * propagated to keep the agent fail-fast.
 */
public final class LineageProcessor {

    private final LineageParserRegistry parserRegistry;
    private final ReporterRegistry reporterRegistry;

    /**
     * Creates a processor from parser and reporter registries.
     *
     * @param parserRegistry parser registry
     * @param reporterRegistry reporter registry
     */
    public LineageProcessor(LineageParserRegistry parserRegistry, ReporterRegistry reporterRegistry) {
        if (parserRegistry == null) {
            throw new IllegalArgumentException("parserRegistry must not be null");
        }
        if (reporterRegistry == null) {
            throw new IllegalArgumentException("reporterRegistry must not be null");
        }
        this.parserRegistry = parserRegistry;
        this.reporterRegistry = reporterRegistry;
    }

    /**
     * Processes lineage for one Flink job.
     *
     * @param jobId Flink job id
     * @param jobName Flink job name, may be {@code null}
     * @param nodes collected graph nodes
     * @return final lineage event
     */
    public LineageEvent process(String jobId, String jobName, List<? extends LineageNode> nodes) {
        List<LineageDataset> datasets =
                parserRegistry.parse(
                        new ArrayList<>(
                                nodes == null
                                        ? Collections.<LineageNode>emptyList()
                                        : nodes));

        List<LineageDataset> mergedDatasets = LineageResultMerger.merge(datasets);
        LineageCoverageValidator.validate(mergedDatasets);

        LineageEvent event =
                new LineageEvent(
                        EngineType.FLINK,
                        jobId,
                        jobName,
                        System.currentTimeMillis(),
                        filterByDirection(mergedDatasets, LineageDirection.SOURCE),
                        filterByDirection(mergedDatasets, LineageDirection.SINK));
        reporterRegistry.report(event);
        return event;
    }

    private static List<LineageDataset> filterByDirection(
            List<LineageDataset> datasets, LineageDirection direction) {
        List<LineageDataset> result = new ArrayList<>();
        for (LineageDataset dataset : datasets) {
            if (dataset.getDirection() == direction) {
                result.add(dataset);
            }
        }
        return result;
    }
}
