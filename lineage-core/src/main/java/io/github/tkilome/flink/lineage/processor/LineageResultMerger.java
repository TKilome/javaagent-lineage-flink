package io.github.tkilome.flink.lineage.processor;

import io.github.tkilome.flink.lineage.model.LineageDataset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deduplicates parser results while preserving first-seen order.
 *
 * <p>Parsers may discover the same dataset from multiple related nodes, such as writer and committer
 * nodes. Deduplication uses {@link LineageDataset#identityKey()}.
 */
final class LineageResultMerger {

    private LineageResultMerger() {}

    /**
     * Merges duplicate datasets.
     *
     * @param datasets raw parser output
     * @return deduplicated datasets
     */
    static List<LineageDataset> merge(List<LineageDataset> datasets) {
        Map<String, LineageDataset> byIdentity = new LinkedHashMap<>();
        for (LineageDataset dataset : datasets == null ? Collections.<LineageDataset>emptyList() : datasets) {
            if (dataset != null) {
                byIdentity.putIfAbsent(dataset.identityKey(), dataset);
            }
        }
        return new ArrayList<>(byIdentity.values());
    }
}
