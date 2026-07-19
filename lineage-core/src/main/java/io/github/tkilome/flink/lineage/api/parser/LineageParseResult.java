package io.github.tkilome.flink.lineage.api.parser;

import io.github.tkilome.flink.lineage.model.LineageDataset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Result returned by a lineage parser for one node.
 *
 * <p>The result distinguishes three states:
 *
 * <ul>
 *   <li>not matched: this parser did not find its endpoint in the node;
 *   <li>matched with empty datasets: this parser found an endpoint, but it did not contain enough
 *       metadata to form a complete dataset;
 *   <li>matched with datasets: this parser found one or more complete source/sink datasets.
 * </ul>
 */
public final class LineageParseResult {

    private static final LineageParseResult NOT_MATCHED =
            new LineageParseResult(false, Collections.emptyList());

    private final boolean matched;
    private final List<LineageDataset> datasets;

    private LineageParseResult(boolean matched, List<LineageDataset> datasets) {
        this.matched = matched;
        this.datasets =
                Collections.unmodifiableList(
                        new ArrayList<>(datasets == null ? Collections.emptyList() : datasets));
    }

    /**
     * Creates a result for nodes not recognized by the parser.
     *
     * @return not-matched result
     */
    public static LineageParseResult notMatched() {
        return NOT_MATCHED;
    }

    /**
     * Creates a result for nodes recognized by the parser.
     *
     * @param datasets parsed datasets; empty means matched but no complete endpoint was extracted
     * @return matched result
     */
    public static LineageParseResult matched(List<LineageDataset> datasets) {
        return new LineageParseResult(true, datasets);
    }

    /**
     * Returns whether this parser recognized at least one endpoint in the node.
     *
     * @return {@code true} when matched
     */
    public boolean isMatched() {
        return matched;
    }

    /**
     * Returns parsed datasets.
     *
     * @return immutable dataset list
     */
    public List<LineageDataset> getDatasets() {
        return datasets;
    }
}
