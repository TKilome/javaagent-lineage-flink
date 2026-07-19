package io.github.tkilome.flink.lineage.processor;

import io.github.tkilome.flink.lineage.context.ParserContext;
import io.github.tkilome.flink.lineage.context.LineageNode;
import io.github.tkilome.flink.lineage.api.factory.LineageParserFactory;
import io.github.tkilome.flink.lineage.api.parser.LineageParseResult;
import io.github.tkilome.flink.lineage.api.parser.LineageParser;
import io.github.tkilome.flink.lineage.model.LineageDataset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Invokes all registered parsers for each lineage node.
 *
 * <p>A node may be ignored by all parsers, handled by one parser, or contribute datasets through
 * multiple parsers. Source and sink are dataset directions, not parser ownership rules.
 */
public final class LineageParserRegistry {

    private final List<LineageParserFactory> factories;

    /**
     * Creates a parser registry.
     *
     * @param factories parser factories; {@code null} is treated as empty
     */
    public LineageParserRegistry(List<LineageParserFactory> factories) {
        this.factories =
                Collections.unmodifiableList(
                        new ArrayList<>(factories == null ? Collections.emptyList() : factories));
    }

    /**
     * Parses datasets from one node by invoking every parser.
     *
     * @param node collected runtime node
     * @return parsed datasets, or empty list when no parser extracts complete datasets
     */
    public List<LineageDataset> parse(LineageNode node) {
        return parse(Collections.singletonList(node));
    }

    /**
     * Parses datasets from all nodes by invoking every parser with a job-level parser context.
     *
     * @param nodes collected runtime nodes
     * @return parsed datasets, or empty list when no parser extracts complete datasets
     */
    public List<LineageDataset> parse(List<? extends LineageNode> nodes) {
        List<LineageNode> allNodes =
                Collections.unmodifiableList(
                        new ArrayList<>(nodes == null ? Collections.emptyList() : nodes));
        ParserContext context = new DefaultParserContext(allNodes);
        List<LineageParser> parsers = new ArrayList<>();
        for (LineageParserFactory factory : factories) {
            parsers.add(factory.create(context));
        }

        List<LineageDataset> result = new ArrayList<>();
        for (LineageNode node : allNodes) {
            for (LineageParser parser : parsers) {
                LineageParseResult parseResult = parser.parse(node);
                if (parseResult != null && parseResult.isMatched()) {
                    result.addAll(parseResult.getDatasets());
                }
            }
        }
        return result;
    }

    private static final class DefaultParserContext implements ParserContext {

        private final List<LineageNode> nodes;

        private DefaultParserContext(List<LineageNode> nodes) {
            this.nodes = nodes;
        }

        @Override
        public Collection<LineageNode> nodes() {
            return nodes;
        }
    }
}
