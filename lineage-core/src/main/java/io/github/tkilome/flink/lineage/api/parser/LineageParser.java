package io.github.tkilome.flink.lineage.api.parser;

import io.github.tkilome.flink.lineage.context.LineageNode;

/**
 * Parses lineage endpoints from a collected lineage node.
 *
 * <p>A parser is invoked for every node. It should return {@link LineageParseResult#notMatched()}
 * when it does not recognize the node, {@link LineageParseResult#matched(java.util.List)} with an
 * empty list when it recognizes an endpoint but cannot form a complete dataset from that endpoint,
 * and a non-empty matched result when it extracts source or sink datasets.
 */
@FunctionalInterface
public interface LineageParser {
    /**
     * Parses lineage endpoints from the given node.
     *
     * @param node collected runtime node
     * @return parse result for the node
     */
    LineageParseResult parse(LineageNode node);
}
