package io.github.tkilome.flink.lineage.context;

import java.util.Collection;

/**
 * Context passed to parser factories.
 *
 * <p>The context is created once for a job and exposes all collected lineage nodes. Most parsers can
 * ignore it and parse a single node, while graph-aware parsers can scan sibling nodes before parsing
 * a matched node.
 */
public interface ParserContext {

    /**
     * Returns all collected nodes for the current job.
     *
     * @return immutable or read-only collection of job nodes
     */
    Collection<LineageNode> nodes();
}
