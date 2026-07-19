package io.github.tkilome.flink.lineage.instrumentation;

import io.github.tkilome.flink.lineage.context.LineageNode;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a Flink 1.20 {@link StreamGraph} into version-neutral lineage nodes.
 */
public final class FlinkStreamGraphAccessor {

    private FlinkStreamGraphAccessor() {}

    /**
     * Wraps every {@link StreamNode} in the graph as a {@link LineageNode}.
     *
     * @param streamGraph Flink stream graph
     * @return lineage node wrappers
     */
    public static List<LineageNode> toLineageNodes(StreamGraph streamGraph) {
        List<LineageNode> result = new ArrayList<>();
        for (StreamNode node : streamGraph.getStreamNodes()) {
            result.add(new FlinkStreamNode(node));
        }
        return result;
    }
}
