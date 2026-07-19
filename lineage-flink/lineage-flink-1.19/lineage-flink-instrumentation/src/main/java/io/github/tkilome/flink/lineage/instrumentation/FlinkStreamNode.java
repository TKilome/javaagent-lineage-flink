package io.github.tkilome.flink.lineage.instrumentation;

import io.github.tkilome.flink.lineage.context.LineageNode;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Flink 1.19 {@link StreamNode} wrapper exposed through the core {@link LineageNode} contract.
 *
 * <p>The native StreamNode remains available to parsers through {@link #getNativeNode()}, while the
 * core module only depends on the version-neutral interface.
 */
final class FlinkStreamNode implements LineageNode {

    private final StreamNode streamNode;

    /**
     * Creates a lineage node wrapper.
     *
     * @param streamNode Flink StreamNode
     */
    FlinkStreamNode(StreamNode streamNode) {
        this.streamNode = streamNode;
    }

    @Override
    public String getNodeId() {
        return String.valueOf(streamNode.getId());
    }

    @Override
    public String getNodeName() {
        return streamNode.getOperatorName();
    }

    @Override
    public Object getNativeNode() {
        return streamNode;
    }

    @Override
    public ClassLoader getRuntimeClassLoader() {
        return streamNode.getClass().getClassLoader();
    }

    @Override
    public List<String> getInputNodeIds() {
        return toStringList(streamNode.getInEdgeIndices());
    }

    @Override
    public List<String> getOutputNodeIds() {
        return toStringList(streamNode.getOutEdgeIndices());
    }

    @Override
    public Map<String, Object> getAttributes() {
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("operatorFactory", streamNode.getOperatorFactory());
        attributes.put("operatorName", getNodeName());
        return Collections.unmodifiableMap(attributes);
    }

    private static List<String> toStringList(Object values) {
        if (!(values instanceof Iterable<?>)) {
            return Collections.emptyList();
        }
        List<String> result = new java.util.ArrayList<>();
        for (Object value : (Iterable<?>) values) {
            result.add(String.valueOf(value));
        }
        return result;
    }
}
