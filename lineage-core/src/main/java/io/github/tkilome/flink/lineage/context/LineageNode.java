package io.github.tkilome.flink.lineage.context;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Version-neutral view of a runtime execution node collected by instrumentation.
 *
 * <p>The native node is intentionally exposed as {@link Object} so core APIs do not depend on Flink
 * classes. Version-specific parsers may inspect it when they know the expected runtime type.
 */
public interface LineageNode {
    /**
     * Returns the node id in the runtime graph.
     *
     * @return node id as string
     */
    String getNodeId();

    /**
     * Returns the runtime node name or operator name.
     *
     * @return node name, or {@code null} when unavailable
     */
    String getNodeName();

    /**
     * Returns the original runtime node object.
     *
     * @return native node object, or {@code null} when unavailable
     */
    Object getNativeNode();

    /**
     * Returns the classloader that loaded the native node.
     *
     * @return runtime classloader, or {@code null} when unavailable
     */
    ClassLoader getRuntimeClassLoader();

    /**
     * Returns ids of upstream input nodes.
     *
     * @return immutable or defensive list of input node ids
     */
    List<String> getInputNodeIds();

    /**
     * Returns ids of downstream output nodes.
     *
     * @return immutable or defensive list of output node ids
     */
    List<String> getOutputNodeIds();

    /**
     * Returns additional node attributes extracted by instrumentation.
     *
     * @return attribute map for parser use
     */
    Map<String, Object> getAttributes();

    /**
     * Creates an empty test node with only node id populated.
     *
     * @param nodeId node id
     * @return empty lineage node
     */
    static LineageNode empty(final String nodeId) {
        return new LineageNode() {
            @Override
            public String getNodeId() {
                return nodeId;
            }

            @Override
            public String getNodeName() {
                return null;
            }

            @Override
            public Object getNativeNode() {
                return null;
            }

            @Override
            public ClassLoader getRuntimeClassLoader() {
                return null;
            }

            @Override
            public List<String> getInputNodeIds() {
                return Collections.emptyList();
            }

            @Override
            public List<String> getOutputNodeIds() {
                return Collections.emptyList();
            }

            @Override
            public Map<String, Object> getAttributes() {
                return Collections.emptyMap();
            }
        };
    }
}
