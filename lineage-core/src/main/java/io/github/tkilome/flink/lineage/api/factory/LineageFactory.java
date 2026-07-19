package io.github.tkilome.flink.lineage.api.factory;

/**
 * Base contract for all lineage extension factories loaded through {@link java.util.ServiceLoader}.
 *
 * <p>The factory name is diagnostic only. It is used in logs and exception messages, and is not used
 * for component matching, routing, or version selection.
 */
public interface LineageFactory {
    /**
     * Returns a human-readable factory name for diagnostics.
     *
     * @return non-blank display name of this factory
     */
    String factoryName();

    /**
     * Returns the component type provided by this factory.
     *
     * @return extension component type
     */
    LineageComponentType componentType();
}
