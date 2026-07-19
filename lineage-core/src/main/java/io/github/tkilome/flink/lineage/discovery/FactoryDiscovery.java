package io.github.tkilome.flink.lineage.discovery;

import io.github.tkilome.flink.lineage.exception.LineageAgentInitializationException;
import io.github.tkilome.flink.lineage.api.factory.LineageFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * Discovers lineage extension factories through {@link ServiceLoader}.
 *
 * <p>Phase 1 loads from the system classloader because all lineage jars are expected to be placed in
 * {@code $FLINK_HOME/lib}. The agent does not create a custom classloader.
 */
public final class FactoryDiscovery {

    private FactoryDiscovery() {}

    /**
     * Discovers factories from the system classloader.
     *
     * @return discovered factories
     */
    public static List<LineageFactory> discoverFromSystemClassLoader() {
        return discover(ClassLoader.getSystemClassLoader());
    }

    /**
     * Discovers factories from the given classloader.
     *
     * @param classLoader classloader used by {@link ServiceLoader}
     * @return discovered factories
     */
    static List<LineageFactory> discover(ClassLoader classLoader) {
        if (classLoader == null) {
            throw new LineageAgentInitializationException("runtime class loader must not be null");
        }
        try {
            List<LineageFactory> factories = new ArrayList<>();
            for (LineageFactory factory :
                    ServiceLoader.load(LineageFactory.class, classLoader)) {
                factories.add(factory);
            }
            return factories;
        } catch (ServiceConfigurationError error) {
            throw new LineageAgentInitializationException(
                    "Failed to discover lineage factories", error);
        }
    }
}
