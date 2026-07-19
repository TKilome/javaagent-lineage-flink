package io.github.tkilome.flink.lineage.agent;

import io.github.tkilome.flink.lineage.discovery.FactoryDiscovery;
import io.github.tkilome.flink.lineage.runtime.LineageRuntime;
import io.github.tkilome.flink.lineage.api.factory.LineageFactory;

import java.lang.instrument.Instrumentation;
import java.util.List;

/**
 * Java agent entry point.
 *
 * <p>The JVM calls {@link #premain(String, Instrumentation)} before the Flink JobManager process
 * starts executing user job submission logic. The agent discovers all lineage factories from the
 * system classloader and initializes the runtime. Initialization failures are intentionally
 * propagated so the job does not continue without lineage processing.
 */
public final class LineageAgent {

    private LineageAgent() {}

    /**
     * Agent premain hook configured by the agent jar manifest.
     *
     * @param agentArgs raw {@code -javaagent} argument; currently unused because phase 1 has no
     *     external config file
     * @param instrumentation JVM instrumentation handle
     */
    public static void premain(String agentArgs, Instrumentation instrumentation) {
        List<LineageFactory> factories = FactoryDiscovery.discoverFromSystemClassLoader();
        LineageRuntime.initialize(factories, instrumentation);
    }
}
