package io.github.tkilome.flink.lineage.api.instrumentation;

import java.lang.instrument.Instrumentation;

/**
 * Runtime component responsible for installing bytecode instrumentation.
 *
 * <p>Implementations are version-specific and may directly depend on the Flink APIs for their target
 * version. Any failure during installation should be propagated to stop job submission.
 */
@FunctionalInterface
public interface LineageInstrumentation {
    /**
     * Installs instrumentation into the current JVM.
     *
     * @param instrumentation JVM instrumentation handle passed to the Java agent
     */
    void installInstrumentation(Instrumentation instrumentation);
}
