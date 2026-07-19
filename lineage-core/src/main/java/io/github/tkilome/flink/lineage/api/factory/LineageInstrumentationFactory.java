package io.github.tkilome.flink.lineage.api.factory;

import io.github.tkilome.flink.lineage.api.instrumentation.LineageInstrumentation;
import io.github.tkilome.flink.lineage.context.InstrumentationContext;

/**
 * Factory for creating the Flink-version-specific instrumentation component.
 *
 * <p>Exactly one instrumentation factory must be available at agent initialization time. The user is
 * responsible for putting the factory matching the running Flink version on the classpath.
 */
public interface LineageInstrumentationFactory extends LineageFactory {
    /**
     * Returns {@link LineageComponentType#INSTRUMENTATION}.
     *
     * @return instrumentation component type
     */
    @Override
    default LineageComponentType componentType() {
        return LineageComponentType.INSTRUMENTATION;
    }

    /**
     * Creates the instrumentation instance.
     *
     * @param context initialization context provided by the core runtime
     * @return instrumentation implementation
     */
    LineageInstrumentation create(InstrumentationContext context);
}
