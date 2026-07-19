package io.github.tkilome.flink.lineage.api.factory;

import io.github.tkilome.flink.lineage.context.ReporterContext;
import io.github.tkilome.flink.lineage.api.reporter.LineageReporter;

/**
 * Factory for creating lineage reporters.
 *
 * <p>At least one reporter factory must be available at agent initialization time. Reporter failures
 * are propagated and block job submission.
 */
public interface LineageReporterFactory extends LineageFactory {
    /**
     * Returns {@link LineageComponentType#REPORTER}.
     *
     * @return reporter component type
     */
    @Override
    default LineageComponentType componentType() {
        return LineageComponentType.REPORTER;
    }

    /**
     * Creates the reporter instance.
     *
     * @param context reporter context provided by the core runtime
     * @return reporter implementation
     */
    LineageReporter create(ReporterContext context);
}
