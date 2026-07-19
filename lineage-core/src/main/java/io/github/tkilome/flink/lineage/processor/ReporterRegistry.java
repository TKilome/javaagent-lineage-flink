package io.github.tkilome.flink.lineage.processor;

import io.github.tkilome.flink.lineage.context.ReporterContext;
import io.github.tkilome.flink.lineage.exception.LineageReportingException;
import io.github.tkilome.flink.lineage.model.LineageEvent;
import io.github.tkilome.flink.lineage.api.reporter.LineageReporter;
import io.github.tkilome.flink.lineage.api.factory.LineageReporterFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Dispatches completed lineage events to all configured reporters.
 *
 * <p>Reporter failures are not swallowed. A failing reporter blocks job submission according to the
 * agent's fail-fast contract.
 */
public final class ReporterRegistry {

    private static final ReporterContext EMPTY_CONTEXT = new ReporterContext() {};

    private final List<LineageReporterFactory> factories;

    /**
     * Creates a reporter registry.
     *
     * @param factories reporter factories; {@code null} is treated as empty
     */
    public ReporterRegistry(List<LineageReporterFactory> factories) {
        this.factories =
                Collections.unmodifiableList(
                        new ArrayList<>(factories == null ? Collections.emptyList() : factories));
    }

    /**
     * Reports an event to every reporter factory.
     *
     * @param event completed lineage event
     */
    public void report(LineageEvent event) {
        for (LineageReporterFactory factory : factories) {
            try {
                LineageReporter reporter = factory.create(EMPTY_CONTEXT);
                reporter.report(event);
            } catch (LineageReportingException error) {
                throw error;
            } catch (RuntimeException error) {
                throw new LineageReportingException(
                        "Lineage reporter failed: " + factory.factoryName(), error);
            }
        }
    }
}
