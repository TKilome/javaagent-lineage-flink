package io.github.tkilome.flink.lineage.instrumentation;

import io.github.tkilome.flink.lineage.context.InstrumentationContext;
import io.github.tkilome.flink.lineage.api.instrumentation.LineageInstrumentation;
import io.github.tkilome.flink.lineage.api.factory.LineageInstrumentationFactory;

/**
 * ServiceLoader factory for the Flink 1.19 instrumentation module.
 *
 * <p>The Java class name intentionally does not contain the Flink version. The Maven artifact and
 * factory display name carry version information.
 */
public final class FlinkInstrumentationFactory implements LineageInstrumentationFactory {

    @Override
    public String factoryName() {
        return "flink-1.19-instrumentation";
    }

    @Override
    public LineageInstrumentation create(InstrumentationContext context) {
        return new FlinkInstrumentation();
    }
}
