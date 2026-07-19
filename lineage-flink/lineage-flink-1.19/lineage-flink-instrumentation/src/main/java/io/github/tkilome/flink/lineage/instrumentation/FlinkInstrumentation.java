package io.github.tkilome.flink.lineage.instrumentation;

import io.github.tkilome.flink.lineage.api.instrumentation.LineageInstrumentation;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.JavaModule;

import java.lang.instrument.Instrumentation;
import java.security.ProtectionDomain;

/**
 * Installs Flink 1.19 bytecode instrumentation.
 *
 * <p>The interception point is {@code PipelineExecutorUtils#getJobGraph(...)} because both
 * DataStream and Flink SQL submissions produce a {@code JobGraph} through this path. The advice
 * runs after successful method return and fails fast when lineage processing fails.
 */
final class FlinkInstrumentation implements LineageInstrumentation {

    /**
     * Installs Byte Buddy advice on {@code PipelineExecutorUtils#getJobGraph}.
     *
     * @param instrumentation JVM instrumentation handle; {@code null} is ignored for unit tests
     */
    @Override
    public void installInstrumentation(Instrumentation instrumentation) {
        if (instrumentation == null) {
            return;
        }
        new AgentBuilder.Default()
                .type(
                        ElementMatchers.named(
                                "org.apache.flink.client.deployment.executors.PipelineExecutorUtils"))
                .transform(
                        new AgentBuilder.Transformer() {
                            @Override
                            public DynamicType.Builder<?> transform(
                                    DynamicType.Builder<?> builder,
                                    TypeDescription typeDescription,
                                    ClassLoader classLoader,
                                    JavaModule module,
                                    ProtectionDomain protectionDomain) {
                                return builder.visit(
                                        Advice.to(PipelineExecutorUtilsGetJobGraphAdvice.class)
                                                .on(
                                                        ElementMatchers.named("getJobGraph")
                                                                .and(ElementMatchers.isStatic())
                                                                .and(ElementMatchers.takesArguments(3))));
                            }
                        })
                .installOn(instrumentation);
    }
}
