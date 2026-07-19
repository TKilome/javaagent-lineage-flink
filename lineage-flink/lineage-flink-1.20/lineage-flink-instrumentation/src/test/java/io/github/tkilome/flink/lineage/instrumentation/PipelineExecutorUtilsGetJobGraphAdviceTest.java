package io.github.tkilome.flink.lineage.instrumentation;

import org.junit.jupiter.api.Test;

class PipelineExecutorUtilsGetJobGraphAdviceTest {

    @Test
    void ignoresNonStreamGraphPipeline() {
        PipelineExecutorUtilsGetJobGraphAdvice.onExit(null, null);
    }
}
