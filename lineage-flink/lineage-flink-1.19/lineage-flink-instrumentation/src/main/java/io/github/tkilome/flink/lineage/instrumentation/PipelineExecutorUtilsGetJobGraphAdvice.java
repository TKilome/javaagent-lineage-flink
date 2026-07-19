package io.github.tkilome.flink.lineage.instrumentation;

import io.github.tkilome.flink.lineage.context.LineageNode;
import io.github.tkilome.flink.lineage.runtime.LineageRuntime;
import net.bytebuddy.asm.Advice;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.util.List;

/**
 * Byte Buddy advice executed after Flink creates a {@link JobGraph}.
 *
 * <p>The advice only processes calls where the input pipeline is a {@link StreamGraph} and the
 * returned value is a {@link JobGraph}. It extracts job id, job name, and StreamGraph nodes, then
 * delegates to the core runtime. Exceptions are not suppressed so job submission fails when lineage
 * cannot be processed.
 */
public final class PipelineExecutorUtilsGetJobGraphAdvice {

    private PipelineExecutorUtilsGetJobGraphAdvice() {}

    /**
     * Handles successful exits from {@code PipelineExecutorUtils#getJobGraph}.
     *
     * @param pipeline method argument 0, expected to be {@link StreamGraph}
     * @param jobGraph method return value, expected to be {@link JobGraph}
     */
    @Advice.OnMethodExit
    static void onExit(@Advice.Argument(0) Object pipeline, @Advice.Return Object jobGraph) {
        if (!(pipeline instanceof StreamGraph)) {
            return;
        }
        if (!(jobGraph instanceof JobGraph)) {
            return;
        }

        StreamGraph streamGraph = (StreamGraph) pipeline;
        JobGraph flinkJobGraph = (JobGraph) jobGraph;
        String jobId = String.valueOf(flinkJobGraph.getJobID());
        String jobName = streamGraph.getJobName();
        List<LineageNode> nodes = FlinkStreamGraphAccessor.toLineageNodes(streamGraph);
        LineageRuntime.processLineage(jobId, jobName, nodes);
    }
}
