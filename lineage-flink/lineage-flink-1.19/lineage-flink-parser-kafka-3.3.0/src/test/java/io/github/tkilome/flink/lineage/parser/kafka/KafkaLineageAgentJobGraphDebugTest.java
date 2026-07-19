package io.github.tkilome.flink.lineage.parser.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * End-to-end debug test for the Java Agent entry point.
 *
 * <p>This test builds a real {@link StreamGraph} with {@link KafkaSource} and {@link KafkaSink},
 * then calls {@link PipelineExecutorUtils#getJobGraph}. It does not connect to Kafka and does not
 * submit the job to any Flink cluster.
 *
 * <p>To debug the actual agent interception in IDEA:
 *
 * <ol>
 *   <li>Build the agent jar: {@code mvn -o -pl lineage-core -am package}
 *   <li>Run this test method with VM options:
 *       {@code
 *       -javaagent:/Users/dongjiaxin/code/java/flink-1.19-origin/flink-annotation/flink-lineage-java-agent/lineage-core/target/lineage-core-1.0.0-SNAPSHOT.jar}
 *   <li>Set breakpoints in {@code PipelineExecutorUtilsGetJobGraphAdvice},
 *       {@code LineageRuntime#processLineage}, and {@code KafkaLineageParserFactory}.
 * </ol>
 *
 * <p>The test classpath contains the instrumentation, Kafka parser, and logging reporter modules
 * through test-scoped dependencies, so ServiceLoader can discover all required lineage components
 * when the javaagent starts.
 */
class KafkaLineageAgentJobGraphDebugTest {

    @Test
    void generatesJobGraphFromRealKafkaSourceAndSinkWithoutSubmittingClusterJob()
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers("broker-a:9092,broker-b:9092")
                        .setGroupId("lineage-agent-debug")
                        .setTopics("orders-input")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers("broker-a:9092,broker-b:9092")
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic("orders-output")
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "lineage-kafka-source")
                .name("lineage-kafka-source")
                .uid("lineage-kafka-source")
                .map(value -> value)
                .name("lineage-pass-through")
                .uid("lineage-pass-through")
                .sinkTo(sink)
                .name("lineage-kafka-sink")
                .uid("lineage-kafka-sink");

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setJobName("lineage-agent-kafka-debug");

        JobGraph jobGraph =
                PipelineExecutorUtils.getJobGraph(
                        streamGraph,
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader());

        assertNotNull(jobGraph.getJobID());
        assertEquals("lineage-agent-kafka-debug", jobGraph.getName());
    }
}
