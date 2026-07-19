package io.github.tkilome.flink.lineage.parser.kafka;

import io.github.tkilome.flink.lineage.api.parser.LineageParseResult;
import io.github.tkilome.flink.lineage.context.LineageNode;
import io.github.tkilome.flink.lineage.model.LineageDataset;
import io.github.tkilome.flink.lineage.model.LineageDirection;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.StatementSetImpl;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.ModifyOperation;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaSqlLineageParserFactoryTest {

    @Test
    void generatesJobGraphAndParsesKafkaSourceAndSinkFromFlinkSqlWithoutSubmittingClusterJob()
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "CREATE TABLE orders_input (\n"
                        + "  order_id STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = 'orders-sql-input',\n"
                        + "  'properties.bootstrap.servers' = 'broker-a:9092,broker-b:9092',\n"
                        + "  'properties.group.id' = 'lineage-sql-test',\n"
                        + "  'scan.startup.mode' = 'earliest-offset',\n"
                        + "  'format' = 'raw'\n"
                        + ")");
        tableEnv.executeSql(
                "CREATE TABLE orders_output (\n"
                        + "  order_id STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = 'orders-sql-output',\n"
                        + "  'properties.bootstrap.servers' = 'broker-a:9092,broker-b:9092',\n"
                        + "  'format' = 'raw'\n"
                        + ")");

        StreamStatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql("INSERT INTO orders_output SELECT order_id FROM orders_input");

        for (Transformation<?> transformation :
                translate(tableEnv, ((StatementSetImpl<?>) statementSet).getOperations())) {
            env.addOperator(transformation);
        }

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setJobName("lineage-agent-kafka-sql-debug");

        JobGraph jobGraph =
                PipelineExecutorUtils.getJobGraph(
                        streamGraph,
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader());

        assertNotNull(jobGraph.getJobID());
        assertEquals("lineage-agent-kafka-sql-debug", jobGraph.getName());

        List<LineageDataset> datasets = parseDatasets(streamGraph);

        assertTrue(
                datasets.stream()
                        .anyMatch(
                                dataset ->
                                        dataset.getDirection() == LineageDirection.SOURCE
                                                && "orders-sql-input".equals(dataset.getName())
                                                && "broker-a:9092,broker-b:9092"
                                                        .equals(dataset.getNamespace())));
        assertTrue(
                datasets.stream()
                        .anyMatch(
                                dataset ->
                                        dataset.getDirection() == LineageDirection.SINK
                                                && "orders-sql-output".equals(dataset.getName())
                                                && "broker-a:9092,broker-b:9092"
                                                        .equals(dataset.getNamespace())));
    }

    @SuppressWarnings("unchecked")
    private static List<Transformation<?>> translate(
            StreamTableEnvironment tableEnv, List<ModifyOperation> operations) throws Exception {
        Method translate = TableEnvironmentImpl.class.getDeclaredMethod("translate", List.class);
        translate.setAccessible(true);
        return (List<Transformation<?>>) translate.invoke(tableEnv, operations);
    }

    private static List<LineageDataset> parseDatasets(StreamGraph streamGraph) {
        KafkaLineageParserFactory factory = new KafkaLineageParserFactory();
        List<LineageDataset> datasets = new ArrayList<>();
        for (StreamNode streamNode : streamGraph.getStreamNodes()) {
            LineageParseResult result =
                    factory.create(null)
                            .parse(
                                    node(
                                            streamNode.getOperatorName(),
                                            streamNode.getOperatorFactory(),
                                            true,
                                            true));
            if (result.isMatched()) {
                datasets.addAll(result.getDatasets());
            }
        }
        return datasets;
    }

    private static LineageNode node(
            final String name,
            final Object operatorFactory,
            final boolean hasInput,
            final boolean hasOutput) {
        return new LineageNode() {
            @Override
            public String getNodeId() {
                return "1";
            }

            @Override
            public String getNodeName() {
                return name;
            }

            @Override
            public Object getNativeNode() {
                return null;
            }

            @Override
            public ClassLoader getRuntimeClassLoader() {
                return getClass().getClassLoader();
            }

            @Override
            public List<String> getInputNodeIds() {
                return hasInput ? Collections.singletonList("0") : Collections.emptyList();
            }

            @Override
            public List<String> getOutputNodeIds() {
                return hasOutput ? Collections.singletonList("2") : Collections.emptyList();
            }

            @Override
            public Map<String, Object> getAttributes() {
                Map<String, Object> attributes = new HashMap<>();
                attributes.put("operatorFactory", operatorFactory);
                return attributes;
            }
        };
    }
}
