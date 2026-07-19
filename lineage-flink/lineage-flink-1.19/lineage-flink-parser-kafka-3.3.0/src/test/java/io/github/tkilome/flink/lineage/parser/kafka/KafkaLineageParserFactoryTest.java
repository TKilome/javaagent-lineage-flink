package io.github.tkilome.flink.lineage.parser.kafka;

import io.github.tkilome.flink.lineage.api.parser.LineageParseResult;
import io.github.tkilome.flink.lineage.context.LineageNode;
import io.github.tkilome.flink.lineage.model.KafkaDatasetProperties;
import io.github.tkilome.flink.lineage.model.LineageDataset;
import io.github.tkilome.flink.lineage.model.LineageDirection;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaLineageParserFactoryTest {

    @Test
    void parsesKafkaSourceAndSinkFromRealStreamGraphWithoutSubmittingJob() {
        KafkaLineageParserFactory factory = new KafkaLineageParserFactory();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers("broker-a:9092,broker-b:9092")
                        .setGroupId("lineage-streamgraph-test")
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
        List<LineageDataset> datasets = new ArrayList<>();
        for (StreamNode streamNode : streamGraph.getStreamNodes()) {
            LineageParseResult result =
                    factory.create(null).parse(node(streamNode.getOperatorName(), streamNode.getOperatorFactory(), true, true));
            if (result.isMatched()) {
                datasets.addAll(result.getDatasets());
            }
        }

        assertTrue(
                datasets.stream()
                        .anyMatch(
                                dataset ->
                                        dataset.getDirection() == LineageDirection.SOURCE
                                                && "orders-input".equals(dataset.getName())
                                                && "broker-a:9092,broker-b:9092"
                                                        .equals(dataset.getNamespace())));
        assertTrue(
                datasets.stream()
                        .anyMatch(
                                dataset ->
                                        dataset.getDirection() == LineageDirection.SINK
                                                && "orders-output".equals(dataset.getName())
                                                && "broker-a:9092,broker-b:9092"
                                                        .equals(dataset.getNamespace())));
    }

    @Test
    void parsesKafkaSourceTopicsAndBootstrapServersFromSourceOperatorFactory() {
        KafkaLineageParserFactory factory = new KafkaLineageParserFactory();
        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers("broker-a:9092,broker-b:9092")
                        .setGroupId("lineage-test")
                        .setTopics("orders", "payments")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();
        SourceOperatorFactory<String> operatorFactory =
                new SourceOperatorFactory<>(source, WatermarkStrategy.noWatermarks());
        LineageNode node = node("Kafka Source", operatorFactory, false, true);

        LineageParseResult result = factory.create(null).parse(node);

        assertTrue(result.isMatched());
        List<LineageDataset> datasets = result.getDatasets();
        assertEquals(2, datasets.size());
        assertKafkaSourceDataset(datasets.get(0), "orders");
        assertKafkaSourceDataset(datasets.get(1), "payments");
    }

    @Test
    void parsesKafkaSinkTopicAndBootstrapServersFromSinkWriterOperatorFactory() {
        KafkaLineageParserFactory factory = new KafkaLineageParserFactory();
        KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers("broker-a:9092,broker-b:9092")
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic("order-output")
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .build();
        SinkWriterOperatorFactory<String, ?> operatorFactory = new SinkWriterOperatorFactory<>(sink);
        LineageNode node = node("Kafka Sink", operatorFactory, true, false);

        LineageParseResult result = factory.create(null).parse(node);

        assertTrue(result.isMatched());
        List<LineageDataset> datasets = result.getDatasets();
        assertEquals(1, datasets.size());
        assertKafkaSinkDataset(datasets.get(0), "order-output");
    }

    @Test
    void returnsNotMatchedForNonKafkaSourceOperatorFactory() {
        KafkaLineageParserFactory factory = new KafkaLineageParserFactory();

        LineageParseResult result = factory.create(null).parse(node("Map", new Object(), true, true));

        assertFalse(result.isMatched());
        assertEquals(Collections.emptyList(), result.getDatasets());
    }

    private static void assertKafkaSourceDataset(LineageDataset dataset, String topic) {
        assertEquals(LineageDirection.SOURCE, dataset.getDirection());
        assertEquals(KafkaDatasetProperties.CONNECTOR, dataset.getConnector());
        assertEquals("broker-a:9092,broker-b:9092", dataset.getNamespace());
        assertEquals(topic, dataset.getName());
        assertEquals(topic, dataset.getProperties().get(KafkaDatasetProperties.TOPIC));
        assertEquals(
                "broker-a:9092,broker-b:9092",
                dataset.getProperties().get(KafkaDatasetProperties.BOOTSTRAP_SERVERS));
    }

    private static void assertKafkaSinkDataset(LineageDataset dataset, String topic) {
        assertEquals(LineageDirection.SINK, dataset.getDirection());
        assertEquals(KafkaDatasetProperties.CONNECTOR, dataset.getConnector());
        assertEquals("broker-a:9092,broker-b:9092", dataset.getNamespace());
        assertEquals(topic, dataset.getName());
        assertEquals(topic, dataset.getProperties().get(KafkaDatasetProperties.TOPIC));
        assertEquals(
                "broker-a:9092,broker-b:9092",
                dataset.getProperties().get(KafkaDatasetProperties.BOOTSTRAP_SERVERS));
    }

    private static LineageNode node(
            final String name, final Object operatorFactory, final boolean hasInput, final boolean hasOutput) {
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
