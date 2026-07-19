package io.github.tkilome.flink.lineage.parser.kafka;

import io.github.tkilome.flink.lineage.api.factory.LineageParserFactory;
import io.github.tkilome.flink.lineage.api.parser.LineageParseResult;
import io.github.tkilome.flink.lineage.api.parser.LineageParser;
import io.github.tkilome.flink.lineage.context.LineageNode;
import io.github.tkilome.flink.lineage.context.ParserContext;
import io.github.tkilome.flink.lineage.model.KafkaDatasetProperties;
import io.github.tkilome.flink.lineage.model.LineageDataset;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Kafka parser factory for Flink 1.20 with Kafka connector 3.4.0-1.20.
 *
 * <p>The parser uses version-specific Flink/Kafka types. It does not perform generic recursive
 * reflection over arbitrary connector objects. Private fields without public getters are accessed by
 * fixed field names for this exact supported version.
 */
public final class KafkaLineageParserFactory implements LineageParserFactory {

    private static final String TOPIC_LIST_SUBSCRIBER =
            "org.apache.flink.connector.kafka.source.enumerator.subscriber.TopicListSubscriber";
    private static final String CACHING_TOPIC_SELECTOR =
            "org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder$CachingTopicSelector";
    private static final String CONSTANT_TOPIC_SELECTOR =
            "org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder$ConstantTopicSelector";
    private static final String DYNAMIC_KAFKA_RECORD_SERIALIZATION_SCHEMA =
            "org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaRecordSerializationSchema";

    @Override
    public String factoryName() {
        return "kafka-3.4.0-1.20-parser";
    }

    /**
     * Creates a Kafka parser.
     *
     * @param context parser context
     * @return parser that emits Kafka datasets from supported Kafka endpoints
     */
    @Override
    public LineageParser create(ParserContext context) {
        return KafkaLineageParserFactory::parseKafkaEndpoints;
    }

    private static LineageParseResult parseKafkaEndpoints(LineageNode node) {
        LineageParseResult sourceResult = parseKafkaSource(node);
        LineageParseResult sinkResult = parseKafkaSink(node);
        if (!sourceResult.isMatched() && !sinkResult.isMatched()) {
            return LineageParseResult.notMatched();
        }

        List<LineageDataset> datasets = new ArrayList<>();
        datasets.addAll(sourceResult.getDatasets());
        datasets.addAll(sinkResult.getDatasets());
        return LineageParseResult.matched(datasets);
    }

    private static LineageParseResult parseKafkaSource(LineageNode node) {
        if (node == null) {
            return LineageParseResult.notMatched();
        }
        Object operatorFactory = node.getAttributes().get("operatorFactory");
        if (!(operatorFactory instanceof SourceOperatorFactory<?>)) {
            return LineageParseResult.notMatched();
        }

        Source<?, ?, ?> source = readField(operatorFactory, SourceOperatorFactory.class, "source", Source.class);
        if (!(source instanceof KafkaSource<?>)) {
            return LineageParseResult.notMatched();
        }

        KafkaSource<?> kafkaSource = (KafkaSource<?>) source;
        List<String> topics = readTopics(kafkaSource);
        String bootstrapServers = readBootstrapServers(kafkaSource);
        if (topics.isEmpty() || bootstrapServers == null) {
            return LineageParseResult.matched(Collections.emptyList());
        }

        List<LineageDataset> datasets = new ArrayList<>();
        for (String topic : topics) {
            datasets.add(KafkaDatasetProperties.source(topic, bootstrapServers));
        }
        return LineageParseResult.matched(datasets);
    }

    private static LineageParseResult parseKafkaSink(LineageNode node) {
        if (node == null) {
            return LineageParseResult.notMatched();
        }
        Object operatorFactory = node.getAttributes().get("operatorFactory");
        if (!(operatorFactory instanceof SinkWriterOperatorFactory<?, ?>)) {
            return LineageParseResult.notMatched();
        }

        Sink<?> sink = ((SinkWriterOperatorFactory<?, ?>) operatorFactory).getSink();
        if (!(sink instanceof KafkaSink<?>)) {
            return LineageParseResult.notMatched();
        }

        KafkaSink<?> kafkaSink = (KafkaSink<?>) sink;
        String topic = readSinkTopic(kafkaSink);
        String bootstrapServers = readSinkBootstrapServers(kafkaSink);
        if (topic == null || bootstrapServers == null) {
            return LineageParseResult.matched(Collections.emptyList());
        }

        return LineageParseResult.matched(
                Collections.singletonList(KafkaDatasetProperties.sink(topic, bootstrapServers)));
    }

    private static List<String> readTopics(KafkaSource<?> source) {
        Object subscriber = readField(source, KafkaSource.class, "subscriber", Object.class);
        if (subscriber == null || !TOPIC_LIST_SUBSCRIBER.equals(subscriber.getClass().getName())) {
            return Collections.emptyList();
        }
        Collection<?> topics = readField(subscriber, subscriber.getClass(), "topics", Collection.class);
        if (topics == null || topics.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<>();
        for (Object topic : topics) {
            if (topic instanceof String && !((String) topic).trim().isEmpty()) {
                result.add(((String) topic).trim());
            }
        }
        return result;
    }

    private static String readBootstrapServers(KafkaSource<?> source) {
        Properties props = readField(source, KafkaSource.class, "props", Properties.class);
        if (props == null) {
            return null;
        }
        String bootstrapServers = props.getProperty(KafkaDatasetProperties.BOOTSTRAP_SERVERS);
        return bootstrapServers == null || bootstrapServers.trim().isEmpty()
                ? null
                : bootstrapServers.trim();
    }

    private static String readSinkBootstrapServers(KafkaSink<?> sink) {
        Properties props = readField(sink, KafkaSink.class, "kafkaProducerConfig", Properties.class);
        if (props == null) {
            return null;
        }
        String bootstrapServers = props.getProperty(KafkaDatasetProperties.BOOTSTRAP_SERVERS);
        return bootstrapServers == null || bootstrapServers.trim().isEmpty()
                ? null
                : bootstrapServers.trim();
    }

    private static String readSinkTopic(KafkaSink<?> sink) {
        KafkaRecordSerializationSchema<?> recordSerializer =
                readField(
                        sink,
                        KafkaSink.class,
                        "recordSerializer",
                        KafkaRecordSerializationSchema.class);
        if (recordSerializer == null) {
            return null;
        }

        if (DYNAMIC_KAFKA_RECORD_SERIALIZATION_SCHEMA.equals(recordSerializer.getClass().getName())) {
            Collection<?> topics =
                    readField(recordSerializer, recordSerializer.getClass(), "topics", Collection.class);
            return readSingleTopic(topics);
        }

        Object topicSelector =
                readField(
                        recordSerializer,
                        recordSerializer.getClass(),
                        "topicSelector",
                        Object.class);
        return readFixedTopic(topicSelector);
    }

    private static String readSingleTopic(Collection<?> topics) {
        if (topics == null || topics.size() != 1) {
            return null;
        }
        Object topic = topics.iterator().next();
        return topic instanceof String && !((String) topic).trim().isEmpty()
                ? ((String) topic).trim()
                : null;
    }

    private static String readFixedTopic(Object topicSelector) {
        if (topicSelector == null) {
            return null;
        }
        Object unwrapped = topicSelector;
        if (CACHING_TOPIC_SELECTOR.equals(topicSelector.getClass().getName())) {
            unwrapped = readField(topicSelector, topicSelector.getClass(), "topicSelector", Object.class);
        }
        if (unwrapped != null && CONSTANT_TOPIC_SELECTOR.equals(unwrapped.getClass().getName())) {
            String topic = readField(unwrapped, unwrapped.getClass(), "topic", String.class);
            return topic == null || topic.trim().isEmpty() ? null : topic.trim();
        }
        Object capturedTopic = readSerializedLambdaCapturedArg(unwrapped, 0);
        if (capturedTopic instanceof String && !((String) capturedTopic).trim().isEmpty()) {
            return ((String) capturedTopic).trim();
        }
        return null;
    }

    private static Object readSerializedLambdaCapturedArg(Object lambda, int index) {
        if (lambda == null) {
            return null;
        }
        try {
            Method writeReplace = lambda.getClass().getDeclaredMethod("writeReplace");
            writeReplace.setAccessible(true);
            Object serializedLambda = writeReplace.invoke(lambda);
            Method getCapturedArg =
                    serializedLambda.getClass().getDeclaredMethod("getCapturedArg", int.class);
            return getCapturedArg.invoke(serializedLambda, index);
        } catch (ReflectiveOperationException | RuntimeException error) {
            return null;
        }
    }

    private static <T> T readField(
            Object target, Class<?> declaringClass, String fieldName, Class<T> expectedType) {
        if (target == null) {
            return null;
        }
        try {
            Field field = declaringClass.getDeclaredField(fieldName);
            field.setAccessible(true);
            Object value = field.get(target);
            return expectedType.isInstance(value) ? expectedType.cast(value) : null;
        } catch (ReflectiveOperationException | RuntimeException error) {
            throw new IllegalStateException(
                    "Failed to access "
                            + declaringClass.getName()
                            + "#"
                            + fieldName
                            + " for Kafka lineage parsing",
                    error);
        }
    }
}
