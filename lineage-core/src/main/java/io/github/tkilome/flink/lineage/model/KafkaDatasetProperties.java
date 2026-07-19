package io.github.tkilome.flink.lineage.model;

/** Stable Kafka lineage dataset property names and factories. */
public final class KafkaDatasetProperties {

    public static final String CONNECTOR = "kafka";
    public static final String TOPIC = "topic";
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    private KafkaDatasetProperties() {}

    public static LineageDataset source(String topic, String bootstrapServers) {
        return dataset(LineageDirection.SOURCE, topic, bootstrapServers);
    }

    public static LineageDataset sink(String topic, String bootstrapServers) {
        return dataset(LineageDirection.SINK, topic, bootstrapServers);
    }

    private static LineageDataset dataset(
            LineageDirection direction, String topic, String bootstrapServers) {
        return LineageDatasetBuilder.dataset(direction, CONNECTOR, topic)
                .namespace(bootstrapServers)
                .property(TOPIC, topic)
                .property(BOOTSTRAP_SERVERS, bootstrapServers)
                .build();
    }
}
