package io.github.tkilome.flink.lineage.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaDatasetPropertiesTest {

    @Test
    void createsKafkaSourceDatasetWithStableKeys() {
        LineageDataset dataset = KafkaDatasetProperties.source("orders", "broker:9092");

        assertEquals(LineageDirection.SOURCE, dataset.getDirection());
        assertEquals("kafka", dataset.getConnector());
        assertEquals("broker:9092", dataset.getNamespace());
        assertEquals("orders", dataset.getName());
        assertEquals("orders", dataset.getProperties().get(KafkaDatasetProperties.TOPIC));
        assertEquals(
                "broker:9092",
                dataset.getProperties().get(KafkaDatasetProperties.BOOTSTRAP_SERVERS));
    }
}
