package io.github.tkilome.flink.lineage.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class LineageDatasetBuilderTest {

    @Test
    void buildsDatasetAndSkipsBlankOptionalProperties() {
        LineageDataset dataset =
                LineageDatasetBuilder.dataset(LineageDirection.SOURCE, "kafka", "orders")
                        .namespace("broker:9092")
                        .property("topic", "orders")
                        .optionalProperty("blank", " ")
                        .build();

        assertEquals(LineageDirection.SOURCE, dataset.getDirection());
        assertEquals("kafka", dataset.getConnector());
        assertEquals("broker:9092", dataset.getNamespace());
        assertEquals("orders", dataset.getName());
        assertEquals("orders", dataset.getProperties().get("topic"));
        assertFalse(dataset.getProperties().containsKey("blank"));
    }
}
