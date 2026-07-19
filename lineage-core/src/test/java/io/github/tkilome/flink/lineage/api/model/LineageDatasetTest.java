package io.github.tkilome.flink.lineage.model;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LineageDatasetTest {

    @Test
    void createsImmutableDatasetWithStableIdentity() {
        LineageDataset dataset =
                new LineageDataset(
                        LineageDirection.SOURCE,
                        "kafka",
                        "cluster-a",
                        "orders",
                        Collections.singletonMap("topic", "orders"));

        assertEquals(LineageDirection.SOURCE, dataset.getDirection());
        assertEquals("kafka", dataset.getConnector());
        assertEquals("cluster-a", dataset.getNamespace());
        assertEquals("orders", dataset.getName());
        assertEquals("SOURCE\u0000kafka\u0000cluster-a\u0000orders", dataset.identityKey());
        assertThrows(
                UnsupportedOperationException.class,
                () -> dataset.getProperties().put("x", "y"));
    }

    @Test
    void rejectsMissingRequiredFields() {
        IllegalArgumentException missingDirection =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new LineageDataset(
                                        null,
                                        "kafka",
                                        null,
                                        "orders",
                                        Collections.emptyMap()));
        assertTrue(missingDirection.getMessage().contains("direction"));

        IllegalArgumentException missingConnector =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new LineageDataset(
                                        LineageDirection.SINK,
                                        " ",
                                        null,
                                        "orders",
                                        Collections.emptyMap()));
        assertTrue(missingConnector.getMessage().contains("connector"));
    }
}
