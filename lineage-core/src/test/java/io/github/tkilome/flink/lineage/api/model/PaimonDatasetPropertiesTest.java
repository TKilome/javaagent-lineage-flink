package io.github.tkilome.flink.lineage.model;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PaimonDatasetPropertiesTest {

    @Test
    void createsExactTableDatasetWithStableKeys() {
        LineageDataset dataset =
                PaimonDatasetProperties.exactTable(
                        LineageDirection.SINK,
                        "EXACT_TABLE",
                        "sales.orders",
                        "s3://warehouse",
                        "/tables/orders");

        assertEquals(LineageDirection.SINK, dataset.getDirection());
        assertEquals("paimon", dataset.getConnector());
        assertEquals("s3://warehouse", dataset.getNamespace());
        assertEquals("sales.orders", dataset.getName());
        assertEquals("EXACT_TABLE", dataset.getProperties().get(PaimonDatasetProperties.SINK_MODE));
        assertEquals("sales.orders", dataset.getProperties().get(PaimonDatasetProperties.FULL_NAME));
        assertEquals("/tables/orders", dataset.getProperties().get(PaimonDatasetProperties.TABLE_PATH));
    }

    @Test
    void createsCombinedCdcSinkDatasetWithWildcardName() {
        LineageDataset dataset =
                PaimonDatasetProperties.combinedCdcSink(
                        "s3://warehouse",
                        "ods",
                        Collections.singletonMap(
                                PaimonDatasetProperties.SOURCE_TABLE_INCLUDE_PATTERN, "order_.*"));

        assertEquals(LineageDirection.SINK, dataset.getDirection());
        assertEquals("ods.*", dataset.getName());
        assertEquals("CDC_COMBINED", dataset.getProperties().get(PaimonDatasetProperties.SINK_MODE));
        assertEquals("*", dataset.getProperties().get(PaimonDatasetProperties.TARGET_TABLE));
        assertEquals(
                "order_.*",
                dataset.getProperties().get(PaimonDatasetProperties.SOURCE_TABLE_INCLUDE_PATTERN));
    }
}
