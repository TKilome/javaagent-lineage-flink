package io.github.tkilome.flink.lineage.reporter.http;

import io.github.tkilome.flink.lineage.model.KafkaDatasetProperties;
import io.github.tkilome.flink.lineage.model.EngineType;
import io.github.tkilome.flink.lineage.model.LineageDataset;
import io.github.tkilome.flink.lineage.model.LineageDirection;
import io.github.tkilome.flink.lineage.model.LineageEvent;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LineageEventJsonSerializerTest {

    @Test
    void serializesLineageEvent() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(KafkaDatasetProperties.TOPIC, "orders");
        properties.put(KafkaDatasetProperties.BOOTSTRAP_SERVERS, "broker:9092");
        LineageEvent event =
                new LineageEvent(
                        EngineType.FLINK,
                        "job-1",
                        "job-name",
                        123L,
                        Collections.singletonList(
                                new LineageDataset(
                                        LineageDirection.SOURCE,
                                        KafkaDatasetProperties.CONNECTOR,
                                        "broker:9092",
                                        "orders",
                                        properties)),
                        Collections.emptyList());

        String json = LineageEventJsonSerializer.toJson(event);

        assertTrue(json.contains("\"jobId\":\"job-1\""));
        assertTrue(json.contains("\"engineType\":\"flink\""));
        assertTrue(json.contains("\"jobName\":\"job-name\""));
        assertTrue(json.contains("\"timestamp\":123"));
        assertTrue(json.contains("\"sources\""));
        assertTrue(json.contains("\"connector\":\"kafka\""));
        assertTrue(json.contains("\"namespace\":\"broker:9092\""));
        assertTrue(json.contains("\"name\":\"orders\""));
        assertTrue(json.contains("\"bootstrap.servers\":\"broker:9092\""));
        assertTrue(json.contains("\"sinks\":[]"));
    }
}
