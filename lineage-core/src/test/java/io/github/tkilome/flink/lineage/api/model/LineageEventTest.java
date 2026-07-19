package io.github.tkilome.flink.lineage.model;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LineageEventTest {

    @Test
    void storesImmutableSourcesAndSinks() {
        LineageDataset source =
                new LineageDataset(
                        LineageDirection.SOURCE,
                        "kafka",
                        null,
                        "source-topic",
                        Collections.emptyMap());
        LineageDataset sink =
                new LineageDataset(
                        LineageDirection.SINK,
                        "kafka",
                        null,
                        "sink-topic",
                        Collections.emptyMap());

        LineageEvent event =
                new LineageEvent(
                        EngineType.FLINK,
                        "job-1",
                        "kafka-to-kafka",
                        123L,
                        Collections.singletonList(source),
                        Collections.singletonList(sink));

        assertEquals(EngineType.FLINK, event.getEngineType());
        assertEquals("job-1", event.getJobId());
        assertEquals(Collections.singletonList(source), event.getSources());
        assertEquals(Collections.singletonList(sink), event.getSinks());
        assertThrows(UnsupportedOperationException.class, () -> event.getSources().clear());
    }

    @Test
    void rejectsMissingJobId() {
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new LineageEvent(
                                        EngineType.FLINK,
                                        " ",
                                        "job",
                                        1L,
                                        Collections.emptyList(),
                                        Collections.emptyList()));
        assertTrue(error.getMessage().contains("jobId"));
    }

    @Test
    void rejectsMissingEngineType() {
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new LineageEvent(
                                        null,
                                        "job-1",
                                        "job",
                                        1L,
                                        Collections.emptyList(),
                                        Collections.emptyList()));
        assertTrue(error.getMessage().contains("engineType"));
    }
}
