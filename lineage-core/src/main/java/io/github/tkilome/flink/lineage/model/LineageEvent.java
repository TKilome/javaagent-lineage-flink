package io.github.tkilome.flink.lineage.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Final job-level lineage event emitted by the agent.
 *
 * <p>The event contains the producing engine, job id, job name, timestamp, source datasets, and
 * sink datasets.
 */
public final class LineageEvent {

    private final EngineType engineType;
    private final String jobId;
    private final String jobName;
    private final long timestamp;
    private final List<LineageDataset> sources;
    private final List<LineageDataset> sinks;

    /**
     * Creates a lineage event.
     *
     * @param engineType compute engine that produced the event
     * @param jobId Flink job id
     * @param jobName Flink job name, may be {@code null}
     * @param timestamp event creation time in epoch milliseconds
     * @param sources source datasets
     * @param sinks sink datasets
     */
    public LineageEvent(
            EngineType engineType,
            String jobId,
            String jobName,
            long timestamp,
            List<LineageDataset> sources,
            List<LineageDataset> sinks) {
        this.engineType = requireNonNull(engineType, "engineType");
        this.jobId = requireText(jobId, "jobId");
        this.jobName = normalizeNullable(jobName);
        this.timestamp = timestamp;
        this.sources = immutableCopy(sources);
        this.sinks = immutableCopy(sinks);
    }

    /**
     * Creates a Flink lineage event.
     *
     * @param jobId Flink job id
     * @param jobName Flink job name, may be {@code null}
     * @param timestamp event creation time in epoch milliseconds
     * @param sources source datasets
     * @param sinks sink datasets
     */
    public LineageEvent(
            String jobId,
            String jobName,
            long timestamp,
            List<LineageDataset> sources,
            List<LineageDataset> sinks) {
        this(EngineType.FLINK, jobId, jobName, timestamp, sources, sinks);
    }

    /**
     * Returns the compute engine that produced the event.
     *
     * @return engine type
     */
    public EngineType getEngineType() {
        return engineType;
    }

    /**
     * Returns Flink job id.
     *
     * @return job id
     */
    public String getJobId() {
        return jobId;
    }

    /**
     * Returns Flink job name.
     *
     * @return job name, or {@code null}
     */
    public String getJobName() {
        return jobName;
    }

    /**
     * Returns event creation time.
     *
     * @return epoch milliseconds
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns immutable source dataset list.
     *
     * @return source datasets
     */
    public List<LineageDataset> getSources() {
        return sources;
    }

    /**
     * Returns immutable sink dataset list.
     *
     * @return sink datasets
     */
    public List<LineageDataset> getSinks() {
        return sinks;
    }

    private static List<LineageDataset> immutableCopy(List<LineageDataset> datasets) {
        return Collections.unmodifiableList(
                new ArrayList<>(datasets == null ? Collections.emptyList() : datasets));
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static String normalizeNullable(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }
}
