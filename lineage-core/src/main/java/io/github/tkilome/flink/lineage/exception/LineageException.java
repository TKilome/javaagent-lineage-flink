package io.github.tkilome.flink.lineage.exception;

/**
 * Base unchecked exception for lineage agent failures.
 *
 * <p>The agent intentionally uses unchecked exceptions so failures can propagate through Flink
 * submission code and prevent the job from starting without valid lineage.
 */
public class LineageException extends RuntimeException {
    public LineageException(String message) {
        super(message);
    }

    public LineageException(String message, Throwable cause) {
        super(message, cause);
    }
}
