package io.github.tkilome.flink.lineage.exception;

/**
 * Thrown when parsed lineage does not satisfy minimum source/sink coverage.
 */
public class LineageValidationException extends LineageException {
    public LineageValidationException(String message) {
        super(message);
    }
}
