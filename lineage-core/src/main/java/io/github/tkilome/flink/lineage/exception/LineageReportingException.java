package io.github.tkilome.flink.lineage.exception;

/**
 * Thrown when a reporter cannot serialize or deliver a lineage event.
 */
public final class LineageReportingException extends LineageException {
    public LineageReportingException(String message, Throwable cause) {
        super(message, cause);
    }
}
