package io.github.tkilome.flink.lineage.exception;

/**
 * Thrown when a matched parser cannot extract required lineage metadata.
 */
public final class LineageParsingException extends LineageException {
    public LineageParsingException(String message) {
        super(message);
    }

    public LineageParsingException(String message, Throwable cause) {
        super(message, cause);
    }
}
