package io.github.tkilome.flink.lineage.exception;

/**
 * Thrown when the agent cannot initialize its factory registry or instrumentation.
 */
public final class LineageAgentInitializationException extends LineageException {
    public LineageAgentInitializationException(String message) {
        super(message);
    }

    public LineageAgentInitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
