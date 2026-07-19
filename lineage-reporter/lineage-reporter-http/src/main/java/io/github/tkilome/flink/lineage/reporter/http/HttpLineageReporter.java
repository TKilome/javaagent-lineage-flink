package io.github.tkilome.flink.lineage.reporter.http;

import io.github.tkilome.flink.lineage.api.reporter.LineageReporter;
import io.github.tkilome.flink.lineage.exception.LineageReportingException;
import io.github.tkilome.flink.lineage.model.LineageEvent;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

/** Reporter that posts lineage events to an HTTP endpoint synchronously. */
final class HttpLineageReporter implements LineageReporter {

    private final HttpReporterConfig config;

    HttpLineageReporter(HttpReporterConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        this.config = config;
    }

    @Override
    public void report(LineageEvent event) {
        String payload;
        try {
            payload = LineageEventJsonSerializer.toJson(event);
        } catch (RuntimeException error) {
            throw new LineageReportingException("Failed to serialize lineage event for HTTP reporter", error);
        }

        int attempts = config.getRetries() + 1;
        LineageReportingException lastFailure = null;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                int status = post(payload);
                if (status >= 200 && status < 300) {
                    return;
                }
                LineageReportingException failure =
                        new LineageReportingException(
                                "HTTP lineage reporter failed with status " + status, null);
                if (status < 500 || attempt == attempts) {
                    throw failure;
                }
                lastFailure = failure;
            } catch (IOException error) {
                lastFailure =
                        new LineageReportingException("HTTP lineage reporter request failed", error);
                if (attempt == attempts) {
                    throw lastFailure;
                }
            }
        }
        throw lastFailure == null
                ? new LineageReportingException("HTTP lineage reporter failed", null)
                : lastFailure;
    }

    private int post(String payload) throws IOException {
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        HttpURLConnection connection = (HttpURLConnection) config.getUrl().openConnection();
        connection.setRequestMethod("POST");
        connection.setConnectTimeout(config.getConnectTimeoutMs());
        connection.setReadTimeout(config.getReadTimeoutMs());
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        connection.setRequestProperty("Accept", "application/json");
        if (config.getToken() != null) {
            connection.setRequestProperty("Authorization", "Bearer " + config.getToken());
        }
        connection.setFixedLengthStreamingMode(bytes.length);
        try (OutputStream output = connection.getOutputStream()) {
            output.write(bytes);
        }
        try {
            return connection.getResponseCode();
        } finally {
            connection.disconnect();
        }
    }
}
