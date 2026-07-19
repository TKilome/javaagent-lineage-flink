package io.github.tkilome.flink.lineage.reporter.http;

import io.github.tkilome.flink.lineage.exception.LineageReportingException;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Configuration for the HTTP lineage reporter.
 *
 * <p>Configuration is read from system properties first, then environment variables.
 */
final class HttpReporterConfig {

    private static final String URL_PROPERTY = "lineage.reporter.http.url";
    private static final String TOKEN_PROPERTY = "lineage.reporter.http.token";
    private static final String CONNECT_TIMEOUT_PROPERTY = "lineage.reporter.http.connectTimeoutMs";
    private static final String READ_TIMEOUT_PROPERTY = "lineage.reporter.http.readTimeoutMs";
    private static final String RETRIES_PROPERTY = "lineage.reporter.http.retries";

    private static final String URL_ENV = "LINEAGE_REPORTER_HTTP_URL";
    private static final String TOKEN_ENV = "LINEAGE_REPORTER_HTTP_TOKEN";
    private static final String CONNECT_TIMEOUT_ENV = "LINEAGE_REPORTER_HTTP_CONNECT_TIMEOUT_MS";
    private static final String READ_TIMEOUT_ENV = "LINEAGE_REPORTER_HTTP_READ_TIMEOUT_MS";
    private static final String RETRIES_ENV = "LINEAGE_REPORTER_HTTP_RETRIES";

    private final URL url;
    private final String token;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final int retries;

    private HttpReporterConfig(
            URL url, String token, int connectTimeoutMs, int readTimeoutMs, int retries) {
        this.url = url;
        this.token = normalizeNullable(token);
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.retries = retries;
    }

    static HttpReporterConfig fromSystem() {
        return fromValues(
                read(URL_PROPERTY, URL_ENV, null),
                read(TOKEN_PROPERTY, TOKEN_ENV, null),
                parsePositiveInt(
                        read(CONNECT_TIMEOUT_PROPERTY, CONNECT_TIMEOUT_ENV, "3000"),
                        CONNECT_TIMEOUT_PROPERTY),
                parsePositiveInt(
                        read(READ_TIMEOUT_PROPERTY, READ_TIMEOUT_ENV, "5000"),
                        READ_TIMEOUT_PROPERTY),
                parseRetries(read(RETRIES_PROPERTY, RETRIES_ENV, "0")));
    }

    static HttpReporterConfig fromValues(
            String url, String token, int connectTimeoutMs, int readTimeoutMs, int retries) {
        if (connectTimeoutMs <= 0) {
            throw new LineageReportingException("HTTP reporter connect timeout must be positive", null);
        }
        if (readTimeoutMs <= 0) {
            throw new LineageReportingException("HTTP reporter read timeout must be positive", null);
        }
        if (retries < 0 || retries > 3) {
            throw new LineageReportingException("HTTP reporter retries must be between 0 and 3", null);
        }
        String normalizedUrl = normalizeNullable(url);
        if (normalizedUrl == null) {
            throw new LineageReportingException("HTTP reporter url is required", null);
        }
        try {
            return new HttpReporterConfig(
                    new URL(normalizedUrl), token, connectTimeoutMs, readTimeoutMs, retries);
        } catch (MalformedURLException error) {
            throw new LineageReportingException("HTTP reporter url is invalid: " + normalizedUrl, error);
        }
    }

    URL getUrl() {
        return url;
    }

    String getToken() {
        return token;
    }

    int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    int getRetries() {
        return retries;
    }

    private static String read(String property, String env, String defaultValue) {
        String value = normalizeNullable(System.getProperty(property));
        if (value != null) {
            return value;
        }
        value = normalizeNullable(System.getenv(env));
        return value == null ? defaultValue : value;
    }

    private static int parsePositiveInt(String value, String name) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                throw new NumberFormatException("not positive");
            }
            return parsed;
        } catch (NumberFormatException error) {
            throw new LineageReportingException(name + " must be a positive integer", error);
        }
    }

    private static int parseRetries(String value) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0 || parsed > 3) {
                throw new NumberFormatException("out of range");
            }
            return parsed;
        } catch (NumberFormatException error) {
            throw new LineageReportingException(RETRIES_PROPERTY + " must be an integer between 0 and 3", error);
        }
    }

    private static String normalizeNullable(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }
}
