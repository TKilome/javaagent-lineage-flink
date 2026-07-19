package io.github.tkilome.flink.lineage.reporter.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.tkilome.flink.lineage.exception.LineageReportingException;
import io.github.tkilome.flink.lineage.model.KafkaDatasetProperties;
import io.github.tkilome.flink.lineage.model.LineageDataset;
import io.github.tkilome.flink.lineage.model.LineageDirection;
import io.github.tkilome.flink.lineage.model.LineageEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpLineageReporterTest {

    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void postsLineageEventJson() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> contentType = new AtomicReference<>();
        AtomicReference<String> body = new AtomicReference<>();
        startServer(
                exchange -> {
                    method.set(exchange.getRequestMethod());
                    contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
                    body.set(readRequestBody(exchange));
                    exchange.sendResponseHeaders(204, -1);
                });

        new HttpLineageReporter(config(serverUrl("/lineage"), null, 0)).report(event());

        assertEquals("POST", method.get());
        assertEquals("application/json; charset=UTF-8", contentType.get());
        assertTrue(body.get().contains("\"engineType\":\"flink\""));
        assertTrue(body.get().contains("\"jobId\":\"job-1\""));
        assertTrue(body.get().contains("\"sources\""));
    }

    @Test
    void sendsBearerTokenWhenConfigured() throws Exception {
        AtomicReference<String> authorization = new AtomicReference<>();
        startServer(
                exchange -> {
                    authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                    exchange.sendResponseHeaders(200, -1);
                });

        new HttpLineageReporter(config(serverUrl("/lineage"), "secret", 0)).report(event());

        assertEquals("Bearer secret", authorization.get());
    }

    @Test
    void throwsWhenServerReturnsError() throws Exception {
        startServer(exchange -> exchange.sendResponseHeaders(500, -1));

        HttpLineageReporter reporter = new HttpLineageReporter(config(serverUrl("/lineage"), null, 0));

        assertThrows(LineageReportingException.class, () -> reporter.report(event()));
    }

    @Test
    void retriesServerErrors() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        startServer(
                exchange -> {
                    if (calls.incrementAndGet() == 1) {
                        exchange.sendResponseHeaders(500, -1);
                    } else {
                        exchange.sendResponseHeaders(200, -1);
                    }
                });

        new HttpLineageReporter(config(serverUrl("/lineage"), null, 1)).report(event());

        assertEquals(2, calls.get());
    }

    private void startServer(ExchangeHandler handler) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/lineage", handler::handle);
        server.start();
    }

    private String serverUrl(String path) {
        return "http://127.0.0.1:" + server.getAddress().getPort() + path;
    }

    private static HttpReporterConfig config(String url, String token, int retries) {
        return HttpReporterConfig.fromValues(url, token, 1000, 1000, retries);
    }

    private static LineageEvent event() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(KafkaDatasetProperties.TOPIC, "orders");
        properties.put(KafkaDatasetProperties.BOOTSTRAP_SERVERS, "broker:9092");
        return new LineageEvent(
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
    }

    private static String readRequestBody(HttpExchange exchange) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = exchange.getRequestBody().read(buffer)) >= 0) {
            output.write(buffer, 0, read);
        }
        return new String(output.toByteArray(), StandardCharsets.UTF_8);
    }

    private interface ExchangeHandler {
        void handle(HttpExchange exchange) throws IOException;
    }
}
