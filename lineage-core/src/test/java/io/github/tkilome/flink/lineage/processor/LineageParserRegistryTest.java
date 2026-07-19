package io.github.tkilome.flink.lineage.processor;

import io.github.tkilome.flink.lineage.api.parser.LineageParser;
import io.github.tkilome.flink.lineage.api.parser.LineageParseResult;
import io.github.tkilome.flink.lineage.context.ParserContext;
import io.github.tkilome.flink.lineage.context.LineageNode;
import io.github.tkilome.flink.lineage.api.factory.LineageParserFactory;
import io.github.tkilome.flink.lineage.model.LineageDataset;
import io.github.tkilome.flink.lineage.model.LineageDirection;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LineageParserRegistryTest {

    @Test
    void returnsNoDatasetsWhenNoParserMatches() {
        LineageParserRegistry registry =
                new LineageParserRegistry(
                        Collections.singletonList(factory("kafka", LineageParseResult.notMatched())));

        assertEquals(Collections.emptyList(), registry.parse(LineageNode.empty("1")));
    }

    @Test
    void collectsDatasetsFromAllMatchedParsers() {
        LineageParserRegistry registry =
                new LineageParserRegistry(
                        Arrays.asList(
                                factory("other", LineageParseResult.notMatched()),
                                factory("kafka-source", matchedDataset("orders")),
                                factory("kafka-sink", matchedDataset("orders-out"))));

        List<LineageDataset> datasets = registry.parse(LineageNode.empty("1"));

        assertEquals(2, datasets.size());
        assertEquals("orders", datasets.get(0).getName());
        assertEquals("orders-out", datasets.get(1).getName());
    }

    @Test
    void treatsMatchedEmptyAsNoDatasetForThisNode() {
        LineageParserRegistry registry =
                new LineageParserRegistry(
                        Arrays.asList(
                                factory("kafka-source", LineageParseResult.matched(Collections.emptyList())),
                                factory("kafka-sink", matchedDataset("orders-out"))));

        List<LineageDataset> datasets = registry.parse(LineageNode.empty("1"));

        assertEquals(1, datasets.size());
        assertEquals("orders-out", datasets.get(0).getName());
    }

    @Test
    void parserFactoryReceivesAllNodesInContext() {
        LineageParserRegistry registry =
                new LineageParserRegistry(
                        Collections.singletonList(
                                new LineageParserFactory() {
                                    @Override
                                    public String factoryName() {
                                        return "context-aware";
                                    }

                                    @Override
                                    public LineageParser create(ParserContext context) {
                                        return node ->
                                                matchedDataset(
                                                        "nodes-" + context.nodes().size());
                                    }
                                }));

        List<LineageDataset> datasets =
                registry.parse(Arrays.asList(LineageNode.empty("1"), LineageNode.empty("2")));

        assertEquals(2, datasets.size());
        assertEquals("nodes-2", datasets.get(0).getName());
        assertEquals("nodes-2", datasets.get(1).getName());
    }

    @Test
    void createsParserOncePerFactoryForOneJobParse() {
        AtomicInteger creates = new AtomicInteger();
        LineageParserRegistry registry =
                new LineageParserRegistry(
                        Collections.singletonList(
                                new LineageParserFactory() {
                                    @Override
                                    public String factoryName() {
                                        return "cached-parser";
                                    }

                                    @Override
                                    public LineageParser create(ParserContext context) {
                                        creates.incrementAndGet();
                                        return node -> matchedDataset(node.getNodeId());
                                    }
                                }));

        List<LineageDataset> datasets =
                registry.parse(Arrays.asList(LineageNode.empty("1"), LineageNode.empty("2")));

        assertEquals(1, creates.get());
        assertEquals(2, datasets.size());
    }

    private static LineageParserFactory factory(
            final String name, final LineageParseResult parseResult) {
        return new LineageParserFactory() {
            @Override
            public String factoryName() {
                return name;
            }

            @Override
            public LineageParser create(ParserContext context) {
                return node -> parseResult;
            }
        };
    }

    private static LineageParseResult matchedDataset(String topic) {
        return LineageParseResult.matched(
                Collections.singletonList(
                        new LineageDataset(
                                LineageDirection.SOURCE,
                                "kafka",
                                null,
                                topic,
                                Collections.emptyMap())));
    }
}
