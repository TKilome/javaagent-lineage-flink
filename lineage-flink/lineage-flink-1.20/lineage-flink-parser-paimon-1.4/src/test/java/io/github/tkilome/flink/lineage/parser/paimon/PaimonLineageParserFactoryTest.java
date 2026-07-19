package io.github.tkilome.flink.lineage.parser.paimon;

import io.github.tkilome.flink.lineage.api.parser.LineageParseResult;
import io.github.tkilome.flink.lineage.context.LineageNode;
import io.github.tkilome.flink.lineage.context.ParserContext;
import io.github.tkilome.flink.lineage.model.LineageDataset;
import io.github.tkilome.flink.lineage.model.LineageDirection;
import io.github.tkilome.flink.lineage.model.PaimonDatasetProperties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.sink.RowDataStoreWriteOperator;
import org.apache.paimon.flink.sink.cdc.CdcDynamicTableParsingProcessFunction;
import org.apache.paimon.flink.sink.cdc.CdcRecordStoreMultiWriteOperator;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.FlinkSource;
import org.apache.paimon.flink.source.PendingSplitsCheckpoint;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonLineageParserFactoryTest {

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void parsesPaimonSourceFromSourceOperatorFactory() {
        PaimonLineageParserFactory factory = new PaimonLineageParserFactory();
        TestFlinkSource source =
                new TestFlinkSource(
                        new TestReadBuilder(
                                table(
                                        "file:/warehouse",
                                        "ods.orders",
                                        "file:/warehouse/ods/orders")));
        SourceOperatorFactory operatorFactory =
                new SourceOperatorFactory<>(source, WatermarkStrategy.noWatermarks());

        LineageParseResult result = factory.create(context()).parse(node("source", operatorFactory));

        assertTrue(result.isMatched());
        assertEquals(1, result.getDatasets().size());
        assertPaimonDataset(
                result.getDatasets().get(0),
                LineageDirection.SOURCE,
                null,
                "ods.orders",
                "EXACT_TABLE");
        assertEquals(
                "file:/warehouse/ods/orders",
                result.getDatasets().get(0).getProperties().get(PaimonDatasetProperties.TABLE_PATH));
    }

    @Test
    void parsesExactTableSinkFromCoordinatedFactory() {
        PaimonLineageParserFactory factory = new PaimonLineageParserFactory();
        RowDataStoreWriteOperator.CoordinatedFactory operatorFactory =
                new RowDataStoreWriteOperator.CoordinatedFactory(
                        table("file:/warehouse", "dwd.orders", "file:/warehouse/dwd/orders"),
                        null,
                        "lineage-test");

        LineageParseResult result = factory.create(context()).parse(node("sink", operatorFactory));

        assertTrue(result.isMatched());
        assertEquals(1, result.getDatasets().size());
        assertPaimonDataset(
                result.getDatasets().get(0),
                LineageDirection.SINK,
                null,
                "dwd.orders",
                "EXACT_TABLE");
    }

    @Test
    void parsesCombinedCdcSinkWhenWriterAndParserContextExist() {
        PaimonLineageParserFactory factory = new PaimonLineageParserFactory();
        RichCdcMultiplexRecordEventParser eventParser =
                richParser("db_.*", "tmp_.*", "orders_.*", "test_.*", tableNameConverter());
        CdcDynamicTableParsingProcessFunction userFunction =
                new TestCdcDynamicTableParsingProcessFunction(eventParser, "ods", "file:/warehouse");
        ProcessOperator<Object, Object> processOperator = new ProcessOperator<>(userFunction);
        LineageNode processNode = node("process", SimpleOperatorFactory.of(processOperator));
        LineageNode writerNode =
                node(
                        "writer",
                        new CdcRecordStoreMultiWriteOperator.Factory(
                                null, null, "lineage-test", new Options()));

        LineageParseResult result =
                factory.create(context(processNode, writerNode)).parse(writerNode);

        assertTrue(result.isMatched());
        assertEquals(1, result.getDatasets().size());
        LineageDataset dataset = result.getDatasets().get(0);
        assertEquals(LineageDirection.SINK, dataset.getDirection());
        assertEquals(PaimonDatasetProperties.CONNECTOR, dataset.getConnector());
        assertEquals("file:/warehouse", dataset.getNamespace());
        assertEquals("ods.*", dataset.getName());
        assertEquals("CDC_COMBINED", dataset.getProperties().get(PaimonDatasetProperties.SINK_MODE));
        assertEquals("ods", dataset.getProperties().get(PaimonDatasetProperties.TARGET_DATABASE));
        assertEquals("*", dataset.getProperties().get(PaimonDatasetProperties.TARGET_TABLE));
        assertEquals("db_.*", dataset.getProperties().get(PaimonDatasetProperties.SOURCE_DATABASE_INCLUDE_PATTERN));
        assertEquals("tmp_.*", dataset.getProperties().get(PaimonDatasetProperties.SOURCE_DATABASE_EXCLUDE_PATTERN));
        assertEquals("orders_.*", dataset.getProperties().get(PaimonDatasetProperties.SOURCE_TABLE_INCLUDE_PATTERN));
        assertEquals("test_.*", dataset.getProperties().get(PaimonDatasetProperties.SOURCE_TABLE_EXCLUDE_PATTERN));
        assertEquals("true", dataset.getProperties().get(PaimonDatasetProperties.TABLE_NAME_CASE_SENSITIVE));
        assertEquals("true", dataset.getProperties().get(PaimonDatasetProperties.TABLE_NAME_MERGE_SHARDS));
        assertEquals("pre_", dataset.getProperties().get(PaimonDatasetProperties.TABLE_NAME_PREFIX));
        assertEquals("_suf", dataset.getProperties().get(PaimonDatasetProperties.TABLE_NAME_SUFFIX));
        assertEquals("{\"source_db\":\"ods_\"}", dataset.getProperties().get(PaimonDatasetProperties.DATABASE_PREFIX_MAPPING));
        assertEquals("{\"source_db\":\"_bak\"}", dataset.getProperties().get(PaimonDatasetProperties.DATABASE_SUFFIX_MAPPING));
        assertEquals(
                "{\"source_db.source_orders\":\"orders\"}",
                dataset.getProperties().get(PaimonDatasetProperties.TABLE_NAME_MAPPING));
        assertFalse(dataset.getProperties().containsKey("dbIncludingPattern"));
        assertFalse(dataset.getProperties().containsKey("tblIncludingPattern"));
        assertFalse(dataset.getProperties().containsKey("cdcSourceDbIncludingPattern"));
        assertFalse(dataset.getProperties().containsKey("tableNameConverterClass"));
        assertFalse(dataset.getProperties().containsKey("tableNameConverter.caseSensitive"));
        assertFalse(dataset.getProperties().containsKey("tableResolvedAt"));
    }

    @Test
    void combinedCdcWriterWithoutParserContextIsMatchedButEmpty() {
        PaimonLineageParserFactory factory = new PaimonLineageParserFactory();
        LineageParseResult result =
                factory.create(context())
                        .parse(
                                node(
                                        "writer",
                                        new CdcRecordStoreMultiWriteOperator.Factory(
                                                null, null, "lineage-test", new Options())));

        assertTrue(result.isMatched());
        assertTrue(result.getDatasets().isEmpty());
    }

    @Test
    void unmatchedNodeReturnsNotMatched() {
        PaimonLineageParserFactory factory = new PaimonLineageParserFactory();

        LineageParseResult result = factory.create(context()).parse(node("map", new Object()));

        assertFalse(result.isMatched());
    }

    private static FileStoreTable table(String warehouse, String fullName, String tablePath) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("path", tablePath);
        CatalogEnvironment catalogEnvironment = CatalogEnvironment.empty();
        return (FileStoreTable)
                Proxy.newProxyInstance(
                        PaimonLineageParserFactoryTest.class.getClassLoader(),
                        new Class<?>[] {FileStoreTable.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "fullName":
                                case "name":
                                    return fullName;
                                case "options":
                                    return options;
                                case "catalogEnvironment":
                                    return catalogEnvironment;
                                case "partitionKeys":
                                case "primaryKeys":
                                    return Collections.emptyList();
                                case "comment":
                                case "statistics":
                                    return Optional.empty();
                                case "copy":
                                case "copyWithoutTimeTravel":
                                    return proxy;
                                case "toString":
                                    return fullName;
                                default:
                                    return defaultValue(method.getReturnType());
                            }
                        });
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (boolean.class.equals(type)) {
            return false;
        }
        if (char.class.equals(type)) {
            return '\0';
        }
        if (byte.class.equals(type)) {
            return (byte) 0;
        }
        if (short.class.equals(type)) {
            return (short) 0;
        }
        if (int.class.equals(type)) {
            return 0;
        }
        if (long.class.equals(type)) {
            return 0L;
        }
        if (float.class.equals(type)) {
            return 0F;
        }
        if (double.class.equals(type)) {
            return 0D;
        }
        return null;
    }

    private static RichCdcMultiplexRecordEventParser richParser(
            String dbIncludingPattern,
            String dbExcludingPattern,
            String tblIncludingPattern,
            String tblExcludingPattern,
            TableNameConverter tableNameConverter) {
        return new RichCdcMultiplexRecordEventParser(
                null,
                Pattern.compile(tblIncludingPattern),
                Pattern.compile(tblExcludingPattern),
                Pattern.compile(dbIncludingPattern),
                Pattern.compile(dbExcludingPattern),
                tableNameConverter,
                Collections.emptySet());
    }

    private static TableNameConverter tableNameConverter() {
        Map<String, String> dbPrefix = new LinkedHashMap<>();
        dbPrefix.put("source_db", "ods_");
        Map<String, String> dbSuffix = new LinkedHashMap<>();
        dbSuffix.put("source_db", "_bak");
        Map<String, String> tableMapping = new LinkedHashMap<>();
        tableMapping.put("source_db.source_orders", "orders");
        return new TableNameConverter(
                true, true, dbPrefix, dbSuffix, "pre_", "_suf", tableMapping);
    }

    private static void assertPaimonDataset(
            LineageDataset dataset,
            LineageDirection direction,
            String namespace,
            String name,
            String mode) {
        assertEquals(direction, dataset.getDirection());
        assertEquals(PaimonDatasetProperties.CONNECTOR, dataset.getConnector());
        assertEquals(namespace, dataset.getNamespace());
        assertEquals(name, dataset.getName());
        assertEquals(
                mode,
                dataset.getProperties()
                        .get(
                                direction == LineageDirection.SOURCE
                                        ? PaimonDatasetProperties.SOURCE_MODE
                                        : PaimonDatasetProperties.SINK_MODE));
        if (namespace == null) {
            assertFalse(dataset.getProperties().containsKey(PaimonDatasetProperties.WAREHOUSE));
        } else {
            assertEquals(namespace, dataset.getProperties().get(PaimonDatasetProperties.WAREHOUSE));
        }
        assertEquals(name, dataset.getProperties().get(PaimonDatasetProperties.FULL_NAME));
    }

    private static ParserContext context(LineageNode... nodes) {
        List<LineageNode> allNodes = Arrays.asList(nodes);
        return new ParserContext() {
            @Override
            public Collection<LineageNode> nodes() {
                return allNodes;
            }
        };
    }

    private static LineageNode node(String nodeId, Object operatorFactory) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("operatorFactory", operatorFactory);
        return new LineageNode() {
            @Override
            public String getNodeId() {
                return nodeId;
            }

            @Override
            public String getNodeName() {
                return nodeId;
            }

            @Override
            public Object getNativeNode() {
                return null;
            }

            @Override
            public ClassLoader getRuntimeClassLoader() {
                return operatorFactory == null ? null : operatorFactory.getClass().getClassLoader();
            }

            @Override
            public List<String> getInputNodeIds() {
                return Collections.emptyList();
            }

            @Override
            public List<String> getOutputNodeIds() {
                return Collections.emptyList();
            }

            @Override
            public Map<String, Object> getAttributes() {
                return attributes;
            }
        };
    }

    private static final class TestCdcDynamicTableParsingProcessFunction
            extends CdcDynamicTableParsingProcessFunction {
        private final RichCdcMultiplexRecordEventParser parser;
        private final String warehouse;

        private TestCdcDynamicTableParsingProcessFunction(
                RichCdcMultiplexRecordEventParser parser, String targetDatabase, String warehouse) {
            super(
                    targetDatabase,
                    (CatalogLoader) () -> null,
                    (EventParser.Factory) () -> parser);
            this.parser = parser;
            this.warehouse = warehouse;
        }
    }

    private static final class TestFlinkSource extends FlinkSource {
        private TestFlinkSource(ReadBuilder readBuilder) {
            super(readBuilder, null, null, false);
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
                SplitEnumeratorContext<FileStoreSourceSplit> enumContext,
                PendingSplitsCheckpoint checkpoint) {
            return null;
        }
    }

    private static final class TestReadBuilder implements ReadBuilder {
        private final Table table;

        private TestReadBuilder(Table table) {
            this.table = table;
        }

        @Override
        public String tableName() {
            return table.fullName();
        }

        @Override
        public RowType readType() {
            return null;
        }

        @Override
        public ReadBuilder withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public ReadBuilder withPartitionFilter(Map<String, String> partitionFilter) {
            return this;
        }

        @Override
        public ReadBuilder withPartitionFilter(PartitionPredicate partitionPredicate) {
            return this;
        }

        @Override
        public ReadBuilder withBucket(int bucket) {
            return this;
        }

        @Override
        public ReadBuilder withBucketFilter(Filter<Integer> bucketFilter) {
            return this;
        }

        @Override
        public ReadBuilder withReadType(RowType readType) {
            return this;
        }

        @Override
        public ReadBuilder withProjection(int[] projection) {
            return this;
        }

        @Override
        public ReadBuilder withLimit(int limit) {
            return this;
        }

        @Override
        public ReadBuilder withTopN(TopN topN) {
            return this;
        }

        @Override
        public ReadBuilder withShard(int shardIndexOfThisSubtask, int shardNumberOfParallelSubtasks) {
            return this;
        }

        @Override
        public ReadBuilder withRowRanges(List<Range> rowRanges) {
            return this;
        }

        @Override
        public ReadBuilder withRowRangeIndex(RowRangeIndex rowRangeIndex) {
            return this;
        }

        @Override
        public ReadBuilder dropStats() {
            return this;
        }

        @Override
        public TableScan newScan() {
            return null;
        }

        @Override
        public StreamTableScan newStreamScan() {
            return null;
        }

        @Override
        public TableRead newRead() {
            return null;
        }
    }
}
