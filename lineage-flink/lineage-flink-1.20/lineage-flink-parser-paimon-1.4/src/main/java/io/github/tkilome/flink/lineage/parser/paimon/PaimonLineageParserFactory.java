package io.github.tkilome.flink.lineage.parser.paimon;

import io.github.tkilome.flink.lineage.api.factory.LineageParserFactory;
import io.github.tkilome.flink.lineage.api.parser.LineageParseResult;
import io.github.tkilome.flink.lineage.api.parser.LineageParser;
import io.github.tkilome.flink.lineage.context.LineageNode;
import io.github.tkilome.flink.lineage.context.ParserContext;
import io.github.tkilome.flink.lineage.exception.LineageParsingException;
import io.github.tkilome.flink.lineage.model.LineageDataset;
import io.github.tkilome.flink.lineage.model.LineageDirection;
import io.github.tkilome.flink.lineage.model.PaimonDatasetProperties;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.sink.cdc.CdcDynamicTableParsingProcessFunction;
import org.apache.paimon.flink.sink.cdc.CdcRecordStoreMultiWriteOperator;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.flink.source.FlinkSource;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Paimon parser factory for Flink 1.20 and Paimon 1.4 runtime classes.
 *
 * <p>This module intentionally avoids compile-time Paimon dependencies because the target runtime
 * connector jar is supplied by the Flink cluster. Matching is still version-specific: it relies on
 * exact Paimon/Flink class names and fixed fields or methods for this adapter line.
 */
public final class PaimonLineageParserFactory implements LineageParserFactory {

    private static final String PATH = "path";

    private static final String TABLE_WRITE_COORDINATED_FACTORY =
            "org.apache.paimon.flink.sink.TableWriteOperator$CoordinatedFactory";

    @Override
    public String factoryName() {
        return "paimon-1.4-1.20-parser";
    }

    /**
     * Creates a Paimon parser. The parser pre-scans all job nodes to capture CDC COMBINED pattern
     * context from the upstream dynamic table parsing process function.
     *
     * @param context parser context containing all StreamGraph nodes
     * @return parser that emits supported Paimon source and sink datasets
     */
    @Override
    public LineageParser create(ParserContext context) {
        CombinedCdcContext combinedCdcContext = findCombinedCdcContext(context);
        return node -> parsePaimonEndpoints(node, combinedCdcContext);
    }

    private static LineageParseResult parsePaimonEndpoints(
            LineageNode node, CombinedCdcContext combinedCdcContext) {
        LineageParseResult sourceResult = parseSource(node);
        LineageParseResult exactSinkResult = parseExactTableSink(node);
        LineageParseResult combinedSinkResult = parseCombinedCdcSink(node, combinedCdcContext);
        if (!sourceResult.isMatched()
                && !exactSinkResult.isMatched()
                && !combinedSinkResult.isMatched()) {
            return LineageParseResult.notMatched();
        }

        List<LineageDataset> datasets = new ArrayList<>();
        datasets.addAll(sourceResult.getDatasets());
        datasets.addAll(exactSinkResult.getDatasets());
        datasets.addAll(combinedSinkResult.getDatasets());
        return LineageParseResult.matched(datasets);
    }

    private static LineageParseResult parseSource(LineageNode node) {
        Object operatorFactory = operatorFactory(node);
        if (!(operatorFactory instanceof SourceOperatorFactory<?>)) {
            return LineageParseResult.notMatched();
        }
        Object source = readField(operatorFactory, "source");
        if (!(source instanceof FlinkSource)) {
            return LineageParseResult.notMatched();
        }

        Object readBuilder = readField(source, "readBuilder");
        Table table = readField(readBuilder, "table", Table.class);
        LineageDataset dataset = exactTableDataset(LineageDirection.SOURCE, "EXACT_TABLE", table);
        return LineageParseResult.matched(
                dataset == null ? Collections.emptyList() : Collections.singletonList(dataset));
    }

    private static LineageParseResult parseExactTableSink(LineageNode node) {
        Object operatorFactory = operatorFactory(node);
        if (!isInstanceOf(operatorFactory, TABLE_WRITE_COORDINATED_FACTORY)) {
            return LineageParseResult.notMatched();
        }

        Table table = readField(operatorFactory, "table", Table.class);
        LineageDataset dataset = exactTableDataset(LineageDirection.SINK, "EXACT_TABLE", table);
        return LineageParseResult.matched(
                dataset == null ? Collections.emptyList() : Collections.singletonList(dataset));
    }

    private static LineageParseResult parseCombinedCdcSink(
            LineageNode node, CombinedCdcContext combinedCdcContext) {
        Object operatorFactory = operatorFactory(node);
        if (!(operatorFactory instanceof CdcRecordStoreMultiWriteOperator.Factory)) {
            return LineageParseResult.notMatched();
        }
        if (combinedCdcContext == null) {
            return LineageParseResult.matched(Collections.emptyList());
        }

        Map<String, String> properties = new LinkedHashMap<>();
        putIfPresent(
                properties,
                PaimonDatasetProperties.SOURCE_DATABASE_INCLUDE_PATTERN,
                combinedCdcContext.getSourceDatabaseIncludePattern());
        putIfPresent(
                properties,
                PaimonDatasetProperties.SOURCE_DATABASE_EXCLUDE_PATTERN,
                combinedCdcContext.getSourceDatabaseExcludePattern());
        putIfPresent(
                properties,
                PaimonDatasetProperties.SOURCE_TABLE_INCLUDE_PATTERN,
                combinedCdcContext.getSourceTableIncludePattern());
        putIfPresent(
                properties,
                PaimonDatasetProperties.SOURCE_TABLE_EXCLUDE_PATTERN,
                combinedCdcContext.getSourceTableExcludePattern());
        putAllIfPresent(properties, combinedCdcContext.getTableNameConverterProperties());

        return LineageParseResult.matched(
                Collections.singletonList(
                        PaimonDatasetProperties.combinedCdcSink(
                                combinedCdcContext.getWarehouse(),
                                combinedCdcContext.getTargetDatabase(),
                                properties)));
    }

    private static LineageDataset exactTableDataset(
            LineageDirection direction, String mode, Table table) {
        if (table == null) {
            return null;
        }
        String fullName = trimToNull(table.fullName());
        if (fullName == null) {
            return null;
        }
        String warehouse = readWarehouse(table);
        String tablePath = readTablePath(table);

        return PaimonDatasetProperties.exactTable(direction, mode, fullName, warehouse, tablePath);
    }

    private static CombinedCdcContext findCombinedCdcContext(ParserContext context) {
        if (context == null || context.nodes() == null) {
            return null;
        }
        for (LineageNode node : context.nodes()) {
            Object operator = unwrapOperator(operatorFactory(node));
            if (!(operator instanceof ProcessOperator<?, ?>)) {
                continue;
            }
            Object userFunction = ((ProcessOperator<?, ?>) operator).getUserFunction();
            if (!(userFunction instanceof CdcDynamicTableParsingProcessFunction<?>)) {
                continue;
            }
            RichCdcMultiplexRecordEventParser parser = findRichCdcParser(userFunction);
            if (parser == null) {
                continue;
            }
            return new CombinedCdcContext(
                    readFieldAsString(userFunction, "database"),
                    readPattern(parser, "dbIncludingPattern"),
                    readPattern(parser, "dbExcludingPattern"),
                    readPattern(parser, "tblIncludingPattern"),
                    readPattern(parser, "tblExcludingPattern"),
                    readWarehouseFromCombinedFunction(userFunction),
                    tableNameConverterProperties(readTableNameConverter(parser)));
        }
        return null;
    }

    private static Map<String, String> tableNameConverterProperties(
            TableNameConverter tableNameConverter) {
        if (tableNameConverter == null) {
            return Collections.emptyMap();
        }
        Map<String, String> properties = new LinkedHashMap<>();
        putIfPresent(
                properties,
                PaimonDatasetProperties.TABLE_NAME_CASE_SENSITIVE,
                readFieldAsString(tableNameConverter, "caseSensitive"));
        putIfPresent(
                properties,
                PaimonDatasetProperties.TABLE_NAME_MERGE_SHARDS,
                readFieldAsString(tableNameConverter, "mergeShards"));
        putIfPresent(
                properties,
                PaimonDatasetProperties.TABLE_NAME_PREFIX,
                readFieldAsString(tableNameConverter, "prefix"));
        putIfPresent(
                properties,
                PaimonDatasetProperties.TABLE_NAME_SUFFIX,
                readFieldAsString(tableNameConverter, "suffix"));
        putIfPresent(
                properties,
                PaimonDatasetProperties.DATABASE_PREFIX_MAPPING,
                mapFieldAsJson(tableNameConverter, "dbPrefix"));
        putIfPresent(
                properties,
                PaimonDatasetProperties.DATABASE_SUFFIX_MAPPING,
                mapFieldAsJson(tableNameConverter, "dbSuffix"));
        putIfPresent(
                properties,
                PaimonDatasetProperties.TABLE_NAME_MAPPING,
                mapFieldAsJson(tableNameConverter, "tableMapping"));
        return properties;
    }

    private static RichCdcMultiplexRecordEventParser findRichCdcParser(Object userFunction) {
        for (Field field : allFields(userFunction.getClass())) {
            try {
                field.setAccessible(true);
                Object value = field.get(userFunction);
                if (value instanceof RichCdcMultiplexRecordEventParser) {
                    return (RichCdcMultiplexRecordEventParser) value;
                }
                Object created = createParser(value);
                if (created instanceof RichCdcMultiplexRecordEventParser) {
                    return (RichCdcMultiplexRecordEventParser) created;
                }
            } catch (IllegalAccessException | RuntimeException error) {
                throw parsingException("Failed to inspect Paimon CDC parser field " + field.getName(), error);
            }
        }
        return null;
    }

    private static Object createParser(Object parserFactory) {
        if (parserFactory == null) {
            return null;
        }
        return invoke(parserFactory, "create");
    }

    private static TableNameConverter readTableNameConverter(
            RichCdcMultiplexRecordEventParser parser) {
        return readField(parser, "tableNameConverter", TableNameConverter.class);
    }

    private static String readWarehouse(Table table) {
        if (!(table instanceof FileStoreTable)) {
            return null;
        }
        CatalogContext catalogContext = ((FileStoreTable) table).catalogEnvironment().catalogContext();
        return catalogContext == null ? null : trimToNull(catalogContext.options().get(CatalogOptions.WAREHOUSE));
    }

    private static String readTablePath(Table table) {
        Map<String, String> options = table.options();
        return options == null ? null : trimToNull(options.get(PATH));
    }

    private static String readWarehouseFromCombinedFunction(Object userFunction) {
        String warehouse = trimToNull(readFieldAsString(userFunction, PaimonDatasetProperties.WAREHOUSE));
        if (warehouse != null) {
            return warehouse;
        }
        Object catalogLoader = readField(userFunction, "catalogLoader");
        Object catalog = invoke(catalogLoader, "load");
        Object catalogContext = invoke(catalog, "catalogContext");
        Object options = invoke(catalogContext, "options");
        return optionValue(options, PaimonDatasetProperties.WAREHOUSE);
    }

    private static Object operatorFactory(LineageNode node) {
        return node == null || node.getAttributes() == null
                ? null
                : node.getAttributes().get("operatorFactory");
    }

    private static Object unwrapOperator(Object operatorFactory) {
        Object operator = invoke(operatorFactory, "getOperator");
        return operator == null ? operatorFactory : operator;
    }

    private static String readPattern(Object target, String fieldName) {
        Object value = readField(target, fieldName);
        if (value instanceof Pattern) {
            return ((Pattern) value).pattern();
        }
        return trimToNull(value == null ? null : String.valueOf(value));
    }

    private static String readFieldAsString(Object target, String fieldName) {
        Object value = readField(target, fieldName);
        return trimToNull(value == null ? null : String.valueOf(value));
    }

    private static String mapFieldAsJson(Object target, String fieldName) {
        Object value = readField(target, fieldName);
        if (!(value instanceof Map<?, ?>)) {
            return null;
        }
        Map<?, ?> map = (Map<?, ?>) value;
        StringBuilder result = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!first) {
                result.append(',');
            }
            result.append('"')
                    .append(escapeJson(String.valueOf(entry.getKey())))
                    .append("\":\"")
                    .append(escapeJson(String.valueOf(entry.getValue())))
                    .append('"');
            first = false;
        }
        return result.append('}').toString();
    }

    private static String escapeJson(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String optionValue(Object options, String key) {
        if (options == null) {
            return null;
        }
        if (options instanceof Map<?, ?>) {
            Object value = ((Map<?, ?>) options).get(key);
            return trimToNull(value == null ? null : String.valueOf(value));
        }
        Object map = invoke(options, "toMap");
        if (map instanceof Map<?, ?>) {
            Object value = ((Map<Object, Object>) map).get(key);
            return trimToNull(value == null ? null : String.valueOf(value));
        }
        Object value = invoke(options, "getString", new Class<?>[] {String.class}, new Object[] {key});
        return trimToNull(value == null ? null : String.valueOf(value));
    }

    private static Object readField(Object target, String fieldName) {
        if (target == null) {
            return null;
        }
        for (Field field : allFields(target.getClass())) {
            if (!field.getName().equals(fieldName)) {
                continue;
            }
            try {
                field.setAccessible(true);
                return field.get(target);
            } catch (IllegalAccessException | RuntimeException error) {
                throw parsingException(
                        "Failed to access " + target.getClass().getName() + "#" + fieldName,
                        error);
            }
        }
        return null;
    }

    private static <T> T readField(Object target, String fieldName, Class<T> expectedType) {
        Object value = readField(target, fieldName);
        return expectedType.isInstance(value) ? expectedType.cast(value) : null;
    }

    private static Object invoke(Object target, String methodName) {
        return invoke(target, methodName, new Class<?>[0], new Object[0]);
    }

    private static Object invoke(
            Object target, String methodName, Class<?>[] parameterTypes, Object[] arguments) {
        if (target == null) {
            return null;
        }
        try {
            Method method = target.getClass().getMethod(methodName, parameterTypes);
            method.setAccessible(true);
            return method.invoke(target, arguments);
        } catch (NoSuchMethodException error) {
            return null;
        } catch (ReflectiveOperationException | RuntimeException error) {
            throw parsingException(
                    "Failed to invoke " + target.getClass().getName() + "#" + methodName,
                    error);
        }
    }

    private static boolean isInstanceOf(Object value, String className) {
        if (value == null) {
            return false;
        }
        try {
            Class<?> type = Class.forName(className, false, value.getClass().getClassLoader());
            return type.isInstance(value);
        } catch (ClassNotFoundException error) {
            return false;
        }
    }

    private static List<Field> allFields(Class<?> type) {
        List<Field> fields = new ArrayList<>();
        Class<?> current = type;
        while (current != null && current != Object.class) {
            Collections.addAll(fields, current.getDeclaredFields());
            current = current.getSuperclass();
        }
        return fields;
    }

    private static void putIfPresent(Map<String, String> properties, String key, String value) {
        String normalized = trimToNull(value);
        if (normalized != null) {
            properties.put(key, normalized);
        }
    }

    private static void putAllIfPresent(
            Map<String, String> properties, Map<String, String> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : values.entrySet()) {
            putIfPresent(properties, entry.getKey(), entry.getValue());
        }
    }

    private static String trimToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private static LineageParsingException parsingException(String message, Throwable cause) {
        return new LineageParsingException(message, cause);
    }

    private static final class CombinedCdcContext {
        private final String targetDatabase;
        private final String sourceDatabaseIncludePattern;
        private final String sourceDatabaseExcludePattern;
        private final String sourceTableIncludePattern;
        private final String sourceTableExcludePattern;
        private final String warehouse;
        private final Map<String, String> tableNameConverterProperties;

        private CombinedCdcContext(
                String targetDatabase,
                String sourceDatabaseIncludePattern,
                String sourceDatabaseExcludePattern,
                String sourceTableIncludePattern,
                String sourceTableExcludePattern,
                String warehouse,
                Map<String, String> tableNameConverterProperties) {
            this.targetDatabase = targetDatabase;
            this.sourceDatabaseIncludePattern = sourceDatabaseIncludePattern;
            this.sourceDatabaseExcludePattern = sourceDatabaseExcludePattern;
            this.sourceTableIncludePattern = sourceTableIncludePattern;
            this.sourceTableExcludePattern = sourceTableExcludePattern;
            this.warehouse = warehouse;
            this.tableNameConverterProperties =
                    tableNameConverterProperties == null
                            ? Collections.emptyMap()
                            : tableNameConverterProperties;
        }

        private String getTargetDatabase() {
            return targetDatabase;
        }

        private String getSourceDatabaseIncludePattern() {
            return sourceDatabaseIncludePattern;
        }

        private String getSourceDatabaseExcludePattern() {
            return sourceDatabaseExcludePattern;
        }

        private String getSourceTableIncludePattern() {
            return sourceTableIncludePattern;
        }

        private String getSourceTableExcludePattern() {
            return sourceTableExcludePattern;
        }

        private String getWarehouse() {
            return warehouse;
        }

        private Map<String, String> getTableNameConverterProperties() {
            return tableNameConverterProperties;
        }
    }
}
