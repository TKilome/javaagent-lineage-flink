package io.github.tkilome.flink.lineage.model;

import java.util.Map;

/** Stable Paimon lineage dataset property names and factories. */
public final class PaimonDatasetProperties {

    public static final String CONNECTOR = "paimon";
    public static final String WAREHOUSE = "warehouse";
    public static final String FULL_NAME = "fullName";
    public static final String TABLE_PATH = "tablePath";
    public static final String SOURCE_MODE = "sourceMode";
    public static final String SINK_MODE = "sinkMode";
    public static final String TARGET_DATABASE = "targetDatabase";
    public static final String TARGET_TABLE = "targetTable";
    public static final String SOURCE_DATABASE_INCLUDE_PATTERN = "sourceDatabaseIncludePattern";
    public static final String SOURCE_DATABASE_EXCLUDE_PATTERN = "sourceDatabaseExcludePattern";
    public static final String SOURCE_TABLE_INCLUDE_PATTERN = "sourceTableIncludePattern";
    public static final String SOURCE_TABLE_EXCLUDE_PATTERN = "sourceTableExcludePattern";
    public static final String TABLE_NAME_CASE_SENSITIVE = "tableNameCaseSensitive";
    public static final String TABLE_NAME_MERGE_SHARDS = "tableNameMergeShards";
    public static final String TABLE_NAME_PREFIX = "tableNamePrefix";
    public static final String TABLE_NAME_SUFFIX = "tableNameSuffix";
    public static final String DATABASE_PREFIX_MAPPING = "databasePrefixMapping";
    public static final String DATABASE_SUFFIX_MAPPING = "databaseSuffixMapping";
    public static final String TABLE_NAME_MAPPING = "tableNameMapping";

    private PaimonDatasetProperties() {}

    public static LineageDataset exactTable(
            LineageDirection direction,
            String mode,
            String fullName,
            String warehouse,
            String tablePath) {
        String modeKey = direction == LineageDirection.SOURCE ? SOURCE_MODE : SINK_MODE;
        return LineageDatasetBuilder.dataset(direction, CONNECTOR, fullName)
                .namespace(warehouse)
                .property(modeKey, mode)
                .optionalProperty(WAREHOUSE, warehouse)
                .property(FULL_NAME, fullName)
                .optionalProperty(TABLE_PATH, tablePath)
                .build();
    }

    public static LineageDataset combinedCdcSink(
            String warehouse, String targetDatabase, Map<String, String> properties) {
        String name = trimToNull(targetDatabase) == null ? "*" : targetDatabase.trim() + ".*";
        return LineageDatasetBuilder.dataset(LineageDirection.SINK, CONNECTOR, name)
                .namespace(warehouse)
                .property(SINK_MODE, "CDC_COMBINED")
                .optionalProperty(TARGET_DATABASE, targetDatabase)
                .property(TARGET_TABLE, "*")
                .optionalProperty(WAREHOUSE, warehouse)
                .properties(properties)
                .build();
    }

    private static String trimToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }
}
