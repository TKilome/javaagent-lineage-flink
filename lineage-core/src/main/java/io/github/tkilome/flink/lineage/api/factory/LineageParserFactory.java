package io.github.tkilome.flink.lineage.api.factory;

import io.github.tkilome.flink.lineage.api.parser.LineageParser;
import io.github.tkilome.flink.lineage.context.ParserContext;

/**
 * Factory for creating a connector or engine-specific lineage parser.
 *
 * <p>Parser factories are discovered through {@link java.util.ServiceLoader}. The created parser is
 * invoked for every collected node and decides whether it can extract endpoints from that node.
 */
public interface LineageParserFactory extends LineageFactory {
    /**
     * Returns {@link LineageComponentType#PARSER}.
     *
     * @return parser component type
     */
    @Override
    default LineageComponentType componentType() {
        return LineageComponentType.PARSER;
    }

    /**
     * Creates the parser instance.
     *
     * @param context parser context provided by the core runtime
     * @return parser implementation
     */
    LineageParser create(ParserContext context);
}
