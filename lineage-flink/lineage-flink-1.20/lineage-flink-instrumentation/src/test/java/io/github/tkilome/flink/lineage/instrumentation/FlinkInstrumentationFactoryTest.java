package io.github.tkilome.flink.lineage.instrumentation;

import io.github.tkilome.flink.lineage.api.factory.LineageComponentType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class FlinkInstrumentationFactoryTest {

    @Test
    void createsFlinkInstrumentationFactoryWithoutVersionInClassName() {
        FlinkInstrumentationFactory factory = new FlinkInstrumentationFactory();

        assertEquals("flink-1.20-instrumentation", factory.factoryName());
        assertEquals(LineageComponentType.INSTRUMENTATION, factory.componentType());
        assertNotNull(factory.create(null));
    }
}
