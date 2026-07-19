package io.github.tkilome.flink.lineage.instrumentation;

import org.apache.flink.streaming.api.graph.StreamGraph;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlinkStreamGraphAccessorTest {

    @Test
    void acceptsFlinkStreamGraphTypeDirectly() throws Exception {
        Method method = FlinkStreamGraphAccessor.class.getDeclaredMethod("toLineageNodes", StreamGraph.class);

        assertEquals(StreamGraph.class, method.getParameterTypes()[0]);
    }
}
