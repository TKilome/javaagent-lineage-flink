package io.github.tkilome.flink.lineage.agent;

import io.github.tkilome.flink.lineage.discovery.FactoryDiscoveryTest;
import io.github.tkilome.flink.lineage.runtime.LineageRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LineageAgentTest {

    @BeforeEach
    void setUp() {
        FactoryDiscoveryTest.TestInstrumentationFactory.installCount = 0;
    }

    @AfterEach
    void tearDown() {
        LineageRuntime.resetForTest();
    }

    @Test
    void premainDiscoversFactoriesAndInstallsInstrumentation() {
        LineageAgent.premain("", null);

        assertEquals(1, FactoryDiscoveryTest.TestInstrumentationFactory.installCount);
    }
}
