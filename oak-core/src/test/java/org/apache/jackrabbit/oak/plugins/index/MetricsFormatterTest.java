package org.apache.jackrabbit.oak.plugins.index;

import org.junit.Test;

import static org.junit.Assert.*;

public class MetricsFormatterTest {

    @Test
    public void testAdd() {
        MetricsFormatter metricsFormatter = MetricsFormatter.newBuilder();
        metricsFormatter.add("key", "value");
        metricsFormatter.add("key", 1);
        metricsFormatter.add("key", 1L);
        metricsFormatter.add("key", true);
        String result = metricsFormatter.build();
        assertEquals("{\"key\":\"value\",\"key\":1,\"key\":1,\"key\":true}", result);
        assertThrows(IllegalStateException.class, () -> metricsFormatter.add("key", "value"));
        assertEquals("{\"key\":\"value\",\"key\":1,\"key\":1,\"key\":true}", metricsFormatter.build());
    }
}