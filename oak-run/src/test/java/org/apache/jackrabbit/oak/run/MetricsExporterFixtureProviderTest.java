/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.run;

import java.util.HashMap;
import java.util.Map;

import io.prometheus.client.exporter.PushGateway;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.jackrabbit.oak.run.MetricsExporterFixtureProvider.ExportMetricsArgs;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for MetricsExporterFixtureProvider
 */
public class MetricsExporterFixtureProviderTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void checkCorrectPushGatewayInit() throws Exception {
        OptionParser parser = new OptionParser();
        DataStoreOptions dataStoreOptions = new DataStoreOptions(parser);

        OptionSet option = parser.parse("--export-metrics", "pushgateway;localhost:9091;key1=value1,key2=value2");
        dataStoreOptions.configure(option);

        MetricsExporterFixture metricsExporterFixture =
            MetricsExporterFixtureProvider.create(dataStoreOptions, new DefaultWhiteboard());

        assertEquals("pushgateway", metricsExporterFixture.getExporterType().name());
        Object metricsExporter = metricsExporterFixture.getMetricsExporter();
        assertTrue(metricsExporter instanceof PushGateway);
    }

    @Test
    public void testMetricArgs() throws Exception {
        String option = "pushgateway;localhost:9091;key1=value1,key2=value2";
        Map<String, String> expectedMap = new HashMap<>();
        expectedMap.put("key1", "value1");
        expectedMap.put("key2", "value2");

        ExportMetricsArgs metricsArgs = new ExportMetricsArgs(option);

        assertEquals("pushgateway", metricsArgs.getExporterType().name());
        assertEquals("localhost:9091", metricsArgs.getPushUri());
        assertEquals(expectedMap, metricsArgs.getPushMap());
    }

    @Test
    public void testMetricArgsNoType() throws Exception {
        expectedEx.expect(java.lang.IllegalArgumentException.class);

        String option = "localhost:9091;key1=value1,key2=value2";

        ExportMetricsArgs metricsArgs = new ExportMetricsArgs(option);
    }

    @Test
    public void testMetricArgsWrongType() throws Exception {
        expectedEx.expect(java.lang.IllegalArgumentException.class);

        String option = "wrongtype:localhost:9091;key1=value1,key2=value2";

        ExportMetricsArgs metricsArgs = new ExportMetricsArgs(option);
    }

    @Test
    public void testMetricArgsNoProps() throws Exception {
        String option = "pushgateway;localhost:9091";

        ExportMetricsArgs metricsArgs = new ExportMetricsArgs(option);

        assertEquals("pushgateway", metricsArgs.getExporterType().name());
        assertEquals("localhost:9091", metricsArgs.getPushUri());
        assertEquals(new HashMap<>(), metricsArgs.getPushMap());
    }

    @Test
    public void testMetricArgsNoUrlNoMap() throws Exception {
        expectedEx.expect(java.lang.IllegalArgumentException.class);

        String option = "pushgateway";

        ExportMetricsArgs metricsArgs = new ExportMetricsArgs(option);
    }

    @Test
    public void testMetricArgsNoUrl() throws Exception {
        expectedEx.expect(java.lang.IllegalArgumentException.class);

        String option = "pushgateway:key1=value1,key2=value2";
        Map<String, String> expectedMap = new HashMap<>();
        expectedMap.put("key1", "value1");
        expectedMap.put("key2", "value2");

        ExportMetricsArgs metricsArgs = new ExportMetricsArgs(option);
        assertEquals("pushgateway", metricsArgs.getExporterType().name());
        assertNotEquals(expectedMap, metricsArgs.getPushMap());
        assertNotEquals("localhost:9091", metricsArgs.getPushUri());
    }
}
