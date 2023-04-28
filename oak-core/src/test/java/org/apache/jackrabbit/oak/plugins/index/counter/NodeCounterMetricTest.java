/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.counter;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.*;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.counter.jmx.NodeCounter;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.Before;
import org.junit.Test;

import javax.management.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.Assert.*;

public class NodeCounterMetricTest {
    Whiteboard wb;
    NodeStore nodeStore;
    Root root;
    QueryEngine qe;
    ContentSession session;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final MetricStatisticsProvider statsProvider =
            new MetricStatisticsProvider(ManagementFactory.getPlatformMBeanServer(), executor);

    @Before
    public void before() throws Exception {
        session = createRepository().login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
    }

    @Test
    public void testMetricWhenAddingNodes() throws CommitFailedException, MalformedObjectNameException, IOException,
            ReflectionException, InstanceNotFoundException, AttributeNotFoundException, MBeanException {
        ApproximateCounter.setSeed(1);
        runAsyncIndex();
        addNodes(5, 5000); // add some random nodes and wait for the NodeCounterIndex to appear

        MBeanServerConnection server = ManagementFactory.getPlatformMBeanServer();
        String name = "org.apache.jackrabbit.oak:name=NODE_COUNT_FROM_ROOT,type=Metrics";
        Long nodeCountMetric = (Long) server.getAttribute(new ObjectName(name), "Count");
        long count = NodeCounter.getEstimatedNodeCount(nodeStore.getRoot(), "/", false);
        assertEquals(count, nodeCountMetric.longValue());
    }

    @Test
    public void testMetricWhenDeletingNodes() throws CommitFailedException, MalformedObjectNameException,
            ReflectionException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, IOException {
        ApproximateCounter.setSeed(42);
        runAsyncIndex();
        addNodes(10, 2000);

        MBeanServerConnection server = ManagementFactory.getPlatformMBeanServer();
        String name = "org.apache.jackrabbit.oak:name=NODE_COUNT_FROM_ROOT,type=Metrics";
        Long nodeCountMetric = (Long) server.getAttribute(new ObjectName(name), "Count");
        long count = NodeCounter.getEstimatedNodeCount(nodeStore.getRoot(), "/", false);
        assertEquals(count, nodeCountMetric.longValue());

        // delete enough nodes for the node counter to be updated
        deleteNodes(10, 200);
        count = NodeCounter.getEstimatedNodeCount(nodeStore.getRoot(), "/", false);
        nodeCountMetric = (Long) server.getAttribute(new ObjectName(name), "Count");
        assertEquals(count, nodeCountMetric.longValue());
    }

    protected ContentRepository createRepository() {
        nodeStore = new MemoryNodeStore();
        Oak oak = new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(new PropertyIndexEditorProvider())
                .with(new NodeCounterEditorProvider())
                .with(ManagementFactory.getPlatformMBeanServer())
                //Effectively disable async indexing auto run
                //such that we can control run timing as per test requirement
                .withAsyncIndexing("async", TimeUnit.DAYS.toSeconds(1));

        wb = oak.getWhiteboard();
        return oak.createContentRepository();
    }

    private void deleteNodes(int n, int m) throws CommitFailedException {
        for (int i = 0; i < n; i++) {
            if (nodeExists("test" + i)) {
                Tree t = root.getTree("/").getChild("test" + i);
                for (int j = 0; j < m; j++) {
                    t.getChild("n" + j).remove();
                }
                root.commit();
                runAsyncIndex();
            }
        }
    }

    private void addNodes(int n, int m) throws CommitFailedException {
        for (int i = 0; i < n; i++) {
            assertTrue("index not ready after 100 iterations", i < 100);
            Tree t = root.getTree("/").addChild("test" + i);
            for (int j = 0; j < m; j++) {
                t.addChild("n" + j);
            }
            root.commit();
            runAsyncIndex();
        }

    }
    private boolean nodeExists(String path) {
        return NodeStateUtils.getNode(nodeStore.getRoot(), path).exists();
    }
    private void runAsyncIndex() {
        Runnable async = WhiteboardUtils.getService(
                wb,
                Runnable.class, (Predicate<Runnable>) input -> input instanceof AsyncIndexUpdate
        );
        assertNotNull(async);
        async.run();
        root.refresh();
    }

}
