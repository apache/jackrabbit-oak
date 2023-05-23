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
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.NoSuchWorkspaceException;
import javax.management.ObjectName;
import javax.management.MalformedObjectNameException;
import javax.management.MBeanServerConnection;
import javax.management.ReflectionException;
import javax.management.InstanceNotFoundException;
import javax.management.AttributeNotFoundException;
import javax.management.MBeanException;
import javax.security.auth.login.LoginException;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;


public class NodeCounterMetricTest {
    private ScheduledExecutorService executor;
    private MetricStatisticsProvider statsProvider;
    private static final String mBeanName = "org.apache.jackrabbit.oak:name=NODE_COUNT_FROM_ROOT,type=Metrics";
    private ObjectName mBeanObjectName;
    private NodeStore nodeStore;
    private Whiteboard wb;
    private ContentRepository repository;
    private ContentSession session;

    @Before
    public void before() throws NoSuchWorkspaceException, LoginException, MalformedObjectNameException {
        executor = Executors.newSingleThreadScheduledExecutor();
        statsProvider = new MetricStatisticsProvider(ManagementFactory.getPlatformMBeanServer(), executor);
        mBeanObjectName = new ObjectName(mBeanName);
        nodeStore = new MemoryNodeStore();
        Oak oak = getOak(nodeStore);
        wb = oak.getWhiteboard();
        repository = oak.createContentRepository();
        session = repository.login(null, null);
    }

    @After
    public void after() throws Exception {
        session.close();
        if (repository instanceof Closeable) {
            ((Closeable) repository).close();
        }
        // we have to deregister the statistics provider after each test case, as the call to
        // ManagementFactory.getPlatformMBeanServer() would otherwise return the mBean server with the statistics
        // provider from the first test case and reuse it.
        statsProvider.close();
        new ExecutorCloser(executor).close();
    }

    @Test
    public void testMetricWhenAddingNodes() throws CommitFailedException, IOException, ReflectionException,
            InstanceNotFoundException, AttributeNotFoundException, MBeanException {
        Root root = session.getLatestRoot();
        setCounterIndexSeed(root, 2);
        root.commit();

        // add some random nodes and wait for the NodeCounterIndex to appear
        addNodes(10, 5000, root, wb);

        MBeanServerConnection server = ManagementFactory.getPlatformMBeanServer();
        validateMetricCount(nodeStore, server);
    }

    @Test
    public void testMetricWhenDeletingNodes() throws CommitFailedException, ReflectionException,
            AttributeNotFoundException, InstanceNotFoundException, MBeanException, IOException {
        Root root = session.getLatestRoot();
        setCounterIndexSeed(root, 1);

        MBeanServerConnection server = ManagementFactory.getPlatformMBeanServer();

        addNodes(10, 2000, root, wb);
        validateMetricCount(nodeStore, server);

        // delete enough nodes for the node counter to be updated
        deleteNodes(10, 200, wb, root, nodeStore);
        validateMetricCount(nodeStore, server);

        deleteNodes(2, 5000, wb, root, nodeStore);
        validateMetricCount(nodeStore, server);
    }

    private void validateMetricCount(NodeStore nodeStore, MBeanServerConnection server) throws ReflectionException,
            AttributeNotFoundException, InstanceNotFoundException, MBeanException, IOException {
        long count = NodeCounter.getEstimatedNodeCount(nodeStore.getRoot(), "/", false);
        long nodeCountMetric = (Long) server.getAttribute(mBeanObjectName, "Count");
        assertEquals(count, nodeCountMetric);
    }

    private Oak getOak(NodeStore nodeStore) {
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(new PropertyIndexEditorProvider())
                .with(new NodeCounterEditorProvider().with(statsProvider))
                //Effectively disable async indexing auto run
                //such that we can control run timing as per test requirement
                .withAsyncIndexing("async", TimeUnit.DAYS.toSeconds(1));
    }

    private void setCounterIndexSeed(Root root, int seed) throws CommitFailedException {
        Tree t = root.getTree("/oak:index/counter");
        t.setProperty("seed", seed);
        root.commit();
    }

    private void deleteNodes(int n, int m, Whiteboard wb, Root root, NodeStore nodeStore) throws CommitFailedException {
        for (int i = 0; i < n; i++) {
            if (nodeExists("test" + i, nodeStore)) {
                Tree t = root.getTree("/").getChild("test" + i);
                for (int j = 0; j < m; j++) {
                    Tree child = t.getChild("n" + j);
                    child.remove();
                }
                t.remove();
                root.commit();
                runAsyncIndex(root, wb);
            }
        }
    }

    private void addNodes(int n, int m, Root root, Whiteboard wb) throws CommitFailedException {
        for (int i = 0; i < n; i++) {
            assertTrue("index not ready after 100 iterations", i < 100);
            Tree t = root.getTree("/").addChild("test" + i);
            for (int j = 0; j < m; j++) {
                t.addChild("n" + j);
            }
            root.commit();
            runAsyncIndex(root, wb);
        }

    }

    private boolean nodeExists(String path, NodeStore nodeStore) {
        return NodeStateUtils.getNode(nodeStore.getRoot(), path).exists();
    }

    private void runAsyncIndex(Root root, Whiteboard wb) {
        Runnable async = WhiteboardUtils.getService(
                wb,
                Runnable.class, (Predicate<Runnable>) input -> input instanceof AsyncIndexUpdate
        );
        assertNotNull(async);
        async.run();
        root.refresh();
    }

}
