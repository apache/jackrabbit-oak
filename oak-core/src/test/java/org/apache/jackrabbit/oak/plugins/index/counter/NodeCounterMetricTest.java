package org.apache.jackrabbit.oak.plugins.index.counter;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.*;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.counter.jmx.NodeCounter;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.SimpleStats;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static org.junit.Assert.*;

public class NodeCounterMetricTest {
    Whiteboard wb;
    NodeStore nodeStore;
    Root root;
    QueryEngine qe;
    ContentSession session;

    @Before
    public void before() throws Exception {
        session = createRepository().login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();

    }

    @Test
    public void testNodeCounterMetric() throws CommitFailedException {
        // create the nodeCounter index
        runAsyncIndex();
        for(int i=0; !nodeExists("oak:index/counter/:index"); i++) {
            assertTrue("index not ready after 100 iterations", i < 100);
            Tree t = root.getTree("/").addChild("test" + i);
            for (int j = 0; j < 100; j++) {
                t.addChild("n" + j);
            }
            root.commit();
            runAsyncIndex();
        }

        CounterStats nodeCounterMetrics = NodeCounterEditor.initNodeCounterMetric(new SimpleStats(new AtomicLong(), SimpleStats.Type.COUNTER),  nodeStore.getRoot());
        long count = NodeCounter.getEstimatedNodeCount(nodeStore.getRoot(), "/", false);
        assertEquals(count, nodeCounterMetrics.getCount());
    }

    protected ContentRepository createRepository() {
        nodeStore = new MemoryNodeStore();
        Oak oak = new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(new PropertyIndexEditorProvider())
                .with(new NodeCounterEditorProvider())
                //Effectively disable async indexing auto run
                //such that we can control run timing as per test requirement
                .withAsyncIndexing("async", TimeUnit.DAYS.toSeconds(1));

        wb = oak.getWhiteboard();
        return oak.createContentRepository();
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
