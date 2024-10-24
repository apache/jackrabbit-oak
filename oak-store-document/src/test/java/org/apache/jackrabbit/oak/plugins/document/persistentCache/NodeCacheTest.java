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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import org.apache.jackrabbit.guava.common.cache.RemovalCause;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStateCache;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.NamePathRev;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Counting;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NodeCacheTest {

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));
    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentStore store;
    private DocumentNodeStore ns;
    private NodeCache<PathRev, DocumentNodeState> nodeCache;
    private NodeCache<NamePathRev, DocumentNodeState.Children> nodeChildren;
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private StatisticsProvider statsProvider = new DefaultStatisticsProvider(executor);

    @After
    public void shutDown(){
        new ExecutorCloser(executor).close();
    }

    @Test
    public void testAsyncCache() throws Exception{
        initializeNodeStore(true);
        ns.setNodeStateCache(new PathExcludingCache("/c"));

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a").child("b");
        builder.child("c").child("d");
        AbstractDocumentNodeState root = (AbstractDocumentNodeState) ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        PathRev prc = new PathRev(Path.fromString("/c"), root.getRootRevision());
        PathRev pra = new PathRev(Path.fromString("/a"), root.getRootRevision());
        Counting counter = nodeCache.getPersistentCacheStats().getPutRejectedAsCachedInSecCounter();
        long count0 = counter.getCount();

        //Adding this should be rejected
        nodeCache.evicted(prc, (DocumentNodeState) root.getChildNode("c"), RemovalCause.SIZE);
        long count1 = counter.getCount();
        assertTrue(count1 > count0);

        //Adding this should NOT be rejected
        nodeCache.evicted(pra, (DocumentNodeState) root.getChildNode("a"), RemovalCause.SIZE);
        long count2 = counter.getCount();
        assertEquals(count1 , count2);
    }

    @Test
    public void testSyncCachePut() throws Exception {
        initializeNodeStore(false);
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a").child("b");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Do a read again
        ns.getRoot().getChildNode("a").getChildNode("b");

        assertContains(nodeCache, "/a/b");
        assertContains(nodeCache, "/a");
        assertPathNameRevs(nodeChildren, "/a", true);

        ns.setNodeStateCache(new PathExcludingCache("/c"));

        builder = ns.getRoot().builder();
        builder.child("c").child("d");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns.getRoot().getChildNode("c").getChildNode("d");
        assertNotContains(nodeCache, "/c/d");
        assertNotContains(nodeCache, "/c");
        assertPathNameRevs(nodeChildren, "/c", false);
    }

    @Test
    public void cachePredicateSync() throws Exception{
        Path a = Path.fromString("/a");
        initializeNodeStore(false, b -> b.setNodeCachePathPredicate(
                path -> path != null && (a.equals(path) || a.isAncestorOf(path))
        ));

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a").child("c1");
        builder.child("b").child("c2");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Do a read again
        ns.getRoot().getChildNode("a").getChildNode("c1");
        ns.getRoot().getChildNode("b").getChildNode("c2");

        assertNotContains(nodeCache, "/b");
        assertNotContains(nodeCache, "/b/c2");
        assertContains(nodeCache, "/a");
        assertContains(nodeCache, "/a/c1");
    }

    // OAK-7153
    @Test
    public void persistentCacheAccessForIncludedPathOnly() throws Exception {
        Path a = Path.fromString("/a");
        initializeNodeStore(false, b -> b.setNodeCachePathPredicate(
                path -> path != null && (a.equals(path) || a.isAncestorOf(path))
        ));

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("x");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // clean caches
        ns.getNodeCache().invalidateAll();
        ns.getNodeChildrenCache().invalidateAll();

        MeterStats stats = statsProvider.getMeter("PersistentCache.NodeCache.node.REQUESTS", StatsOptions.DEFAULT);
        // hasChildNode() is not cached and will cause a request
        // to the persistent cache
        long requests = stats.getCount() + 1;
        ns.getRoot().hasChildNode("a");
        assertEquals(requests, stats.getCount());

        // next call must not cause request to persistent cache
        // because path is not included
        ns.getRoot().hasChildNode("b");
        assertEquals(requests, stats.getCount());
    }

    @Test
    public void localDiffCache() throws Exception {
        initializeNodeStore(false);
        // initialize a second cluster node using the same document store
        DocumentNodeStore ns2 = builderProvider.newBuilder().setClusterId(2)
                .setDocumentStore(store).setAsyncDelay(0).build();
        // sync the two cluster nodes
        ns2.runBackgroundOperations();
        ns.runBackgroundOperations();

        MeterStats stats = statsProvider.getMeter("PersistentCache.NodeCache.local_diff.REQUESTS", StatsOptions.DEFAULT);

        NodeState r1 = ns.getRoot();

        // external change from first cluster node POV
        NodeBuilder builder = ns2.getRoot().builder();
        builder.child("x");
        ns2.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // sync
        ns2.runBackgroundOperations();
        ns.runBackgroundOperations();

        long requests = stats.getCount();
        // diff for external change
        NodeState r2 = ns.getRoot();
        JsopDiff.diffToJsop(r1, r2);
        // must not use local_diff persistent cache
        assertEquals(requests, stats.getCount());
    }

    private void initializeNodeStore(boolean asyncCache) {
        initializeNodeStore(asyncCache, b -> {});
    }

    private void initializeNodeStore(boolean asyncCache, Consumer<DocumentMK.Builder> processor) {
        store = new MemoryDocumentStore();
        DocumentMK.Builder builder = builderProvider.newBuilder()
                .setDocumentStore(store)
                .setAsyncDelay(0)
                .setStatisticsProvider(statsProvider);

        if (asyncCache){
            builder.setPersistentCache("target/persistentCache,time");
        }else {
            builder.setPersistentCache("target/persistentCache,time,-async");
        }

        processor.accept(builder);

        ns = builder.getNodeStore();
        nodeCache = (NodeCache<PathRev, DocumentNodeState>) ns.getNodeCache();
        nodeChildren = (NodeCache<NamePathRev, DocumentNodeState.Children>) ns.getNodeChildrenCache();
    }


    private static <V extends CacheValue> void assertContains(NodeCache<PathRev, V> cache, String path) {
        assertPathRevs(cache, path, true);
    }

    private static <V extends CacheValue> void assertNotContains(NodeCache<PathRev, V> cache, String path) {
        assertPathRevs(cache, path, false);
    }

    private static <V extends CacheValue> void assertPathRevs(NodeCache<PathRev, V> cache, String path, boolean contains) {
        List<PathRev> revs = getPathRevs(cache, path);
        List<PathRev> matchingRevs = new ArrayList<>();
        for (PathRev pr : revs) {
            if (cache.getGenerationalMap().containsKey(pr)) {
                matchingRevs.add(pr);
            }
        }

        if (contains && matchingRevs.isEmpty()) {
            fail(String.format("Expecting entry for [%s]. Did not found in %s", path, matchingRevs));
        }

        if (!contains && !matchingRevs.isEmpty()) {
            fail(String.format("Expecting entry for [%s]. Found %s", path, revs));
        }
    }

    private static <V extends CacheValue> void assertPathNameRevs(NodeCache<NamePathRev, V> cache, String path, boolean contains) {
        List<NamePathRev> revs = getPathNameRevs(cache, path);
        List<NamePathRev> matchingRevs = new ArrayList<>();
        for (NamePathRev pr : revs) {
            if (cache.getGenerationalMap().containsKey(pr)) {
                matchingRevs.add(pr);
            }
        }

        if (contains && matchingRevs.isEmpty()) {
            fail(String.format("Expecting entry for [%s]. Did not found in %s", path, matchingRevs));
        }

        if (!contains && !matchingRevs.isEmpty()) {
            fail(String.format("Expecting entry for [%s]. Found %s", path, revs));
        }
    }

    private static <V extends CacheValue> List<PathRev> getPathRevs(NodeCache<PathRev, V> cache, String path) {
        List<PathRev> revs = new ArrayList<>();
        for (PathRev pr : cache.asMap().keySet()) {
            if (pr.getPath().toString().equals(path)) {
                revs.add(pr);
            }
        }
        return revs;
    }

    private static <V extends CacheValue> List<NamePathRev> getPathNameRevs(NodeCache<NamePathRev, V> cache, String path) {
        List<NamePathRev> revs = new ArrayList<>();
        for (NamePathRev pr : cache.asMap().keySet()) {
            if (pr.getPath().toString().equals(path)) {
                revs.add(pr);
            }
        }
        return revs;
    }

    private static class PathExcludingCache implements DocumentNodeStateCache {
        private final String excludeRoot;

        private PathExcludingCache(String excludeRoot) {
            this.excludeRoot = excludeRoot;
        }

        @Override
        public AbstractDocumentNodeState getDocumentNodeState(Path path, RevisionVector rootRevision,
                                                              RevisionVector lastRev) {
            return null;
        }

        @Override
        public boolean isCached(Path path) {
            if (path.toString().startsWith(excludeRoot)) {
                return true;
            }
            return false;
        }
    };
}