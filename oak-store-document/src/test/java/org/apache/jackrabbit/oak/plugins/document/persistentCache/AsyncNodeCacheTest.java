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

import org.apache.jackrabbit.oak.cache.api.CacheBuilder;

import org.junit.Assert;


import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.jackrabbit.oak.cache.api.EvictionCause;
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
import org.junit.Before;
import org.apache.jackrabbit.oak.plugins.document.DocumentCacheFeatureTestSupport;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Production persistence behavior with a scoped ASYNC opt-in. */
public class AsyncNodeCacheTest {

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));
    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentStore store;
    private DocumentNodeStore ns;
    private AutoCloseable features;

    @Before
    public void enableAsyncMaintenance() {
        features = DocumentCacheFeatureTestSupport.enableAsyncMaintenance();
    }

    @After
    public void restoreFeatures() throws Exception {
        features.close();
    }

    private AsyncNodeCache<PathRev, DocumentNodeState> nodeCache;
    private AsyncNodeCache<NamePathRev, DocumentNodeState.Children> nodeChildren;
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private StatisticsProvider statsProvider = new DefaultStatisticsProvider(executor);

    @After
    public void shutDown(){
        new ExecutorCloser(executor).close();
    }

    /** Async persistence respects the secondary cache's path selection. */
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

        nodeCache.put(prc, (DocumentNodeState) root.getChildNode("c"));
        nodeCache.getIfPresent(prc);
        nodeCache.evicted(prc, nodeCache.memCache.asMap().get(prc), EvictionCause.SIZE);
        long count1 = counter.getCount();
        Assert.assertTrue(count1 > count0);

        nodeCache.put(pra, (DocumentNodeState) root.getChildNode("a"));
        nodeCache.getIfPresent(pra);
        nodeCache.evicted(pra, nodeCache.memCache.asMap().get(pra), EvictionCause.SIZE);
        long count2 = counter.getCount();
        Assert.assertEquals(count1 , count2);
    }

    // A delayed old callback leaves the accessed replacement eligible for persistence.
    @Test
    public void staleEvictionDoesNotConsumeReplacementMetadata() throws Exception {
        initializeNodeStore(true);
        ns.setNodeStateCache(new PathExcludingCache("/c"));

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a");
        AbstractDocumentNodeState root = (AbstractDocumentNodeState) ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        DocumentNodeState oldValue = (DocumentNodeState) root.getChildNode("a");
        DocumentNodeState replacement = oldValue.fromExternalChange();
        PathRev key = new PathRev(Path.fromString("/a"), root.getRootRevision());
        MeterStats persistedPuts = statsProvider.getMeter(
                "PersistentCache.NodeCache.node.CACHE_PUT", StatsOptions.DEFAULT);

        nodeCache.put(key, oldValue);
        CacheEntry<DocumentNodeState> oldEntry = nodeCache.memCache.asMap().get(key);
        nodeCache.put(key, replacement);
        Assert.assertSame(replacement, nodeCache.getIfPresent(key));

        long putsBeforeEviction = persistedPuts.getCount();
        nodeCache.evicted(key, oldEntry, EvictionCause.SIZE);
        Assert.assertEquals(putsBeforeEviction, persistedPuts.getCount());

        nodeCache.evicted(key, nodeCache.memCache.asMap().get(key), EvictionCause.SIZE);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (persistedPuts.getCount() == putsBeforeEviction && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        Assert.assertEquals(putsBeforeEviction + 1, persistedPuts.getCount());
    }

    /** Bulk reads combine memory and disk hits, excluding misses. */
    @Test
    public void bulkReadIncludesMemoryAndPersistentValuesAndSkipsMisses() throws Exception {
        initializeNodeStore(false);
        DocumentNodeState value = ns.getRoot();
        PathRev memoryKey = new PathRev(Path.fromString("/memory"), value.getRootRevision());
        PathRev persistentKey = new PathRev(Path.fromString("/persistent"), value.getRootRevision());
        PathRev absentKey = new PathRev(Path.fromString("/absent"), value.getRootRevision());
        nodeCache.put(memoryKey, value);
        nodeCache.put(persistentKey, value);
        nodeCache.memCache.invalidate(persistentKey);

        long requestsBefore = nodeCache.getPersistentCacheStats().getRequestCount();
        long hitsBefore = nodeCache.getPersistentCacheStats().getHitCount();
        Map<PathRev, DocumentNodeState> result = nodeCache.getAllPresent(
                Arrays.asList(memoryKey, persistentKey, absentKey));

        Assert.assertEquals(2, result.size());
        Assert.assertSame(value, result.get(memoryKey));
        Assert.assertSame(value, result.get(persistentKey));
        Assert.assertEquals(requestsBefore + 2, nodeCache.getPersistentCacheStats().getRequestCount());
        Assert.assertEquals(hitsBefore + 1, nodeCache.getPersistentCacheStats().getHitCount());
    }

    /** Synchronous persistence writes committed node values. */
    @Test
    public void testSyncCachePut() throws Exception {
        initializeNodeStore(false);
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a").child("b");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

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

    /** Synchronous persistence excludes paths outside the predicate. */
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
        ns.getNodeCache().invalidateAll();
        ns.getNodeChildrenCache().invalidateAll();

        MeterStats stats = statsProvider.getMeter("PersistentCache.NodeCache.node.REQUESTS", StatsOptions.DEFAULT);
        // hasChildNode() is not cached and will cause a request
        // to the persistent cache
        long requests = stats.getCount() + 1;
        ns.getRoot().hasChildNode("a");
        Assert.assertEquals(requests, stats.getCount());

        // next call must not cause request to persistent cache
        // because path is not included
        ns.getRoot().hasChildNode("b");
        Assert.assertEquals(requests, stats.getCount());
    }

    /** External changes must not read the local-diff disk cache. */
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

        ns2.runBackgroundOperations();
        ns.runBackgroundOperations();

        long requests = stats.getCount();
        // diff for external change
        NodeState r2 = ns.getRoot();
        JsopDiff.diffToJsop(r1, r2);
        // must not use local_diff persistent cache
        Assert.assertEquals(requests, stats.getCount());
    }

    private void initializeNodeStore(boolean asyncCache) {
        initializeNodeStore(asyncCache, b -> {});
    }

    private void initializeNodeStore(boolean asyncCache, Consumer<DocumentMK.Builder> processor) {
        store = new MemoryDocumentStore();
        DocumentMK.Builder builder = builderProvider.newBuilder()
                .setCacheMaintenanceMode(CacheType.NODE, CacheBuilder.MaintenanceMode.ASYNC)
                .setCacheMaintenanceMode(CacheType.CHILDREN, CacheBuilder.MaintenanceMode.ASYNC)
                .setCacheMaintenanceMode(CacheType.LOCAL_DIFF, CacheBuilder.MaintenanceMode.ASYNC)
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
        nodeCache = (AsyncNodeCache<PathRev, DocumentNodeState>) ns.getNodeCache();
        nodeChildren = (AsyncNodeCache<NamePathRev, DocumentNodeState.Children>) ns.getNodeChildrenCache();
    }


    private static <V extends CacheValue> void assertContains(AsyncNodeCache<PathRev, V> cache, String path) {
        assertPathRevs(cache, path, true);
    }

    private static <V extends CacheValue> void assertNotContains(AsyncNodeCache<PathRev, V> cache, String path) {
        assertPathRevs(cache, path, false);
    }

    private static <V extends CacheValue> void assertPathRevs(AsyncNodeCache<PathRev, V> cache, String path, boolean contains) {
        List<PathRev> revs = getPathRevs(cache, path);
        List<PathRev> matchingRevs = new ArrayList<>();
        for (PathRev pr : revs) {
            if (cache.getGenerationalMap().containsKey(pr)) {
                matchingRevs.add(pr);
            }
        }

        if (contains && matchingRevs.isEmpty()) {
            Assert.fail(String.format("Expecting entry for [%s]. Did not found in %s", path, matchingRevs));
        }

        if (!contains && !matchingRevs.isEmpty()) {
            Assert.fail(String.format("Expecting entry for [%s]. Found %s", path, revs));
        }
    }

    private static <V extends CacheValue> void assertPathNameRevs(AsyncNodeCache<NamePathRev, V> cache, String path, boolean contains) {
        List<NamePathRev> revs = getPathNameRevs(cache, path);
        List<NamePathRev> matchingRevs = new ArrayList<>();
        for (NamePathRev pr : revs) {
            if (cache.getGenerationalMap().containsKey(pr)) {
                matchingRevs.add(pr);
            }
        }

        if (contains && matchingRevs.isEmpty()) {
            Assert.fail(String.format("Expecting entry for [%s]. Did not found in %s", path, matchingRevs));
        }

        if (!contains && !matchingRevs.isEmpty()) {
            Assert.fail(String.format("Expecting entry for [%s]. Found %s", path, revs));
        }
    }

    private static <V extends CacheValue> List<PathRev> getPathRevs(AsyncNodeCache<PathRev, V> cache, String path) {
        List<PathRev> revs = new ArrayList<>();
        for (PathRev pr : cache.asMap().keySet()) {
            if (pr.getPath().toString().equals(path)) {
                revs.add(pr);
            }
        }
        return revs;
    }

    private static <V extends CacheValue> List<NamePathRev> getPathNameRevs(AsyncNodeCache<NamePathRev, V> cache, String path) {
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
