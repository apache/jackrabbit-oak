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

import java.io.File;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import com.google.common.base.Predicate;
import com.google.common.cache.RemovalCause;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStateCache;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Counting;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NodeCacheTest {

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));
    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    private DocumentNodeStore ns;
    private NodeCache<PathRev, DocumentNodeState> nodeCache;
    private NodeCache<PathRev, DocumentNodeState.Children> nodeChildren;
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

        PathRev prc = new PathRev("/c", root.getRootRevision());
        PathRev pra = new PathRev("/a", root.getRootRevision());
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
        assertContains(nodeChildren, "/a");

        ns.setNodeStateCache(new PathExcludingCache("/c"));

        builder = ns.getRoot().builder();
        builder.child("c").child("d");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns.getRoot().getChildNode("c").getChildNode("d");
        assertNotContains(nodeCache, "/c/d");
        assertNotContains(nodeCache, "/c");
        assertNotContains(nodeChildren, "/c");
    }

    @Test
    public void cachePredicateSync() throws Exception{
        PathFilter pf = new PathFilter(asList("/a"), emptyList());
        Predicate<String> p = path -> pf.filter(path) == PathFilter.Result.INCLUDE;
        initializeNodeStore(false, b -> b.setNodeCachePredicate(p));

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
    @Ignore
    @Test
    public void persistentCacheAccessForIncludedPathOnly() throws Exception {
        PathFilter pf = new PathFilter(singletonList("/a"), emptyList());
        Predicate<String> p = path -> pf.filter(path) == PathFilter.Result.INCLUDE;
        initializeNodeStore(false, b -> b.setNodeCachePredicate(p));

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

    private void initializeNodeStore(boolean asyncCache) {
        initializeNodeStore(asyncCache, b -> {});
    }

    private void initializeNodeStore(boolean asyncCache, Consumer<DocumentMK.Builder> processor) {
        DocumentMK.Builder builder = builderProvider.newBuilder()
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
        nodeChildren = (NodeCache<PathRev, DocumentNodeState.Children>) ns.getNodeChildrenCache();
    }


    private static <V> void assertContains(NodeCache<PathRev, V> cache, String path) {
        assertPathRevs(cache, path, true);
    }

    private static <V> void assertNotContains(NodeCache<PathRev, V> cache, String path) {
        assertPathRevs(cache, path, false);
    }

    private static <V> void assertPathRevs(NodeCache<PathRev, V> cache, String path, boolean contains) {
        List<PathRev> revs = getPathRevs(cache, path);
        List<PathRev> matchingRevs = Lists.newArrayList();
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

    private static <V> List<PathRev> getPathRevs(NodeCache<PathRev, V> cache, String path) {
        List<PathRev> revs = Lists.newArrayList();
        for (PathRev pr : cache.asMap().keySet()) {
            if (pr.getPath().equals(path)) {
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
        public AbstractDocumentNodeState getDocumentNodeState(String path, RevisionVector rootRevision,
                                                              RevisionVector lastRev) {
            return null;
        }

        @Override
        public boolean isCached(String path) {
            if (path.startsWith(excludeRoot)) {
                return true;
            }
            return false;
        }
    };
}