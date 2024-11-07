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
package org.apache.jackrabbit.oak.plugins.document.prefetch;

import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore.SYS_PROP_PREFETCH;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.junit.TemporarySystemProperty;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.CountingDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.Nullable;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.jackrabbit.guava.common.base.Stopwatch;

public class CacheWarmingTest {

    @Rule
    public TemporarySystemProperty systemProperties = new TemporarySystemProperty();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    private static Logger LOG = LoggerFactory.getLogger(CacheWarmingTest.class);

    private @Nullable MongoConnection mongoConnection;

    private CountingMongoDatabase db;

    @Before
    public void enablePrefetch() {
        System.setProperty(SYS_PROP_PREFETCH, "true");
    }

    @AfterClass
    public static void cleanUp() {
        MongoUtils.dropCollections(MongoUtils.DB);
    }

    @Test
    public void noop1() {
        DocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(store).build();
        ns.prefetch(null, null);
    }

    @Test
    public void noop2() {
        DocumentMK mk = builderProvider.newBuilder().open();
        DocumentStore s = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        ns.prefetch(null, null);
    }

    protected Clock getTestClock() throws InterruptedException {
        return Clock.SIMPLE;
    }

    private DocumentStore newMongoDocumentStore() throws InterruptedException {
        mongoConnection = connectionFactory.getConnection();
        assumeNotNull(mongoConnection);
        db = new CountingMongoDatabase(mongoConnection.getDatabase());
        MongoUtils.dropCollections(db);
        DocumentMK.Builder builder = new DocumentMK.Builder()
                .clock(getTestClock()).setAsyncDelay(0);
        MongoDocumentStore store = new MongoDocumentStore(mongoConnection.getMongoClient(), db, builder);
//        mk = builder.setDocumentStore(store).open();
        return store;
    }

    @Test
    public void simple_nocleanCaches_noprefetch() throws Exception {
        doSimple(false, false);
    }

    @Test
    public void simple_cleanCaches_noprefetch() throws Exception {
        doSimple(true, false);
    }

    @Test
    public void simple_cleanCaches_withprefetch() throws Exception {
        doSimple(true, true);
    }

    @Test
    public void simple_nocleanCaches_withprefetch() throws Exception {
        doSimple(false, true);
    }

    @Test
    public void prefetch() throws Exception {
        DocumentStore ds = newMongoDocumentStore();
        CacheWarming cw = new CacheWarming(ds);
        DocumentNodeStore store = builderProvider.newBuilder().setAsyncDelay(0)
                .setDocumentStore(ds).getNodeStore();
        SortedSet<String> children = new TreeSet<String>();
        NodeBuilder builder = store.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            String name = "c" + i;
            children.add("/" + name + "/" + name);
            builder.child(name).child(name);
        }
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store.runBackgroundOperations();

        store.getMBean().cleanAllCaches();

        cw.prefetch(children, store.getRoot());

        for (String p : children) {
            assertNotNull(ds.getIfCached(Collection.NODES, getIdFromPath(p)));
        }

        List<String> paths = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            paths.add("/does/not/exist-" + i);
        }
        cw.prefetch(paths, store.getRoot());
        int numRawFindCalls = getRawFindCalls();
        assertNull(ds.find(Collection.NODES, getIdFromPath(paths.get(0))));
        assertEquals(0, getRawFindCalls() - numRawFindCalls);
    }

    private void doSimple(boolean cleanCaches, boolean prefetch)
            throws InterruptedException, CommitFailedException {
        LOG.info("=== doSimple( cleanCaches = " + cleanCaches + ", prefetch = " + prefetch + " )");
        Stopwatch sw = Stopwatch.createStarted();
        DocumentStore ds = newMongoDocumentStore();
        final CountingDocumentStore cds = new CountingDocumentStore(ds);
        logAndReset("after init      ", cds, sw);
        DocumentNodeStore store = builderProvider.newBuilder().setAsyncDelay(0)
                .setDocumentStore(cds).getNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        SortedSet<String> children = new TreeSet<String>();
        // create a bunch of nodes
        // make it 4 levels deep to avoid things like 'readChildren' to be able to use optimizations such as query()
        for (int i = 0; i < 4*1024; i++) {
            String name = "c" + i;
            children.add("/" + name + "/" + name + "/" + name + "/" + name);
            builder.child(name).child(name).child(name).child(name);
        }
        logAndReset("before merge    ", cds, sw);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store.runBackgroundOperations();
        logAndReset("after merge     ", cds, sw);
        if (cleanCaches) {
            // clear the cache via mbean
            store.getMBean().cleanAllCaches();
            // also clear the documentstore cache
            store.getDocumentStore().invalidateCache();
            logAndReset("after invalidate", cds, sw);
        }
        DocumentNodeState root = store.getRoot();
        if (prefetch) {
            final List<String> paths = new ArrayList<>(children);
            final java.util.Collection<String> withParents = withParents(paths);
            withParents.remove("/");
            store.prefetch(withParents, root);
            logAndReset("after prefetch  ", cds, sw);
        }
        // read the children again
        for (String aChild : root.getChildNodeNames()) {
            assertTrue(root.getChildNode(aChild).getChildNode(aChild).getChildNode(aChild).getChildNode(aChild).exists());
        }
        // raw find calls must be reasonably low with prefetch
        int rawFindCalls = getRawFindCalls();
        logAndReset("read            ", cds, sw);
        if (prefetch) {
            // OAK-9883 - this assertion is not stable, disable for now
            // assertThat(rawFindCalls, lessThan(10));
        }
    }

    public static java.util.Collection<String> withParents(java.util.Collection<String> paths) {
        Set<String> s = new HashSet<>();
        for (String aPath : paths) {
            Path p = Path.fromString(aPath);
            while (p != null) {
                s.add(p.toString());
                p = p.getParent();
            }
        }
        return new ArrayList<>(s);
    }

    private void logAndReset(String context,
                             CountingDocumentStore cds,
                             Stopwatch sw) {
        LOG.info(context
                + " -> createOrUpdateCalls = " + cds.getNumCreateOrUpdateCalls(Collection.NODES)
                + ", findCalls = " + cds.getNumFindCalls(Collection.NODES)
                + ", queryCalls = " + cds.getNumQueryCalls(Collection.NODES)
                + ", removalCalls = " + cds.getNumRemoveCalls(Collection.NODES)
                + ", rawFindCalls = " + getRawFindCalls()
                + ", " + sw);
        db.getCachedCountingCollection("nodes").resetFindCounter();
        cds.resetCounters();
        sw.reset().start();
    }

    private int getRawFindCalls() {
        return db.getCachedCountingCollection("nodes").getFindCounter();
    }
}
