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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.size;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.DOCUMENT_NS;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.DOCUMENT_RDB;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.MEMORY_NS;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.getFixtures;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.NUM_REVS_THRESHOLD;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.PREV_SPLIT_FACTOR;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class VersionGarbageCollectorIT {

    private DocumentStoreFixture fixture;

    private Clock clock;

    private DocumentNodeStore store;

    private VersionGarbageCollector gc;

    private ExecutorService execService;

    public VersionGarbageCollectorIT(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> fixtures() throws IOException {
        List<Object[]> fixtures = Lists.newArrayList();
        if (getFixtures().contains(MEMORY_NS)) {
            fixtures.add(new Object[] { new DocumentStoreFixture.MemoryFixture() });
        }

        DocumentStoreFixture mongo = new DocumentStoreFixture.MongoFixture();
        if (getFixtures().contains(DOCUMENT_NS) && mongo.isAvailable()) {
            fixtures.add(new Object[] { mongo });
        }

        DocumentStoreFixture rdb = new DocumentStoreFixture.RDBFixture();
        if (getFixtures().contains(DOCUMENT_RDB) && rdb.isAvailable()) {
            fixtures.add(new Object[] { rdb });
        }
        return fixtures;
    }

    @Before
    public void setUp() throws InterruptedException {
        execService = Executors.newCachedThreadPool();
        clock = new Clock.Virtual();
        store = new DocumentMK.Builder()
                .clock(clock)
                .setLeaseCheck(false)
                .setDocumentStore(fixture.createDocumentStore())
                .setAsyncDelay(0)
                .getNodeStore();
        gc = store.getVersionGarbageCollector();

        //Baseline the clock
        clock.waitUntil(Revision.getCurrentTimestamp());
    }

    @After
    public void tearDown() throws Exception {
        store.dispose();
        Revision.resetClockToDefault();
        execService.shutdown();
        execService.awaitTermination(1, MINUTES);
    }

    @Test
    public void gcIgnoredForCheckpoint() throws Exception {
        long expiryTime = 100, maxAge = 20;

        Revision cp = Revision.fromString(store.checkpoint(expiryTime));

        //Fast forward time to future but before expiry of checkpoint
        clock.waitUntil(cp.getTimestamp() + expiryTime - maxAge);
        VersionGCStats stats = gc.gc(maxAge, TimeUnit.MILLISECONDS);
        assertTrue(stats.ignoredGCDueToCheckPoint);

        //Fast forward time to future such that checkpoint get expired
        clock.waitUntil(clock.getTime() + expiryTime + 1);
        stats = gc.gc(maxAge, TimeUnit.MILLISECONDS);
        assertFalse("GC should be performed", stats.ignoredGCDueToCheckPoint);
    }

    @Test
    public void testGCDeletedDocument() throws Exception{
        //1. Create nodes
        NodeBuilder b1 = store.getRoot().builder();
        b1.child("x").child("y");
        b1.child("z");
        store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        long maxAge = 1; //hours
        long delta = TimeUnit.MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(Revision.getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc.gc(maxAge, HOURS);
        assertEquals(0, stats.deletedDocGCCount);

        //Remove x/y
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("x").child("y").remove();
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store.runBackgroundOperations();

        //2. Check that a deleted doc is not collected before
        //maxAge
        //Clock cannot move back (it moved forward in #1) so double the maxAge
        clock.waitUntil(clock.getTime() + delta);
        stats = gc.gc(maxAge*2, HOURS);
        assertEquals(0, stats.deletedDocGCCount);

        //3. Check that deleted doc does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc.gc(maxAge*2, HOURS);
        assertEquals(1, stats.deletedDocGCCount);

        //4. Check that a revived doc (deleted and created again) does not get gc
        NodeBuilder b3 = store.getRoot().builder();
        b3.child("z").remove();
        store.merge(b3, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeBuilder b4 = store.getRoot().builder();
        b4.child("z");
        store.merge(b4, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        stats = gc.gc(maxAge*2, HOURS);
        assertEquals(0, stats.deletedDocGCCount);

    }

    @Test
    public void gcSplitDocs() throws Exception {
        gcSplitDocsInternal("foo");
    }
    
    @Test
    public void gcLongPathSplitDocs() throws Exception {
        gcSplitDocsInternal(Strings.repeat("sub", 120));
    }
    
    private void gcSplitDocsInternal(String subNodeName) throws Exception {
        long maxAge = 1; //hrs
        long delta = TimeUnit.MINUTES.toMillis(10);

        NodeBuilder b1 = store.getRoot().builder();
        b1.child("test").child(subNodeName).child("bar");
        b1.child("test2").child(subNodeName);
        store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Commit on a node which has a child and where the commit root
        // is parent
        for (int i = 0; i < NUM_REVS_THRESHOLD; i++) {
            b1 = store.getRoot().builder();
            //This updates a middle node i.e. one which has child bar
            //Should result in SplitDoc of type PROP_COMMIT_ONLY
            b1.child("test").child(subNodeName).setProperty("prop",i);

            //This should result in SplitDoc of type DEFAULT_NO_CHILD
            b1.child("test2").child(subNodeName).setProperty("prop", i);
            store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        store.runBackgroundOperations();

        List<NodeDocument> previousDocTestFoo =
                ImmutableList.copyOf(getDoc("/test/" + subNodeName).getAllPreviousDocs());
        List<NodeDocument> previousDocTestFoo2 =
                ImmutableList.copyOf(getDoc("/test2/" + subNodeName).getAllPreviousDocs());

        assertEquals(1, previousDocTestFoo.size());
        assertEquals(1, previousDocTestFoo2.size());

        assertEquals(SplitDocType.COMMIT_ROOT_ONLY, previousDocTestFoo.get(0).getSplitDocType());
        assertEquals(SplitDocType.DEFAULT_LEAF, previousDocTestFoo2.get(0).getSplitDocType());

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge) + delta);
        VersionGCStats stats = gc.gc(maxAge, HOURS);
        assertEquals(2, stats.splitDocGCCount);

        //Previous doc should be removed
        assertNull(getDoc(previousDocTestFoo.get(0).getPath()));
        assertNull(getDoc(previousDocTestFoo2.get(0).getPath()));

        //Following would not work for Mongo as the delete happened on the server side
        //And entries from cache are not evicted
        //assertTrue(ImmutableList.copyOf(getDoc("/test2/foo").getAllPreviousDocs()).isEmpty());
    }

    // OAK-1729
    @Test
    public void gcIntermediateDocs() throws Exception {
        long maxAge = 1; //hrs
        long delta = TimeUnit.MINUTES.toMillis(10);

        NodeBuilder b1 = store.getRoot().builder();
        // adding the test node will cause the commit root to be placed
        // on the root document, because the children flag is set on the
        // root document
        b1.child("test");
        store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertTrue(getDoc("/test").getLocalRevisions().isEmpty());
        // setting the test property afterwards will use the new test document
        // as the commit root. this what we want for the test.
        b1 = store.getRoot().builder();
        b1.child("test").setProperty("test", "value");
        store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertTrue(!getDoc("/test").getLocalRevisions().isEmpty());

        for (int i = 0; i < PREV_SPLIT_FACTOR; i++) {
            for (int j = 0; j < NUM_REVS_THRESHOLD; j++) {
                b1 = store.getRoot().builder();
                b1.child("test").setProperty("prop", i * NUM_REVS_THRESHOLD + j);
                store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            }
            store.runBackgroundOperations();
        }
        // trigger another split, now that we have 10 previous docs
        // this will create an intermediate previous doc
        store.addSplitCandidate(Utils.getIdFromPath("/test"));
        store.runBackgroundOperations();

        Map<Revision, Range> prevRanges = getDoc("/test").getPreviousRanges();
        boolean hasIntermediateDoc = false;
        for (Map.Entry<Revision, Range> entry : prevRanges.entrySet()) {
            if (entry.getValue().getHeight() > 0) {
                hasIntermediateDoc = true;
                break;
            }
        }
        assertTrue("Test data does not have intermediate previous docs",
                hasIntermediateDoc);

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge) + delta);
        VersionGCStats stats = gc.gc(maxAge, HOURS);
        assertEquals(10, stats.splitDocGCCount);

        DocumentNodeState test = getDoc("/test").getNodeAtRevision(
                store, store.getHeadRevision(), null);
        assertNotNull(test);
        assertTrue(test.hasProperty("test"));
    }

    // OAK-1779
    @Test
    public void cacheConsistency() throws Exception {
        long maxAge = 1; //hrs
        long delta = TimeUnit.MINUTES.toMillis(10);

        Set<String> names = Sets.newHashSet();
        NodeBuilder b1 = store.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            String name = "test-" + i;
            b1.child(name);
            names.add(name);
        }
        store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        for (ChildNodeEntry entry : store.getRoot().getChildNodeEntries()) {
            entry.getNodeState();
        }

        b1 = store.getRoot().builder();
        b1.getChildNode("test-7").remove();
        names.remove("test-7");
    
        store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge) + delta);

        VersionGCStats stats = gc.gc(maxAge, HOURS);
        assertEquals(1, stats.deletedDocGCCount);

        Set<String> children = Sets.newHashSet();
        for (ChildNodeEntry entry : store.getRoot().getChildNodeEntries()) {
            children.add(entry.getName());
        }
        assertEquals(names, children);
    }

    // OAK-1793
    @Test
    public void gcPrevWithMostRecentModification() throws Exception {
        long maxAge = 1; //hrs
        long delta = TimeUnit.MINUTES.toMillis(10);

        for (int i = 0; i < NUM_REVS_THRESHOLD + 1; i++) {
            NodeBuilder builder = store.getRoot().builder();
            builder.child("foo").setProperty("prop", "v" + i);
            builder.child("bar").setProperty("prop", "v" + i);
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }

        store.runBackgroundOperations();

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge) + delta);

        VersionGCStats stats = gc.gc(maxAge, HOURS);
        assertEquals(2, stats.splitDocGCCount);

        NodeDocument doc = getDoc("/foo");
        assertNotNull(doc);
        DocumentNodeState state = doc.getNodeAtRevision(
                store, store.getHeadRevision(), null);
        assertNotNull(state);
    }

    // OAK-1791
    @Test
    public void gcDefaultLeafSplitDocs() throws Exception {
        Revision.setClock(clock);

        NodeBuilder builder = store.getRoot().builder();
        builder.child("test").setProperty("prop", -1);
        merge(store, builder);

        String id = Utils.getIdFromPath("/test");
        long start = Revision.getCurrentTimestamp();
        // simulate continuous writes once a second for one day
        // collect garbage older than one hour
        int hours = 24;
        if (fixture instanceof DocumentStoreFixture.MongoFixture) {
            // only run for 6 hours on MongoDB to
            // keep time to run on a reasonable level
            hours = 6;
        }
        for (int i = 0; i < 3600 * hours; i++) {
            clock.waitUntil(start + i * 1000);
            builder = store.getRoot().builder();
            builder.child("test").setProperty("prop", i);
            merge(store, builder);
            if (i % 10 == 0) {
                store.runBackgroundOperations();
            }
            // trigger GC twice an hour
            if (i % 1800 == 0) {
                gc.gc(1, HOURS);
                NodeDocument doc = store.getDocumentStore().find(NODES, id);
                assertNotNull(doc);
                int numPrevDocs = Iterators.size(doc.getAllPreviousDocs());
                assertTrue("too many previous docs: " + numPrevDocs,
                        numPrevDocs < 70);
            }
        }
        NodeDocument doc = store.getDocumentStore().find(NODES, id);
        assertNotNull(doc);
        int numRevs = size(doc.getValueMap("prop").entrySet());
        assertTrue("too many revisions: " + numRevs, numRevs < 6000);
    }

    // OAK-2778
    @Test
    public void gcWithConcurrentModification() throws Exception {
        Revision.setClock(clock);
        DocumentStore ds = store.getDocumentStore();

        // create test content
        createTestNode("foo");
        createTestNode("bar");

        // remove again
        NodeBuilder builder = store.getRoot().builder();
        builder.getChildNode("foo").remove();
        builder.getChildNode("bar").remove();
        merge(store, builder);

        // wait one hour
        clock.waitUntil(clock.getTime() + HOURS.toMillis(1));

        final BlockingQueue<NodeDocument> docs = Queues.newSynchronousQueue();
        VersionGCSupport gcSupport = new VersionGCSupport(store.getDocumentStore()) {
            @Override
            public Iterable<NodeDocument> getPossiblyDeletedDocs(long lastModifiedTime) {
                return filter(super.getPossiblyDeletedDocs(lastModifiedTime),
                        new Predicate<NodeDocument>() {
                            @Override
                            public boolean apply(NodeDocument input) {
                                try {
                                    docs.put(input);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                return true;
                            }
                        });
            }
        };
        final VersionGarbageCollector gc = new VersionGarbageCollector(store, gcSupport);
        // start GC -> will try to remove /foo and /bar
        Future<VersionGCStats> f = execService.submit(new Callable<VersionGCStats>() {
            @Override
            public VersionGCStats call() throws Exception {
                return gc.gc(30, MINUTES);
            }
        });

        NodeDocument doc = docs.take();
        String name = PathUtils.getName(doc.getPath());
        // recreate node, which hasn't been removed yet
        name = name.equals("foo") ? "bar" : "foo";
        builder = store.getRoot().builder();
        builder.child(name);
        merge(store, builder);

        // loop over child node entries -> will populate nodeChildrenCache
        for (ChildNodeEntry cne : store.getRoot().getChildNodeEntries()) {
            cne.getName();
        }
        // invalidate cached DocumentNodeState
        DocumentNodeState state = (DocumentNodeState) store.getRoot().getChildNode(name);
        store.invalidateNodeCache(state.getPath(), state.getRevision());

        while (!f.isDone()) {
            docs.poll();
        }

        // read children again after GC finished
        List<String> names = Lists.newArrayList();
        for (ChildNodeEntry cne : store.getRoot().getChildNodeEntries()) {
            names.add(cne.getName());
        }
        assertEquals(1, names.size());

        doc = ds.find(NODES, Utils.getIdFromPath("/" + names.get(0)));
        assertNotNull(doc);
        assertEquals(0, Iterators.size(doc.getAllPreviousDocs()));

        VersionGCStats stats = f.get();
        assertEquals(1, stats.deletedDocGCCount);
        assertEquals(2, stats.splitDocGCCount);
    }

    private void createTestNode(String name) throws CommitFailedException {
        DocumentStore ds = store.getDocumentStore();
        NodeBuilder builder = store.getRoot().builder();
        builder.child(name);
        merge(store, builder);
        String id = Utils.getIdFromPath("/" + name);
        int i = 0;
        while (ds.find(NODES, id).getPreviousRanges().isEmpty()) {
            builder = store.getRoot().builder();
            builder.getChildNode(name).setProperty("p", i++);
            merge(store, builder);
            store.runBackgroundOperations();
        }
    }

    private void merge(DocumentNodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private NodeDocument getDoc(String path){
        return store.getDocumentStore().find(NODES, Utils.getIdFromPath(path), 0);
    }

}
