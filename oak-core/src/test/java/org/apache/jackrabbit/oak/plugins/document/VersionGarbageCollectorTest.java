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
import java.util.concurrent.TimeUnit;

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
public class VersionGarbageCollectorTest {

    private DocumentStoreFixture fixture;

    private Clock clock;

    private DocumentNodeStore store;

    private VersionGarbageCollector gc;

    public VersionGarbageCollectorTest(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> fixtures() throws IOException {
        List<Object[]> fixtures = Lists.newArrayList();
        fixtures.add(new Object[] {new DocumentStoreFixture.MemoryFixture()});

        DocumentStoreFixture mongo = new DocumentStoreFixture.MongoFixture();
        if(mongo.isAvailable()){
           fixtures.add(new Object[] {mongo});
        }
        return fixtures;
    }

    @Before
    public void setUp() throws InterruptedException {
        clock = new Clock.Virtual();
        store = new DocumentMK.Builder()
                .clock(clock)
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
        VersionGCStats stats = gc.gc(maxAge, TimeUnit.HOURS);
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
        stats = gc.gc(maxAge*2, TimeUnit.HOURS);
        assertEquals(0, stats.deletedDocGCCount);

        //3. Check that deleted doc does get collected post maxAge
        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(maxAge*2) + delta);

        stats = gc.gc(maxAge*2, TimeUnit.HOURS);
        assertEquals(1, stats.deletedDocGCCount);

        //4. Check that a revived doc (deleted and created again) does not get gc
        NodeBuilder b3 = store.getRoot().builder();
        b3.child("z").remove();
        store.merge(b3, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeBuilder b4 = store.getRoot().builder();
        b4.child("z");
        store.merge(b4, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(maxAge*2) + delta);
        stats = gc.gc(maxAge*2, TimeUnit.HOURS);
        assertEquals(0, stats.deletedDocGCCount);

    }

    @Test
    public void gcSplitDocs() throws Exception{
        long maxAge = 1; //hrs
        long delta = TimeUnit.MINUTES.toMillis(10);

        NodeBuilder b1 = store.getRoot().builder();
        b1.child("test").child("foo").child("bar");
        b1.child("test2").child("foo");
        store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Commit on a node which has a child and where the commit root
        // is parent
        for (int i = 0; i < NodeDocument.NUM_REVS_THRESHOLD; i++) {
            b1 = store.getRoot().builder();
            //This updates a middle node i.e. one which has child bar
            //Should result in SplitDoc of type PROP_COMMIT_ONLY
            b1.child("test").child("foo").setProperty("prop",i);

            //This should result in SplitDoc of type DEFAULT_NO_CHILD
            b1.child("test2").child("foo").setProperty("prop", i);
            store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        store.runBackgroundOperations();

        List<NodeDocument> previousDocTestFoo =
                ImmutableList.copyOf(getDoc("/test/foo").getAllPreviousDocs());
        List<NodeDocument> previousDocTestFoo2 =
                ImmutableList.copyOf(getDoc("/test2/foo").getAllPreviousDocs());

        assertEquals(1, previousDocTestFoo.size());
        assertEquals(1, previousDocTestFoo2.size());

        assertEquals(SplitDocType.COMMIT_ROOT_ONLY, previousDocTestFoo.get(0).getSplitDocType());
        assertEquals(SplitDocType.DEFAULT_LEAF, previousDocTestFoo2.get(0).getSplitDocType());

        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(maxAge) + delta);
        VersionGCStats stats = gc.gc(maxAge, TimeUnit.HOURS);
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

        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(maxAge) + delta);
        VersionGCStats stats = gc.gc(maxAge, TimeUnit.HOURS);
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

        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(maxAge) + delta);

        VersionGCStats stats = gc.gc(maxAge, TimeUnit.HOURS);
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

        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(maxAge) + delta);

        VersionGCStats stats = gc.gc(maxAge, TimeUnit.HOURS);
        assertEquals(2, stats.splitDocGCCount);

        NodeDocument doc = getDoc("/foo");
        assertNotNull(doc);
        DocumentNodeState state = doc.getNodeAtRevision(
                store, store.getHeadRevision(), null);
        assertNotNull(state);
    }

    private NodeDocument getDoc(String path){
        return store.getDocumentStore().find(NODES, Utils.getIdFromPath(path), 0);
    }

}