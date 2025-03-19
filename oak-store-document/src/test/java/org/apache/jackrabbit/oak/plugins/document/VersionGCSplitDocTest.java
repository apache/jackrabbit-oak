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

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.getFixtures;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.DOCUMENT_MEM;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.DOCUMENT_NS;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.DOCUMENT_RDB;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.NO_BINARY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class VersionGCSplitDocTest {

    @Rule
    public final DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentStoreFixture fixture;

    private ExecutorService execService;

    private DocumentStore store;

    private DocumentNodeStore ns;

    private VersionGarbageCollector gc;

    private String longpath;

    private Clock clock;

    public VersionGCSplitDocTest(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> fixtures() throws IOException {
        List<Object[]> fixtures = new ArrayList<>();
        DocumentStoreFixture mongo = new DocumentStoreFixture.MongoFixture();
        if (getFixtures().contains(DOCUMENT_NS) && mongo.isAvailable()) {
            fixtures.add(new Object[] { mongo });
        }

        DocumentStoreFixture rdb = new DocumentStoreFixture.RDBFixture();
        if (getFixtures().contains(DOCUMENT_RDB) && rdb.isAvailable()) {
            fixtures.add(new Object[] { rdb });
        }
        if (fixtures.isEmpty() || getFixtures().contains(DOCUMENT_MEM)) {
            fixtures.add(new Object[] { new DocumentStoreFixture.MemoryFixture() });
        }

        return fixtures;
    }

    @Before
    public void setUp() throws Exception {
        StringBuffer longpath = new StringBuffer();
        while (longpath.length() < 380) {
            longpath.append("thisisaverylongpath");
        }
        this.longpath = longpath.toString();

        clock = new Clock.Virtual();
        store = fixture.createDocumentStore();
        if (fixture.getName().equals("MongoDB")) {
            MongoUtils.dropCollections(MongoUtils.DB);
        }

        execService = Executors.newCachedThreadPool();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);

        ns = builderProvider.newBuilder().clock(clock).setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        gc = ns.getVersionGarbageCollector();
    }

    private void createDefaultNoBranchSplitDocument(DocumentNodeStore ns, String parent) throws CommitFailedException {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("createNoBranchSplitDocument" + longpath).child(parent).child("bar");
        merge(ns, builder);

        for (int i = 0; i < 5; i++) {
            builder = ns.getRoot().builder();
            builder.child("createNoBranchSplitDocument" + longpath).child(parent).setProperty("p", "value-" + i);
            merge(ns, builder);
        }
        ns.runBackgroundOperations();
        String id = Utils.getIdFromPath("/" + "createNoBranchSplitDocument" + longpath + "/" + parent);
        NodeDocument doc = store.find(NODES, id);
        assertNotNull(doc);
        for (UpdateOp op : SplitOperations.forDocument(doc, ns, ns.getHeadRevision(), NO_BINARY, 5)) {
            ns.getDocumentStore().createOrUpdate(NODES, op);
        }
    }

    private void createCommitOnlyAndNoChildSplitDocument(DocumentNodeStore ns, String parent1, String parent2,
            String child) throws CommitFailedException {
        NodeBuilder b1 = ns.getRoot().builder();
        b1.child("createCommitOnlyAndNoChildSplitDocument" + longpath).child(parent1).child(child).child("bar");
        b1.child("createCommitOnlyAndNoChildSplitDocument" + longpath).child(parent2).child(child);
        ns.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Commit on a node which has a child and where the commit root
        // is parent
        for (int i = 0; i < NodeDocument.NUM_REVS_THRESHOLD; i++) {
            b1 = ns.getRoot().builder();
            //This updates a middle node i.e. one which has child bar
            //Should result in SplitDoc of type PROP_COMMIT_ONLY
            b1.child("createCommitOnlyAndNoChildSplitDocument" + longpath).child(parent1).child(child)
                    .setProperty("prop", i);

            //This should result in SplitDoc of type DEFAULT_NO_CHILD
            b1.child("createCommitOnlyAndNoChildSplitDocument" + longpath).child(parent2).child(child)
                    .setProperty("prop", i);
            ns.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
    }

    private void createCommitOnlySplitDocument(DocumentNodeStore ns, String parent1, String parent2, String child)
            throws CommitFailedException {
        NodeBuilder b1 = ns.getRoot().builder();
        b1.child("createCommitOnlySplitDocument" + longpath).child(parent1).child(child).child("bar");
        b1.child("createCommitOnlySplitDocument" + longpath).child(parent2).child(child);
        ns.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Commit on a node which has a child and where the commit root
        // is parent
        for (int i = 0; i < 2 * NodeDocument.NUM_REVS_THRESHOLD; i++) {
            b1 = ns.getRoot().builder();
            //This updates a middle node i.e. one which has child bar
            //Should result in SplitDoc of type PROP_COMMIT_ONLY
            b1.child("createCommitOnlySplitDocument" + longpath).child(parent1).child(child).setProperty("prop", i);

            b1.child("createCommitOnlySplitDocument" + longpath).child("child-" + i);
            ns.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
    }

    private void createDefaultLeafSplitDocument(DocumentNodeStore ns, String parent1, String parent2, String child)
            throws CommitFailedException {
        NodeBuilder b1 = ns.getRoot().builder();
        b1.child("createDefaultLeafSplitDocument" + longpath).child(parent1).child(child).child("bar");
        b1.child("createDefaultLeafSplitDocument" + longpath).child(parent2).child(child);
        ns.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Commit on a node which has a child and where the commit root
        // is parent
        for (int i = 0; i < NodeDocument.NUM_REVS_THRESHOLD; i++) {
            //This should result in SplitDoc of type DEFAULT_NO_CHILD (aka DEFAULT_LEAF)
            b1 = ns.getRoot().builder();
            b1.child("createDefaultLeafSplitDocument" + longpath).child(parent2).child(child).setProperty("prop", i);
            ns.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
    }

    @After
    public void tearDown() throws Exception {
        execService.shutdown();
        execService.awaitTermination(1, MINUTES);
        ns.dispose();
        fixture.dispose();
    }

    @AfterClass
    public static void resetClock() {
        Revision.resetClockToDefault();
    }

    private Future<VersionGCStats> gc() {
        // run gc in a separate thread
        return execService.submit(new Callable<VersionGCStats>() {
            @Override
            public VersionGCStats call() throws Exception {
                return gc.gc(1, TimeUnit.MILLISECONDS);
            }
        });
    }

    private void merge(DocumentNodeStore store, NodeBuilder builder) throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @Test
    public void emptyGC() throws Exception {
        assertEquals(0, gc().get().splitDocGCCount);
    }

    private int countNodeDocuments() {
        return store.query(NODES, NodeDocument.MIN_ID_VALUE, NodeDocument.MAX_ID_VALUE, Integer.MAX_VALUE).size();
    }

    private int countStalePrev() {
        int cnt = 0;
        List<NodeDocument> nodes = store.query(NODES, NodeDocument.MIN_ID_VALUE, NodeDocument.MAX_ID_VALUE,
                Integer.MAX_VALUE);
        for (NodeDocument nodeDocument : nodes) {
            cnt += nodeDocument.getStalePrev().size();
        }
        return cnt;
    }

    @Test
    public void commitOnlyAndNoChild() throws Exception {
        createCommitOnlyAndNoChildSplitDocument(ns, "parent1", "parent2", "child");

        // perform a change to make sure the sweep rev will be newer than
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(NodeDocument.MODIFIED_IN_SECS_RESOLUTION * 2));
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("qux");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        ns.runBackgroundOperations();

        // with OAK-10526 split doc maxRev is now set to now
        // the split doc type 70 GC on mongo uses sweepRev
        // so to get 70 GCed we need to advance sweepRev
        // hence instead of a 1 HOUR wait, we now do :
        // wait 1 min
        clock.waitUntil(clock.getTime() + MINUTES.toMillis(1));

        // to advance sweepRev : unrelated change + sweep
        builder = ns.getRoot().builder();
        builder.child("unrelated");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns.runBackgroundOperations();

        // wait 59 min
        clock.waitUntil(clock.getTime() + MINUTES.toMillis(59));

        int nodesBeforeGc = countNodeDocuments();
        assertEquals(0, countStalePrev());
        final VersionGCStats stats = gc().get();
        int nodesAfterGc = countNodeDocuments();
        assertEquals(3, countStalePrev());
        assertEquals(3, nodesBeforeGc - nodesAfterGc);
        assertEquals(3, stats.splitDocGCCount);
    }

    @Test
    public void commitOnly() throws Exception {
        createCommitOnlySplitDocument(ns, "parent1", "parent2", "child");

        // perform a change to make sure the sweep rev will be newer than
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(NodeDocument.MODIFIED_IN_SECS_RESOLUTION * 2));
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("qux");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        ns.runBackgroundOperations();

        // wait one hour
        clock.waitUntil(clock.getTime() + HOURS.toMillis(1));

        int nodesBeforeGc = countNodeDocuments();
        assertEquals(0, countStalePrev());
        final VersionGCStats stats = gc().get();
        int nodesAfterGc = countNodeDocuments();
        System.out.println("before gc : " + nodesBeforeGc + ", after gc : " + nodesAfterGc);
        assertTrue(countStalePrev() >= 1);
        assertTrue(nodesBeforeGc - nodesAfterGc >= 1);
        assertTrue(stats.splitDocGCCount >= 1);
    }

    @Test
    public void defaultLeaf() throws Exception {
        createDefaultLeafSplitDocument(ns, "parent1", "parent2", "child");

        // perform a change to make sure the sweep rev will be newer than
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(NodeDocument.MODIFIED_IN_SECS_RESOLUTION * 2));
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("qux");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        ns.runBackgroundOperations();

        // wait one hour
        clock.waitUntil(clock.getTime() + HOURS.toMillis(1));

        int nodesBeforeGc = countNodeDocuments();
        assertEquals(0, countStalePrev());
        final VersionGCStats stats = gc().get();
        int nodesAfterGc = countNodeDocuments();
        assertEquals(1, countStalePrev());
        assertEquals(1, nodesBeforeGc - nodesAfterGc);
        assertEquals(1, stats.splitDocGCCount);
    }

    @Test
    public void defaultNoBranch() throws Exception {
        createDefaultNoBranchSplitDocument(ns, "aparent");

        // perform a change to make sure the sweep rev will be newer than
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(NodeDocument.MODIFIED_IN_SECS_RESOLUTION * 2));
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("qux");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // wait one hour
        clock.waitUntil(clock.getTime() + HOURS.toMillis(1));

        ns.runBackgroundOperations();

        int nodesBeforeGc = countNodeDocuments();
        assertEquals(0, countStalePrev());
        final VersionGCStats stats = gc().get();
        int nodesAfterGc = countNodeDocuments();
        assertEquals(1, countStalePrev());
        assertEquals(1, nodesBeforeGc - nodesAfterGc);
        assertEquals(1, stats.splitDocGCCount);
    }

}
