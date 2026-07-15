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
package org.apache.jackrabbit.oak.plugins.document;

import static org.apache.jackrabbit.oak.plugins.document.RecoveryHandler.NOOP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.mongodb.client.MongoDatabase;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopStream;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * A set of simple cluster tests.
 */
public class ClusterTest {

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    private static final boolean MONGO_DB = false;

    private List<DocumentMK> mks = new ArrayList<>();
    private MemoryDocumentStore ds;
    private MemoryBlobStore bs;

    @After
    public void resetClock() {
        ClusterNodeInfo.resetClockToDefault();
    }

    @Test
    public void threeNodes() throws Exception {
        DocumentMK mk1 = createMK(1, 0);
        DocumentMK mk2 = createMK(2, 0);
        DocumentMK mk3 = createMK(3, 0);

        mk1.commit("/", "+\"test\":{}", null, null);
        mk2.commit("/", "+\"a\":{}", null, null);
        mk3.commit("/", "+\"b\":{}", null, null);
        mk2.backgroundWrite();
        mk2.backgroundRead();
        mk3.backgroundWrite();
        mk3.backgroundRead();
        mk1.backgroundWrite();
        mk1.backgroundRead();
        mk2.backgroundWrite();
        mk2.backgroundRead();
        mk3.backgroundWrite();
        mk3.backgroundRead();

        mk2.commit("/", "^\"test/x\":1", null, null);
        String n3 = mk3.getNodes("/test", mk3.getHeadRevision(), 0, 0, 10, null);
        // mk3 didn't see the previous change yet;
        // it is questionable if this should prevent any changes to this node
        // (currently it does not)
        assertEquals("{\":childNodeCount\":0}", n3);
        mk3.commit("/", "^\"test/y\":2", null, null);

        mk3.backgroundWrite();
        mk3.backgroundRead();
        mk1.backgroundWrite();
        mk1.backgroundRead();

        String r1 = mk1.getHeadRevision();
        String n1 = mk1.getNodes("/test", r1, 0, 0, 10, null);
        // mk1 only sees the change of mk3 so far
        assertEquals("{\"y\":2,\":childNodeCount\":0}", n1);

        mk2.backgroundWrite();
        mk2.backgroundRead();
        mk1.backgroundWrite();
        mk1.backgroundRead();

        String r1b = mk1.getHeadRevision();
        String n1b = mk1.getNodes("/test", r1b, 0, 0, 10, null);
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(n1b);
        // mk1 now sees both changes
        assertEquals(1L, obj.get("x"));
        assertEquals(2L, obj.get("y"));
        assertEquals(0L, obj.get(":childNodeCount"));
    }

    @Test
    public void clusterNodeInfoLease() throws InterruptedException {
        Clock c = new Clock.Virtual();
        c.waitUntil(System.currentTimeMillis());
        ClusterNodeInfo.setClock(c);

        MemoryDocumentStore store = new MemoryDocumentStore();
        ClusterNodeInfo c1, c2;
        c1 = ClusterNodeInfo.getInstance(store, NOOP, "m1", null, 0);
        assertEquals(1, c1.getId());
        // expire lease
        c.waitUntil(c1.getLeaseEndTime() + ClusterNodeInfo.DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS);

        // using a NOOP RecoveryHandler must prevent use of expired clusterId 1 (OAK-7316)
        c2 = ClusterNodeInfo.getInstance(store, NOOP, "m1", null, 0);
        assertEquals(2, c2.getId());
    }

    @Test
    public void openCloseOpen() {
        MemoryDocumentStore ds = new MemoryDocumentStore();
        MemoryBlobStore bs = new MemoryBlobStore();

        DocumentMK mk1 = createMK(1, 0, ds, bs);
        mk1.commit("/", "+\"a\": {}", null, null);
        mk1.commit("/", "-\"a\"", null, null);
        mk1.runBackgroundOperations();

        DocumentMK mk2 = createMK(2, 0, ds, bs);
        mk2.commit("/", "+\"a\": {}", null, null);
        mk2.commit("/", "-\"a\"", null, null);
        mk2.runBackgroundOperations();

        DocumentMK mk3 = createMK(3, 0, ds, bs);
        mk3.commit("/", "+\"a\": {}", null, null);
        mk3.commit("/", "-\"a\"", null, null);
        mk3.runBackgroundOperations();

        DocumentMK mk4 = createMK(4, 0, ds, bs);
        mk4.commit("/", "+\"a\": {}", null, null);
        mk4.runBackgroundOperations();

        DocumentMK mk5 = createMK(5, 0, ds, bs);
        mk5.commit("/", "-\"a\"", null, null);
        mk5.commit("/", "+\"a\": {}", null, null);
    }

    @Test
    public void clusterBranchInVisibility() throws InterruptedException {
        DocumentMK mk1 = createMK(1);
        mk1.commit("/", "+\"regular\": {}", null, null);
        String b1 = mk1.branch(null);
        String b2 = mk1.branch(null);
        b1 = mk1.commit("/", "+\"branchVisible\": {}", b1, null);
        b2 = mk1.commit("/", "+\"branchInvisible\": {}", b2, null);
        mk1.merge(b1, null);
        // mk1.merge only becomes visible to mk2 after async delay
        // therefore dispose mk1 now to make sure it flushes
        // unsaved last revisions
        disposeMK(mk1);

        DocumentMK mk2 = createMK(2);
        String nodes = mk2.getNodes("/", null, 0, 0, 100, null);
        assertEquals("{\"branchVisible\":{},\"regular\":{},\":childNodeCount\":2}", nodes);
    }

    /**
     * Test for OAK-1254
     */
    @Test
    public void clusterBranchRebase() throws Exception {
        DocumentMK mk1 = createMK(1, 0);
        mk1.commit("/", "+\"test\":{}", null, null);
        mk1.runBackgroundOperations();
        DocumentMK mk2 = createMK(2, 0);
        DocumentMK mk3 = createMK(3, 0);

        DocumentNodeStore ns3 = mk3.getNodeStore();
        // the next line is required for the test even if it
        // just reads from the node store. do not remove!
        traverse(ns3.getRoot(), "/");

        String b3 = mk3.branch(null);
        b3 = mk3.commit("/", "+\"mk3\":{}", b3, null);
        assertTrue(mk3.nodeExists("/test", b3));

        mk2.commit("/", "+\"test/mk21\":{}", null, null);
        mk2.runBackgroundOperations();

        mk3.runBackgroundOperations(); // pick up changes from mk2
        String base3 = mk3.getHeadRevision();

        assertFalse(mk3.nodeExists("/test/mk21", b3));
        b3 = mk3.rebase(b3, base3);

        mk2.commit("/", "+\"test/mk22\":{}", null, null);
        mk2.runBackgroundOperations();

        mk3.runBackgroundOperations(); // pick up changes from mk2

        DocumentNodeState base = ns3.getNode(Path.ROOT, RevisionVector.fromString(base3));
        assertNotNull(base);
        NodeState branchHead = ns3.getNode(Path.ROOT, RevisionVector.fromString(b3));
        assertNotNull(branchHead);
        TrackingDiff diff = new TrackingDiff();
        branchHead.compareAgainstBaseState(base, diff);
        assertEquals(1, diff.added.size());
        assertEquals(Set.of("/mk3"), diff.added);
        assertEquals(Set.of(), diff.deleted);
    }

    @Test
    public void clusterNodeInfo() {
        MemoryDocumentStore store = new MemoryDocumentStore();
        ClusterNodeInfo c1, c2;

        c1 = ClusterNodeInfo.getInstance(store, NOOP, "m1", null, 0);
        assertEquals(1, c1.getId());
        c1.dispose();

        // get the same id
        c1 = ClusterNodeInfo.getInstance(store, NOOP, "m1", null, 0);
        assertEquals(1, c1.getId());
        c1.dispose();

        // a different machine
        // must get inactive id (OAK-7316)
        c1 = ClusterNodeInfo.getInstance(store, NOOP, "m2", null, 0);
        assertEquals(1, c1.getId());

        // yet another machine
        c2 = ClusterNodeInfo.getInstance(store, NOOP, "m3", "/a", 0);
        assertEquals(2, c2.getId());

        c1.dispose();
        c2.dispose();

        // must acquire same id as before with matching machineId/instanceId
        c1 = ClusterNodeInfo.getInstance(store, NOOP, "m3", "/a", 0);
        assertEquals(2, c1.getId());

        c1.dispose();

        c1 = ClusterNodeInfo.getInstance(store, NOOP, "m3", "/b", 0);
        assertEquals(1, c1.getId());

        c1.dispose();
    }

    @Test
    public void conflict() {
        DocumentMK mk1 = createMK(1, 0);
        DocumentMK mk2 = createMK(2, 0);

        String m1r0 = mk1.getHeadRevision();
        String m2r0 = mk2.getHeadRevision();

        mk1.commit("/", "+\"test\":{}", m1r0, null);
        try {
            mk2.commit("/", "+\"test\":{}", m2r0, null);
            fail();
        } catch (DocumentStoreException e) {
            // expected
        }
        mk1.runBackgroundOperations();
        mk2.runBackgroundOperations();

        // node becomes visible after running background operations
        String n1 = mk1.getNodes("/", mk1.getHeadRevision(), 0, 0, 10, null);
        String n2 = mk2.getNodes("/", mk2.getHeadRevision(), 0, 0, 10, null);
        assertEquals(n1, n2);
    }

    @Test
    public void revisionVisibility() throws InterruptedException {
        DocumentMK mk1 = createMK(1);
        DocumentMK mk2 = createMK(2);

        String m2h;
        m2h = mk2.getNodes("/", mk2.getHeadRevision(), 0, 0, 2, null);
        assertEquals("{\":childNodeCount\":0}", m2h);
        String oldHead = mk2.getHeadRevision();

        mk1.commit("/", "+\"test\":{}", null, null);
        String m1h = mk1.getNodes("/", mk1.getHeadRevision(), 0, 0, 2, null);
        assertEquals("{\"test\":{},\":childNodeCount\":1}", m1h);

        // not available yet...
        assertEquals("{\":childNodeCount\":0}", m2h);
        m2h = mk2.getNodes("/test", mk2.getHeadRevision(), 0, 0, 2, null);

        // the delay is 10 ms - wait at most 1000 millis
        for (int i = 0; i < 100; i++) {
            Thread.sleep(10);
            if (mk1.getPendingWriteCount() > 0) {
                continue;
            }
            if (mk2.getHeadRevision().equals(oldHead)) {
                continue;
            }
            break;
        }

        // so now it should be available
        m2h = mk2.getNodes("/", mk2.getHeadRevision(), 0, 0, 5, null);
        assertEquals("{\"test\":{},\":childNodeCount\":1}", m2h);
    }

    @Test
    public void rollbackAfterConflict() {
        DocumentMK mk1 = createMK(1);
        DocumentMK mk2 = createMK(2);

        String m1r0 = mk1.getHeadRevision();
        String m2r0 = mk2.getHeadRevision();

        mk1.commit("/", "+\"test\":{}", m1r0, null);
        try {
            mk2.commit("/", "+\"a\": {} +\"test\":{}", m2r0, null);
            fail();
        } catch (DocumentStoreException e) {
            // expected
        }
        mk2.commit("/", "+\"a\": {}", null, null);
    }

    @Test
    public void diffManyChildrenReadWriteMode() throws Exception {
        DocumentMK mk1 = createMK(1, 0);
        DocumentMK mk2 = createMK(2, 0);
        NodeBuilder builder = mk1.getNodeStore().getRoot().builder();
        builder.child("foo1").child("bar1");
        merge(mk1.getNodeStore(), builder);
        mk1.runBackgroundOperations();
        mk2.runBackgroundOperations();
        RevisionVector fromRev = mk1.getNodeStore().getRoot().getLastRevision();
        Thread.sleep(1000);
        builder = mk1.getNodeStore().getRoot().builder();
        builder.getChildNode("foo1").getChildNode("bar1").setProperty("test", "test");
        merge(mk1.getNodeStore(), builder);
        disposeMK(mk1);
        Thread.sleep(1000);
        mk1 = createMK(1, 0);
        DocumentMK mk3rw = createMK(3, 0, false);
        DocumentNodeStore ns3rw = mk3rw.getNodeStore();
        RevisionVector toRev = ns3rw.getRoot().getLastRevision();
        Thread.sleep(5000);
        JsopWriter w1 = new JsopStream();
        ns3rw.diffManyChildren(w1, ns3rw.getRoot().getPath(), fromRev, toRev);
        JsopWriter w2 = new JsopStream();
        mk1.getNodeStore().diffManyChildren(w2, mk1.nodeStore.getRoot().getPath(), fromRev, toRev);
        assertEquals(w1.toString(), w2.toString());
    }

    @Test
    public void diffManyChildrenReadOnlyMode() throws Exception {
        if (MONGO_DB) {
            DocumentMK mk1 = createMK(1, 0);
            DocumentMK mk2 = createMK(2, 0);
            NodeBuilder builder = mk1.getNodeStore().getRoot().builder();
            builder.child("foo1").child("bar1");
            merge(mk1.getNodeStore(), builder);
            mk1.runBackgroundOperations();
            mk2.runBackgroundOperations();
            RevisionVector fromRev = mk1.getNodeStore().getRoot().getLastRevision();
            Thread.sleep(1000);
            builder = mk1.getNodeStore().getRoot().builder();
            builder.getChildNode("foo1").getChildNode("bar1").setProperty("test", "test");
            merge(mk1.getNodeStore(), builder);
            disposeMK(mk1);
            Thread.sleep(1000);
            mk1 = createMK(1, 0);
            DocumentMK mk1ro = createMK(1, 0, true);
            DocumentMK mk3rw = createMK(3, 0, false);
            Thread.sleep(5000);
            compareDiffs(mk1.getNodeStore(), mk1ro.getNodeStore(), fromRev, mk1.getNodeStore().getRoot().getLastRevision());
            compareDiffs(mk1.getNodeStore(), mk3rw.getNodeStore(), fromRev, mk1.getNodeStore().getRoot().getLastRevision());
        }
    }

    private void compareDiffs(DocumentNodeStore store1, DocumentNodeStore store2, RevisionVector from, RevisionVector to) {
        JsopWriter w1 = new JsopStream();
        store2.diffManyChildren(w1, store1.getRoot().getPath(), from, to);
        JsopWriter w2 = new JsopStream();
        store1.diffManyChildren(w2, store1.getRoot().getPath(), from, to);
        assertEquals(w1.toString(), w2.toString());
    }

    @Test
    public void fromExternalChange() throws Exception {
        final List<DocumentNodeState> rootStates1 = new ArrayList<>();
        DocumentNodeStore ns1 = createMK(1, 0).getNodeStore();
        ns1.addObserver(new Observer() {
            @Override
            public void contentChanged(@NotNull NodeState root,
                                       @NotNull CommitInfo info) {
                rootStates1.add((DocumentNodeState) root);
            }
        });
        final List<DocumentNodeState> rootStates2 = new ArrayList<>();
        DocumentNodeStore ns2 = createMK(2, 0).getNodeStore();
        ns2.addObserver(new Observer() {
            @Override
            public void contentChanged(@NotNull NodeState root,
                                       @NotNull CommitInfo info) {
                rootStates2.add((DocumentNodeState) root);
            }
        });

        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        rootStates1.clear();
        rootStates2.clear();

        NodeBuilder builder = ns1.getRoot().builder();
        builder.child("foo");
        merge(ns1, builder);

        assertEquals(1, rootStates1.size());
        assertEquals(0, rootStates2.size());
        assertFalse(rootStates1.get(0).isFromExternalChange());

        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        assertEquals(1, rootStates1.size());
        assertEquals(1, rootStates2.size());
        assertTrue(rootStates2.get(0).isFromExternalChange());
        NodeState foo = rootStates2.get(0).getChildNode("foo");
        assertTrue(foo instanceof DocumentNodeState);
        assertTrue(((DocumentNodeState) foo).isFromExternalChange());
    }

    @Before
    @After
    public void clear() {
        for (DocumentMK mk : mks) {
            mk.dispose();
        }
        mks.clear();
        if (MONGO_DB) {
            MongoDatabase db = connectionFactory.getConnection().getDatabase();
            MongoUtils.dropCollections(db);
        }
    }

    private static NodeState merge(NodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        return store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private DocumentMK createMK(int clusterId) {
        return createMK(clusterId, 10);
    }

    private DocumentMK createMK(int clusterId, int asyncDelay, boolean readonly) {
        if (MONGO_DB) {
            MongoConnection connection = connectionFactory.getConnection();
            DocumentMK.Builder builder = new DocumentMK.Builder();
            if (readonly) { builder.setReadOnlyMode(); };
            return register(builder
                    .setMongoDB(connection.getMongoClient(), connection.getDBName())
                    .setClusterId(clusterId).setAsyncDelay(asyncDelay).open());
        } else {
            if (ds == null) {
                ds = new MemoryDocumentStore();
            }
            if (bs == null) {
                bs = new MemoryBlobStore();
            }
            return createMK(clusterId, asyncDelay, ds, bs);
        }
    }

    private DocumentMK createMK(int clusterId, int asyncDelay) {
        return createMK(clusterId, asyncDelay, false);
    }

    private DocumentMK createMK(int clusterId, int asyncDelay,
                             DocumentStore ds, BlobStore bs) {
        return register(new DocumentMK.Builder().setDocumentStore(ds)
                .setBlobStore(bs).setClusterId(clusterId)
                .setAsyncDelay(asyncDelay).open());
    }

    private DocumentMK register(DocumentMK mk) {
        mks.add(mk);
        return mk;
    }

    private void disposeMK(DocumentMK mk) {
        mk.dispose();
        for (int i = 0; i < mks.size(); i++) {
            if (mks.get(i) == mk) {
                mks.remove(i);
            }
        }
    }

    private void traverse(NodeState node, String path) {
        for (ChildNodeEntry child : node.getChildNodeEntries()) {
            traverse(child.getNodeState(), PathUtils.concat(path, child.getName()));
        }
    }
}
