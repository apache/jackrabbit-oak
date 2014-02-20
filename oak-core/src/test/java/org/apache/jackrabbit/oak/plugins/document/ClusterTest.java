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

import java.util.HashSet;
import java.util.Set;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.mongodb.DB;

/**
 * A set of simple cluster tests.
 */
public class ClusterTest {

    private static final boolean MONGO_DB = false;
    // private static final boolean MONGO_DB = true;

    private MemoryDocumentStore ds;
    private MemoryBlobStore bs;

    @Test
    public void threeNodes() throws Exception {
        MemoryDocumentStore ds = new MemoryDocumentStore();
        MemoryBlobStore bs = new MemoryBlobStore();
        DocumentMK.Builder builder;

        builder = new DocumentMK.Builder();
        builder.setDocumentStore(ds).setBlobStore(bs).setAsyncDelay(0);
        DocumentMK mk1 = builder.setClusterId(1).open();
        builder = new DocumentMK.Builder();
        builder.setDocumentStore(ds).setBlobStore(bs).setAsyncDelay(0);
        DocumentMK mk2 = builder.setClusterId(2).open();
        builder = new DocumentMK.Builder();
        builder.setDocumentStore(ds).setBlobStore(bs).setAsyncDelay(0);
        DocumentMK mk3 = builder.setClusterId(3).open();

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
        // mk1 now sees both changes
        assertEquals("{\"x\":1,\"y\":2,\":childNodeCount\":0}", n1b);

        mk1.dispose();
        mk2.dispose();
        mk3.dispose();
    }

    @Test
    public void clusterNodeInfoLease() throws InterruptedException {
        MemoryDocumentStore store = new MemoryDocumentStore();
        ClusterNodeInfo c1, c2;
        c1 = ClusterNodeInfo.getInstance(store, "m1", null);
        assertEquals(1, c1.getId());
        c1.setLeaseTime(1);
        // this will quickly expire
        c1.renewLease(1);
        Thread.sleep(10);
        c2 = ClusterNodeInfo.getInstance(store, "m1", null);
        assertEquals(1, c2.getId());
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

        mk1.dispose();
        mk2.dispose();
        mk3.dispose();
        mk4.dispose();
        mk5.dispose();
    }

    @Test
    public void clusterNodeId() {
        DocumentMK mk1 = createMK(0);
        DocumentMK mk2 = createMK(0);
        assertEquals(1, mk1.getClusterInfo().getId());
        assertEquals(2, mk2.getClusterInfo().getId());
        mk1.dispose();
        mk2.dispose();
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
        mk1.dispose();

        DocumentMK mk2 = createMK(2);
        String nodes = mk2.getNodes("/", null, 0, 0, 100, null);
        assertEquals("{\"branchVisible\":{},\"regular\":{},\":childNodeCount\":2}", nodes);

        mk2.dispose();
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

        KernelNodeStore ns3 = new KernelNodeStore(mk3);
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

        NodeState base = ns3.retrieve(base3); // branch base
        assertNotNull(base);
        NodeState branchHead = ns3.retrieve(b3);
        assertNotNull(branchHead);
        TrackingDiff diff = new TrackingDiff();
        branchHead.compareAgainstBaseState(base, diff);
        assertEquals(1, diff.added.size());
        assertEquals(Sets.newHashSet("/mk3"), diff.added);
        assertEquals(new HashSet<String>(), diff.deleted);

        mk1.dispose();
        mk2.dispose();
        mk3.dispose();
    }

    @Test
    public void clusterNodeInfo() {
        MemoryDocumentStore store = new MemoryDocumentStore();
        ClusterNodeInfo c1, c2, c3, c4;

        c1 = ClusterNodeInfo.getInstance(store, "m1", null);
        assertEquals(1, c1.getId());
        c1.dispose();

        // get the same id
        c1 = ClusterNodeInfo.getInstance(store, "m1", null);
        assertEquals(1, c1.getId());
        c1.dispose();

        // now try to add another one:
        // must get a new id
        c2 = ClusterNodeInfo.getInstance(store, "m2", null);
        assertEquals(2, c2.getId());

        // a different machine
        c3 = ClusterNodeInfo.getInstance(store, "m3", "/a");
        assertEquals(3, c3.getId());

        c2.dispose();
        c3.dispose();

        c3 = ClusterNodeInfo.getInstance(store, "m3", "/a");
        assertEquals(3, c3.getId());

        c3.dispose();

        c4 = ClusterNodeInfo.getInstance(store, "m3", "/b");
        assertEquals(4, c4.getId());

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
        } catch (MicroKernelException e) {
            // expected
        }
        mk1.runBackgroundOperations();
        mk2.runBackgroundOperations();

        // node becomes visible after running background operations
        String n1 = mk1.getNodes("/", mk1.getHeadRevision(), 0, 0, 10, null);
        String n2 = mk2.getNodes("/", mk2.getHeadRevision(), 0, 0, 10, null);
        assertEquals(n1, n2);

        mk1.dispose();
        mk2.dispose();
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

        mk1.dispose();
        mk2.dispose();
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
        } catch (MicroKernelException e) {
            // expected
        }
        mk2.commit("/", "+\"a\": {}", null, null);

        mk1.dispose();
        mk2.dispose();
    }

    @Before
    @After
    public void clear() {
        if (MONGO_DB) {
            DB db = MongoUtils.getConnection().getDB();
            MongoUtils.dropCollections(db);
        }
    }

    private DocumentMK createMK(int clusterId) {
        return createMK(clusterId, 10);
    }

    private DocumentMK createMK(int clusterId, int asyncDelay) {
        if (MONGO_DB) {
            DB db = MongoUtils.getConnection().getDB();
            return new DocumentMK.Builder().setMongoDB(db)
                    .setClusterId(clusterId).setAsyncDelay(asyncDelay).open();
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

    private DocumentMK createMK(int clusterId, int asyncDelay,
                             DocumentStore ds, BlobStore bs) {
        return new DocumentMK.Builder().setDocumentStore(ds).setBlobStore(bs)
                .setClusterId(clusterId).setAsyncDelay(asyncDelay).open();
    }

    private void traverse(NodeState node, String path) {
        for (ChildNodeEntry child : node.getChildNodeEntries()) {
            traverse(child.getNodeState(), PathUtils.concat(path, child.getName()));
        }
    }

    private static class TrackingDiff extends DefaultNodeStateDiff {

        final String path;
        final Set<String> added;
        final Set<String> deleted;
        final Set<String> modified;

        TrackingDiff() {
            this("/", new HashSet<String>(),
                    new HashSet<String>(), new HashSet<String>());
        }

        private TrackingDiff(String path,
                             Set<String> added,
                             Set<String> deleted,
                             Set<String> modified) {
            this.path = path;
            this.added = added;
            this.deleted = deleted;
            this.modified = modified;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            String p = PathUtils.concat(path, name);
            added.add(p);
            return after.compareAgainstBaseState(EMPTY_NODE,
                    new TrackingDiff(p, added, deleted, modified));
        }

        @Override
        public boolean childNodeChanged(String name,
                                        NodeState before,
                                        NodeState after) {
            String p = PathUtils.concat(path, name);
            modified.add(p);
            return after.compareAgainstBaseState(before,
                    new TrackingDiff(p, added, deleted, modified));
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            String p = PathUtils.concat(path, name);
            deleted.add(p);
            return MISSING_NODE.compareAgainstBaseState(before, new TrackingDiff(p, added, deleted, modified));
        }
    }
}
