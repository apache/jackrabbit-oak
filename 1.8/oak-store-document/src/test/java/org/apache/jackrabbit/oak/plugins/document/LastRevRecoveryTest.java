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

import java.util.Map;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getRootDocument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LastRevRecoveryTest {
    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Clock clock;
    private DocumentNodeStore ds1;
    private DocumentNodeStore ds2;
    private int c1Id;
    private int c2Id;
    private MemoryDocumentStore sharedStore;

    @Before
    public void setUp() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
        // disable lease check because we fiddle with the virtual clock
        final boolean leaseCheck = false;
        sharedStore = new MemoryDocumentStore();
        ds1 = builderProvider.newBuilder()
                .clock(clock)
                .setLeaseCheck(leaseCheck)
                .setAsyncDelay(0)
                .setDocumentStore(sharedStore)
                .setClusterId(1)
                .getNodeStore();
        c1Id = ds1.getClusterId();

        ds2 = builderProvider.newBuilder()
                .clock(clock)
                .setLeaseCheck(leaseCheck)
                .setAsyncDelay(0)
                .setDocumentStore(sharedStore)
                .setClusterId(2)
                .getNodeStore();
        c2Id = ds2.getClusterId();
    }

    @After
    public void tearDown() {
        ds1.dispose();
        ds2.dispose();
        ClusterNodeInfo.resetClockToDefault();
        Revision.resetClockToDefault();
    }


    @Test
    public void testRecover() throws Exception {
        //1. Create base structure /x/y
        NodeBuilder b1 = ds1.getRoot().builder();
        b1.child("x").child("y");
        ds1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ds1.runBackgroundOperations();

        //lastRev are persisted directly for new nodes. In case of
        // updates they are persisted via background jobs

        //1.2 Get last rev populated for root node for ds2
        ds2.runBackgroundOperations();
        NodeBuilder b2 = ds2.getRoot().builder();
        b2.child("x").setProperty("f1","b1");
        ds2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ds2.runBackgroundOperations();

        //2. Add a new node /x/y/z
        b2 = ds2.getRoot().builder();
        b2.child("x").child("y").child("z").setProperty("foo", "bar");
        ds2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Refresh DS1
        ds1.runBackgroundOperations();

        NodeDocument z1 = getDocument(ds1, "/x/y/z");
        NodeDocument y1 = getDocument(ds1, "/x/y");
        NodeDocument x1 = getDocument(ds1, "/x");

        Revision zlastRev2 = z1.getLastRev().get(c2Id);
        // /x/y/z is a new node and does not have a _lastRev
        assertNull(zlastRev2);
        Revision head2 = ds2.getHeadRevision().getRevision(c2Id);

        //lastRev should not be updated for C #2
        assertNull(y1.getLastRev().get(c2Id));

        LastRevRecoveryAgent recovery = new LastRevRecoveryAgent(ds1);

        //Do not pass y1 but still y1 should be updated
        recovery.recover(Lists.newArrayList(x1, z1), c2Id);

        //Post recovery the lastRev should be updated for /x/y and /x
        assertEquals(head2, getDocument(ds1, "/x/y").getLastRev().get(c2Id));
        assertEquals(head2, getDocument(ds1, "/x").getLastRev().get(c2Id));
        assertEquals(head2, getDocument(ds1, "/").getLastRev().get(c2Id));
    }

    // OAK-3079
    @Test
    public void recoveryWithoutRootUpdate() throws Exception {
        String clusterId = String.valueOf(c1Id);
        ClusterNodeInfoDocument doc = sharedStore.find(CLUSTER_NODES, clusterId);

        NodeBuilder builder = ds1.getRoot().builder();
        builder.child("x").child("y").child("z");
        merge(ds1, builder);
        ds1.dispose();

        // reset clusterNodes entry to simulate a crash
        sharedStore.remove(CLUSTER_NODES, clusterId);
        sharedStore.create(CLUSTER_NODES, newArrayList(updateOpFromDocument(doc)));

        // 'wait' until lease expires
        clock.waitUntil(doc.getLeaseEndTime() + 1);

        // run recovery on ds2
        LastRevRecoveryAgent agent = new LastRevRecoveryAgent(ds2);
        Iterable<Integer> clusterIds = agent.getRecoveryCandidateNodes();
        assertTrue(Iterables.contains(clusterIds, c1Id));
        assertEquals("must not recover any documents",
                0, agent.recover(c1Id));
    }

    // OAK-3488
    @Test
    public void recoveryWithTimeout() throws Exception {
        String clusterId = String.valueOf(c1Id);
        ClusterNodeInfoDocument doc = sharedStore.find(CLUSTER_NODES, clusterId);

        NodeBuilder builder = ds1.getRoot().builder();
        builder.child("x").child("y").child("z");
        merge(ds1, builder);
        ds1.dispose();

        // reset clusterNodes entry to simulate a crash
        sharedStore.remove(CLUSTER_NODES, clusterId);
        sharedStore.create(CLUSTER_NODES, newArrayList(updateOpFromDocument(doc)));

        // 'wait' until lease expires
        clock.waitUntil(doc.getLeaseEndTime() + 1);

        // simulate ongoing recovery by cluster node 2
        MissingLastRevSeeker seeker = new MissingLastRevSeeker(sharedStore, clock);
        seeker.acquireRecoveryLock(c1Id, c2Id);

        // run recovery from ds1
        LastRevRecoveryAgent a1 = new LastRevRecoveryAgent(ds1);
        // use current time -> do not wait for recovery of other agent
        assertEquals(-1, a1.recover(c1Id, clock.getTime()));

        seeker.releaseRecoveryLock(c1Id, true);

        assertEquals(0, a1.recover(c1Id, clock.getTime() + 1000));
    }

    // OAK-3488
    @Test
    public void failStartupOnRecoveryTimeout() throws Exception {
        String clusterId = String.valueOf(c1Id);
        ClusterNodeInfoDocument doc = sharedStore.find(CLUSTER_NODES, clusterId);

        NodeBuilder builder = ds1.getRoot().builder();
        builder.child("x").child("y").child("z");
        merge(ds1, builder);
        ds1.dispose();

        // reset clusterNodes entry to simulate a crash
        sharedStore.remove(CLUSTER_NODES, clusterId);
        sharedStore.create(CLUSTER_NODES, newArrayList(updateOpFromDocument(doc)));

        // 'wait' until lease expires
        clock.waitUntil(doc.getLeaseEndTime() + 1);
        // make sure ds2 lease is still fine
        ds2.getClusterInfo().renewLease();

        // simulate ongoing recovery by cluster node 2
        MissingLastRevSeeker seeker = new MissingLastRevSeeker(sharedStore, clock);
        assertTrue(seeker.acquireRecoveryLock(c1Id, c2Id));

        // attempt to restart ds1 while lock is acquired
        try {
            ds1 = new DocumentMK.Builder()
                    .clock(clock)
                    .setDocumentStore(sharedStore)
                    .setClusterId(c1Id)
                    .getNodeStore();
            fail("DocumentStoreException expected");
        } catch (DocumentStoreException e) {
            // expected
        }
        seeker.releaseRecoveryLock(c1Id, true);
    }

    // OAK-3488
    @Test
    public void breakRecoveryLockWithExpiredLease() throws Exception {
        String clusterId = String.valueOf(c1Id);
        ClusterNodeInfoDocument info1 = sharedStore.find(CLUSTER_NODES, clusterId);

        NodeBuilder builder = ds1.getRoot().builder();
        builder.child("x").child("y").child("z");
        merge(ds1, builder);
        ds1.dispose();

        // reset clusterNodes entry to simulate a crash of ds1
        sharedStore.remove(CLUSTER_NODES, clusterId);
        sharedStore.create(CLUSTER_NODES, newArrayList(updateOpFromDocument(info1)));

        // 'wait' until lease expires
        clock.waitUntil(info1.getLeaseEndTime() + 1);
        // make sure ds2 lease is still fine
        ds2.getClusterInfo().renewLease();

        // start of recovery by ds2
        MissingLastRevSeeker seeker = new MissingLastRevSeeker(sharedStore, clock);
        assertTrue(seeker.acquireRecoveryLock(c1Id, c2Id));
        // simulate crash of ds2
        ClusterNodeInfoDocument info2 = sharedStore.find(CLUSTER_NODES, String.valueOf(c2Id));
        ds2.dispose();
        // reset clusterNodes entry
        sharedStore.remove(CLUSTER_NODES, String.valueOf(c2Id));
        sharedStore.create(CLUSTER_NODES, newArrayList(updateOpFromDocument(info2)));
        // 'wait' until ds2's lease expires
        clock.waitUntil(info2.getLeaseEndTime() + 1);

        info1 = sharedStore.find(CLUSTER_NODES, clusterId);
        assertTrue(seeker.isRecoveryNeeded(info1));
        assertTrue(info1.isBeingRecovered());

        // restart ds1
        ds1 = builderProvider.newBuilder()
                .clock(clock)
                .setLeaseCheck(false)
                .setAsyncDelay(0)
                .setDocumentStore(sharedStore)
                .setClusterId(1)
                .getNodeStore();
        info1 = sharedStore.find(CLUSTER_NODES, clusterId);
        assertFalse(seeker.isRecoveryNeeded(info1));
        assertFalse(info1.isBeingRecovered());
    }

    @Test
    public void recoveryMustNotPerformInitialSweep() throws Exception {
        String clusterId = String.valueOf(c1Id);
        ClusterNodeInfoDocument info1 = sharedStore.find(CLUSTER_NODES, clusterId);

        NodeBuilder builder = ds1.getRoot().builder();
        builder.child("x").child("y").child("z");
        merge(ds1, builder);
        ds1.dispose();

        // reset clusterNodes entry to simulate a crash of ds1
        sharedStore.remove(CLUSTER_NODES, clusterId);
        sharedStore.create(CLUSTER_NODES, newArrayList(updateOpFromDocument(info1)));

        // remove the sweep revision as well
        UpdateOp op = new UpdateOp(Utils.getIdFromPath("/"), false);
        op.removeMapEntry("_sweepRev", new Revision(0, 0, c1Id));
        assertNotNull(sharedStore.findAndUpdate(NODES, op));
        NodeDocument doc = getRootDocument(sharedStore);
        assertNull(doc.getSweepRevisions().getRevision(c1Id));

        // 'wait' until lease expires
        clock.waitUntil(info1.getLeaseEndTime() + 1);
        // make sure ds2 lease is still fine
        ds2.getClusterInfo().renewLease();

        // run recovery on ds2 for ds1
        LastRevRecoveryAgent agent = new LastRevRecoveryAgent(ds2);
        Iterable<Integer> clusterIds = agent.getRecoveryCandidateNodes();
        assertTrue(Iterables.contains(clusterIds, c1Id));
        // nothing to recover
        assertEquals("must not recover any documents",
                0, agent.recover(c1Id));
        // must not set sweep revision
        doc = getRootDocument(sharedStore);
        assertNull(doc.getSweepRevisions().getRevision(c1Id));
    }

    private NodeDocument getDocument(DocumentNodeStore nodeStore, String path) {
        return nodeStore.getDocumentStore().find(Collection.NODES, Utils.getIdFromPath(path));
    }

    private static void merge(NodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static UpdateOp updateOpFromDocument(Document doc) {
        UpdateOp op = new UpdateOp(doc.getId(), true);
        for (String key : doc.keySet()) {
            if (key.equals(Document.ID)) {
                continue;
            }
            Object obj = doc.get(key);
            if (obj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<Revision, String> map = (Map<Revision, String>) obj;
                for (Map.Entry<Revision, String> entry : map.entrySet()) {
                    op.setMapEntry(key, entry.getKey(), entry.getValue());
                }
            } else {
                if (obj instanceof Boolean) {
                    op.set(key, (Boolean) obj);
                } else if (obj instanceof Number) {
                    op.set(key, ((Number) obj).longValue());
                } else if (obj != null) {
                    op.set(key, obj.toString());
                } else {
                    op.set(key, null);
                }
            }
        }
        return op;
    }
}
