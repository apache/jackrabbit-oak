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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.COLLISIONS;
import static org.apache.jackrabbit.oak.plugins.document.Path.ROOT;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getRootDocument;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CollisionTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    
    private static final AtomicInteger COUNTER = new AtomicInteger();

    private Clock clock;

    private List<Object> branches = new ArrayList<>();

    @Before
    public void before() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
    }

    @After
    public void after() {
        branches.clear();
    }

    @AfterClass
    public static void afterClass() {
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
    }

    // OAK-2342
    @Test
    public void purge() throws Exception {
        DocumentMK mk1 = newBuilder().setClusterId(1).open();
        DocumentNodeStore ns1 = mk1.getNodeStore();
        DocumentStore store = ns1.getDocumentStore();

        DocumentMK mk2 = newBuilder().setClusterId(2)
                .setDocumentStore(store).open();
        DocumentNodeStore ns2 = mk2.getNodeStore();

        createCollision(mk1);
        createCollision(mk2);

        String id = getIdFromPath("/");
        assertEquals(2, getDocument(store, id).getLocalMap(COLLISIONS).size());

        // restart node store
        ns1.dispose();
        mk1 = newBuilder().setClusterId(1)
                .setDocumentStore(store).open();
        ns1 = mk1.getNodeStore();

        // must purge collision for clusterId 1
        assertEquals(1, getDocument(store, id).getLocalMap(COLLISIONS).size());
        ns1.dispose();

        // restart other node store
        ns2.dispose();
        mk2 = newBuilder().setClusterId(2)
                .setDocumentStore(store).open();
        ns2 = mk2.getNodeStore();

        // must purge collision for clusterId 2
        assertEquals(0, getDocument(store, id).getLocalMap(COLLISIONS).size());
        ns2.dispose();
    }

    @Test
    public void isConflicting() throws CommitFailedException {
        DocumentNodeStore ns = newBuilder()
                .setAsyncDelay(0).getNodeStore();
        DocumentStore store = ns.getDocumentStore();
        String id = Utils.getIdFromPath("/test");

        NodeBuilder b = ns.getRoot().builder();
        b.child("test").setProperty("p", "a");
        // test:{p:"a"}
        Revision r1 = merge(ns, b).getRevision(ns.getClusterId());
        assertNotNull(r1);

        NodeDocument doc = getDocument(store, id);
        // concurrent create
        Revision c = ns.newRevision();
        UpdateOp op = new UpdateOp(id, true);
        NodeDocument.setDeleted(op, c, false);
        Collision col = newCollision(doc, r1, op, c, ns);
        assertTrue(col.isConflicting());
        // concurrent change
        op = new UpdateOp(id, false);
        op.setMapEntry("p", c, "b");
        col = newCollision(doc, r1, op, c, ns);
        assertTrue(col.isConflicting());

        b = ns.getRoot().builder();
        b.child("test").setProperty("p", "b");
        // test:{p:"b"}
        Revision r2 = merge(ns, b).getRevision(ns.getClusterId());
        assertNotNull(r2);

        doc = getDocument(store, id);
        // concurrent delete
        c = ns.newRevision();
        op = new UpdateOp(id, false);
        op.setDelete(true);
        NodeDocument.setDeleted(op, c, true);
        col = newCollision(doc, r2, op, c, ns);
        assertTrue(col.isConflicting());
        // concurrent conflicting property set
        op = new UpdateOp(id, false);
        op.setMapEntry("p", c, "c");
        col = newCollision(doc, r2, op, c, ns);
        assertTrue(col.isConflicting());
        // concurrent non-conflicting property set
        op = new UpdateOp(id, false);
        op.setMapEntry("q", c, "a");
        col = newCollision(doc, r2, op, c, ns);
        assertFalse(col.isConflicting());

        b = ns.getRoot().builder();
        b.child("test").remove();
        // test (removed)
        Revision r3 = merge(ns, b).getRevision(ns.getClusterId());
        assertNotNull(r3);

        doc = getDocument(store, id);
        // concurrent delete
        c = ns.newRevision();
        op = new UpdateOp(id, false);
        op.setDelete(true);
        NodeDocument.setDeleted(op, c, true);
        col = newCollision(doc, r3, op, c, ns);
        assertTrue(col.isConflicting());
        // concurrent conflicting property set
        op = new UpdateOp(id, false);
        op.setMapEntry("p", c, "d");
        col = newCollision(doc, r3, op, c, ns);
        assertTrue(col.isConflicting());
    }

    private static Collision newCollision(@NotNull NodeDocument document,
                                          @NotNull Revision theirRev,
                                          @NotNull UpdateOp ourOp,
                                          @NotNull Revision ourRev,
                                          @NotNull RevisionContext context) {
        return new Collision(document, theirRev, ourOp, ourRev, context,
                RevisionVector.fromString(""));
    }

    @Test
    public void collisionOnOrphanedBranch() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = newBuilder()
                .setDocumentStore(store).setAsyncDelay(0)
                .setUpdateLimit(10).build();

        NodeBuilder builder = ns.getRoot().builder();
        // force a branch commit
        for (int i = 0; i < 20; i++) {
            builder.child("n-" + i).setProperty("p", "v");
        }
        retainBranches(ns);
        ns.dispose();
        
        NodeDocument root = store.find(NODES, Utils.getIdFromPath(ROOT));
        assertNotNull(root);
        assertThat(root.getLocalBranchCommits(), not(empty()));

        // make sure start timestamp of node store is higher than
        // branch commit revision timestamp
        clock.getTimeIncreasing();

        // start it up again
        ns = newBuilder()
                .setDocumentStore(store).setAsyncDelay(0)
                .setUpdateLimit(10).build();
        ns.updateClusterState();

        root = store.find(NODES, Utils.getIdFromPath(ROOT));
        assertNotNull(root);
        // on init the DocumentNodeStore removes orphaned
        // branch commit entries on the root document
        assertThat(root.getLocalBranchCommits(), empty());

        // but some changes are still there
        Path p = Path.fromString("/n-0");
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath(p));
        assertNotNull(doc);
        assertThat(doc.getLocalBranchCommits(), not(empty()));

        builder = ns.getRoot().builder();
        builder.child("n-0");
        merge(ns, builder);

        // must not create a collision marker for a branch commit
        // that cannot be merged
        assertNoCollisions(store, ROOT);
    }

    @Test
    public void collisionOnForeignOrphanedBranch() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = newBuilder()
                .setDocumentStore(store).setAsyncDelay(0)
                .setUpdateLimit(10).setClusterId(1).build();
        DocumentNodeStore ns2 = newBuilder()
                .setDocumentStore(store).setAsyncDelay(0)
                .setUpdateLimit(10).setClusterId(2).build();

        NodeBuilder builder = ns1.getRoot().builder();
        // force a branch commit
        for (int i = 0; i < 20; i++) {
            builder.child("n-" + i).setProperty("p", "v");
        }
        retainBranches(ns1);
        ns1.dispose();
        ns2.updateClusterState();

        NodeDocument root = store.find(NODES, Utils.getIdFromPath(ROOT));
        assertNotNull(root);
        assertThat(root.getLocalBranchCommits(), not(empty()));

        // but some changes are still there
        Path p = Path.fromString("/n-0");
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath(p));
        assertNotNull(doc);
        assertThat(doc.getLocalBranchCommits(), not(empty()));

        builder = ns2.getRoot().builder();
        builder.child("n-0");
        merge(ns2, builder);

        // must create a collision marker for a branch commit because
        // it is not known when ns1 was stopped
        root = store.find(NODES, Utils.getIdFromPath(ROOT));
        assertNotNull(root);
        assertThat(root.getLocalMap(COLLISIONS).keySet(), not(empty()));
    }

    @Test
    public void collisionOnForeignOrphanedBranchAfterRestart() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = newBuilder()
                .setDocumentStore(store).setAsyncDelay(0)
                .setUpdateLimit(10).setClusterId(1).build();
        DocumentNodeStore ns2 = newBuilder()
                .setDocumentStore(store).setAsyncDelay(0)
                .setUpdateLimit(10).setClusterId(2).build();

        NodeBuilder builder = ns1.getRoot().builder();
        // force a branch commit
        for (int i = 0; i < 20; i++) {
            builder.child("n-" + i).setProperty("p", "v");
        }
        retainBranches(ns1);
        ns1.dispose();

        NodeDocument root = store.find(NODES, Utils.getIdFromPath(ROOT));
        assertNotNull(root);
        assertThat(root.getLocalBranchCommits(), not(empty()));

        // make sure start timestamp of node store is higher than
        // branch commit revision timestamp
        clock.getTimeIncreasing();

        // start it up again
        ns1 = newBuilder()
                .setDocumentStore(store).setAsyncDelay(0)
                .setUpdateLimit(10).setClusterId(1).build();

        root = store.find(NODES, Utils.getIdFromPath(ROOT));
        assertNotNull(root);
        // on init the DocumentNodeStore removes orphaned
        // branch commit entries on the root document
        assertThat(root.getLocalBranchCommits(), empty());

        // but some changes are still there
        Path p = Path.fromString("/n-0");
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath(p));
        assertNotNull(doc);
        assertThat(doc.getLocalBranchCommits(), not(empty()));

        ns2.updateClusterState();

        builder = ns2.getRoot().builder();
        builder.child("n-0");
        merge(ns2, builder);

        // must not create a collision marker for a branch commit
        // from a clusterId that is inactive
        assertNoCollisions(store, ROOT);
    }

    @Test
    public void collisionWithBranchOnForeignOrphanedBranchAfterRestart() throws Exception {
        int updateLimit = 10;
        DocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = newBuilder()
                .setDocumentStore(store).setAsyncDelay(0)
                .setUpdateLimit(updateLimit).setClusterId(1).build();
        DocumentNodeStore ns2 = newBuilder()
                .setDocumentStore(store).setAsyncDelay(0)
                .setUpdateLimit(updateLimit).setClusterId(2).build();

        NodeBuilder builder = ns1.getRoot().builder();
        // force a branch commit
        for (int i = 0; i < updateLimit * 2; i++) {
            builder.child("n-" + i).setProperty("p", "v");
        }
        retainBranches(ns1);
        ns1.dispose();

        NodeDocument root = store.find(NODES, Utils.getIdFromPath(ROOT));
        assertNotNull(root);
        assertThat(root.getLocalBranchCommits(), not(empty()));

        // make sure start timestamp of node store is higher than
        // branch commit revision timestamp
        clock.getTimeIncreasing();

        // start it up again
        ns1 = newBuilder()
                .setDocumentStore(store).setAsyncDelay(0)
                .setUpdateLimit(updateLimit).setClusterId(1).build();

        root = store.find(NODES, Utils.getIdFromPath(ROOT));
        assertNotNull(root);
        // on init the DocumentNodeStore removes orphaned
        // branch commit entries on the root document
        assertThat(root.getLocalBranchCommits(), empty());

        // but some changes are still there
        Path p = Path.fromString("/n-0");
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath(p));
        assertNotNull(doc);
        assertThat(doc.getLocalBranchCommits(), not(empty()));

        ns2.updateClusterState();

        builder = ns2.getRoot().builder();
        builder.child("n-0");
        // force a branch commit
        for (int i = 0; i < updateLimit * 2; i++) {
            builder.child("test").child("c-" + i);
        }
        merge(ns2, builder);

        // must not create a collision marker for a branch commit
        // from a clusterId that is inactive
        assertNoCollisions(store, ROOT);
    }

    private static void assertNoCollisions(DocumentStore store, Path p) {
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath(p));
        assertNotNull(doc);
        assertThat(doc.getLocalMap(COLLISIONS).keySet(), empty());
    }

    @NotNull
    private static RevisionVector merge(DocumentNodeStore ns, NodeBuilder nb)
            throws CommitFailedException {
        ns.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return ns.getHeadRevision();
    }

    @NotNull
    private static NodeDocument getDocument(DocumentStore store, String id) {
        NodeDocument doc = store.find(NODES, id);
        assertNotNull(doc);
        return doc;
    }

    private void createCollision(DocumentMK mk) throws Exception {
        String nodeName = "test-" + COUNTER.getAndIncrement();
        // create branch
        String b = mk.branch(null);
        mk.commit("/", "+\"" + nodeName + "\":{\"p\":\"a\"}", b, null);

        // commit a change resulting in a collision on branch
        mk.commit("/", "+\"" + nodeName + "\":{\"p\":\"b\"}", null, null);
    }

    private DocumentMK.Builder newBuilder() {
        return builderProvider.newBuilder().clock(clock);
    }

    /**
     * Add all known branch referents to {@link #branches} to prevent clean up
     * of orphaned branches.
     */
    private void retainBranches(DocumentNodeStore ns) {
        NodeDocument root = getRootDocument(ns.getDocumentStore());
        for (Revision r : root.getLocalBranchCommits()) {
            RevisionVector branchRev = new RevisionVector(r.asBranchRevision());
            Branch b = ns.getBranches().getBranch(branchRev);
            if (b == null) {
                continue;
            }
            Branch.BranchReference ref = b.getRef();
            if (ref == null) {
                continue;
            }
            Object obj = ref.get();
            if (obj != null) {
                branches.add(obj);
            }
        }
    }
}
