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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.FailingDocumentStore.FailedUpdateOpListener;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static java.util.Collections.emptySet;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.DEFAULT_LEASE_DURATION_MILLIS;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.isDeletedEntry;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.childBuilder;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.disposeQuietly;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation.Type.SET_MAP_ENTRY;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.plugins.memory.StringPropertyState.stringProperty;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that write after with a clusterId after the leased timed out and
 * recovery was performed.
 */
public class WriteAfterRecoveryTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private final FailingDocumentStore store = new FailingDocumentStore(new MemoryDocumentStore());

    private final Clock clock = new Clock.Virtual();

    private DocumentNodeStore ns1;

    @Before
    public void setUp() throws Exception {
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
        ns1 = createNodeStore(1);
    }

    @After
    public void cleanUp() {
        ClusterNodeInfo.resetClockToDefault();
        Revision.resetClockToDefault();
    }

    @Test
    public void oneNodeAdded() throws Exception {
        merge(ns1, createChild("/a"), true);

        List<UpdateOp> failed = new ArrayList<>();
        store.addListener(filter(failed, ADD_NODE_OPS));
        store.fail().after(0).eternally();
        try {
            merge(ns1, createChild("/a/b"), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            // expected
        }
        disposeQuietly(ns1);
        store.fail().never();

        // wait until lease expires
        clockWait(DEFAULT_LEASE_DURATION_MILLIS);

        DocumentNodeStore ns2 = createNodeStore(2);
        assertTrue(ns2.getLastRevRecoveryAgent().isRecoveryNeeded());
        assertEquals(0, ns2.getLastRevRecoveryAgent().recover(1));

        // 'late write'
        store.createOrUpdate(NODES, failed);
        Set<Revision> lateWriteRevisions = getRevisions(failed);

        // revive clusterId 1
        ns1 = createNodeStore(1);
        merge(ns1, createChild("/d"), true);

        ns2.runBackgroundOperations();
        assertTrue(getNode(ns2.getRoot(), "/d").exists());
        assertFalse(getNode(ns2.getRoot(), "/a/b").exists());
        try {
            merge(ns2, createChild("/a/b"), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            assertThat(e.getMessage(), containsString("older than base"));
        }

        Set<Revision> inconsistent = checkConsistency(ns2, "/a/b");
        assertEquals(lateWriteRevisions, inconsistent);
    }

    @Test
    public void multipleNodesAdded() throws Exception {
        merge(ns1, createChild("/a"), true);

        List<UpdateOp> failed = new ArrayList<>();
        store.addListener(filter(failed, ADD_NODE_OPS));
        store.fail().after(0).eternally();
        try {
            merge(ns1, createChild("/a/b/c"), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            // expected
        }
        disposeQuietly(ns1);
        store.fail().never();

        // wait until lease expires
        clockWait(DEFAULT_LEASE_DURATION_MILLIS);

        DocumentNodeStore ns2 = createNodeStore(2);
        assertTrue(ns2.getLastRevRecoveryAgent().isRecoveryNeeded());
        assertEquals(0, ns2.getLastRevRecoveryAgent().recover(1));

        // 'late write'
        store.createOrUpdate(NODES, failed);
        Set<Revision> lateWriteRevisions = getRevisions(failed);

        // revive clusterId 1
        ns1 = createNodeStore(1);
        merge(ns1, createChild("/d"), true);

        ns2.runBackgroundOperations();
        assertTrue(getNode(ns2.getRoot(), "/d").exists());
        assertFalse(getNode(ns2.getRoot(), "/a/b/c").exists());
        try {
            merge(ns2, createChild("/a/b/c"), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            assertThat(e.getMessage(), containsString("older than base"));
        }

        Set<Revision> inconsistent = checkConsistency(ns2, "/a/b", "/a/b/c");
        assertEquals(lateWriteRevisions, inconsistent);
    }

    @Test
    public void orphanedNodesAdded() throws Exception {
        merge(ns1, createChild("/a"), true);

        List<UpdateOp> failed = new ArrayList<>();
        store.addListener(filter(failed, ADD_NODE_OPS));
        // let merge create some nodes
        store.fail().after(2).eternally();
        try {
            merge(ns1, createChild("/a/b/c/d/e/f"), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            // expected
        }
        disposeQuietly(ns1);
        store.fail().never();

        // wait until lease expires
        clockWait(DEFAULT_LEASE_DURATION_MILLIS);

        DocumentNodeStore ns2 = createNodeStore(2);
        assertTrue(ns2.getLastRevRecoveryAgent().isRecoveryNeeded());
        assertEquals(1, ns2.getLastRevRecoveryAgent().recover(1));

        // 'late write'
        store.createOrUpdate(NODES, failed);
        Set<Revision> lateWriteRevisions = getRevisions(failed);

        // revive clusterId 1
        ns1 = createNodeStore(1);
        merge(ns1, createChild("/d"), true);

        ns2.runBackgroundOperations();
        assertTrue(getNode(ns2.getRoot(), "/d").exists());
        assertFalse(getNode(ns2.getRoot(), "/a/b").exists());
        // can create nodes that recovery removed ...
        merge(ns2, createChild("/a/b/c"), false);
        // ... but not those written after recovery
        try {
            merge(ns2, createChild("/a/b/c/d/e/f"), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            assertThat(e.getMessage(), containsString("older than base"));
        }

        Set<Revision> inconsistent = checkConsistency(ns2, "/a/b", "/a/b/c", "/a/b/c/d", "/a/b/c/d/e", "/a/b/c/d/e/f");
        assertEquals(lateWriteRevisions, inconsistent);
    }

    @Test
    public void oneNodeRemoved() throws Exception {
        merge(ns1, createChild("/a/b"), true);

        List<UpdateOp> failed = new ArrayList<>();
        store.addListener(filter(failed, REMOVE_NODE_OPS));
        store.fail().after(0).eternally();
        try {
            merge(ns1, removeChild("/a/b"), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            // expected
        }
        disposeQuietly(ns1);
        store.fail().never();

        // wait until lease expires
        clockWait(DEFAULT_LEASE_DURATION_MILLIS);

        DocumentNodeStore ns2 = createNodeStore(2);
        assertTrue(ns2.getLastRevRecoveryAgent().isRecoveryNeeded());
        assertEquals(0, ns2.getLastRevRecoveryAgent().recover(1));

        // 'late write'
        store.createOrUpdate(NODES, failed);
        Set<Revision> lateWriteRevisions = getRevisions(failed);

        // revive clusterId 1
        ns1 = createNodeStore(1);
        assertEquals(lateWriteRevisions, checkConsistency(ns1, "/a/b"));
        merge(ns1, createChild("/d"), true);
        // ns2 needs a background read to see the inconsistency
        assertEquals(emptySet(), checkConsistency(ns2, "/a/b"));

        ns2.runBackgroundOperations();
        assertTrue(getNode(ns2.getRoot(), "/d").exists());
        assertTrue(getNode(ns2.getRoot(), "/a/b").exists());
        try {
            merge(ns2, removeChild("/a/b"), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            assertThat(e.getMessage(), containsString("already deleted"));
        }
        try {
            // trying to set a property on such a node also fails
            merge(ns2, setProperty("/a/b", "p", "v"), true);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            assertThat(e.getMessage(), containsString("already deleted"));
        }

        Set<Revision> inconsistent = checkConsistency(ns2, "/a/b");
        assertEquals(lateWriteRevisions, inconsistent);

        // writing a property on the parent node of an inconsistency makes the
        // late-write change visible and the inconsistency becomes unnoticeable
        merge(ns1, setProperty("/a", "p", "v"), true);
        ns2.runBackgroundOperations();
        assertFalse(getNode(ns2.getRoot(), "/a/b").exists());
        assertEquals(emptySet(), checkConsistency(ns2, "/a/b"));
    }

    @Test
    public void multipleNodesRemoved() throws Exception {
        merge(ns1, createChild("/a/b/c"), true);

        List<UpdateOp> failed = new ArrayList<>();
        store.addListener(filter(failed, REMOVE_NODE_OPS));
        store.fail().after(0).eternally();
        try {
            merge(ns1, removeChild("/a/b"), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            // expected
        }
        disposeQuietly(ns1);
        store.fail().never();

        // wait until lease expires
        clockWait(DEFAULT_LEASE_DURATION_MILLIS);

        DocumentNodeStore ns2 = createNodeStore(2);
        assertTrue(ns2.getLastRevRecoveryAgent().isRecoveryNeeded());
        assertEquals(0, ns2.getLastRevRecoveryAgent().recover(1));

        // 'late write'
        store.createOrUpdate(NODES, failed);
        Set<Revision> lateWriteRevisions = getRevisions(failed);

        // revive clusterId 1
        ns1 = createNodeStore(1);
        merge(ns1, createChild("/d"), true);

        ns2.runBackgroundOperations();
        assertTrue(getNode(ns2.getRoot(), "/d").exists());
        assertTrue(getNode(ns2.getRoot(), "/a/b/c").exists());
        try {
            merge(ns2, removeChild("/a/b"), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            assertThat(e.getMessage(), containsString("already deleted"));
        }

        Set<Revision> inconsistent = checkConsistency(ns2, "/a/b", "/a/b/c");
        assertEquals(lateWriteRevisions, inconsistent);
    }

    @Test
    public void orphanedNodesRemoved() throws Exception {
        merge(ns1, createChild("/a/b/c/d/e/f"), true);

        List<UpdateOp> failed = new ArrayList<>();
        store.addListener(filter(failed, REMOVE_NODE_OPS));
        // let merge remove some nodes and then fail
        store.fail().after(2).eternally();
        try {
            merge(ns1, removeChild("/a/b"), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            // expected
        }
        disposeQuietly(ns1);
        store.fail().never();

        // wait until lease expires
        clockWait(DEFAULT_LEASE_DURATION_MILLIS);

        DocumentNodeStore ns2 = createNodeStore(2);
        assertTrue(ns2.getLastRevRecoveryAgent().isRecoveryNeeded());
        assertEquals(1, ns2.getLastRevRecoveryAgent().recover(1));

        // 'late write'
        store.createOrUpdate(NODES, failed);
        Set<Revision> lateWriteRevisions = getRevisions(failed);

        // revive clusterId 1
        ns1 = createNodeStore(1);
        merge(ns1, createChild("/d"), true);

        ns2.runBackgroundOperations();
        assertTrue(getNode(ns2.getRoot(), "/d").exists());
        assertTrue(getNode(ns2.getRoot(), "/a/b/c/d/e/f").exists());
        // add a node
        try {
            merge(ns2, createChild("/a/b/c/d/e/f/g"), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            assertThat(e.getMessage(), containsString("already deleted"));
        }
        // can set a property on a node where recovery reverted the delete ...
        merge(ns2, setProperty("/a/b", "p", "v"), false);
        // ... but cannot set a property on a node deleted after recovery
        try {
            merge(ns2, setProperty("/a/b/c/d/e/f", "p", "v"), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            assertThat(e.getMessage(), containsString("already deleted"));
        }

        Set<Revision> inconsistent = checkConsistency(ns2, "/a/b", "/a/b/c", "/a/b/c/d", "/a/b/c/d/e", "/a/b/c/d/e/f", "/a/b/c/d/e/f/g");
        assertEquals(lateWriteRevisions, inconsistent);
    }

    @Test
    public void propertyChanged() throws Exception {
        merge(ns1, compose(
                createChild("/a/b"),
                setProperty("/a/b", "p", "u")
        ), true);

        List<UpdateOp> failed = new ArrayList<>();
        store.addListener(filter(failed, setPropertyOps("p")));
        store.fail().after(0).eternally();
        try {
            merge(ns1, compose(
                    setProperty("/a/b", "p", "v"),
                    createChild("/a/b/c")
            ), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            // expected
        }
        disposeQuietly(ns1);
        store.fail().never();

        // wait until lease expires
        clockWait(DEFAULT_LEASE_DURATION_MILLIS);

        DocumentNodeStore ns2 = createNodeStore(2);
        assertTrue(ns2.getLastRevRecoveryAgent().isRecoveryNeeded());
        assertEquals(0, ns2.getLastRevRecoveryAgent().recover(1));

        // 'late write'
        store.createOrUpdate(NODES, failed);
        Set<Revision> lateWriteRevisions = getRevisions(failed);

        // revive clusterId 1
        ns1 = createNodeStore(1);
        merge(ns1, createChild("/d"), true);
        assertEquals(stringProperty("p", "u"), getNode(ns1.getRoot(), "/a/b").getProperty("p"));
        NodeDocument b = store.find(NODES, getIdFromPath("/a/b"));
        assertNotNull(b);
        assertNotNull(b.getNodeAtRevision(ns1, ns1.getHeadRevision(), null).getProperty("p"));

        ns2.runBackgroundOperations();
        assertTrue(getNode(ns2.getRoot(), "/d").exists());
        assertEquals(stringProperty("p", "u"), getNode(ns2.getRoot(), "/a/b").getProperty("p"));

        // current implementation does not fail with conflict when property
        // is overwritten, but consistency check can still detect it
        Set<Revision> inconsistent = checkConsistency(ns2, "/a/b", "/a/b/c");
        assertEquals(lateWriteRevisions, inconsistent);

        // by making another change on a property with an inconsistency,
        // that inconsistency then becomes unnoticeable going forward
        assertEquals(lateWriteRevisions, checkConsistency(ns2, "/a/b"));
        merge(ns2, setProperty("/a/b", "p", "v2"), true);
        assertEquals(emptySet(), checkConsistency(ns2, "/a/b"));
    }

    @Test
    public void propertyRemoved() throws Exception {
        merge(ns1, compose(
                createChild("/a/b"),
                setProperty("/a/b", "p", "u")
        ), true);

        List<UpdateOp> failed = new ArrayList<>();
        store.addListener(filter(failed, setPropertyOps("p")));
        store.fail().after(0).eternally();
        try {
            merge(ns1, compose(
                    removeProperty("/a/b", "p"),
                    createChild("/a/b/c")
            ), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            // expected
        }
        disposeQuietly(ns1);
        store.fail().never();

        // wait until lease expires
        clockWait(DEFAULT_LEASE_DURATION_MILLIS);

        DocumentNodeStore ns2 = createNodeStore(2);
        assertTrue(ns2.getLastRevRecoveryAgent().isRecoveryNeeded());
        assertEquals(0, ns2.getLastRevRecoveryAgent().recover(1));

        // 'late write'
        store.createOrUpdate(NODES, failed);
        Set<Revision> lateWriteRevisions = getRevisions(failed);

        // revive clusterId 1
        ns1 = createNodeStore(1);
        merge(ns1, createChild("/d"), true);
        assertEquals(stringProperty("p", "u"), getNode(ns1.getRoot(), "/a/b").getProperty("p"));
        NodeDocument b = store.find(NODES, getIdFromPath("/a/b"));
        assertNotNull(b);
        assertNull(b.getNodeAtRevision(ns1, ns1.getHeadRevision(), null).getProperty("p"));

        ns2.runBackgroundOperations();
        assertTrue(getNode(ns2.getRoot(), "/d").exists());
        assertEquals(stringProperty("p", "u"), getNode(ns2.getRoot(), "/a/b").getProperty("p"));

        // current implementation does not fail with conflict when property
        // is overwritten, but consistency check can still detect it
        Set<Revision> inconsistent = checkConsistency(ns2, "/a/b", "/a/b/c");
        assertEquals(lateWriteRevisions, inconsistent);

        // by setting a new property on a node with an inconsistency with the
        // clusterId that created the inconsistency, that inconsistency then
        // becomes unnoticeable going forward
        assertEquals(lateWriteRevisions, checkConsistency(ns2, "/a/b"));
        merge(ns1, setProperty("/a/b", "q", "v"), true);
        ns2.runBackgroundOperations();
        assertEquals(emptySet(), checkConsistency(ns2, "/a/b"));
        // and property 'p' now appears removed
        assertNull(getNode(ns2.getRoot(), "/a/b").getProperty("p"));
    }

    @Test
    public void propertySet() throws Exception {
        merge(ns1, createChild("/a/b"), true);

        List<UpdateOp> failed = new ArrayList<>();
        store.addListener(filter(failed, setPropertyOps("p")));
        store.fail().after(0).eternally();
        try {
            merge(ns1, compose(
                    setProperty("/a/b", "p", "u"),
                    createChild("/a/b/c")
            ), false);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            // expected
        }
        disposeQuietly(ns1);
        store.fail().never();

        // wait until lease expires
        clockWait(DEFAULT_LEASE_DURATION_MILLIS);

        DocumentNodeStore ns2 = createNodeStore(2);
        assertTrue(ns2.getLastRevRecoveryAgent().isRecoveryNeeded());
        assertEquals(0, ns2.getLastRevRecoveryAgent().recover(1));

        // 'late write'
        store.createOrUpdate(NODES, failed);
        Set<Revision> lateWriteRevisions = getRevisions(failed);

        // revive clusterId 1
        ns1 = createNodeStore(1);
        merge(ns1, createChild("/d"), true);
        assertNull(getNode(ns1.getRoot(), "/a/b").getProperty("p"));
        NodeDocument b = store.find(NODES, getIdFromPath("/a/b"));
        assertNotNull(b);
        assertNotNull(b.getNodeAtRevision(ns1, ns1.getHeadRevision(), null).getProperty("p"));

        ns2.runBackgroundOperations();
        assertTrue(getNode(ns2.getRoot(), "/d").exists());
        assertNull(getNode(ns2.getRoot(), "/a/b").getProperty("p"));

        // current implementation does not fail with conflict when property
        // is overwritten, but consistency check can still detect it
        Set<Revision> inconsistent = checkConsistency(ns2, "/a/b", "/a/b/c");
        assertEquals(lateWriteRevisions, inconsistent);
    }

    private static Change setProperty(String path,
                                      String name,
                                      String value) {
        return root -> {
            childBuilder(root, path).setProperty(name, value);
            return root;
        };
    }

    private static Change removeProperty(String path, String name) {
        return root -> {
            childBuilder(root, path).removeProperty(name);
            return root;
        };
    }

    private static Change createChild(String path) {
        return root -> {
            childBuilder(root, path);
            return root;
        };
    }

    private static Change removeChild(String path) {
        return root -> {
            NodeBuilder node = root;
            for (String name : PathUtils.elements(path)) {
                node = node.getChildNode(name);
            }
            if (node.exists()) {
                node.remove();
            }
            return root;
        };
    }

    private static NodeState merge(DocumentNodeStore store,
                                   NodeBuilder builder,
                                   boolean runBackgroundOps)
            throws CommitFailedException {
        NodeState root = TestUtils.merge(store, builder);
        if (runBackgroundOps) {
            store.runBackgroundOperations();
        }
        return root;
    }

    interface Change {

        NodeBuilder apply(NodeBuilder root);
    }

    private static Change compose(Change... changes) {
        return root -> {
            Arrays.stream(changes).forEach(change -> change.apply(root));
            return root;
        };
    }

    public static NodeState merge(DocumentNodeStore store,
                                  Change change,
                                  boolean runBackgroundOps)
            throws CommitFailedException {
        return merge(store, change.apply(store.getRoot().builder()), runBackgroundOps);
    }

    private void clockWait(int durationMillis) throws InterruptedException {
        clock.waitUntil(clock.getTime() + durationMillis);
    }

    private static FailedUpdateOpListener filter(List<UpdateOp> ops,
                                                 Predicate<UpdateOp> predicate) {
        return op -> {
            if (predicate.test(op)) {
                ops.add(op);
            }
        };
    }

    private static final Predicate<UpdateOp> ADD_NODE_OPS = updateOp -> {
        for (UpdateOp.Key key : updateOp.getChanges().keySet()) {
            if (isDeletedEntry(key.getName())
                    && updateOp.getChanges().get(key).value.equals("false")) {
                return true;
            }
        }
        return false;
    };

    private static final Predicate<UpdateOp> REMOVE_NODE_OPS = updateOp -> {
        for (UpdateOp.Key key : updateOp.getChanges().keySet()) {
            if (isDeletedEntry(key.getName())
                    && updateOp.getChanges().get(key).value.equals("true")) {
                return true;
            }
        }
        return false;
    };

    private static Predicate<UpdateOp> setPropertyOps(String propertyName) {
        return updateOp -> {
            for (UpdateOp.Key key : updateOp.getChanges().keySet()) {
                if (propertyName.equals(key.getName())
                        && updateOp.getChanges().get(key).type == SET_MAP_ENTRY) {
                    return true;
                }
            }
            return false;
        };
    }

    private DocumentNodeStore createNodeStore(int clusterId) {
        return builderProvider.newBuilder()
                .setDocumentStore(store)
                .clock(clock)
                .setClusterId(clusterId)
                .setAsyncDelay(0)
                .build();
    }

    private static Set<Revision> getRevisions(List<UpdateOp> failed) {
        return failed.stream()
                .flatMap(updateOp -> updateOp.getChanges().keySet().stream().map(UpdateOp.Key::getRevision))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    private Set<Revision> checkConsistency(DocumentNodeStore ns,
                                           String... paths) {
        Set<Revision> revisions = new HashSet<>();
        DocumentNodeState root = ns.getRoot();
        for (String path : paths) {
            NodeDocument doc = ns.getDocumentStore().find(NODES, getIdFromPath(path));
            if (doc != null) {
                new Consistency(root, doc).check(ns, revisions::add);
            }
        }
        return revisions;
    }
}
