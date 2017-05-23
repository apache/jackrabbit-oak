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

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.NO_BINARY;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getAllDocuments;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getRootDocument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DocumentNodeStoreSweepTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Clock clock;

    private FailingDocumentStore store;

    private DocumentNodeStore ns;

    @Before
    public void before() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
        store = new FailingDocumentStore(new MemoryDocumentStore());
        ns = createDocumentNodeStore(0);
    }

    @AfterClass
    public static void resetClock() {
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
    }

    @Test
    public void simple() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);

        RevisionVector head = ns.getHeadRevision();
        ns.dispose();

        NodeDocument rootDoc = getRootDocument(store);
        assertNotNull(rootDoc);
        // after dispose, the sweep revision must be at least the head revision
        Revision localHead = head.getRevision(ns.getClusterId());
        assertNotNull(localHead);
        assertFalse(rootDoc.getSweepRevisions().isRevisionNewer(localHead));
    }

    @Test
    public void rollbackFailed() throws Exception {
        createUncommittedChanges();
        // after a new head and a background sweep, the
        // uncommitted changes must be cleaned up
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);
        ns.runBackgroundUpdateOperations();

        ns.runBackgroundSweepOperation();
        assertCleanStore();
    }

    @Test
    public void rollbackFailedWithDispose() throws Exception {
        createUncommittedChanges();
        // dispose must trigger sweeper
        ns.dispose();
        assertCleanStore();
    }

    @Test
    public void sameClusterNodeRecovery() throws Exception {
        int clusterId = ns.getClusterId();
        createUncommittedChanges();

        // simulate a crashed node store
        crashDocumentNodeStore();

        // store must be clean after restart
        ns = createDocumentNodeStore(clusterId);
        assertCleanStore();
    }

    @Test
    public void recoveryAfterGC() throws Exception {
        int clusterId = ns.getClusterId();

        for (int i = 0; i < 5; i++) {
            NodeBuilder builder = ns.getRoot().builder();
            builder.child("foo").child("node-" + i);
            merge(ns, builder);
            // root document must contain a revisions entry for this commit
            Revision r = ns.getHeadRevision().getRevision(clusterId);
            assertNotNull(r);
            assertTrue(getRootDocument(store).getLocalRevisions().containsKey(r));
        }
        // split the root
        NodeDocument doc = getRootDocument(store);
        List<UpdateOp> ops = SplitOperations.forDocument(doc, ns,
                ns.getHeadRevision(), NO_BINARY, 2);
        String prevId = null;
        for (UpdateOp op : ops) {
            if (Utils.isPreviousDocId(op.getId())) {
                prevId = op.getId();
            }
        }
        // there must be an operation for a split doc
        assertNotNull(prevId);
        store.createOrUpdate(Collection.NODES, ops);
        ns.runBackgroundOperations();

        // wait an hour
        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(1));

        // do some other changes not followed by background update
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo").child("node-0").child("bar");
        merge(ns, builder);

        // run GC
        ns.getVersionGarbageCollector().gc(30, TimeUnit.MINUTES);
        // now the split document must be gone
        assertNull(store.find(Collection.NODES, prevId));

        // simulate a crashed node store
        crashDocumentNodeStore();

        // store must be clean after restart
        ns = createDocumentNodeStore(clusterId);
        assertCleanStore();

        // and nodes must still exist
        for (int i = 0; i < 5; i++) {
            assertNodeExists("/foo/node-" + i);
            assertNodeExists("/foo/node-0/bar");
        }
    }

    @Test
    public void otherClusterNodeRecovery() throws Exception {
        int clusterId = ns.getClusterId();
        createUncommittedChanges();

        // simulate a crashed node store
        crashDocumentNodeStore();

        // start a new node store with a different clusterId
        ns = createDocumentNodeStore(clusterId + 1);

        // wait for lease to expire
        clock.waitUntil(clock.getTime() + ClusterNodeInfo.DEFAULT_LEASE_DURATION_MILLIS);
        // then run recovery for the other cluster node
        assertTrue(ns.getLastRevRecoveryAgent().recover(clusterId) > 0);
        // now the store must be clean
        assertCleanStore();
    }

    @Test
    public void pre18ClusterNodeRecovery() throws Exception {
        int clusterId = ns.getClusterId();
        createUncommittedChanges();
        // add a node, but do not run background write
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("node");
        merge(ns, builder);

        // simulate a crashed node store
        crashDocumentNodeStore();
        // and remove the sweep revision for clusterId
        // this will look like an upgraded and crashed pre 1.8 node store
        UpdateOp op = new UpdateOp(getIdFromPath(ROOT_PATH), false);
        op.removeMapEntry("_sweepRev", new Revision(0, 0, clusterId));
        assertNotNull(store.findAndUpdate(Collection.NODES, op));
        NodeDocument rootDoc = getRootDocument(store);
        assertNull(rootDoc.getSweepRevisions().getRevision(clusterId));

        // wait for lease to expire
        clock.waitUntil(clock.getTime() + ClusterNodeInfo.DEFAULT_LEASE_DURATION_MILLIS);

        // start a new node store with a different clusterId
        ns = createDocumentNodeStore(clusterId + 1);

        // then run recovery for the other cluster node
        assertTrue(ns.getLastRevRecoveryAgent().recover(clusterId) > 0);
        // must not set a sweep revision
        rootDoc = getRootDocument(store);
        assertNull(rootDoc.getSweepRevisions().getRevision(clusterId));
    }

    @Test
    public void lowerSweepLimit() throws Exception {
        ns.dispose();
        // restart with a document store that tracks queries
        final Map<String, Long> queries = Maps.newHashMap();
        store = new FailingDocumentStore(new MemoryDocumentStore() {
            @Nonnull
            @Override
            public <T extends Document> List<T> query(Collection<T> collection,
                                                      String fromKey,
                                                      String toKey,
                                                      String indexedProperty,
                                                      long startValue,
                                                      int limit) {
                queries.put(indexedProperty, startValue);
                return super.query(collection, fromKey, toKey,
                        indexedProperty, startValue, limit);
            }
        });
        ns = createDocumentNodeStore(0);

        createUncommittedChanges();
        // get the revision of the uncommitted changes
        Revision r = null;
        for (NodeDocument d : Utils.getAllDocuments(store)) {
            if (d.getPath().startsWith("/node-")) {
                r = Iterables.getFirst(d.getAllChanges(), null);
                break;
            }
        }
        assertNotNull(r);
        // after a new head and a background sweep, the
        // uncommitted changes must be cleaned up
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);
        queries.clear();
        ns.runBackgroundOperations();
        assertCleanStore();
        // sweeper must have looked at most recently modified documents
        Long modified = queries.get(NodeDocument.MODIFIED_IN_SECS);
        assertNotNull(modified);
        long startValue = NodeDocument.getModifiedInSecs(r.getTimestamp());
        assertEquals(startValue, modified.longValue());
    }

    private void assertNodeExists(String path) {
        NodeState n = ns.getRoot();
        for (String name : PathUtils.elements(path)) {
            n = n.getChildNode(name);
            assertTrue(name + " does not exist", n.exists());
        }
    }

    private void createUncommittedChanges() throws Exception {
        ns.setMaxBackOffMillis(0);
        NodeBuilder builder = ns.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            builder.child("node-" + i);
        }

        store.fail().after(5).eternally();
        try {
            merge(ns, builder);
            fail("must fail with exception");
        } catch (CommitFailedException e) {
            // expected
        }
        store.fail().never();

        // store must now contain uncommitted changes
        NodeDocument doc = null;
        for (NodeDocument d : Utils.getAllDocuments(store)) {
            if (d.getPath().startsWith("/node-")) {
                doc = d;
                break;
            }
        }
        assertNotNull(doc);
        assertNull(doc.getNodeAtRevision(ns, ns.getHeadRevision(), null));
        SortedMap<Revision, String> deleted = doc.getLocalDeleted();
        assertEquals(1, deleted.size());
        assertNull(ns.getCommitValue(deleted.firstKey(), doc));
    }

    private void assertCleanStore() {
        for (NodeDocument doc : getAllDocuments(store)) {
            for (Revision c : doc.getAllChanges()) {
                String commitValue = ns.getCommitValue(c, doc);
                assertTrue("Revision " + c + " on " + doc.getId() + " is not committed: " + commitValue,
                        Utils.isCommitted(commitValue));
            }
        }
    }

    private DocumentNodeStore createDocumentNodeStore(int clusterId) {
        return builderProvider.newBuilder().setDocumentStore(store)
                .setClusterId(clusterId).clock(clock).setAsyncDelay(0)
                .getNodeStore();
    }

    private void crashDocumentNodeStore() {
        // prevent writing anything in dispose
        store.fail().after(0).eternally();
        try {
            ns.dispose();
            fail("must fail with an exception");
        } catch (DocumentStoreException e) {
            // expected
        }
        // allow writes again
        store.fail().never();
    }

}
