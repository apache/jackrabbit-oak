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

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.COMMIT_ROOT;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.setCommitRoot;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.setModified;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation.Type.REMOVE_MAP_ENTRY;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation.Type.SET_MAP_ENTRY;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class NodeDocumentSweeperTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Clock clock;
    private DocumentNodeStore ns;
    private DocumentMK mk;
    private DocumentStore store;
    private MissingLastRevSeeker seeker;

    @Before
    public void before() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        DocumentMK.Builder builder = builderProvider.newBuilder();
        builder.clock(clock).setAsyncDelay(0);
        Revision.setClock(clock);
        mk = builder.open();
        ns = builder.getNodeStore();
        store = ns.getDocumentStore();
        seeker = builder.createMissingLastRevSeeker();
    }

    @AfterClass
    public static void resetClock() {
        Revision.resetClockToDefault();
    }

    @Test
    public void sweepUncommittedBeforeHead() throws Exception {
        Revision uncommitted = ns.newRevision();
        NodeBuilder b = ns.getRoot().builder();
        b.child("test");
        merge(ns, b);
        ns.runBackgroundUpdateOperations();

        UpdateOp op = new UpdateOp(getIdFromPath("/test"), false);
        op.setMapEntry("foo", uncommitted, "value");
        setCommitRoot(op, uncommitted, 0);
        setModified(op, uncommitted);
        assertNotNull(store.findAndUpdate(NODES, op));

        List<UpdateOp> ops = Lists.newArrayList();
        Revision nextSweepStart = sweep(ops);

        assertEquals(ns.getHeadRevision().getRevision(ns.getClusterId()), nextSweepStart);
        assertEquals(1, ops.size());
        op = ops.get(0);
        Map<Key, Operation> changes = op.getChanges();
        assertEquals(2, changes.size());
        Operation o = changes.get(new Key(COMMIT_ROOT, uncommitted));
        assertNotNull(o);
        assertEquals(REMOVE_MAP_ENTRY, o.type);
        o = changes.get(new Key("foo", uncommitted));
        assertNotNull(o);
        assertEquals(REMOVE_MAP_ENTRY, o.type);
    }

    @Test
    public void sweepUncommittedAfterHead() throws Exception {
        NodeBuilder b = ns.getRoot().builder();
        b.child("test");
        merge(ns, b);
        ns.runBackgroundUpdateOperations();

        Revision uncommitted = ns.newRevision();
        UpdateOp op = new UpdateOp(getIdFromPath("/test"), false);
        op.setMapEntry("foo", uncommitted, "value");
        setCommitRoot(op, uncommitted, 0);
        setModified(op, uncommitted);
        assertNotNull(store.findAndUpdate(NODES, op));

        List<UpdateOp> ops = Lists.newArrayList();
        Revision nextSweepStart = sweep(ops);

        assertEquals(ns.getHeadRevision().getRevision(ns.getClusterId()), nextSweepStart);
        // must not sweep change newer than head
        assertEquals(0, ops.size());
    }

    @Test
    public void sweepUnmergedBranchCommit() throws Exception {
        NodeBuilder b = ns.getRoot().builder();
        b.child("test");
        merge(ns, b);

        String branchRev = mk.branch(null);
        mk.commit("/test", "^\"foo\":\"value\"", branchRev, null);

        // force a new head revision newer than branch commit
        b = ns.getRoot().builder();
        b.child("bar");
        merge(ns, b);
        ns.runBackgroundUpdateOperations();

        List<UpdateOp> ops = Lists.newArrayList();
        Revision nextSweepStart = sweep(ops);

        // must not touch branch
        assertEquals(ns.getHeadRevision().getRevision(ns.getClusterId()), nextSweepStart);
        assertEquals(0, ops.size());
    }

    @Test
    public void sweepMergedBranch() throws Exception {
        String branchRev = mk.branch(null);
        branchRev = mk.commit("/", "+\"foo\":{}", branchRev, null);
        branchRev = mk.commit("/", "+\"bar\":{}", branchRev, null);
        branchRev = mk.commit("/", "+\"baz\":{}", branchRev, null);
        mk.merge(branchRev, null);
        ns.runBackgroundUpdateOperations();

        List<UpdateOp> ops = Lists.newArrayList();
        Revision nextSweepStart = sweep(ops);

        assertEquals(ns.getHeadRevision().getRevision(ns.getClusterId()), nextSweepStart);
        assertEquals(0, ops.size());
    }

    @Test
    public void updatePre18Branch() throws Exception {
        String branchRev = mk.branch(null);
        branchRev = mk.commit("/", "+\"foo\":{}", branchRev, null);
        mk.merge(branchRev, null);
        ns.runBackgroundUpdateOperations();

        // simulate a pre 1.8 branch commit by removing the branch commit entry
        NodeDocument doc = store.find(NODES, getIdFromPath("/foo"));
        assertNotNull(doc);
        assertEquals(1, doc.getLocalBranchCommits().size());
        UpdateOp op = new UpdateOp(doc.getId(), false);
        for (Revision r : doc.getLocalBranchCommits()) {
            NodeDocument.removeBranchCommit(op, r);
        }
        assertNotNull(store.findAndUpdate(NODES, op));

        List<UpdateOp> ops = Lists.newArrayList();
        Revision nextSweepStart = sweep(ops);

        assertEquals(ns.getHeadRevision().getRevision(ns.getClusterId()), nextSweepStart);
        assertEquals(1, ops.size());
        op = ops.get(0);
        Map<Key, Operation> changes = op.getChanges();
        assertEquals(1, changes.size());
        Key k = changes.keySet().iterator().next();
        assertEquals("_bc", k.getName());
        assertEquals(SET_MAP_ENTRY, changes.get(k).type);
    }

    private Revision sweep(final List<UpdateOp> ops) throws Exception {
        NodeDocumentSweeper sweeper = new NodeDocumentSweeper(ns, false);
        Revision startRev = ns.getSweepRevisions().getRevision(ns.getClusterId());
        assertNotNull(startRev);
        Iterable<NodeDocument> docs = seeker.getCandidates(startRev.getTimestamp());
        return sweeper.sweep(docs, new NodeDocumentSweepListener() {
            @Override
            public void sweepUpdate(Map<String, UpdateOp> updates)
                    throws DocumentStoreException {
                ops.addAll(updates.values());
            }
        });
    }
}
