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

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentNodeStoreBranchesTest {

    static final Logger LOG = LoggerFactory.getLogger(DocumentNodeStoreBranchesTest.class);

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @BeforeClass
    public static void disableJournalDiff() {
        System.setProperty(DocumentNodeStore.SYS_PROP_DISABLE_JOURNAL, "true");
    }

    @AfterClass
    public static void reset() {
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
        System.clearProperty(DocumentNodeStore.SYS_PROP_DISABLE_JOURNAL);
    }

    @Test
    public void commitHookChangesOnBranchWithInterference() throws Exception {
        Clock c = new Clock.Virtual();
        c.waitUntil(System.currentTimeMillis());
        Revision.setClock(c);
        ClusterNodeInfo.setClock(c);

        // enough nodes that diffManyChildren() is called
        final int NUM_NODES = DocumentMK.MANY_CHILDREN_THRESHOLD * 2;
        LOG.info("create new dns");
        Builder nsBuilder = builderProvider.newBuilder();
        nsBuilder.setAsyncDelay(0).clock(c);
        final DocumentNodeStore ns = nsBuilder.getNodeStore();

        // 0) initialization
        {
            LOG.info("initialization");
            NodeBuilder initBuilder = ns.getRoot().builder();
            for(int i=0; i<NUM_NODES; i++) {
                initBuilder.child("child"+i);
            }
            ns.merge(initBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }


        // 1) do more than UPDATE_LIMIT changes
        LOG.info("starting doing many changes to force a branch commit");
        NodeBuilder rootBuilder = ns.getRoot().builder();
        int totalUpdates = 5 * DocumentMK.UPDATE_LIMIT;
        int updateShare = totalUpdates / NUM_NODES;
        for(int i=0; i<NUM_NODES; i++) {
            NodeBuilder childBuilder = rootBuilder.child("child"+i);
            childBuilder.child("grandChild"+i);
            childBuilder.setProperty("p1", "originalValue");
            for(int j=0; j<updateShare; j++) {
                childBuilder.setProperty("someProperty" + j, "sameValue");
            }
        }

        // 2) wait 6 sec
        LOG.info("after purge was triggered above, 'waiting' 6sec");
        c.waitUntil(c.getTime() + 6000);

        // 3) now in another 'session', do another merge - to change the head
        LOG.info("in another session, do some unrelated changes to change the head");
        NodeBuilder parallelBuilder = ns.getRoot().builder();
        parallelBuilder.child("unrelated").setProperty("anyProp", "anywhere");
        ns.merge(parallelBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // 4) now merge the first session - this should now fail
        LOG.info("now merge the original builder - this should cause not all children to be visited");
        ns.merge(rootBuilder, CompositeHook.compose(Arrays.<CommitHook> asList(new TestHook("p"), new TestHook("q"))),
                CommitInfo.EMPTY);

        DocumentNodeState root = ns.getRoot();

        for(int i=0; i<NUM_NODES; i++) {
            NodeState child = root.getChildNode("child"+i);
            assertTrue(child.exists());
            assertEquals("test", child.getProperty("p1").getValue(Type.STRING));
        }
    }

    // OAK-8106
    @Test
    public void resetBranch() throws Exception {
        final long branchCommits = 5;
        final int updateLimit = 100;
        final CountingDocumentStore store = new CountingDocumentStore(new MemoryDocumentStore());
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setUpdateLimit(updateLimit).setAsyncDelay(0)
                .setDocumentStore(store).getNodeStore();
        ns.setMaxBackOffMillis(0); // do not retry merges
        NodeBuilder nb  = ns.getRoot().builder();
        for (int i = 0; i < branchCommits; i++) {
            NodeBuilder child = nb.child("foo").child("node-" + i);
            for (int j = 0; j < updateLimit; j++) {
                child.child("node-" + j).setProperty("p", "v");
            }
        }

        // add some other node to force a rebase
        NodeBuilder nb2 = ns.getRoot().builder();
        nb2.setProperty("foo", "v");
        TestUtils.merge(ns, nb2);

        try {
            ns.merge(nb, new CommitHook() {
                @NotNull
                @Override
                public NodeState processCommit(NodeState before,
                                               NodeState after,
                                               CommitInfo info)
                        throws CommitFailedException {
                    // add more nodes and then fail the commit to trigger a reset
                    NodeBuilder nb = after.builder().child("bar");
                    for (int i = 0; i < branchCommits; i++) {
                        NodeBuilder child = nb.child("node-" + i);
                        for (int j = 0; j < updateLimit; j++) {
                            child.child("node-" + j);
                        }
                    }
                    // reset counter before throwing the exception to measure
                    // the number of calls for the reset
                    store.resetCounters();
                    throw new CommitFailedException(CommitFailedException.OAK, 1, "failure");
                }
            }, CommitInfo.EMPTY);
            fail("Merge must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            // expected
        }

        long numCreateOrUpdate = store.getNumCreateOrUpdateCalls(NODES);
        assertThat(numCreateOrUpdate, lessThanOrEqualTo(branchCommits + 1));

        Path bar = Path.fromString("/bar");
        // verify reset cleaned up properly
        for (NodeDocument doc : Utils.getAllDocuments(store)) {
            Path p = doc.getPath();
            if (bar.isAncestorOf(p) || bar.equals(p)) {
                String path = p.toString();
                assertThat(path, doc.getLocalRevisions().keySet(), is(empty()));
                assertThat(path, doc.getLocalCommitRoot().keySet(), is(empty()));
                assertThat(path, doc.getDeleted().keySet(), is(empty()));
                assertThat(path, doc.getLocalBranchCommits(), is(empty()));
                assertTrue(path, doc.wasDeletedOnce());
            }
        }
    }

    // OAK-8106
    @Test
    public void resetBranchCreatedByCommitHook() {
        final long branchCommits = 5;
        final int updateLimit = 100;
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setUpdateLimit(updateLimit).setAsyncDelay(0)
                .getNodeStore();
        ns.setMaxBackOffMillis(0); // do not retry merges
        NodeBuilder nb  = ns.getRoot().builder();
        nb.child("foo");
        try {
            ns.merge(nb, new CommitHook() {
                @NotNull
                @Override
                public NodeState processCommit(NodeState before,
                                               NodeState after,
                                               CommitInfo info)
                        throws CommitFailedException {
                    // add more nodes and then fail the commit to trigger a reset
                    NodeBuilder nb = after.builder().child("bar");
                    for (int i = 0; i < branchCommits; i++) {
                        NodeBuilder child = nb.child("node-" + i);
                        for (int j = 0; j < updateLimit; j++) {
                            child.child("node-" + j);
                        }
                    }
                    throw new CommitFailedException(CommitFailedException.OAK, 1, "failure");
                }
            }, CommitInfo.EMPTY);
            fail("Merge must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            // expected
        }

        Path bar = Path.fromString("/bar");
        // verify reset cleaned up properly
        for (NodeDocument doc : Utils.getAllDocuments(ns.getDocumentStore())) {
            Path p = doc.getPath();
            if (bar.isAncestorOf(p) || bar.equals(p)) {
                String path = p.toString();
                assertThat(path, doc.getLocalRevisions().keySet(), is(empty()));
                assertThat(path, doc.getLocalCommitRoot().keySet(), is(empty()));
                assertThat(path, doc.getDeleted().keySet(), is(empty()));
                assertThat(path, doc.getLocalBranchCommits(), is(empty()));
                assertTrue(path, doc.wasDeletedOnce());
            }
        }
    }

    // OAK-8106
    @Test
    public void resetBranchWithFinalRebaseBranchCommit()
            throws CommitFailedException {
        final long branchCommits = 5;
        final int updateLimit = 100;
        final DocumentNodeStore ns = builderProvider.newBuilder()
                .setUpdateLimit(updateLimit).setAsyncDelay(0)
                .getNodeStore();
        ns.setMaxBackOffMillis(0); // do not retry merges
        NodeBuilder nb  = ns.getRoot().builder();
        nb.child("foo");
        try {
            ns.merge(nb, new CommitHook() {
                @NotNull
                @Override
                public NodeState processCommit(NodeState before,
                                               NodeState after,
                                               CommitInfo info)
                        throws CommitFailedException {
                    // add more nodes to create branch commits
                    DocumentRootBuilder nb = (DocumentRootBuilder) after.builder();
                    for (int i = 0; i < branchCommits; i++) {
                        NodeBuilder child = nb.child("bar").child("node-" + i);
                        for (int j = 0; j < updateLimit; j++) {
                            child.child("node-" + j);
                        }
                        // add a node with a different merge
                        addNodes(ns, "/baz/node-" + i);
                        // now force a rebase
                        nb.rebase();
                    }
                    // eventually fail the commit to trigger a reset
                    throw new CommitFailedException(CommitFailedException.OAK, Integer.MAX_VALUE, "failure");
                }
            }, CommitInfo.EMPTY);
            fail("Merge must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            if (e.getCode() != Integer.MAX_VALUE) {
                throw e;
            }
            // otherwise expected
        }

        Path bar = Path.fromString("/bar");
        // verify reset cleaned up properly
        for (NodeDocument doc : Utils.getAllDocuments(ns.getDocumentStore())) {
            Path p = doc.getPath();
            if (bar.isAncestorOf(p) || bar.equals(p)) {
                String path = p.toString();
                assertThat(path, doc.getLocalRevisions().keySet(), is(empty()));
                assertThat(path, doc.getLocalCommitRoot().keySet(), is(empty()));
                assertThat(path, doc.getDeleted().keySet(), is(empty()));
                assertThat(path, doc.getLocalBranchCommits(), is(empty()));
                assertTrue(path, doc.wasDeletedOnce());
            }
        }
    }

    // OAK-8106
    @Test
    public void resetBranchWithFirstRebaseBranchCommit()
            throws CommitFailedException {
        final long branchCommits = 5;
        final int updateLimit = 100;
        final DocumentNodeStore ns = builderProvider.newBuilder()
                .setUpdateLimit(updateLimit).setAsyncDelay(0)
                .getNodeStore();
        ns.setMaxBackOffMillis(0); // do not retry merges
        NodeBuilder nb  = ns.getRoot().builder();
        nb.child("foo");
        try {
            ns.merge(nb, new CommitHook() {
                @NotNull
                @Override
                public NodeState processCommit(NodeState before,
                                               NodeState after,
                                               CommitInfo info)
                        throws CommitFailedException {
                    // add more nodes to create branch commits
                    DocumentRootBuilder nb = (DocumentRootBuilder) after.builder();
                    for (int i = 0; i < branchCommits; i++) {
                        // add a node with a different merge
                        addNodes(ns, "/baz/node-" + i);
                        // now force a rebase
                        nb.rebase();
                        // and add nodes until a branch commit is created
                        NodeBuilder child = nb.child("bar").child("node-" + i);
                        for (int j = 0; j < updateLimit; j++) {
                            child.child("node-" + j);
                        }
                    }
                    // eventually fail the commit to trigger a reset
                    throw new CommitFailedException(CommitFailedException.OAK, Integer.MAX_VALUE, "failure");
                }
            }, CommitInfo.EMPTY);
            fail("Merge must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            if (e.getCode() != Integer.MAX_VALUE) {
                throw e;
            }
            // otherwise expected
        }

        Path bar = Path.fromString("/bar");
        // verify reset cleaned up properly
        for (NodeDocument doc : Utils.getAllDocuments(ns.getDocumentStore())) {
            Path p = doc.getPath();
            if (bar.isAncestorOf(p) || bar.equals(p)) {
                String path = p.toString();
                assertThat(path, doc.getLocalRevisions().keySet(), is(empty()));
                assertThat(path, doc.getLocalCommitRoot().keySet(), is(empty()));
                assertThat(path, doc.getDeleted().keySet(), is(empty()));
                assertThat(path, doc.getLocalBranchCommits(), is(empty()));
                assertTrue(path, doc.wasDeletedOnce());
            }
        }
    }

    private void addNodes(DocumentNodeStore ns, String... paths)
            throws CommitFailedException {
        NodeBuilder nb = ns.getRoot().builder();
        for (String p : paths) {
            NodeBuilder b = nb;
            for (String name : PathUtils.elements(p)) {
                b = b.child(name);
            }
        }
        TestUtils.merge(ns, nb);
    }

    private static class TestEditor extends DefaultEditor {

        private final NodeBuilder builder;
        private final String prefix;

        TestEditor(NodeBuilder builder, String prefix) {
            this.builder = builder;
            this.prefix = prefix;
        }

        @Override
        public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
            return new TestEditor(builder.child(name), prefix);
        }

        @Override
        public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
            return new TestEditor(builder.child(name), prefix);
        }

        @Override
        public void propertyAdded(PropertyState after) throws CommitFailedException {
            if (after.getName().startsWith(prefix)) {
                builder.setProperty(after.getName(), "test");
            }
        }
    }

    private static class TestHook extends EditorHook {

        TestHook(final String prefix) {
            super(new EditorProvider() {
                @Nullable
                @Override
                public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder, CommitInfo info)
                        throws CommitFailedException {
                    return new TestEditor(builder, prefix);
                }
            });
        }
    }
}
