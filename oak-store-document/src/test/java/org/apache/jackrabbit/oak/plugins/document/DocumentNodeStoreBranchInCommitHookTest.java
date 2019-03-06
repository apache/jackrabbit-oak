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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isCommitted;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DocumentNodeStoreBranchInCommitHookTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    // OAK-7401
    @Test
    public void manyChangesInCommitHooks() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0).setUpdateLimit(10).build();

        // get initial branch commits
        NodeDocument root = Utils.getRootDocument(ns.getDocumentStore());
        int initialNumBranchCommits = numBranchCommits(root);

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("test");
        ns.merge(builder, new TestHook(17), CommitInfo.EMPTY);

        // must have created branch commits
        root = Utils.getRootDocument(ns.getDocumentStore());
        assertTrue(numBranchCommits(root) > initialNumBranchCommits);
    }

    // OAK-7401
    @Test
    public void manyChangesInMultipleCommitHooks() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0).setUpdateLimit(10).build();

        // get initial branch commits
        NodeDocument root = Utils.getRootDocument(ns.getDocumentStore());
        int initialNumBranchCommits = numBranchCommits(root);

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("test");
        TestHook h1 = new TestHook(17);
        TestHook h2 = new TestHook(23);
        ns.merge(builder, new CompositeHook(h1, h2), CommitInfo.EMPTY);

        // must have created branch commits
        root = Utils.getRootDocument(ns.getDocumentStore());
        assertTrue(numBranchCommits(root) > initialNumBranchCommits);

        h1.assertChanges(ns.getRoot());
        h2.assertChanges(ns.getRoot());
    }

    // OAK-7401
    @Test
    public void manyChangesInCommitHooksMultipleBranchCommits() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0).setUpdateLimit(10).build();

        // get initial branch commits
        NodeDocument root = Utils.getRootDocument(ns.getDocumentStore());
        int initialNumBranchCommits = numBranchCommits(root);

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("test");
        TestHook hook = new TestHook(41);
        ns.merge(builder, hook, CommitInfo.EMPTY);

        // must have created branch commits
        root = Utils.getRootDocument(ns.getDocumentStore());
        assertTrue(numBranchCommits(root) > initialNumBranchCommits);

        hook.assertChanges(ns.getRoot());
    }

    // OAK-7401
    @Test
    public void commitHookTriggersBranchWithConflict() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0).setUpdateLimit(10).build();

        // get initial branch commits
        NodeDocument root = Utils.getRootDocument(ns.getDocumentStore());
        int initialNumBranchCommits = numBranchCommits(root);

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("test");
        TestHook hook = new TestHook(41);
        ns.merge(builder, new CompositeHook(hook,
                        new FailingHook(1)), // fail once
                CommitInfo.EMPTY);

        // must have created branch commits
        root = Utils.getRootDocument(ns.getDocumentStore());
        assertTrue(numBranchCommits(root) > initialNumBranchCommits);

        hook.assertChanges(ns.getRoot());

        // must not leave behind unmerged branch commits (OAK-8106)
        root = Utils.getRootDocument(ns.getDocumentStore());
        for (String value : root.getLocalRevisions().values()) {
            if (!Utils.isCommitted(value)) {
                fail("Must not leave unmerged branch commits behind");
            }
        }
    }

    private int numBranchCommits(NodeDocument root) {
        int numBranchCommits = 0;
        for (Map.Entry<Revision, String> entry : root.getLocalRevisions().entrySet()) {
            Revision rev = entry.getKey();
            String value = entry.getValue();
            if (isCommitted(value) && !Utils.resolveCommitRevision(rev, value).equals(rev)) {
                // we have a merged branch commit
                numBranchCommits++;
            }
        }
        return numBranchCommits;
    }

    private static class FailingHook implements CommitHook {

        private int countDown;

        FailingHook(int numFailures) {
            this.countDown = numFailures;
        }

        @NotNull
        @Override
        public NodeState processCommit(NodeState before,
                                       NodeState after,
                                       CommitInfo info)
                throws CommitFailedException {
            if (countDown-- > 0) {
                throw new ConflictException("Conflict",
                        new Revision(0, 0, 1))
                        .asCommitFailedException();
            }
            return after;
        }
    }

    private static class TestHook extends EditorHook {

        private static final AtomicInteger COUNTER = new AtomicInteger();

        private final TestEditorProvider editorProvider;

        TestHook(int numChanges) {
            this(new TestEditorProvider("parent-" + COUNTER.getAndIncrement(), numChanges));
        }

        private TestHook(TestEditorProvider editorProvider) {
            super(editorProvider);
            this.editorProvider = editorProvider;
        }

        void assertChanges(NodeState root) {
            NodeState parent = root.getChildNode(editorProvider.parentName);
            for (int i = 0; i < editorProvider.numChanges; i++) {
                String msg = "Missing node: /" + editorProvider.parentName + "/n-" + i;
                assertTrue(msg, parent.hasChildNode("n-" + i));
            }
        }
    }

    private static class TestEditorProvider implements EditorProvider {

        private final String parentName;
        private final int numChanges;

        TestEditorProvider(String parentName, int numChanges) {
            this.parentName = parentName;
            this.numChanges = numChanges;
        }

        @Override
        public Editor getRootEditor(NodeState before,
                                    NodeState after,
                                    NodeBuilder builder,
                                    CommitInfo info) {
            return new TestEditor(parentName, numChanges, builder);
        }
    }

    private static class TestEditor extends DefaultEditor {

        private final String parentName;
        private final int numChanges;
        private final NodeBuilder builder;

        TestEditor(String parentName, int numChanges, NodeBuilder builder) {
            this.parentName = parentName;
            this.numChanges = numChanges;
            this.builder = builder;
        }

        @Override
        public void enter(NodeState before, NodeState after) {
            NodeBuilder stuff = builder.child(parentName);
            for (int i = 0; i < numChanges; i++) {
                String name = "n-" + i;
                assertFalse(stuff.hasChildNode(name));
                stuff.child(name);
            }
        }
    }
}
