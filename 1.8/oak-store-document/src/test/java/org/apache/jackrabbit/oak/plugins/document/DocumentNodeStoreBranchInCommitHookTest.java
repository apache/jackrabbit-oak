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

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isCommitted;
import static org.junit.Assert.assertTrue;

public class DocumentNodeStoreBranchInCommitHookTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    // OAK-7401
    @Ignore
    @Test
    public void manyChangesInCommitHooks() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0).setUpdateLimit(10).build();

        // get initial branch commits
        NodeDocument root = Utils.getRootDocument(ns.getDocumentStore());
        int initialNumBranchCommits = numBranchCommits(root);

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("test");
        ns.merge(builder, new TestHook(), CommitInfo.EMPTY);

        // must have created branch commits
        root = Utils.getRootDocument(ns.getDocumentStore());
        assertTrue(numBranchCommits(root) > initialNumBranchCommits);
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

    private static class TestHook extends EditorHook {

        TestHook() {
            super(new EditorProvider() {
                @Override
                public Editor getRootEditor(NodeState before,
                                            NodeState after,
                                            NodeBuilder builder,
                                            CommitInfo info) {
                    return new TestEditor(builder);
                }
            });
        }
    }

    private static class TestEditor extends DefaultEditor {

        private final NodeBuilder builder;

        TestEditor(NodeBuilder builder) {
            this.builder = builder;
        }

        @Override
        public void enter(NodeState before, NodeState after) {
            NodeBuilder stuff = builder.child("parent");
            for (int i = 0; i < 17; i++) {
                stuff.child("n-" + i);
            }
        }
    }
}
