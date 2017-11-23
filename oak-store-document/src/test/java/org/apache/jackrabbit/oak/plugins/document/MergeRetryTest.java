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

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

/**
 * Test for conflicts introduced by commit hook, which can be overcome by
 * retrying BranchState.merge().
 */
public class MergeRetryTest {

    // this hook adds a 'foo' child if it does not exist
    private static final CommitHook HOOK = new EditorHook(new EditorProvider() {
        @CheckForNull
        @Override
        public Editor getRootEditor(
                NodeState before, NodeState after, final NodeBuilder builder,
                CommitInfo info) throws CommitFailedException {
            return new DefaultEditor() {
                @Override
                public void enter(NodeState before, NodeState after)
                        throws CommitFailedException {
                    if (!builder.hasChildNode("foo")) {
                        builder.child("foo");
                    }
                }
            };
        }
    });

    /**
     * Test for OAK-1198
     */
    @Test
    public void retryInMemory() throws Exception {
        MemoryDocumentStore ds = new MemoryDocumentStore();
        MemoryBlobStore bs = new MemoryBlobStore();

        DocumentNodeStore ns1 = createMK(1, 1000, ds, bs);
        DocumentNodeStore ns2 = createMK(2, 1000, ds, bs);
        try {
            NodeBuilder builder1 = ns1.getRoot().builder();
            builder1.child("bar");

            NodeBuilder builder2 = ns2.getRoot().builder();
            builder2.child("qux");

            ns1.merge(builder1, HOOK, CommitInfo.EMPTY);
            ns2.merge(builder2, HOOK, CommitInfo.EMPTY);
        } finally {
            ns1.dispose();
            ns2.dispose();
        }
    }


    /**
     * Test for OAK-1202
     */
    @Test
    public void retryPersisted() throws Exception {
        MemoryDocumentStore ds = new MemoryDocumentStore();
        MemoryBlobStore bs = new MemoryBlobStore();

        DocumentNodeStore ns1 = createMK(1, 1000, ds, bs);
        DocumentNodeStore ns2 = createMK(2, 1000, ds, bs);
        try {
            NodeBuilder builder1 = ns1.getRoot().builder();
            createTree(builder1.child("bar"), 2);

            NodeBuilder builder2 = ns2.getRoot().builder();
            createTree(builder2.child("qux"), 2);

            ns1.merge(builder1, HOOK, CommitInfo.EMPTY);
            ns2.merge(builder2, HOOK, CommitInfo.EMPTY);
        } finally {
            ns1.dispose();
            ns2.dispose();
        }
    }

    private void createTree(NodeBuilder node, int level) {
        if (level-- > 0) {
            for (int i = 0; i < 10; i++) {
                NodeBuilder child = node.child("node-" + i);
                for (int p = 0; p < 10; p++) {
                    child.setProperty("p-" + p, "value");
                }
                createTree(child, level);
            }
        }
    }

    private DocumentNodeStore createMK(int clusterId, int asyncDelay,
            DocumentStore ds, BlobStore bs) {
        return new DocumentMK.Builder().setDocumentStore(ds).setBlobStore(bs)
                .setClusterId(clusterId).setAsyncDelay(asyncDelay).getNodeStore();
    }
}
