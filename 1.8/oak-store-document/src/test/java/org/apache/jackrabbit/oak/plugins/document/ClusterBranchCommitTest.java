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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusterBranchCommitTest {

    @Rule
    public final DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore ns1;
    private DocumentNodeStore ns2;

    @Before
    public void before() {
        DocumentStore store = new MemoryDocumentStore();
        ns1 = builderProvider.newBuilder().setDocumentStore(store)
                .setAsyncDelay(0).setClusterId(1).getNodeStore();
        ns2 = builderProvider.newBuilder().setDocumentStore(store)
                .setAsyncDelay(0).setClusterId(2).getNodeStore();
    }

    @Test
    public void branchCommit() throws Exception {
        NodeBuilder builder = ns1.getRoot().builder();
        builder.child("test");
        merge(ns1, builder);
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        int bc = numBranchCommits(ns2);
        builder = ns2.getRoot().builder();
        int i = 0;
        while (bc == numBranchCommits(ns2)) {
            builder.child("node-" + i++);
        }
        NodeDocument root = Utils.getRootDocument(ns2.getDocumentStore());
        for (String tag : root.getLocalRevisions().values()) {
            if (!Utils.isCommitted(tag)) {
                assertEquals(2, Revision.fromString(tag).getClusterId());
                assertTrue(tag.startsWith("br"));
            }
        }
    }

    private int numBranchCommits(DocumentNodeStore ns) {
        int num = 0;
        NodeDocument root = Utils.getRootDocument(ns.getDocumentStore());
        for (String tag : root.getLocalRevisions().values()) {
            num += Utils.isCommitted(tag) ? 0 : 1;
        }
        return num;
    }

    private static NodeState merge(DocumentNodeStore ns,
                                   NodeBuilder builder)
            throws CommitFailedException {
        return ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
