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

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

public class RebaseTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private CountingDocumentStore store;

    private DocumentNodeStore ns;

    @Before
    public void setup() {
        store = new CountingDocumentStore(new MemoryDocumentStore());
        ns = builderProvider.newBuilder()
                .setDocumentStore(store)
                .setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .setAsyncDelay(0)
                .build();
    }

    @Test
    public void rebase() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        addNodes(builder, 10, 3);
        merge(ns, builder);

        DocumentNodeState root = ns.getRoot();
        DocumentNodeStoreBranch b = ns.createBranch(root);
        builder = root.builder();
        builder.child("node-4").child("node-2").child("x").child("y");
        b.setRoot(builder.getNodeState());

        builder.child("node-4").child("node-2").child("x").child("y").setProperty("foo", "bar");

        ns.invalidateNodeChildrenCache();
        ns.getNodeCache().invalidateAll();

        store.resetCounters();
        ns.rebase(builder);
        assertThat(store.getNumFindCalls(Collection.NODES), lessThan(3));
    }

    private void addNodes(NodeBuilder builder, int num, int level) {
        if (level > 0) {
            for (int i = 0; i < num; i++) {
                addNodes(builder.child("node-" + i), num, level - 1);
            }
        }
    }
}
