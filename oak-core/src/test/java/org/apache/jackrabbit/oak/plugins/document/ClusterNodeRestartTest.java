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
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ClusterNodeRestartTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    // OAK-3549
    @Test
    public void restart() throws Exception {
        MemoryDocumentStore docStore = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setClusterId(1)
                .setAsyncDelay(0).getNodeStore();
        DocumentNodeStore ns2 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setClusterId(2)
                .setAsyncDelay(0).getNodeStore();

        NodeBuilder builder = ns1.getRoot().builder();
        builder.child("foo").child("bar");
        merge(ns1, builder);
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        builder = ns2.getRoot().builder();
        builder.child("foo").child("bar").setProperty("p", "v");
        merge(ns2, builder);
        assertTrue(ns2.getRoot().getChildNode("foo").getChildNode("bar").hasProperty("p"));

        ns2.dispose();

        ns1.runBackgroundOperations();
        assertTrue(ns1.getRoot().getChildNode("foo").getChildNode("bar").hasProperty("p"));

        ns2 = builderProvider.newBuilder()
                .setDocumentStore(docStore).setClusterId(2)
                .setAsyncDelay(0).getNodeStore();
        assertTrue(ns2.getRoot().getChildNode("foo").getChildNode("bar").hasProperty("p"));
    }

    private void merge(NodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
