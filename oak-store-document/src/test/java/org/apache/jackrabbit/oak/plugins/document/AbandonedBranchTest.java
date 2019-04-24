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
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.persistToBranch;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class AbandonedBranchTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void abandonBranch() {
        int clusterId = 1;
        DocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(store).setClusterId(clusterId).build();
        NodeBuilder builder = ns.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            builder.child("test").child("node-" + i);
            persistToBranch(builder);
        }
        ns.dispose();
        ns = builderProvider.newBuilder()
                .setDocumentStore(store).setClusterId(clusterId).build();
        assertFalse(ns.getRoot().hasChildNode("test"));
        NodeDocument doc = Utils.getRootDocument(store);
        assertThat(doc.getLocalBranchCommits(), empty());
    }
}
