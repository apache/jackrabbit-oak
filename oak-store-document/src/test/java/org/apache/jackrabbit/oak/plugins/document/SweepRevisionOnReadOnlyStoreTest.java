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
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class SweepRevisionOnReadOnlyStoreTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore ns1;
    private DocumentNodeStore ns2;

    @Before
    public void setup() {
        DocumentStore store = new MemoryDocumentStore();
        ns1 = builderProvider.newBuilder().setDocumentStore(store)
                .setAsyncDelay(0).getNodeStore();
        ns2 = builderProvider.newBuilder().setDocumentStore(store)
                .setAsyncDelay(0).setReadOnlyMode().getNodeStore();
    }

    @Test
    public void backgroundOperationUpdatesSweepRevision() throws Exception {
        addNode(ns1, "foo");
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();
        RevisionVector sweepRev = ns2.getSweepRevisions();

        addNode(ns1, "bar");
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        assertFalse(sweepRev.equals(ns2.getSweepRevisions()));
    }

    private static void addNode(NodeStore ns, String name)
            throws CommitFailedException {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child(name);
        TestUtils.merge(ns, builder);
    }
}
