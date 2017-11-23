/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertNull;

public class UpdateLimitTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore ns;
    private DocumentStore store;
    private String testId;

    @Before
    public void before() {
        ns = builderProvider.newBuilder().getNodeStore();
        store = ns.getDocumentStore();
        testId = Utils.getIdFromPath("/test");
    }

    @Test
    public void rebase() throws Exception {
        NodeBuilder builder = someChanges(ns.getRoot().builder());
        for (int i = 0; i < 20; i++) {
            ns.rebase(builder);
            assertNull(store.find(Collection.NODES, testId));
        }
    }

    @Test
    public void reset() {
        NodeBuilder builder = someChanges(ns.getRoot().builder());
        for (int i = 0; i < 20; i++) {
            ns.reset(builder);
            someChanges(builder);
            assertNull(store.find(Collection.NODES, testId));
        }
    }

    private NodeBuilder someChanges(NodeBuilder root) {
        NodeBuilder test = root.child("test");
        for (int i = 0; i < 23; i++) {
            test.child("node-" + i);
        }
        return root;
    }
}
