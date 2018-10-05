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

import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.junit.Assert.assertTrue;

public class ManyClusterNodesTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private static final int NUM_CLUSTER_NODES = 8;

    private static final long MB = 1024 * 1024;

    private DocumentStore ds;

    private List<DocumentNodeStore> stores = newArrayList();

    @Before
    public void before() throws Exception {
        ds = new MemoryDocumentStore();
        for (int i = 0; i < NUM_CLUSTER_NODES; i++) {
            stores.add(builderProvider.newBuilder()
                    .setClusterId(i + 1)
                    .setDocumentStore(ds)
                    .setAsyncDelay(0)
                    .getNodeStore());
            stores.get(i).runBackgroundOperations();
        }
    }

    // OAK-5479
    @Test
    public void manyMultiValuedPropertyChanges() throws Exception {
        String value = "some-long-value-that-will-bloat-the-document";
        String id = Utils.getIdFromPath("/test");
        for (int i = 0; i < 1000; i++) {
            DocumentNodeStore ns = stores.get(i % NUM_CLUSTER_NODES);
            ns.runBackgroundOperations();
            NodeBuilder builder = ns.getRoot().builder();
            NodeBuilder test = builder.child("test");
            PropertyState p = test.getProperty("p");
            List<String> values = newArrayList();
            if (p != null) {
                Iterables.addAll(values, p.getValue(Type.STRINGS));
            }
            values.add(value);
            test.setProperty("p", values, Type.STRINGS);
            merge(ns, builder);
            int size = ds.find(Collection.NODES, id).getMemory();
            assertTrue(size < 8 * MB);
            ns.runBackgroundOperations();
        }
    }
}
