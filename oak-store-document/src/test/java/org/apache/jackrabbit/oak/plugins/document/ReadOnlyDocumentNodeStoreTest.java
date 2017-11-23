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
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ReadOnlyDocumentNodeStoreTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void readOnlyCompare() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        // read-only
        DocumentNodeStore ns2 = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).setReadOnlyMode()
                .getNodeStore();
        NodeBuilder builder = ns1.getRoot().builder();
        builder.child("foo");
        merge(ns1, builder);
        ns1.runBackgroundOperations();

        DocumentNodeState s1 = ns2.getRoot();
        ns2.runBackgroundOperations();

        builder = ns1.getRoot().builder();
        builder.child("bar");
        merge(ns1, builder);
        ns1.runBackgroundOperations();

        ns2.runBackgroundOperations();
        DocumentNodeState s2 = ns2.getRoot();

        TrackingDiff diff = new TrackingDiff();
        s2.compareAgainstBaseState(s1, diff);
        assertThat(diff.deleted, is(empty()));
        assertThat(diff.modified, is(empty()));
        assertThat(diff.added, containsInAnyOrder("/foo", "/bar"));
    }

}
