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

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link DocumentNodeStoreHelper}.
 * These assertions intentionally avoid third-party cache types so the same
 * tests can run across cache implementation changes.
 */
public class DocumentNodeStoreHelperTest {

    private DocumentNodeStore store;

    @Before
    public void setUp() {
        store = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                .setAsyncDelay(0)
                .build();
    }

    @After
    public void tearDown() {
        if (store != null) {
            store.dispose();
        }
    }

    @Test
    public void getNodesCacheReturnsNonNull() {
        // This is a smoke test for the helper entry point: callers should always
        // get back a usable cache handle for a live DocumentNodeStore.
        Assert.assertNotNull(DocumentNodeStoreHelper.getNodesCache(store));
    }

    @Test
    public void getNodesCacheExposesNonNullMapView() {
        // This only asserts that the helper exposes a map view; contents and
        // mutability semantics are intentionally out of scope here.
        Assert.assertNotNull(DocumentNodeStoreHelper.getNodesCache(store).asMap());
    }

    @Test
    public void getNodesCacheMapViewReflectsCachedNodeReads() throws Exception {
        // Create content through the normal node-store API so the subsequent read
        // exercises the production cache population path instead of a test-only insert.
        NodeBuilder builder = store.getRoot().builder();
        builder.child("a").child("b");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Clear any startup entries first, then verify a real tree read repopulates
        // the helper-visible map view.
        DocumentNodeStoreHelper.getNodesCache(store).asMap().clear();
        Assert.assertTrue(DocumentNodeStoreHelper.getNodesCache(store).asMap().isEmpty());
        store.getRoot().getChildNode("a").getChildNode("b");

        Assert.assertFalse(DocumentNodeStoreHelper.getNodesCache(store).asMap().isEmpty());
    }
}
