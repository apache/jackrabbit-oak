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
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import com.google.common.collect.Maps;

import static org.junit.Assert.assertTrue;

/**
 * Checks that traversing over many child nodes requests them in batches with
 * an upper limit.
 */
public class ManyChildNodesTest {

    @Test
    public void manyChildNodes() throws Exception {
        TestStore store = new TestStore();
        DocumentMK mk = new DocumentMK.Builder().setDocumentStore(store).open();
        NodeStore ns = mk.getNodeStore();

        NodeBuilder builder = ns.getRoot().builder();
        for (int i = 0; i < DocumentNodeState.MAX_FETCH_SIZE * 2; i++) {
            builder.child("c-" + i);
        }
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store.queries.clear();
        // must fetch in batches
        for (ChildNodeEntry entry : ns.getRoot().getChildNodeEntries()) {
            entry.getName();
        }
        // maximum fetch size is MAX_FETCH_SIZE plus one because
        // DocumentNodeStore will use this value to find out if there
        // are more child nodes than requested
        int maxFetchSize = DocumentNodeState.MAX_FETCH_SIZE + 1;
        for (Map.Entry<String, Integer> e : store.queries.entrySet()) {
            assertTrue(e.getValue() + " > " + maxFetchSize,
                    e.getValue() <= maxFetchSize);
        }
        mk.dispose();
    }
    
    // OAK-2448
    @Test
    public void nodeChildrenCache() throws Exception {
        DocumentNodeStore ns = new DocumentMK.Builder().getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        for (int i = 0; i < 1000; i++) {
            builder.child("c-" + i);
        }
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        // must not create entries for each child node
        assertTrue(ns.getNodeChildrenCacheStats().getElementCount() < 1000);
        
        ns.dispose();
    }

    private final class TestStore extends MemoryDocumentStore {

        Map<String, Integer> queries = Maps.newHashMap();

        @Nonnull
        @Override
        public <T extends Document> List<T> query(Collection<T> collection,
                                                  String fromKey,
                                                  String toKey,
                                                  int limit) {
            if (collection == Collection.NODES) {
                queries.put(fromKey, limit);
            }
            return super.query(collection, fromKey, toKey, limit);
        }
    }
}
