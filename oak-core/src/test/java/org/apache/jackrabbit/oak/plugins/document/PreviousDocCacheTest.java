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

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.NO_BINARY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class PreviousDocCacheTest extends AbstractMongoConnectionTest {

    @Test
    public void cacheTestPrevDocs() throws Exception {
        DocumentNodeStore ns = mk.getNodeStore();
        DocumentStore docStore = ns.getDocumentStore();

        final int SPLIT_THRESHOLD = 10;
        NodeBuilder b;

        //Set property 110 times. Split at each 10. This should lead to 11 leaf prev docs and 1 intermediate prev doc.
        for (int j = 0; j <= SPLIT_THRESHOLD; j++) {
            for (int i = 0; i < SPLIT_THRESHOLD; i++) {
                b = ns.getRoot().builder();
                b.setProperty("foo", "node-" + j + "-" + i);
                merge(ns, b);
            }
            splitDocs(ns, SPLIT_THRESHOLD);
        }

        CacheStats nodesCache = null;
        CacheStats prevDocsCache = null;
        for (CacheStats cacheStats : docStore.getCacheStats()) {
            if ("Document-Documents".equals(cacheStats.getName())) {
                nodesCache = cacheStats;
            } else if ("Document-PrevDocuments".equals(cacheStats.getName())) {
                prevDocsCache = cacheStats;
            }
        }
        assertNotNull("Nodes cache must not be null", nodesCache);
        assertNotNull("Prev docs cache must not be null", prevDocsCache);

        validateFullyLoadedCache(docStore, SPLIT_THRESHOLD, nodesCache, prevDocsCache);

        docStore.invalidateCache();
        assertEquals("No entries expected in nodes cache", 0, nodesCache.getElementCount());
        assertEquals("No entries expected in prev docs cache", 0, prevDocsCache.getElementCount());

        NodeDocument doc = docStore.find(NODES, "0:/");
        assertEquals("Only main doc entry expected in nodes cache", 1, nodesCache.getElementCount());
        assertEquals("No entries expected in prev docs cache", 0, prevDocsCache.getElementCount());

        Iterators.size(doc.getAllPreviousDocs());
        validateFullyLoadedCache(docStore, SPLIT_THRESHOLD, nodesCache, prevDocsCache);
    }

    private void validateFullyLoadedCache(DocumentStore docStore, int splitThreshold, CacheStats nodesCache, CacheStats prevDocsCache) {
        assertEquals("Nodes cache must have 2 elements - '/' and intermediate split doc",
                2, nodesCache.getElementCount());
        assertEquals("Unexpected number of leaf prev docs", splitThreshold + 1, prevDocsCache.getElementCount());

        resetStats(nodesCache, prevDocsCache);
        NodeDocument doc = docStore.getIfCached(NODES, "0:/");
        assertEquals("Root doc must be available in nodes cache", 1, nodesCache.getHitCount());
        assertEquals("Prev docs must not be read", 0, prevDocsCache.getHitCount());

        Iterators.size(doc.getAllPreviousDocs());
        assertEquals("Nodes cache should not have a miss", 0, nodesCache.getMissCount());
        assertEquals("Prev docs cache should not have a miss", 0, prevDocsCache.getMissCount());
    }

    private void resetStats(CacheStats ... cacheStatses) {
        for (CacheStats cacheStats : cacheStatses) {
            cacheStats.resetStats();
        }
    }

    private void splitDocs(DocumentNodeStore ns, int splitDocLimit) {
        DocumentStore store = ns.getDocumentStore();
        NodeDocument doc = Utils.getRootDocument(store);
        List<UpdateOp> ops = SplitOperations.forDocument(doc,
                ns, ns.getHeadRevision(), NO_BINARY,
                splitDocLimit/2);
        assertFalse(ops.isEmpty());
        for (UpdateOp op : ops) {
            if (!op.isNew() ||
                    !store.create(NODES, Collections.singletonList(op))) {
                store.createOrUpdate(NODES, op);
            }
        }
    }

    private static void merge(NodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
