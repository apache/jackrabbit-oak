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
package org.apache.jackrabbit.oak.plugins.document.cache;

import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.locks.NodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.locks.StripedNodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder;
import static org.junit.Assert.assertEquals;

public class NodeDocumentCacheTest {

    private static final String ID = "some-id";

    private final DocumentStore store = new MemoryDocumentStore();

    private final NodeDocumentLocks locks = new StripedNodeDocumentLocks();

    private NodeDocumentCache cache;

    @Before
    public void setup() {
        cache = newDocumentNodeStoreBuilder()
                .buildNodeDocumentCache(store, locks);
    }

    @Test
    public void cacheConsistency() throws Exception {
        NodeDocument current = createDocument(0L);
        NodeDocument updated = createDocument(1L);

        // an update operation starts and registers a tracker
        CacheChangesTracker updateTracker = cache.registerTracker(singleton(ID));
        // the document is invalidated. this informs the update tracker
        cache.invalidate(ID);
        // a query operation starts and registers a tracker
        CacheChangesTracker queryTracker = cache.registerTracker(singleton(ID));
        // the query operation is able to read the document before it is updated
        // but then gets delayed.
        // the update operation wants to put the document into the cache, but
        // can't because its tracker was informed by the cache invalidation
        cache.putNonConflictingDocs(updateTracker, singleton(updated));
        // the query operation wants to put the outdated document into the
        // cache. the cache must not accept this outdated document.
        cache.putNonConflictingDocs(queryTracker, singleton(current));

        assertEquals(updated.getModCount(), cache.get(ID, () -> updated).getModCount());
    }

    private NodeDocument createDocument(long modCount) {
        NodeDocument doc = new NodeDocument(store, modCount);
        doc.put(Document.ID, ID);
        doc.put(Document.MOD_COUNT, modCount);
        return doc;
    }
}
