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

import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.locks.NodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.locks.StripedNodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getKeyLowerLimit;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getKeyUpperLimit;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CacheChangesTrackerTest {

    private DocumentStore ds;

    @Before
    public void createDS() {
        ds = new MemoryDocumentStore();
    }

    @Test
    public void testTracker() {
        List<CacheChangesTracker> list = new ArrayList<CacheChangesTracker>();
        CacheChangesTracker tracker = new CacheChangesTracker(new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String input) {
                return !"ignored".equals(input);
            }
        }, list, 100);

        assertFalse(tracker.mightBeenAffected("xyz"));
        assertFalse(tracker.mightBeenAffected("abc"));

        tracker.putDocument("xyz");
        assertTrue(tracker.mightBeenAffected("xyz"));

        tracker.invalidateDocument("abc");
        assertTrue(tracker.mightBeenAffected("abc"));

        tracker.putDocument("ignored");
        tracker.invalidateDocument("ignored");
        assertFalse(tracker.mightBeenAffected("ignored"));

        tracker.close();
        assertTrue(list.isEmpty());
    }

    @Test
    public void testRegisterChildrenTracker() {
        NodeDocumentCache cache = createCache();
        CacheChangesTracker tracker = cache.registerTracker(getKeyLowerLimit("/parent"), getKeyUpperLimit("/parent"));

        assertFalse(tracker.mightBeenAffected("2:/parent/xyz"));
        assertFalse(tracker.mightBeenAffected("2:/parent/abc"));

        cache.put(createDoc("2:/parent/xyz"));
        assertTrue(tracker.mightBeenAffected("2:/parent/xyz"));

        cache.invalidate("2:/parent/abc");
        assertTrue(tracker.mightBeenAffected("2:/parent/abc"));

        cache.invalidate("2:/other-parent/abc");
        assertFalse(tracker.mightBeenAffected("2:/other-parent/abc"));

        tracker.close();

        cache.invalidate("2:/parent/123");
        assertFalse(tracker.mightBeenAffected("2:/parent/123"));
    }

    @Test
    public void testGetLoaderAffectsTracker() throws ExecutionException {
        NodeDocumentCache cache = createCache();
        CacheChangesTracker tracker = cache.registerTracker(getKeyLowerLimit("/parent"), getKeyUpperLimit("/parent"));

        assertFalse(tracker.mightBeenAffected("2:/parent/xyz"));

        cache.getIfPresent("2:/parent/xyz");
        assertFalse(tracker.mightBeenAffected("2:/parent/xyz"));

        cache.get("2:/parent/xyz", new Callable<NodeDocument>() {
            @Override
            public NodeDocument call() throws Exception {
                return createDoc("2:/parent/xyz");
            }
        });
        assertTrue(tracker.mightBeenAffected("2:/parent/xyz"));
    }

    @Test
    public void testRegisterKeysTracker() {
        NodeDocumentCache cache = createCache();
        CacheChangesTracker tracker = cache.registerTracker(ImmutableSet.of("1:/xyz", "1:/abc", "1:/aaa"));

        assertFalse(tracker.mightBeenAffected("1:/xyz"));
        assertFalse(tracker.mightBeenAffected("1:/abc"));
        assertFalse(tracker.mightBeenAffected("1:/aaa"));

        cache.put(createDoc("1:/xyz"));
        assertTrue(tracker.mightBeenAffected("1:/xyz"));

        cache.invalidate("1:/abc");
        assertTrue(tracker.mightBeenAffected("1:/abc"));

        cache.invalidate("1:/other");
        assertFalse(tracker.mightBeenAffected("1:/other"));

        tracker.close();

        cache.invalidate("1:/aaa");
        assertFalse(tracker.mightBeenAffected("1:/aaa"));
    }


    @Test
    public void testOnlyExternalChanges() {
        NodeDocumentCache cache = createCache();
        CacheChangesTracker tracker = cache.registerTracker(getKeyLowerLimit("/parent"), getKeyUpperLimit("/parent"));

        cache.putNonConflictingDocs(tracker, ImmutableSet.of(createDoc("2:/parent/local")));
        assertFalse(tracker.mightBeenAffected("2:/parent/local"));

        cache.put(createDoc("2:/parent/external"));
        assertTrue(tracker.mightBeenAffected("2:/parent/external"));
    }

    private NodeDocumentCache createCache() {
        Cache<CacheValue, NodeDocument> nodeDocumentsCache = new CacheLIRS<CacheValue, NodeDocument>(10);
        Cache<StringValue, NodeDocument> prevDocumentsCache = new CacheLIRS<StringValue, NodeDocument>(10);
        CacheStats nodeDocumentsCacheStats = Mockito.mock(CacheStats.class);
        CacheStats prevDocumentsCacheStats = Mockito.mock(CacheStats.class);
        NodeDocumentLocks locks = new StripedNodeDocumentLocks();
        return new NodeDocumentCache(nodeDocumentsCache, nodeDocumentsCacheStats, prevDocumentsCache, prevDocumentsCacheStats, locks);
    }

    private NodeDocument createDoc(String id) {
        NodeDocument doc = Collection.NODES.newDocument(ds);
        doc.put("_id", id);
        return doc;
    }
}
