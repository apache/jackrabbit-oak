/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.LocalDiffCache.Diff;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Test;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LocalDiffCacheTest {

    DocumentNodeStore store;

    @After
    public void dispose() {
        if (store != null) {
            store.dispose();
            store = null;
        }
    }

    @Test
    public void simpleDiff() throws Exception{
        TestNodeObserver o = new TestNodeObserver("/");
        store = createMK().getNodeStore();
        store.addObserver(o);

        o.reset();

        DiffCache cache = store.getDiffCache();
        Iterable<CacheStats> stats = cache.getStats();

        NodeBuilder builder = store.getRoot().builder();
        builder.child("a").child("a2").setProperty("foo", "bar");
        builder.child("b");
        merge(store, builder);

        assertTrue(getHitCount(stats) > 0);
        assertEquals(0, getMissCount(stats));
        assertEquals(3, o.added.size());

        builder = store.getRoot().builder();
        builder.child("a").child("a2").removeProperty("foo");

        o.reset();
        resetStats(stats);
        merge(store, builder);

        assertTrue(getHitCount(stats) > 0);
        assertEquals(0, getMissCount(stats));
        assertEquals(1, o.changed.size());
    }

    @Test
    public void diffFromAsString() {
        Map<String, String> changes = Maps.newHashMap();
        changes.put("/", "+\"foo\":{}^\"bar\":{}-\"baz\"");
        changes.put("/foo", "");
        changes.put("/bar", "+\"qux\"");
        changes.put("/bar/qux", "");
        Diff diff = new Diff(changes, 0);

        assertEquals(changes, Diff.fromString(diff.asString()).getChanges());
    }

    @Test
    public void emptyDiff() throws Exception{
        Map<String, String> changes = new HashMap<String, String>();
        Diff diff = new Diff(changes, 100);
        String asString = diff.asString();
        Diff diff2 = Diff.fromString(asString);
        assertEquals(diff, diff2);
    }

    private static DocumentNodeState merge(NodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        return (DocumentNodeState) store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static DocumentMK createMK(){
        return create(new MemoryDocumentStore(), 0);
    }

    private static DocumentMK create(DocumentStore ds, int clusterId){
        return new DocumentMK.Builder()
                .setAsyncDelay(0)
                .setDocumentStore(ds)
                .setClusterId(clusterId)
                .setPersistentCache("target/persistentCache,time")
                .open();
    }

    private static long getHitCount(Iterable<CacheStats> stats) {
        long hitCount = 0;
        for (CacheStats cs : stats) {
            hitCount += cs.getHitCount();
        }
        return hitCount;
    }

    private static long getMissCount(Iterable<CacheStats> stats) {
        long missCount = 0;
        for (CacheStats cs : stats) {
            missCount += cs.getMissCount();
        }
        return missCount;
    }

    private static void resetStats(Iterable<CacheStats> stats) {
        for (CacheStats cs : stats) {
            cs.resetStats();
        }
    }
}
