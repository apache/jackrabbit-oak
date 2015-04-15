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
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.LocalDiffCache.ConsolidatedDiff;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.observation.NodeObserver;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LocalDiffCacheTest {

    DocumentNodeStore store;

    @Test
    public void simpleDiff() throws Exception{
        TestNodeObserver o = new TestNodeObserver("/");
         store = createMK().getNodeStore();
        store.addObserver(o);

        o.reset();

        LocalDiffCache cache = getLocalDiffCache();
        CacheStats stats = cache.getDiffCacheStats();

        DocumentNodeState beforeState = store.getRoot();
        NodeBuilder builder = store.getRoot().builder();
        builder.child("a").child("a2").setProperty("foo", "bar");
        builder.child("b");
        DocumentNodeState afterState = merge(store, builder);

        assertTrue(stats.getHitCount() > 0);
        assertEquals(0, stats.getMissCount());
        assertEquals(3, o.added.size());

        ConsolidatedDiff diff = cache.getDiff(beforeState.getRevision(), afterState.getRevision());
        String serailized = diff.asString();
        ConsolidatedDiff diff2 = ConsolidatedDiff.fromString(serailized);
        assertEquals(diff, diff2);

        builder = store.getRoot().builder();
        builder.child("a").child("a2").removeProperty("foo");

        o.reset();
        stats.resetStats();
        merge(store, builder);

        assertTrue(stats.getHitCount() > 0);
        assertEquals(0, stats.getMissCount());
        assertEquals(1, o.changed.size());

        store.dispose();
    }

    @Test
    public void emptyDiff() throws Exception{
        Map<String, String> changes = new HashMap<String, String>();
        ConsolidatedDiff diff = new ConsolidatedDiff(changes, 100);
        String asString = diff.asString();
        ConsolidatedDiff diff2 = ConsolidatedDiff.fromString(asString);
        assertEquals(diff, diff2);
    }

    private LocalDiffCache getLocalDiffCache(){
        return (LocalDiffCache) store.getLocalDiffCache();
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

    //------------------------------------------------------------< TestNodeObserver >---

    private static class TestNodeObserver extends NodeObserver {
        private final Map<String, Set<String>> added = newHashMap();
        private final Map<String, Set<String>> deleted = newHashMap();
        private final Map<String, Set<String>> changed = newHashMap();
        private final Map<String, Map<String, String>> properties = newHashMap();

        protected TestNodeObserver(String path, String... propertyNames) {
            super(path, propertyNames);
        }

        @Override
        protected void added(
                @Nonnull String path,
                @Nonnull Set<String> added,
                @Nonnull Set<String> deleted,
                @Nonnull Set<String> changed,
                @Nonnull Map<String, String> properties,
                @Nonnull CommitInfo commitInfo) {
            this.added.put(path, newHashSet(added));
            if (!properties.isEmpty()) {
                this.properties.put(path, newHashMap(properties));
            }
        }

        @Override
        protected void deleted(
                @Nonnull String path,
                @Nonnull Set<String> added,
                @Nonnull Set<String> deleted,
                @Nonnull Set<String> changed,
                @Nonnull Map<String, String> properties,
                @Nonnull CommitInfo commitInfo) {
            this.deleted.put(path, newHashSet(deleted));
            if (!properties.isEmpty()) {
                this.properties.put(path, newHashMap(properties));
            }
        }

        @Override
        protected void changed(
                @Nonnull String path,
                @Nonnull Set<String> added,
                @Nonnull Set<String> deleted,
                @Nonnull Set<String> changed,
                @Nonnull Map<String, String> properties,
                @Nonnull CommitInfo commitInfo) {
            this.changed.put(path, newHashSet(changed));
            if (!properties.isEmpty()) {
                this.properties.put(path, newHashMap(properties));
            }
        }

        public void reset(){
            added.clear();
            deleted.clear();
            changed.clear();
            properties.clear();
        }
    }
}
