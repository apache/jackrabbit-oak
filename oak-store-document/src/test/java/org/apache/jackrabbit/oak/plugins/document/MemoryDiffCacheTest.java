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

import java.util.UUID;

import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder.MaintenanceMode;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheType;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MemoryDiffCacheTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void limit() throws Exception {
        DiffCache cache = new MemoryDiffCache(builderProvider.newBuilder()
                .setCacheSegmentCount(1)
                .memoryCacheDistribution(0, 0, 0, 99, 0));
        RevisionVector from = new RevisionVector(Revision.newRevision(1));
        RevisionVector to = new RevisionVector(Revision.newRevision(1));
        DiffCache.Entry entry = cache.newEntry(from, to, false);
        entry.append(Path.ROOT, "^\"foo\":{}");
        entry.append(Path.fromString("/foo"), changes(MemoryDiffCache.CACHE_VALUE_LIMIT));
        entry.done();
        assertNotNull(cache.getChanges(from, to, Path.ROOT, null));
        assertNull(cache.getChanges(from, to, Path.fromString("/foo"), null));
    }

    @Test
    public void invalidateAllClearsAllCachedEntries() {
        DiffCache cache = new MemoryDiffCache(builderProvider.newBuilder()
                .setCacheSegmentCount(1)
                .memoryCacheDistribution(0, 0, 0, 99, 0));
        RevisionVector from = new RevisionVector(Revision.newRevision(1));
        RevisionVector to = new RevisionVector(Revision.newRevision(1));
        DiffCache.Entry entry = cache.newEntry(from, to, false);
        entry.append(Path.ROOT, "^\"foo\":{}");
        entry.done();

        assertNotNull(cache.getChanges(from, to, Path.ROOT, null));
        cache.invalidateAll();
        assertNull(cache.getChanges(from, to, Path.ROOT, null));
    }

    @Test
    public void getStatsReturnsNonEmptyIterable() {
        DiffCache cache = new MemoryDiffCache(builderProvider.newBuilder()
                .setCacheSegmentCount(1)
                .memoryCacheDistribution(0, 0, 0, 99, 0));
        Iterable<AbstractCacheStats> statsIterable = cache.getStats();
        assertNotNull(statsIterable);
        assertTrue(statsIterable.iterator().hasNext());
    }

    @Test
    public void getChangesReturnsNullForUncachedRevisions() {
        DiffCache cache = new MemoryDiffCache(builderProvider.newBuilder()
                .setCacheSegmentCount(1)
                .memoryCacheDistribution(0, 0, 0, 99, 0));
        RevisionVector from = new RevisionVector(Revision.newRevision(1));
        RevisionVector to = new RevisionVector(Revision.newRevision(1));
        assertNull(cache.getChanges(from, to, Path.ROOT, null));
    }

    @Test
    public void doneMakesRootPathChangesReadableFromCache() {
        DiffCache cache = new MemoryDiffCache(builderProvider.newBuilder()
                .setCacheSegmentCount(1)
                .memoryCacheDistribution(0, 0, 0, 99, 0));
        RevisionVector from = new RevisionVector(Revision.newRevision(1));
        RevisionVector to = new RevisionVector(Revision.newRevision(1));
        String rootPathChanges = "^\"foo\":{}";

        DiffCache.Entry entry = cache.newEntry(from, to, false);
        entry.append(Path.ROOT, rootPathChanges);
        entry.done();

        String actualChanges = cache.getChanges(from, to, Path.ROOT, null);
        assertNotNull(actualChanges);
        assertEquals(rootPathChanges, actualChanges);
    }

    /** With the ASYNC feature off, a recursive loader preserves the inner cached diff. */
    @Test
    public void synchronousRecursiveLoaderPreservesTheInnerDiff() {
        boolean previousCaffeine = DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.get();
        boolean previousAsync = DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.getAndSet(false);
        try {
            for (boolean caffeine : new boolean[] {false, true}) {
                DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(caffeine);
                DocumentNodeStoreBuilder<?> builder = builderProvider.newBuilder()
                        .setPersistentCache(null).setCacheMaintenanceMode(CacheType.DIFF, MaintenanceMode.ASYNC);
                assertEquals(MaintenanceMode.SYNC, builder.getCacheMaintenanceMode(CacheType.DIFF));
                DiffCache cache = new MemoryDiffCache(builder);
                RevisionVector from = new RevisionVector(new Revision(1, 0, 1));
                RevisionVector to = new RevisionVector(new Revision(2, 0, 1));
                String inner = "^\"inner\":{}";
                String outer = "^\"outer\":{}";
                assertEquals(outer, cache.getChanges(from, to, Path.ROOT, () -> {
                    assertEquals(inner, cache.getChanges(from, to, Path.ROOT, () -> inner));
                    return outer;
                }));
                assertEquals(inner, cache.getChanges(from, to, Path.ROOT, null));
            }
        } finally {
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(previousCaffeine);
            DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(previousAsync);
        }
    }

    private static String changes(int minLength) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < minLength) {
            sb.append("^\"").append(UUID.randomUUID()).append("\":{}");
        }
        return sb.toString();
    }
}
