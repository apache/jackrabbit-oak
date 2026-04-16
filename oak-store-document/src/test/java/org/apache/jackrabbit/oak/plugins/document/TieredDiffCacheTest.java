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

import java.lang.reflect.Field;

import org.apache.jackrabbit.oak.cache.api.CacheStatsAdapter;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link TieredDiffCache}.
 * These assertions intentionally avoid third-party cache types so the same
 * tests can run across cache implementation changes.
 */
public class TieredDiffCacheTest {

    private static final int CLUSTER_ID = 1;

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private TieredDiffCache buildCache() {
        return new TieredDiffCache(builderProvider.newBuilder()
                .setCacheSegmentCount(1)
                .memoryCacheDistribution(0, 0, 0, 99, 0), CLUSTER_ID);
    }

    @Test
    public void getChangesReturnsNullForUncachedRevision() {
        TieredDiffCache cache = buildCache();
        RevisionVector from = new RevisionVector(Revision.newRevision(CLUSTER_ID));
        RevisionVector to = new RevisionVector(Revision.newRevision(CLUSTER_ID));
        assertNull(cache.getChanges(from, to, Path.ROOT, null));
    }

    @Test
    public void newEntryLocalPopulatesLocalCache() {
        TieredDiffCache cache = buildCache();
        RevisionVector from = new RevisionVector(Revision.newRevision(CLUSTER_ID));
        RevisionVector to = new RevisionVector(Revision.newRevision(CLUSTER_ID));
        String rootPathChanges = "^\"foo\":{}";

        DiffCache.Entry entry = cache.newEntry(from, to, true);
        entry.append(Path.ROOT, rootPathChanges);
        entry.done();

        assertEquals(rootPathChanges, getTier(cache, "localCache").getChanges(from, to, Path.ROOT, null));
        assertNull(getTier(cache, "memoryCache").getChanges(from, to, Path.ROOT, null));
        assertEquals(rootPathChanges, cache.getChanges(from, to, Path.ROOT, null));
    }

    @Test
    public void newEntryExternalPopulatesMemoryCache() {
        TieredDiffCache cache = buildCache();
        RevisionVector from = new RevisionVector(Revision.newRevision(2));
        RevisionVector to = new RevisionVector(Revision.newRevision(2));
        String rootPathChanges = "^\"bar\":{}";

        DiffCache.Entry entry = cache.newEntry(from, to, false);
        entry.append(Path.ROOT, rootPathChanges);
        entry.done();

        assertNull(getTier(cache, "localCache").getChanges(from, to, Path.ROOT, null));
        assertEquals(rootPathChanges, getTier(cache, "memoryCache").getChanges(from, to, Path.ROOT, null));
        assertEquals(rootPathChanges, cache.getChanges(from, to, Path.ROOT, null));
    }

    @Test
    public void getStatsReturnsNonEmptyIterable() {
        TieredDiffCache cache = buildCache();
        Iterable<CacheStatsAdapter> stats = cache.getStats();
        assertNotNull(stats);
        assertTrue(stats.iterator().hasNext());
    }

    @Test
    public void invalidateAllClearsCache() {
        TieredDiffCache cache = buildCache();
        RevisionVector localFrom = new RevisionVector(Revision.newRevision(CLUSTER_ID));
        RevisionVector localTo = new RevisionVector(Revision.newRevision(CLUSTER_ID));
        RevisionVector externalFrom = new RevisionVector(Revision.newRevision(2));
        RevisionVector externalTo = new RevisionVector(Revision.newRevision(2));

        DiffCache.Entry localEntry = cache.newEntry(localFrom, localTo, true);
        localEntry.append(Path.ROOT, "^\"local\":{}");
        localEntry.done();

        DiffCache.Entry externalEntry = cache.newEntry(externalFrom, externalTo, false);
        externalEntry.append(Path.ROOT, "^\"external\":{}");
        externalEntry.done();

        assertNotNull(cache.getChanges(localFrom, localTo, Path.ROOT, null));
        assertNotNull(cache.getChanges(externalFrom, externalTo, Path.ROOT, null));
        cache.invalidateAll();
        assertNull(getTier(cache, "localCache").getChanges(localFrom, localTo, Path.ROOT, null));
        assertNull(getTier(cache, "memoryCache").getChanges(externalFrom, externalTo, Path.ROOT, null));
        assertNull(cache.getChanges(localFrom, localTo, Path.ROOT, null));
        assertNull(cache.getChanges(externalFrom, externalTo, Path.ROOT, null));
    }

    private static DiffCache getTier(TieredDiffCache cache, String fieldName) {
        try {
            Field field = TieredDiffCache.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            return (DiffCache) field.get(cache);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Unable to access diff cache tier " + fieldName, e);
        }
    }
}
