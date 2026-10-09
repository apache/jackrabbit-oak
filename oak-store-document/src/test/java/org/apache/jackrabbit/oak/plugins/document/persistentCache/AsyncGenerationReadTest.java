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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.EvictionCause;
import org.apache.jackrabbit.oak.plugins.document.MemoryDiffCache;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.async.CacheWriteQueue;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.async.TestCacheActionDispatcher;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/** Tests generation handoff in {@link AsyncNodeCache}. */
public class AsyncGenerationReadTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    /** A full or rotating generation must remain eligible for migration on eviction. */
    @Test
    @SuppressWarnings("unchecked")
    public void generationRotationDuringReadKeepsTheValueEligibleForMigration() throws Exception {
        PersistentCache persistence = Mockito.spy(new PersistentCache(folder.newFolder().getAbsolutePath() + ",+asyncDiff"));
        try {
            Cache<MemoryDiffCache.Key, CacheEntry<StringValue>> memory = CacheBuilder
                    .<MemoryDiffCache.Key, CacheEntry<StringValue>>newBuilder().maximumSize(100)
                    .maintenanceMode(CacheBuilder.MaintenanceMode.ASYNC).build();
            AsyncNodeCache<MemoryDiffCache.Key, StringValue> cache = (AsyncNodeCache<MemoryDiffCache.Key, StringValue>)
                    persistence.wrapAsyncMaintenance(null, null, memory, CacheType.DIFF);
            TestCacheActionDispatcher dispatcher = new TestCacheActionDispatcher();
            cache.writeQueue = new CacheWriteQueue<>(dispatcher, persistence, writableMap(cache));
            MemoryDiffCache.Key key = new MemoryDiffCache.Key(Path.ROOT,
                    new RevisionVector(new Revision(1, 0, 1)), new RevisionVector(new Revision(2, 0, 1)));
            StringValue value = new StringValue("diff");
            cache.get(key, k -> value);
            cache.evicted(key, memory.asMap().get(key), EvictionCause.SIZE);
            dispatcher.executeAll();
            memory.invalidate(key);

            Mockito.doReturn(true).when(persistence).needSwitch();
            Assert.assertEquals(value, cache.getIfPresent(key));
            Assert.assertFalse(memory.asMap().get(key).isFromPersistence());
            memory.invalidate(key);

            AtomicBoolean rotate = new AtomicBoolean(true);
            Mockito.doAnswer(invocation -> {
                if (rotate.getAndSet(false)) {
                    rotateGeneration(persistence);
                }
                return false;
            }).when(persistence).needSwitch();

            Assert.assertEquals(value, cache.getIfPresent(key));
            CacheEntry<StringValue> reloaded = memory.asMap().get(key);
            Assert.assertFalse("The value is now in the old generation and must be rewritten", reloaded.isFromPersistence());
            cache.evicted(key, reloaded, EvictionCause.SIZE);
            dispatcher.executeAll();
            memory.invalidate(key);
            rotateGeneration(persistence);
            Assert.assertEquals("Rotation must retain the migrated persistent value", value, cache.getIfPresent(key));
        } finally {
            persistence.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<MemoryDiffCache.Key, StringValue> writableMap(
            AsyncNodeCache<MemoryDiffCache.Key, StringValue> cache) throws Exception {
        Field field = AsyncNodeCache.class.getDeclaredField("map");
        field.setAccessible(true);
        return (Map<MemoryDiffCache.Key, StringValue>) field.get(cache);
    }

    private static void rotateGeneration(PersistentCache cache) {
        Mockito.doReturn(true, true, false).when(cache).needSwitch();
        cache.switchGenerationIfNeeded();
        Mockito.doReturn(false).when(cache).needSwitch();
    }
}
