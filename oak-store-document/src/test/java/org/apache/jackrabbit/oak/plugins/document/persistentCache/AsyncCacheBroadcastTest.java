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
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.plugins.document.MemoryDiffCache;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Cross-wrapper compatibility of broadcast values and invalidations. */
public class AsyncCacheBroadcastTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    /** Both wrappers exchange raw broadcasts and invalidations. */
    @Test
    public void entryMetadataAndLegacyCachesExchangeValuesAndInvalidations() throws Exception {
        MemoryDiffCache.Key key = new MemoryDiffCache.Key(Path.ROOT,
                new RevisionVector(new Revision(1, 0, 1)), new RevisionVector(new Revision(2, 0, 1)));
        for (boolean asyncWrites : new boolean[] {false, true}) {
            String options = ",broadcast=inMemory" + (asyncWrites ? ",+asyncDiff" : "");
            PersistentCache first = new PersistentCache(folder.newFolder().getAbsolutePath() + options);
            PersistentCache second = new PersistentCache(folder.newFolder().getAbsolutePath() + options);
            try {
                Cache<CacheValue, StringValue> entryCache = first.wrapAsyncMaintenance(null, null,
                        CacheBuilder.<CacheValue, CacheEntry<StringValue>>newBuilder().maximumSize(100)
                                .maintenanceMode(CacheBuilder.MaintenanceMode.ASYNC).build(), CacheType.DIFF);
                Cache<CacheValue, StringValue> legacyCache = second.wrap(null, null,
                        CacheBuilder.<CacheValue, StringValue>newBuilder().maximumSize(100)
                                .maintenanceMode(CacheBuilder.MaintenanceMode.SYNC).build(), CacheType.DIFF);
                StringValue original = new StringValue("original");
                StringValue replacement = new StringValue("replacement");
                entryCache.put(key, original);
                awaitValue(legacyCache, key, original);
                legacyCache.put(key, replacement);
                awaitValue(entryCache, key, replacement);
                entryCache.invalidate(key);
                awaitValue(legacyCache, key, null);
                legacyCache.put(key, original);
                awaitValue(entryCache, key, original);
                legacyCache.invalidate(key);
                awaitValue(entryCache, key, null);
            } finally {
                second.close();
                first.close();
            }
        }
    }

    private void awaitValue(Cache<CacheValue, StringValue> cache, CacheValue key, StringValue expected)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        StringValue actual;
        do {
            actual = cache.getIfPresent(key);
            if (expected == null ? actual == null : expected.equals(actual)) {
                return;
            }
            Thread.sleep(10);
        } while (System.nanoTime() < deadline);
        Assert.assertEquals(expected, actual);
    }
}
