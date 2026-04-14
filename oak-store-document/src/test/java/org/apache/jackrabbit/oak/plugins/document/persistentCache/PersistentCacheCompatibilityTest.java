/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.plugins.document.MemoryDiffCache;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

/**
 * Compatibility tests for the persistent cache wrapping layer.
 * These assertions stay on Oak-visible behavior and intentionally avoid direct
 * use of third-party cache APIs in the test code.
 */
public class PersistentCacheCompatibilityTest {

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

    @Test
    public void wrapReturnsUsablePersistentDiffCache() throws Exception {
        // Wrapping a DIFF cache should return a usable cache handle whose
        // observable put/get behavior matches the in-memory base contract.
        CacheHandle handle = openDiffCache("wrap");
        try {
            MemoryDiffCache.Key key = key(0);
            StringValue value = new StringValue("value");
            assertNotNull(handle.cache);
            handle.cache.put(key, value);
            assertEquals(value, handle.cache.getIfPresent(key));
        } finally {
            handle.close();
        }
    }

    @Test
    public void invalidateRemovesOnlyTheRequestedPersistedEntry() throws Exception {
        // Persist two keys, invalidate only one after reopening, then reopen again
        // to prove the removal was durable and did not affect the sibling entry.
        MemoryDiffCache.Key first = key(1);
        MemoryDiffCache.Key second = key(2);

        CacheHandle initial = openDiffCache("invalidateOne");
        try {
            initial.cache.put(first, new StringValue("first"));
            initial.cache.put(second, new StringValue("second"));
        } finally {
            initial.close();
        }

        CacheHandle reopened = openDiffCache("invalidateOne");
        try {
            assertEquals(new StringValue("first"), reopened.cache.getIfPresent(first));
            assertEquals(new StringValue("second"), reopened.cache.getIfPresent(second));
            reopened.cache.invalidate(first);
        } finally {
            reopened.close();
        }

        CacheHandle afterInvalidate = openDiffCache("invalidateOne");
        try {
            assertNull(afterInvalidate.cache.getIfPresent(first));
            assertEquals(new StringValue("second"), afterInvalidate.cache.getIfPresent(second));
        } finally {
            afterInvalidate.close();
        }
    }

    @Test
    public void invalidateAllClearsPersistedEntriesAcrossReopen() throws Exception {
        // Persist entries, clear the wrapped cache, and reopen the persistent layer
        // to verify invalidateAll() removes the durable state as well.
        MemoryDiffCache.Key first = key(1);
        MemoryDiffCache.Key second = key(2);

        CacheHandle initial = openDiffCache("invalidateAll");
        try {
            initial.cache.put(first, new StringValue("first"));
            initial.cache.put(second, new StringValue("second"));
        } finally {
            initial.close();
        }

        CacheHandle reopened = openDiffCache("invalidateAll");
        try {
            assertEquals(new StringValue("first"), reopened.cache.getIfPresent(first));
            assertEquals(new StringValue("second"), reopened.cache.getIfPresent(second));
            reopened.cache.invalidateAll();
        } finally {
            reopened.close();
        }

        CacheHandle afterInvalidate = openDiffCache("invalidateAll");
        try {
            assertNull(afterInvalidate.cache.getIfPresent(first));
            assertNull(afterInvalidate.cache.getIfPresent(second));
        } finally {
            afterInvalidate.close();
        }
    }

    @Test
    public void getWithFunctionPropagatesRuntimeException() throws Exception {
        CacheHandle handle = openDiffCache("loaderFailure");
        RuntimeException failure = new RuntimeException("simulated persistent-cache load failure");

        try {
            handle.cache.get(key(7), k -> {
                throw failure;
            });
            fail("expected RuntimeException");
        } catch (RuntimeException e) {
            assertSame(failure, e);
        } finally {
            handle.close();
        }
    }

    private CacheHandle openDiffCache(String name) throws Exception {
        File directory = new File(tempFolder.getRoot(), name);
        directory.mkdirs();
        PersistentCache persistentCache = new PersistentCache(directory.getAbsolutePath() + ",-async");
        CacheLIRS<MemoryDiffCache.Key, StringValue> base = CacheLIRS.<MemoryDiffCache.Key, StringValue>newBuilder()
                .maximumSize(16)
                .build();
        NodeCache<MemoryDiffCache.Key, StringValue> wrapped = (NodeCache<MemoryDiffCache.Key, StringValue>) persistentCache.wrap(
                null, null, base.asOakCache(), CacheType.DIFF);
        return new CacheHandle(persistentCache, wrapped);
    }

    private static MemoryDiffCache.Key key(int id) {
        RevisionVector from = new RevisionVector(new Revision(0, 0, id));
        RevisionVector to = new RevisionVector(new Revision(1, 0, id));
        return new MemoryDiffCache.Key(Path.fromString("/node-" + id), from, to);
    }

    private static final class CacheHandle {
        private final PersistentCache persistentCache;
        private final NodeCache<MemoryDiffCache.Key, StringValue> cache;

        private CacheHandle(PersistentCache persistentCache,
                            NodeCache<MemoryDiffCache.Key, StringValue> cache) {
            this.persistentCache = persistentCache;
            this.cache = cache;
        }

        private void close() {
            persistentCache.close();
        }
    }
}
