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

import java.lang.ref.WeakReference;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Tests the raw cache and live map contracts of {@link CacheEntries}. */
@RunWith(Parameterized.class)
public class CacheEntriesTest {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> policies() {
        return Arrays.asList(new Object[][] {{"LIRS"}, {"SYNC"}, {"ASYNC"}});
    }

    private final String policy;
    private Cache<StringValue, CacheEntry<StringValue>> memory;
    private CacheEntries<StringValue, StringValue> cache;
    private final StringValue key = new StringValue("key");
    private final StringValue first = new StringValue("first");
    private final StringValue second = new StringValue("second");

    public CacheEntriesTest(String policy) {
        this.policy = policy;
    }

    @Before
    public void createCache() {
        memory = policy.equals("LIRS")
                ? CacheLIRS.<StringValue, CacheEntry<StringValue>>newBuilder().maximumSize(100).build().asOakCache()
                : CacheBuilder.<StringValue, CacheEntry<StringValue>>newBuilder().maximumSize(100)
                        .maintenanceMode(CacheBuilder.MaintenanceMode.valueOf(policy)).recordStats().build();
        cache = new CacheEntries<>(memory);
    }

    /** A replacement state must retain the same canonical key as the weak registry. */
    @Test
    @SuppressWarnings("unchecked")
    public void replacingExpiredStateReplacesItsWeakKey() throws Exception {
        CacheEntry.KeyState<StringValue> original = cache.keyState(key);
        Field field = CacheEntries.class.getDeclaredField("keyStates");
        field.setAccessible(true);
        Map<StringValue, WeakReference<CacheEntry.KeyState<StringValue>>> states =
                (Map<StringValue, WeakReference<CacheEntry.KeyState<StringValue>>>) field.get(cache);
        states.get(key).clear();
        StringValue equalKey = new StringValue("key");
        CacheEntry.KeyState<StringValue> replacement = cache.keyState(equalKey);
        Assert.assertNotSame(original, replacement);
        Assert.assertSame(equalKey, replacement.key);
        Assert.assertSame("The live state must keep the weak registry key reachable",
                equalKey, states.keySet().iterator().next());
        Assert.assertSame(replacement, cache.keyState(new StringValue("key")));
    }

    /** Existing-entry mutations retain their canonical monitor without taking the registry lock. */
    @Test
    public void existingEntryReplacementDoesNotWaitForRegistry() throws Exception {
        cache.put(key, first);
        CacheEntry<StringValue> original = memory.asMap().get(key);
        Field field = CacheEntries.class.getDeclaredField("keyStates");
        field.setAccessible(true);
        Object states = field.get(cache);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            synchronized (states) {
                Future<?> replacement = executor.submit(() -> cache.put(new StringValue("key"), second));
                replacement.get(5, TimeUnit.SECONDS);
            }
            Assert.assertTrue(original.isRetired());
            Assert.assertSame(original.getKeyState(), memory.asMap().get(key).getKeyState());
            Assert.assertSame(second, cache.getIfPresent(key));
        } finally {
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /** A replacement starts with its own access count. */
    @Test
    public void accessesBelongToTheirEntry() {
        cache.put(key, first);
        CacheEntry<StringValue> old = memory.asMap().get(key);
        Assert.assertEquals(0, old.getAccessCount());
        Assert.assertSame(first, cache.getIfPresent(key));
        Assert.assertEquals(1, old.getAccessCount());
        cache.put(key, second);
        Assert.assertTrue(old.isRetired());
        CacheEntry<StringValue> current = memory.asMap().get(key);
        Assert.assertEquals(0, current.getAccessCount());
        Assert.assertSame(second, cache.get(key, k -> { throw new AssertionError("unexpected load"); }));
        Assert.assertEquals(1, current.getAccessCount());
    }

    /** Replacing a disk-loaded value resets its persistence origin. */
    @Test
    public void persistentOriginDoesNotFollowAReplacement() {
        cache.putFromPersistence(key, first, true);
        CacheEntry<StringValue> persistent = memory.asMap().get(key);
        Assert.assertTrue(persistent.isFromPersistence());
        Assert.assertEquals(1, persistent.getAccessCount());
        cache.put(key, first);
        Assert.assertTrue(persistent.isRetired());
        CacheEntry<StringValue> inserted = memory.asMap().get(key);
        Assert.assertNotSame(persistent, inserted);
        Assert.assertFalse(inserted.isFromPersistence());
        Assert.assertEquals(0, inserted.getAccessCount());
    }

    /** Bulk reads expose raw values and count only present entries. */
    @Test
    public void bulkReadsIgnoreMissesAndReturnRawValues() {
        cache.put(key, first);
        Map<StringValue, StringValue> result = cache.getAllPresent(Arrays.asList(key, new StringValue("absent")));
        Assert.assertEquals(1, result.size());
        Assert.assertSame(first, result.get(key));
        Assert.assertEquals(1, memory.asMap().get(key).getAccessCount());
    }

    /** Failed or null loads leave no entry behind. */
    @Test
    public void nullAndFailedLoadsDoNotCreateEntries() {
        if (policy.equals("LIRS")) {
            try {
                cache.get(key, k -> null);
                Assert.fail("CacheLIRS rejects null loader results");
            } catch (NullPointerException expected) {
                Assert.assertTrue(cache.asMap().isEmpty());
            }
        } else {
            Assert.assertNull(cache.get(key, k -> null));
        }
        Assert.assertTrue(cache.asMap().isEmpty());
        RuntimeException failure = new IllegalStateException("load failed");
        try {
            cache.get(key, k -> { throw failure; });
            Assert.fail("expected failure");
        } catch (RuntimeException thrown) {
            Assert.assertSame(failure, thrown);
        }
        Assert.assertTrue(cache.asMap().isEmpty());
        Assert.assertSame(first, cache.get(key, k -> first));
        Assert.assertEquals(1, memory.asMap().get(key).getAccessCount());
    }

    /** A rejected put must leave the current entry eligible. */
    @Test
    public void rejectedNullPutDoesNotRetireTheExistingEntry() {
        cache.put(key, first);
        CacheEntry<StringValue> entry = memory.asMap().get(key);
        try {
            cache.put(key, null);
            Assert.fail("expected null validation");
        } catch (NullPointerException expected) {
            Assert.assertFalse(entry.isRetired());
        }
        Assert.assertSame(first, cache.getIfPresent(key));
    }

    /** Map updates publish values and retire replaced entries. */
    @Test
    public void mapInsertionAndReplacementAreLive() {
        ConcurrentMap<StringValue, StringValue> values = cache.asMap();
        Assert.assertNull(values.putIfAbsent(key, first));
        CacheEntry<StringValue> initial = memory.asMap().get(key);
        Assert.assertSame(first, values.putIfAbsent(key, second));
        Assert.assertFalse(initial.isRetired());
        Assert.assertFalse(values.replace(key, second, first));
        Assert.assertTrue(values.replace(key, first, second));
        Assert.assertTrue(initial.isRetired());
        Assert.assertSame(second, values.replace(key, first));
        Assert.assertSame(first, cache.getIfPresent(key));
        Assert.assertNull(values.replace(new StringValue("absent"), first));
    }

    /** Conditional removal must preserve a nonmatching entry. */
    @Test
    public void mapRemovalsCancelOnlyMatchingEntries() {
        cache.put(key, first);
        CacheEntry<StringValue> initial = memory.asMap().get(key);
        Assert.assertFalse(cache.asMap().remove(key, second));
        Assert.assertFalse(initial.isRetired());
        Assert.assertTrue(cache.asMap().remove(key, first));
        Assert.assertTrue(initial.isRetired());
        Assert.assertNull(cache.getIfPresent(key));
        cache.put(key, second);
        Assert.assertSame(second, cache.asMap().remove(key));
        Assert.assertNull(cache.asMap().remove(key));
    }

    /** No-op computations preserve metadata; replacements reset it. */
    @Test
    public void mapComputationsPreserveNoopMetadataAndPublishRawValues() {
        cache.putFromPersistence(key, first, true);
        CacheEntry<StringValue> initial = memory.asMap().get(key);
        Assert.assertSame(first, cache.asMap().compute(key, (k, v) -> v));
        Assert.assertSame(initial, memory.asMap().get(key));
        Assert.assertFalse(initial.isRetired());
        Assert.assertSame(second, cache.asMap().compute(key, (k, v) -> second));
        Assert.assertTrue(initial.isRetired());
        Assert.assertSame(second, cache.getIfPresent(key));
        Assert.assertNull(cache.asMap().computeIfPresent(key, (k, v) -> null));
        Assert.assertTrue(cache.asMap().isEmpty());
        AtomicInteger loads = new AtomicInteger();
        Assert.assertSame(first, cache.asMap().computeIfAbsent(key, k -> { loads.incrementAndGet(); return first; }));
        Assert.assertSame(first, cache.asMap().computeIfAbsent(key, k -> { loads.incrementAndGet(); return second; }));
        Assert.assertEquals(1, loads.get());
        Assert.assertSame(second, cache.asMap().merge(key, second, (old, added) -> added));
        Assert.assertNull(cache.asMap().merge(key, first, (old, added) -> null));
        Assert.assertTrue(cache.asMap().isEmpty());
    }

    /** Mutating map views updates the cache and retires old entries. */
    @Test
    public void iteratorAndCollectionViewsSupportMutation() {
        cache.put(key, first);
        Iterator<Map.Entry<StringValue, StringValue>> iterator = cache.asMap().entrySet().iterator();
        Map.Entry<StringValue, StringValue> entry = iterator.next();
        Assert.assertSame(first, entry.getValue());
        Assert.assertSame(first, entry.setValue(second));
        Assert.assertSame(second, cache.getIfPresent(key));
        cache.asMap().values().remove(second);
        Assert.assertTrue(cache.asMap().isEmpty());
        cache.put(key, first);
        iterator = cache.asMap().entrySet().iterator();
        iterator.next();
        iterator.remove();
        Assert.assertTrue(cache.asMap().isEmpty());
        cache.put(key, first);
        cache.asMap().keySet().remove(key);
        Assert.assertTrue(cache.asMap().isEmpty());
    }

    /** A retained map view remains usable after cache clear. */
    @Test
    public void clearingDoesNotBreakTheLiveMap() {
        ConcurrentMap<StringValue, StringValue> view = cache.asMap();
        cache.put(key, first);
        CacheEntry<StringValue> old = memory.asMap().get(key);
        view.clear();
        Assert.assertTrue(old.isRetired());
        Assert.assertTrue(view.isEmpty());
        view.put(key, second);
        Assert.assertSame(second, cache.getIfPresent(key));
        cache.invalidateAll(Arrays.asList(key));
        Assert.assertTrue(view.isEmpty());
    }

    /** Empty views preserve the cache API contracts. */
    @Test
    public void emptyMapOperationsAndDelegatedCacheServicesRemainUsable() {
        ConcurrentMap<StringValue, StringValue> view = cache.asMap();
        Assert.assertFalse(view.containsKey(key));
        Assert.assertFalse(view.remove(key, first));
        Assert.assertFalse(view.replace(key, first, second));
        Assert.assertNull(view.computeIfPresent(key, (k, v) -> { throw new AssertionError("absent"); }));
        Assert.assertNull(view.computeIfAbsent(key, k -> null));
        Assert.assertThrows(IllegalStateException.class, () -> view.entrySet().iterator().remove());
        view.put(key, first);
        Assert.assertTrue(view.containsKey(key));
        Assert.assertEquals(1, view.entrySet().size());
        Assert.assertEquals(1, cache.estimatedSize());
        Assert.assertNotNull(cache.stats());
        cache.cleanUp();
        Assert.assertTrue(cache.getUsedWeight() > 0);
        cache.setMaximumWeight(100);
    }

    /** Cleanup preserves the metadata of retained entries. */
    @Test
    public void cleanupRetainsMetadataForSurvivingEntries() {
        cache.putFromPersistence(key, first, true);
        CacheEntry<StringValue> entry = memory.asMap().get(key);
        cache.cleanUp();
        Assert.assertSame(first, cache.getIfPresent(key));
        Assert.assertSame(entry, memory.asMap().get(key));
        Assert.assertTrue(entry.isFromPersistence());
        Assert.assertFalse(entry.isRetired());
        Assert.assertEquals(first.getMemory() + CacheEntry.MEMORY_OVERHEAD, entry.getMemory());
    }
}
