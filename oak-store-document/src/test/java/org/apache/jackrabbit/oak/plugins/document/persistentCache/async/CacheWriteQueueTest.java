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
package org.apache.jackrabbit.oak.plugins.document.persistentCache.async;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests guarded writes accepted by {@link CacheWriteQueue}. */
public class CacheWriteQueueTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));
    private PersistentCache cache;

    @Before
    public void createCache() throws Exception {
        cache = new PersistentCache(folder.newFolder().getAbsolutePath());
    }

    @After
    public void close() { cache.close(); }

    private final StringValue key = new StringValue("key");
    private final StringValue value = new StringValue("value");

    /** Retirement cancels a write already accepted by the queue. */
    @Test
    public void retirementCancelsAnAlreadyQueuedWrite() throws Exception {
        CacheActionDispatcher dispatcher = new CacheActionDispatcher();
        Map<StringValue, StringValue> values = new ConcurrentHashMap<>();
        CacheWriteQueue<StringValue, StringValue> queue = new CacheWriteQueue<>(dispatcher, cache, values);
        AtomicBoolean valid = new AtomicBoolean(true);
        ReentrantLock lock = new ReentrantLock();
        Assert.assertTrue(queue.addPut(key, value, valid::get, lock));
        valid.set(false);
        dispatcher.queue.remove().execute();
        Assert.assertTrue(values.isEmpty());
        Assert.assertFalse(lock.isLocked());
    }

    /** Accepted writes serialize the raw value under the invalidation lock. */
    @Test
    public void validWriteUsesTheOriginalValueUnderTheInvalidationLock() throws Exception {
        CacheActionDispatcher dispatcher = new CacheActionDispatcher();
        ReentrantLock lock = new ReentrantLock();
        AtomicBoolean held = new AtomicBoolean();
        Map<StringValue, StringValue> values = new ConcurrentHashMap<StringValue, StringValue>() {
            @Override
            public StringValue put(StringValue key, StringValue value) {
                held.set(lock.isHeldByCurrentThread());
                return super.put(key, value);
            }
        };
        CacheWriteQueue<StringValue, StringValue> queue = new CacheWriteQueue<>(dispatcher, cache, values);
        Assert.assertTrue(queue.addPut(key, value, () -> true, lock));
        dispatcher.queue.remove().execute();
        Assert.assertSame(value, values.get(key));
        Assert.assertTrue(held.get());
        Assert.assertFalse(lock.isLocked());
    }

    /** A disk-write failure must release the invalidation lock. */
    @Test
    public void failedWriteReleasesTheInvalidationLock() throws Exception {
        CacheActionDispatcher dispatcher = new CacheActionDispatcher();
        ReentrantLock lock = new ReentrantLock();
        Map<StringValue, StringValue> values = new ConcurrentHashMap<StringValue, StringValue>() {
            @Override
            public StringValue put(StringValue key, StringValue value) {
                throw new IllegalStateException("write failed");
            }
        };
        CacheWriteQueue<StringValue, StringValue> queue = new CacheWriteQueue<>(dispatcher, cache, values);
        AtomicInteger recorded = new AtomicInteger();
        Assert.assertTrue(queue.addPut(key, value, () -> true, lock, recorded::incrementAndGet));
        try {
            dispatcher.queue.remove().execute();
            Assert.fail("expected write failure");
        } catch (IllegalStateException expected) {
            Assert.assertEquals("write failed", expected.getMessage());
        }
        Assert.assertFalse(lock.isLocked());
        Assert.assertEquals(0, recorded.get());
    }

    /** Rejects a missing completion callback before accepting any action. */
    @Test
    public void guardedWriteRejectsNullCompletionCallback() {
        CacheActionDispatcher dispatcher = new CacheActionDispatcher();
        CacheWriteQueue<StringValue, StringValue> queue = new CacheWriteQueue<>(dispatcher, cache,
                new ConcurrentHashMap<>());
        Assert.assertThrows(NullPointerException.class,
                () -> queue.addPut(key, value, () -> true, new ReentrantLock(), null));
        Assert.assertTrue(dispatcher.queue.isEmpty());
    }

    /** A queue with no persistent map cannot report a completed write. */
    @Test
    public void missingPersistentMapDoesNotReportSuccess() {
        CacheActionDispatcher dispatcher = new CacheActionDispatcher();
        CacheWriteQueue<StringValue, StringValue> queue = new CacheWriteQueue<>(dispatcher, cache, null);
        AtomicInteger recorded = new AtomicInteger();
        Assert.assertTrue(queue.addPut(key, value, () -> true, new ReentrantLock(), recorded::incrementAndGet));
        dispatcher.queue.remove().execute();
        Assert.assertEquals(0, recorded.get());
    }

    /** Guarded-write support preserves the original queue operations. */
    @Test
    public void legacyWritesAndInvalidationsRemainCompatible() {
        CacheActionDispatcher dispatcher = new CacheActionDispatcher();
        Map<StringValue, StringValue> values = new ConcurrentHashMap<>();
        CacheWriteQueue<StringValue, StringValue> queue = new CacheWriteQueue<>(dispatcher, cache, values);
        Assert.assertTrue(queue.addPut(key, value));
        dispatcher.queue.remove().execute();
        Assert.assertSame(value, values.get(key));
        Assert.assertTrue(queue.addInvalidate(Collections.singleton(key)));
        dispatcher.queue.remove().execute();
        Assert.assertTrue(values.isEmpty());
    }

    /** Guarded writes obey the same bounded queue budget. */
    @Test
    public void guardedWriteHonorsTheQueueMemoryLimit() throws Exception {
        CacheActionDispatcher dispatcher = new CacheActionDispatcher(0);
        CacheWriteQueue<StringValue, StringValue> queue = new CacheWriteQueue<>(dispatcher, cache,
                new ConcurrentHashMap<>());
        Assert.assertFalse(queue.addPut(key, value, () -> true, new ReentrantLock()));
        Assert.assertTrue(dispatcher.queue.isEmpty());
    }
}
