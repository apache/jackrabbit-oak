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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.cache.CacheValue;

/** A value and its persistence eligibility, owned by one memory-cache entry. */
public final class CacheEntry<V extends CacheValue> implements CacheValue {

    public static final int MEMORY_OVERHEAD = 96;

    private final V value;
    // Keeps the weak index's canonical key and mutation monitor alive with this entry.
    private final KeyState<?> keyState;
    final long clearGeneration;
    private final AtomicLong accessCount;
    private final boolean fromPersistence;
    private volatile boolean retired;
    boolean evictionQueued;

    CacheEntry(V value, KeyState<?> keyState, long clearGeneration, boolean fromPersistence, boolean accessed) {
        this.value = Objects.requireNonNull(value);
        this.keyState = keyState;
        this.clearGeneration = clearGeneration;
        this.fromPersistence = fromPersistence;
        this.accessCount = new AtomicLong(accessed ? 1 : 0);
    }

    /** @return the original value exposed to cache callers and persistent serialization */
    public V getValue() {
        return value;
    }

    @Override
    public int getMemory() {
        return (int) Math.min(Integer.MAX_VALUE, (long) value.getMemory() + MEMORY_OVERHEAD);
    }

    KeyState<?> getKeyState() {
        return keyState;
    }

    void accessed() {
        accessCount.incrementAndGet();
    }

    long getAccessCount() {
        return accessCount.get();
    }

    boolean isFromPersistence() {
        return fromPersistence;
    }

    void retire() {
        retired = true;
    }

    boolean isRetired() {
        return retired;
    }

    /** Weakly indexed mutation monitor; it never owns a cached value strongly. */
    static final class KeyState<K> {
        final K key;
        volatile WeakReference<CacheEntry<?>> latest = new WeakReference<>(null);

        KeyState(K key) {
            this.key = key;
        }

        void retireLatest() {
            CacheEntry<?> entry = latest.get();
            if (entry != null) {
                entry.retire();
            }
        }
    }
}
