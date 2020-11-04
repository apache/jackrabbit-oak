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

import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * DocumentStore wrapper which can measure time spent in any call.
 */
public class MeasuringDocumentStore implements DocumentStore {

    private final DocumentStore delegate;
    private volatile long measurement = 0;

    public MeasuringDocumentStore(DocumentStore delegate) {
        this.delegate = delegate;
    }

    public void resetMeasurement() {
        measurement = 0;
    }

    public long getMeasurement() {
        return measurement;
    }

    public long getAndResetMeasurement() {
        final long result = measurement;
        measurement = 0;
        return result;
    }

    private static long now() {
        return System.currentTimeMillis();
    }

    private void done(long start) {
        measurement += (System.currentTimeMillis() - start);
    }

    @Override
    public <T extends Document> @Nullable T find(Collection<T> collection, String key) throws DocumentStoreException {
        final long start = now();
        try {
            return delegate.find(collection, key);
        } finally {
            done(start);
        }
    }

    @Override
    public <T extends Document> @Nullable T find(Collection<T> collection, String key, int maxCacheAge) throws DocumentStoreException {
        final long start = now();
        try {
            return delegate.find(collection, key, maxCacheAge);
        } finally {
            done(start);
        }
    }

    @Override
    public <T extends Document> @NotNull List<T> query(Collection<T> collection, String fromKey, String toKey, int limit)
            throws DocumentStoreException {
        final long start = now();
        try {
            return delegate.query(collection, fromKey, toKey, limit);
        } finally {
            done(start);
        }
    }

    @Override
    public <T extends Document> @NotNull List<T> query(Collection<T> collection, String fromKey, String toKey, String indexedProperty,
            long startValue, int limit) throws DocumentStoreException {
        final long start = now();
        try {
            return delegate.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
        } finally {
            done(start);
        }
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) throws DocumentStoreException {
        final long start = now();
        try {
            delegate.remove(collection, key);
        } finally {
            done(start);
        }
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) throws DocumentStoreException {
        final long start = now();
        try {
            delegate.remove(collection, keys);
        } finally {
            done(start);
        }
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection, Map<String, Long> toRemove) throws DocumentStoreException {
        final long start = now();
        try {
            return delegate.remove(collection, toRemove);
        } finally {
            done(start);
        }
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection, String indexedProperty, long startValue, long endValue)
            throws DocumentStoreException {
        final long start = now();
        try {
            return delegate.remove(collection, indexedProperty, startValue, endValue);
        } finally {
            done(start);
        }
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps)
            throws IllegalArgumentException, DocumentStoreException {
        final long start = now();
        try {
            return delegate.create(collection, updateOps);
        } finally {
            done(start);
        }
    }

    @Override
    public <T extends Document> @Nullable T createOrUpdate(Collection<T> collection, UpdateOp update)
            throws IllegalArgumentException, DocumentStoreException {
        final long start = now();
        try {
            return delegate.createOrUpdate(collection, update);
        } finally {
            done(start);
        }
    }

    @Override
    public <T extends Document> List<T> createOrUpdate(Collection<T> collection, List<UpdateOp> updateOps) throws DocumentStoreException {
        final long start = now();
        try {
            return delegate.createOrUpdate(collection, updateOps);
        } finally {
            done(start);
        }
    }

    @Override
    public <T extends Document> @Nullable T findAndUpdate(Collection<T> collection, UpdateOp update) throws DocumentStoreException {
        final long start = now();
        try {
            return delegate.findAndUpdate(collection, update);
        } finally {
            done(start);
        }
    }

    @Override
    public @Nullable CacheInvalidationStats invalidateCache() {
        final long start = now();
        try {
            return delegate.invalidateCache();
        } finally {
            done(start);
        }
    }

    @Override
    public @Nullable CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        final long start = now();
        try {
            return delegate.invalidateCache(keys);
        } finally {
            done(start);
        }
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String key) {
        final long start = now();
        try {
            delegate.invalidateCache(collection, key);
        } finally {
            done(start);
        }
    }

    @Override
    public void dispose() {
        final long start = now();
        try {
            delegate.dispose();
        } finally {
            done(start);
        }
    }

    @Override
    public <T extends Document> @Nullable T getIfCached(Collection<T> collection, String key) {
        final long start = now();
        try {
            return delegate.getIfCached(collection, key);
        } finally {
            done(start);
        }
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {
        final long start = now();
        try {
            delegate.setReadWriteMode(readWriteMode);
        } finally {
            done(start);
        }
    }

    @Override
    public @Nullable Iterable<CacheStats> getCacheStats() {
        final long start = now();
        try {
            return delegate.getCacheStats();
        } finally {
            done(start);
        }
    }

    @Override
    public Map<String, String> getMetadata() {
        final long start = now();
        try {
            return delegate.getMetadata();
        } finally {
            done(start);
        }
    }

    @Override
    public @NotNull Map<String, String> getStats() {
        final long start = now();
        try {
            return delegate.getStats();
        } finally {
            done(start);
        }
    }

    @Override
    public long determineServerTimeDifferenceMillis() throws UnsupportedOperationException, DocumentStoreException {
        final long start = now();
        try {
            return delegate.determineServerTimeDifferenceMillis();
        } finally {
            done(start);
        }
    }

}
