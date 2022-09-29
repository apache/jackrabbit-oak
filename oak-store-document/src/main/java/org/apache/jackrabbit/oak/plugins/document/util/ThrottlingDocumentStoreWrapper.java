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
package org.apache.jackrabbit.oak.plugins.document.util;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.Throttler;
import org.apache.jackrabbit.oak.plugins.document.ThrottlingStatsCollector;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static java.util.Objects.isNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Wrapper of another DocumentStore that does a throttling check on any method
 * invocation (create, update or delete) and throttled the system if under high load.
 */
public class ThrottlingDocumentStoreWrapper implements DocumentStore {

    private static final Logger LOG = getLogger(ThrottlingDocumentStoreWrapper.class);
    private volatile int lastLogTime = 0;

    @NotNull
    private final DocumentStore store;
    @NotNull
    private final ThrottlingStatsCollector throttlingStatsCollector;

    public ThrottlingDocumentStoreWrapper(final @NotNull DocumentStore store,
                                          final @NotNull ThrottlingStatsCollector throttlingStatsCollector) {
        this.store = store;
        this.throttlingStatsCollector = throttlingStatsCollector;
    }

    @Override
    public <T extends Document> T find(final Collection<T> collection, final String key) {
        return store.find(collection, key);
    }

    @Override
    public <T extends Document> T find(final Collection<T> collection, final String key,
                                       final int maxCacheAge) {
        return store.find(collection, key, maxCacheAge);
    }

    @NotNull
    @Override
    public <T extends Document> List<T> query(final Collection<T> collection, final String fromKey,
                                              final String toKey, final int limit) {
        return store.query(collection, fromKey, toKey, limit);
    }

    @Override
    @NotNull
    public <T extends Document> List<T> query(final Collection<T> collection, final String fromKey,
                                              final String toKey, final String indexedProperty,
                                              final long startValue, final int limit) {
        return store.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) {
        long throttlingTime = performThrottling(collection);
        try {
            store.remove(collection, key);
        } finally {
            throttlingStatsCollector.doneRemove(MILLISECONDS.toNanos(throttlingTime), collection);
        }
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        long throttlingTime = performThrottling(collection);
        try {
            store.remove(collection, keys);
        } finally {
            throttlingStatsCollector.doneRemove(MILLISECONDS.toNanos(throttlingTime), collection);
        }
    }

    @Override
    public <T extends Document> int remove(final Collection<T> collection, final Map<String, Long> toRemove) {
        long throttlingTime = performThrottling(collection);
        int count = 0;
        try {
            count = store.remove(collection, toRemove);
        } finally {
            throttlingStatsCollector.doneRemove(MILLISECONDS.toNanos(throttlingTime), collection);
        }
        return count;
    }

    @Override
    public <T extends Document> int remove(final Collection<T> collection, final String indexedProperty,
                                           final long startValue, final long endValue) throws DocumentStoreException {
        long throttlingTime = performThrottling(collection);
        int count = 0;
        try {
            count = store.remove(collection, indexedProperty, startValue, endValue);
        } finally {
            throttlingStatsCollector.doneRemove(MILLISECONDS.toNanos(throttlingTime), collection);
        }
        return count;
    }

    @Override
    public <T extends Document> boolean create(final Collection<T> collection, final List<UpdateOp> updateOps) {
        long throttlingTime = performThrottling(collection);
        boolean isSuccess = false;
        try {
            isSuccess = store.create(collection, updateOps);
        } finally {
            throttlingStatsCollector.doneCreate(MILLISECONDS.toNanos(throttlingTime), collection,
                    updateOps.stream().map(UpdateOp::getId).collect(toList()), isSuccess);
        }
        return isSuccess;
    }

    @Override
    public <T extends Document> T createOrUpdate(final Collection<T> collection, final UpdateOp update) {
        long throttlingTime = performThrottling(collection);
        T oldDoc = null;
        try {
            oldDoc = store.createOrUpdate(collection, update);
        } finally {
            throttlingStatsCollector.doneFindAndModify(MILLISECONDS.toNanos(throttlingTime), collection, update.getId(),
                    (isNull(oldDoc) && update.isNew()), true, 0);
        }
        return oldDoc;
    }

    @Override
    public <T extends Document> List<T> createOrUpdate(final Collection<T> collection, final List<UpdateOp> updateOps) {
        long throttlingTime = performThrottling(collection);
        List<T> results = null;
        try {
            results = store.createOrUpdate(collection, updateOps);
        } finally {
            throttlingStatsCollector.doneCreateOrUpdate(MILLISECONDS.toNanos(throttlingTime), collection,
                    updateOps.stream().map(UpdateOp::getId).collect(toList()));
        }
        return results;
    }

    @Override
    public <T extends Document> T findAndUpdate(final Collection<T> collection, final UpdateOp update) {
        long throttlingTime = performThrottling(collection);
        T oldDoc = null;
        try {
            oldDoc = store.findAndUpdate(collection, update);
        } finally {
            throttlingStatsCollector.doneFindAndModify(MILLISECONDS.toNanos(throttlingTime), collection, update.getId(),
                    false, true, 0);
        }
        return oldDoc;
    }

    @Override
    public CacheInvalidationStats invalidateCache() {
        return store.invalidateCache();
    }
    
    @Override
    public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        return store.invalidateCache(keys);
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String key) {
        store.invalidateCache(collection, key);
    }

    @Override
    public void dispose() {
        store.dispose();
    }

    @Override
    public <T extends Document> T getIfCached(final Collection<T> collection, final String key) {
        return store.getIfCached(collection, key);
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {
        store.setReadWriteMode(readWriteMode);
    }

    @Override
    public Iterable<CacheStats> getCacheStats() {
        return store.getCacheStats();
    }

    @Override
    public Map<String, String> getMetadata() {
        return store.getMetadata();
    }

    @NotNull
    @Override
    public Map<String, String> getStats() {
        return store.getStats();
    }

    @Override
    public long determineServerTimeDifferenceMillis() {
        return store.determineServerTimeDifferenceMillis();
    }

    /**
     * Return the size limit for node name based on the document store implementation
     *
     * @return node name size limit
     */
    @Override
    public int getNodeNameLimit() {
        return store.getNodeNameLimit();
    }

    /**
     * Return the {@link Throttler} for the underlying store
     * Default is no throttling
     *
     * @return throttler for document store
     */
    @Override
    public Throttler throttler() {
        return store.throttler();
    }

    // helper methods

    private <T extends Document> long performThrottling(final Collection<T> collection) {

        if (CLUSTER_NODES == collection) {
            return 0L;
        }

        final Throttler throttler = throttler();
        long throttleTime = throttler.throttlingTime();

        if (throttleTime == 0) {
            return throttleTime; // no throttling
        }

        try {
            // log message every 10 secs once to reduce noise
            // (decaseconds since 1970 - overflows roughly in year 2650)
            final int currentDecaSecond = (int) (currentTimeMillis() / 10_000);
            if (currentDecaSecond > lastLogTime) {
                lastLogTime = currentDecaSecond;
                LOG.warn("Throttling the system for {} ms for {} collection", throttleTime, collection);
            }
            sleep(throttleTime);
        } catch (InterruptedException e) {
            // swallow the exception and log it
            LOG.error("Error while throttling", e);
        }
        return throttleTime;
    }
}
