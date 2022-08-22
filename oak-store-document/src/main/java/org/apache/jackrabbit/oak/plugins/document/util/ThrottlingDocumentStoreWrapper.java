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
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
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
    public ThrottlingDocumentStoreWrapper(final @NotNull DocumentStore store) {
        this.store = store;
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
        performThrottling(collection);
        store.remove(collection, key);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        performThrottling(collection);
        store.remove(collection, keys);
    }

    @Override
    public <T extends Document> int remove(final Collection<T> collection, final Map<String, Long> toRemove) {
        performThrottling(collection);
        return store.remove(collection, toRemove);
    }

    @Override
    public <T extends Document> int remove(final Collection<T> collection, final String indexedProperty,
                                           final long startValue, final long endValue) throws DocumentStoreException {
        performThrottling(collection);
        return store.remove(collection, indexedProperty, startValue, endValue);
    }

    @Override
    public <T extends Document> boolean create(final Collection<T> collection, final List<UpdateOp> updateOps) {
        performThrottling(collection);
        return store.create(collection, updateOps);
    }

    @Override
    public <T extends Document> T createOrUpdate(final Collection<T> collection, final UpdateOp update) {
        performThrottling(collection);
        return store.createOrUpdate(collection, update);
    }

    @Override
    public <T extends Document> List<T> createOrUpdate(final Collection<T> collection, final List<UpdateOp> updateOps) {
        performThrottling(collection);
        return store.createOrUpdate(collection, updateOps);
    }

    @Override
    public <T extends Document> T findAndUpdate(final Collection<T> collection, final UpdateOp update) {
        performThrottling(collection);
        return store.findAndUpdate(collection, update);
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

    private <T extends Document> void performThrottling(final Collection<T> collection) {

        if (CLUSTER_NODES == collection) {
            return;
        }

        final Throttler throttler = throttler();
        long throttleTime = throttler.throttlingTime();

        if (throttleTime == 0) {
            return; // no throttling
        }

        try {
            // log message every 10 secs once to reduce noise
            // (decaseconds since 1970 - overflows roughly in year 2650)
            final int currentDecaSecond = (int) (currentTimeMillis() / 10_000);
            if (currentDecaSecond - lastLogTime >= 10) {
                lastLogTime = currentDecaSecond;
                LOG.warn("Throttling the system for {} ms for {} collection", throttleTime, collection);
            }
            sleep(throttleTime);
        } catch (InterruptedException e) {
            // swallow the exception and log it
            LOG.error("Error while throttling", e);
        }
    }
}
