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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;

/**
 * A DocumentStore implementation which wraps another store and delegates all
 * calls to it.
 */
public class DocumentStoreWrapper implements DocumentStore, RevisionListener {

    protected final DocumentStore store;

    public DocumentStoreWrapper(DocumentStore store) {
        this.store = store;
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        return store.find(collection, key);
    }

    @Override
    public <T extends Document> T find(Collection<T> collection,
                                       String key,
                                       int maxCacheAge) {
        return store.find(collection, key, maxCacheAge);
    }

    @Nonnull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection,
                                              String fromKey,
                                              String toKey,
                                              int limit) {
        return store.query(collection, fromKey, toKey, limit);
    }

    @Nonnull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection,
                                              String fromKey,
                                              String toKey,
                                              String indexedProperty,
                                              long startValue,
                                              int limit) {
        return store.query(collection, fromKey, toKey,
                indexedProperty, startValue, limit);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection,
                                            String key) {
        store.remove(collection, key);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection,
                                            List<String> keys) {
        store.remove(collection, keys);
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection,
                                           Map<String, Long> toRemove) {
        return store.remove(collection, toRemove);
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection,
                                           String indexedProperty, long startValue, long endValue)
            throws DocumentStoreException {
        return store.remove(collection, indexedProperty, startValue, endValue);
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection,
                                               List<UpdateOp> updateOps) {
        return store.create(collection, updateOps);
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection,
                                                 UpdateOp update) {
        return store.createOrUpdate(collection, update);
    }

    @Override
    public <T extends Document> List<T> createOrUpdate(Collection<T> collection,
                                                       List<UpdateOp> updateOps) {
        return store.createOrUpdate(collection, updateOps);
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                UpdateOp update) {
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
    public <T extends Document> void invalidateCache(Collection<T> collection,
                                                     String key) {
        store.invalidateCache(collection, key);
    }

    @Override
    public void dispose() {
        store.dispose();
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection,
                                              String key) {
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

    @Nonnull
    @Override
    public Map<String, String> getStats() {
        return store.getStats();
    }

    @Override
    public long determineServerTimeDifferenceMillis() {
        return store.determineServerTimeDifferenceMillis();
    }

    @Override
    public void updateAccessedRevision(RevisionVector revision, int currentClusterId) {
        if (store instanceof RevisionListener) {
            ((RevisionListener) store).updateAccessedRevision(revision, currentClusterId);
        }
    }
}
