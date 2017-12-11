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

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.RevisionListener;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;

/**
 * Implements a <code>DocumentStore</code> wrapper which synchronizes on all
 * methods.
 */
public class SynchronizingDocumentStoreWrapper implements DocumentStore, RevisionListener {

    final DocumentStore store;

    public SynchronizingDocumentStoreWrapper(DocumentStore store) {
        this.store = store;
    }

    @Override
    public synchronized <T extends Document> T find(final Collection<T> collection, final String key) {
        return store.find(collection, key);
    }

    @Override
    public synchronized <T extends Document> T find(final Collection<T> collection, final String key, final int maxCacheAge) {
        return store.find(collection, key, maxCacheAge);
    }

    @Override
    @Nonnull
    public synchronized <T extends Document> List<T> query(final Collection<T> collection, final String fromKey,
            final String toKey, final int limit) {
        return store.query(collection, fromKey, toKey, limit);
    }

    @Override
    @Nonnull
    public synchronized <T extends Document> List<T> query(final Collection<T> collection, final String fromKey,
            final String toKey, final String indexedProperty, final long startValue, final int limit) {
        return store.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
    }

    @Override
    public synchronized <T extends Document> void remove(Collection<T> collection, String key) {
        store.remove(collection, key);
    }

    @Override
    public synchronized <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        store.remove(collection, keys);
    }

    @Override
    public synchronized <T extends Document> int remove(Collection<T> collection,
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
    public synchronized <T extends Document> boolean create(final Collection<T> collection, final List<UpdateOp> updateOps) {
        return store.create(collection, updateOps);
    }

    @Override
    public synchronized <T extends Document> T createOrUpdate(final Collection<T> collection, final UpdateOp update) {
        return store.createOrUpdate(collection, update);
    }

    @Override
    public synchronized <T extends Document> List<T> createOrUpdate(Collection<T> collection, List<UpdateOp> updateOps) {
        return store.createOrUpdate(collection, updateOps);
    }

    @Override
    public synchronized <T extends Document> T findAndUpdate(final Collection<T> collection, final UpdateOp update) {
        return store.findAndUpdate(collection, update);
    }

    @Override
    public synchronized CacheInvalidationStats invalidateCache() {
        return store.invalidateCache();
    }

    @Override
    public synchronized CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        return store.invalidateCache(keys);
    }
    
    @Override
    public synchronized <T extends Document> void invalidateCache(Collection<T> collection, String key) {
        store.invalidateCache(collection, key);
    }

    @Override
    public synchronized void dispose() {
        store.dispose();
    }

    @Override
    public synchronized <T extends Document> T getIfCached(final Collection<T> collection, final String key) {
        return store.getIfCached(collection, key);
    }

    @Override
    public synchronized void setReadWriteMode(String readWriteMode) {
        store.setReadWriteMode(readWriteMode);
    }

    @Override
    public synchronized Iterable<CacheStats> getCacheStats() {
        return store.getCacheStats();
    }

    @Override
    public synchronized long determineServerTimeDifferenceMillis() {
        return store.determineServerTimeDifferenceMillis();
    }

    @Override
    public synchronized Map<String, String> getMetadata() {
        return store.getMetadata();
    }

    @Nonnull
    @Override
    public synchronized Map<String, String> getStats() {
        return store.getStats();
    }

    @Override
    public synchronized void updateAccessedRevision(RevisionVector revision, int currentClusterId) {
        if (store instanceof RevisionListener) {
            ((RevisionListener) store).updateAccessedRevision(revision, currentClusterId);
        }
    }
}
