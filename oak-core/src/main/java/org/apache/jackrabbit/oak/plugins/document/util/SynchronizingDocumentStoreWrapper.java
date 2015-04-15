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

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;

/**
 * Implements a <code>DocumentStore</code> wrapper which synchronizes on all
 * methods.
 */
public class SynchronizingDocumentStoreWrapper implements DocumentStore {

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
    public synchronized <T extends Document> List<T> query(final Collection<T> collection, final String fromKey,
            final String toKey, final int limit) {
        return store.query(collection, fromKey, toKey, limit);
    }

    @Override
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
        for(String key : keys){
            remove(collection, key);
        }
    }

    @Override
    public synchronized <T extends Document> boolean create(final Collection<T> collection, final List<UpdateOp> updateOps) {
        return store.create(collection, updateOps);
    }

    @Override
    public synchronized <T extends Document> void update(final Collection<T> collection, final List<String> keys,
            final UpdateOp updateOp) {
        store.update(collection, keys, updateOp);
    }

    @Override
    public synchronized <T extends Document> T createOrUpdate(final Collection<T> collection, final UpdateOp update) {
        return store.createOrUpdate(collection, update);
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
    public synchronized CacheStats getCacheStats() {
        return store.getCacheStats();
    }

    @Override
    public Map<String, String> getMetadata() {
        return store.getMetadata();
    }
}
