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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;

public class SynchronizedDocumentStoreWrapper implements DocumentStore {

    private final DocumentStore store;

    public SynchronizedDocumentStoreWrapper(DocumentStore store) {
        this.store = store;
    }

    @Override
    public synchronized <T extends Document> T find(Collection<T> collection, String key) {
        return store.find(collection, key);
    }

    @Override
    public synchronized <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {
        return store.find(collection, key, maxCacheAge);
    }

    @Override
    public synchronized <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) {
        return store.query(collection, fromKey, toKey, limit);
    }

    @Override
    public synchronized <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey,
            String indexedProperty, long startValue, int limit) {
        return store.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
    }

    @Override
    public synchronized <T extends Document> void remove(Collection<T> collection, String key) {
        store.remove(collection, key);
    }

    @Override
    public synchronized <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {
        return store.create(collection, updateOps);
    }

    @Override
    public synchronized <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) {
        store.update(collection, keys, updateOp);
    }

    @Override
    public synchronized <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update)
            throws MicroKernelException {
        return store.createOrUpdate(collection, update);
    }

    @Override
    public synchronized <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) throws MicroKernelException {
        return store.findAndUpdate(collection, update);
    }

    @Override
    public synchronized void invalidateCache() {
        store.invalidateCache();
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
    public synchronized <T extends Document> T getIfCached(Collection<T> collection, String key) {
        return store.getIfCached(collection, key);
    }

    @Override
    public synchronized void setReadWriteMode(String readWriteMode) {
        store.setReadWriteMode(readWriteMode);
    }
}
