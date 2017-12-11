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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;

public class CountingDocumentStore implements DocumentStore, RevisionListener {

    private DocumentStore delegate;

    //TODO: remove mec
    boolean printStacks;

    class Stats {

        private int numFindCalls;
        private int numQueryCalls;
        private int numRemoveCalls;
        private int numCreateOrUpdateCalls;

    }

    private Map<Collection, Stats> collectionStats = new HashMap<Collection, Stats>();

    public CountingDocumentStore(DocumentStore delegate) {
        this.delegate = delegate;
    }

    public void resetCounters() {
        collectionStats.clear();
    }

    public int getNumFindCalls(Collection collection) {
        return getStats(collection).numFindCalls;
    }

    public int getNumQueryCalls(Collection collection) {
        return getStats(collection).numQueryCalls;
    }

    public int getNumRemoveCalls(Collection collection) {
        return getStats(collection).numRemoveCalls;
    }

    public int getNumCreateOrUpdateCalls(Collection collection) {
        return getStats(collection).numCreateOrUpdateCalls;
    }

    private Stats getStats(Collection collection) {
        if (!collectionStats.containsKey(collection)) {
            Stats s = new Stats();
            collectionStats.put(collection, s);
            return s;
        } else {
            return collectionStats.get(collection);
        }
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        getStats(collection).numFindCalls++;
        if (printStacks) {
            new Exception("find [" + getStats(collection).numFindCalls + "] (" + collection + ") " + key).printStackTrace();
        }
        return delegate.find(collection, key);
    }

    @Override
    public <T extends Document> T find(Collection<T> collection,
                                       String key,
                                       int maxCacheAge) {
        getStats(collection).numFindCalls++;
        if (printStacks) {
            new Exception("find [" + getStats(collection).numFindCalls + "] (" + collection + ") " + key + " [max: " + maxCacheAge + "]").printStackTrace();
        }
        return delegate.find(collection, key, maxCacheAge);
    }

    @Nonnull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection,
                                              String fromKey,
                                              String toKey,
                                              int limit) {
        getStats(collection).numQueryCalls++;
        if (printStacks) {
            new Exception("query1 [" + getStats(collection).numQueryCalls + "] (" + collection + ") " + fromKey + ", to " + toKey + ". limit " + limit).printStackTrace();
        }
        return delegate.query(collection, fromKey, toKey, limit);
    }

    @Nonnull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection,
                                              String fromKey,
                                              String toKey,
                                              String indexedProperty,
                                              long startValue,
                                              int limit) {
        getStats(collection).numQueryCalls++;
        if (printStacks) {
            new Exception("query2 [" + getStats(collection).numQueryCalls + "] (" + collection + ") " + fromKey + ", to " + toKey + ". limit " + limit).printStackTrace();
        }
        return delegate.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection,
                                            String key) {
        getStats(collection).numRemoveCalls++;
        delegate.remove(collection, key);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection,
                                            List<String> keys) {
        getStats(collection).numRemoveCalls++;
        delegate.remove(collection, keys);
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection,
                                           Map<String, Long> toRemove) {
        getStats(collection).numRemoveCalls++;
        return delegate.remove(collection, toRemove);
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection,
                                           String indexedProperty, long startValue, long endValue)
            throws DocumentStoreException {
        getStats(collection).numRemoveCalls++;
        return delegate.remove(collection, indexedProperty, startValue, endValue);
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection,
                                               List<UpdateOp> updateOps) {
        getStats(collection).numCreateOrUpdateCalls++;
        return delegate.create(collection, updateOps);
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection,
                                                 UpdateOp update) {
        getStats(collection).numCreateOrUpdateCalls++;
        return delegate.createOrUpdate(collection, update);
    }

    @Override
    public <T extends Document> List<T> createOrUpdate(Collection<T> collection,
                                                       List<UpdateOp> updateOps) {
        getStats(collection).numCreateOrUpdateCalls++;
        return delegate.createOrUpdate(collection, updateOps);
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                UpdateOp update) {
        getStats(collection).numCreateOrUpdateCalls++;
        return delegate.findAndUpdate(collection, update);
    }

    @Override
    public CacheInvalidationStats invalidateCache() {
        return delegate.invalidateCache();
    }

    @Override
    public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        return delegate.invalidateCache(keys);
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection,
                                                     String key) {
        delegate.invalidateCache(collection, key);
    }

    @Override
    public void dispose() {
        delegate.dispose();
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection,
                                              String key) {
        return delegate.getIfCached(collection, key);
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {
        delegate.setReadWriteMode(readWriteMode);
    }

    @Override
    public Iterable<CacheStats> getCacheStats() {
        return delegate.getCacheStats();
    }

    @Override
    public Map<String, String> getMetadata() {
        return delegate.getMetadata();
    }

    @Nonnull
    @Override
    public Map<String, String> getStats() {
        return delegate.getStats();
    }

    @Override
    public long determineServerTimeDifferenceMillis() {
        return delegate.determineServerTimeDifferenceMillis();
    }

    @Override
    public void updateAccessedRevision(RevisionVector revision, int currentClusterId) {
        if (delegate instanceof RevisionListener) {
            ((RevisionListener) delegate).updateAccessedRevision(revision, currentClusterId);
        }
    }
}
