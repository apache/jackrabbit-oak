/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.util;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.RevisionListener;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;

/**
 * Wrapper of another DocumentStore that does a lease check on any method
 * invocation (read or update) and fails if the lease is not valid.
 * <p>
 * @see "https://issues.apache.org/jira/browse/OAK-2739 for more details"
 */
public final class LeaseCheckDocumentStoreWrapper implements DocumentStore, RevisionListener {

    private final DocumentStore delegate;
    private final ClusterNodeInfo clusterNodeInfo;

    public LeaseCheckDocumentStoreWrapper(final DocumentStore delegate, final ClusterNodeInfo clusterNodeInfo) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate must not be null");
        }
        this.delegate = delegate;
        // clusterNodeInfo is allowed to be null - eg for testing
        this.clusterNodeInfo = clusterNodeInfo;
    }

    private final void performLeaseCheck() {
        if (clusterNodeInfo != null) {
            clusterNodeInfo.performLeaseCheck();
        }
    }

    @Override
    public final <T extends Document> T find(Collection<T> collection, String key) {
        performLeaseCheck();
        return delegate.find(collection, key);
    }

    @Override
    public final <T extends Document> T find(Collection<T> collection, String key,
            int maxCacheAge) {
        performLeaseCheck();
        return delegate.find(collection, key, maxCacheAge);
    }

    @Override
    public final <T extends Document> List<T> query(Collection<T> collection,
            String fromKey, String toKey, int limit) {
        performLeaseCheck();
        return delegate.query(collection, fromKey, toKey, limit);
    }

    @Override
    public final <T extends Document> List<T> query(Collection<T> collection,
            String fromKey, String toKey, String indexedProperty,
            long startValue, int limit) {
        performLeaseCheck();
        return delegate.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
    }

    @Override
    public final <T extends Document> void remove(Collection<T> collection, String key) {
        performLeaseCheck();
        delegate.remove(collection, key);
    }

    @Override
    public final <T extends Document> void remove(Collection<T> collection,
            List<String> keys) {
        performLeaseCheck();
        delegate.remove(collection, keys);
    }

    @Override
    public final <T extends Document> int remove(Collection<T> collection,
            Map<String, Long> toRemove) {
        performLeaseCheck();
        return delegate.remove(collection, toRemove);
    }

    @Override
    public<T extends Document> int remove(Collection<T> collection,
                                          String indexedProperty, long startValue, long endValue)
            throws DocumentStoreException {
        performLeaseCheck();
        return delegate.remove(collection, indexedProperty, startValue, endValue);
    }

    @Override
    public final <T extends Document> boolean create(Collection<T> collection,
            List<UpdateOp> updateOps) {
        performLeaseCheck();
        return delegate.create(collection, updateOps);
    }

    @Override
    public final <T extends Document> T createOrUpdate(Collection<T> collection,
            UpdateOp update) {
        performLeaseCheck();
        return delegate.createOrUpdate(collection, update);
    }

    @Override
    public <T extends Document> List<T> createOrUpdate(Collection<T> collection,
            List<UpdateOp> updateOps) {
        performLeaseCheck();
        return delegate.createOrUpdate(collection, updateOps);
    }

    @Override
    public final <T extends Document> T findAndUpdate(Collection<T> collection,
            UpdateOp update) {
        performLeaseCheck();
        return delegate.findAndUpdate(collection, update);
    }

    @Override
    public final CacheInvalidationStats invalidateCache() {
        performLeaseCheck();
        return delegate.invalidateCache();
    }

    @Override
    public final CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        performLeaseCheck();
        return delegate.invalidateCache(keys);
    }

    @Override
    public final <T extends Document> void invalidateCache(Collection<T> collection,
            String key) {
        performLeaseCheck();
        delegate.invalidateCache(collection, key);
    }

    @Override
    public final void dispose() {
        // this is debatable whether or not a lease check should be done on dispose.
        // I'd say the lease must still be valid as on dispose there could be
        // stuff written to the document store which should only be done
        // when the lease is valid.
        // however.. dispose() is also called as a result of the 'failed lease check stopping'
        // mechanism - and in that case this would just throw an exception and the
        // DocumentNodeStore.dispose() would not correctly finish.
        // so: let's let the dispose ignore the lease state
        delegate.dispose();
    }

    @Override
    public final <T extends Document> T getIfCached(Collection<T> collection,
            String key) {
        performLeaseCheck();
        return delegate.getIfCached(collection, key);
    }

    @Override
    public final void setReadWriteMode(String readWriteMode) {
        performLeaseCheck();
        delegate.setReadWriteMode(readWriteMode);
    }

    @Override
    public final Iterable<CacheStats> getCacheStats() {
        performLeaseCheck();
        return delegate.getCacheStats();
    }

    @Override
    public final Map<String, String> getMetadata() {
        performLeaseCheck();
        return delegate.getMetadata();
    }

    @Nonnull
    @Override
    public Map<String, String> getStats() {
        performLeaseCheck();
        return delegate.getStats();
    }

    @Override
    public long determineServerTimeDifferenceMillis() {
        performLeaseCheck();
        return delegate.determineServerTimeDifferenceMillis();
    }

    @Override
    public void updateAccessedRevision(RevisionVector revision, int currentClusterId) {
        if (delegate instanceof RevisionListener) {
            ((RevisionListener) delegate).updateAccessedRevision(revision, currentClusterId);
        }
    }

}
