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
import java.util.function.Supplier;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.Throttler;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.jetbrains.annotations.NotNull;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper of another DocumentStore that does a lease check on any method
 * invocation (read or update) and fails if the lease is not valid.
 * <p>
 * @see "https://issues.apache.org/jira/browse/OAK-2739 for more details"
 */
public final class LeaseCheckDocumentStoreWrapper implements DocumentStore {

    private final DocumentStore delegate;
    private final ClusterNodeInfo clusterNodeInfo;
    private final Logger LOG = LoggerFactory.getLogger(LeaseCheckDocumentStoreWrapper.class);

    public LeaseCheckDocumentStoreWrapper(final DocumentStore delegate, final ClusterNodeInfo clusterNodeInfo) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate must not be null");
        }
        this.delegate = delegate;
        // clusterNodeInfo is allowed to be null - e.g. for testing
        this.clusterNodeInfo = clusterNodeInfo;
    }

    private void performLeaseCheck(boolean after) {
        if (clusterNodeInfo != null) {
            try {
                clusterNodeInfo.performLeaseCheck();
            } catch (DocumentStoreException ex) {
                if (after) {
                    LOG.error("Potential late write operation detected", ex);
                }
                throw ex;
            }
        }
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        return leaseChecking(() ->
                delegate.find(collection, key));
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key,
                                       int maxCacheAge) {
        return leaseChecking(() ->
                delegate.find(collection, key, maxCacheAge));

    }

    @Override
    public <T extends Document> @NonNull List<T> query(Collection<T> collection,
                                                       String fromKey, String toKey, int limit) {
        return leaseChecking(() ->
                delegate.query(collection, fromKey, toKey, limit));
    }

    @Override
    public <T extends Document> @NonNull List<T> query(Collection<T> collection,
                                                       String fromKey, String toKey, String indexedProperty,
                                                       long startValue, int limit) {
        return leaseChecking(() ->
                delegate.query(collection, fromKey, toKey, indexedProperty, startValue, limit));
    }

    @Override
    @NotNull
    public <T extends Document> List<T> query(final Collection<T> collection, final String fromKey, final String toKey,
                                              final String indexedProperty, final long startValue, final int limit,
                                              final List<String> projection) {
        return leaseChecking(() ->
                delegate.query(collection, fromKey, toKey, indexedProperty, startValue, limit, projection));
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) {
        leaseChecking(() ->
                delegate.remove(collection, key));
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection,
                                            List<String> keys) {
        leaseChecking(() ->
                delegate.remove(collection, keys));
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection,
                                           Map<String, Long> toRemove) {
        return leaseChecking(() ->
                delegate.remove(collection, toRemove));
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection,
                                           String indexedProperty, long startValue, long endValue) {
        return leaseChecking(() ->
                delegate.remove(collection, indexedProperty, startValue, endValue));
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection,
                                               List<UpdateOp> updateOps) {
        return leaseChecking(() ->
                delegate.create(collection, updateOps));
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection,
                                                 UpdateOp update) {
        return leaseChecking(() ->
                delegate.createOrUpdate(collection, update));
    }

    @Override
    public <T extends Document> List<T> createOrUpdate(Collection<T> collection,
                                                       List<UpdateOp> updateOps) {
        return leaseChecking(() ->
                delegate.createOrUpdate(collection, updateOps));
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                UpdateOp update) {
        return leaseChecking(() ->
                delegate.findAndUpdate(collection, update));
    }

    @Override
    @NotNull
    public <T extends Document> List<T> findAndUpdate(@NotNull Collection<T> collection, @NotNull List<UpdateOp> updateOps) {
        return leaseChecking(() ->
                delegate.findAndUpdate(collection, updateOps));
    }

    @Override
    public CacheInvalidationStats invalidateCache() {
        return leaseChecking(() ->
                delegate.invalidateCache());
    }

    @Override
    public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        return leaseChecking(() ->
                delegate.invalidateCache(keys));
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection,
                                                     String key) {
        leaseChecking(() ->
            delegate.invalidateCache(collection, key));
    }

    @Override
    public void dispose() {
        // this is debatable whether a lease check should be done on dispose.
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
    public <T extends Document> T getIfCached(Collection<T> collection,
                                              String key) {
        return leaseChecking(() ->
                delegate.getIfCached(collection, key));
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {
        leaseChecking(() ->
            delegate.setReadWriteMode(readWriteMode));
    }

    @Override
    public Iterable<CacheStats> getCacheStats() {
        return leaseChecking(delegate::getCacheStats);
    }

    @Override
    public Map<String, String> getMetadata() {
        return leaseChecking(delegate::getMetadata);
    }

    @NotNull
    @Override
    public Map<String, String> getStats() {
        return leaseChecking(delegate::getStats);
    }

    @Override
    public long determineServerTimeDifferenceMillis() {
        return leaseChecking(delegate::determineServerTimeDifferenceMillis);
    }

    @Override
    public <T extends Document> void prefetch(Collection<T> collection,
                                              Iterable<String> keys) {
        leaseChecking(() ->
                delegate.prefetch(collection, keys));
    }

    /**
     * Return the size limit for node name based on the document store implementation
     *
     * @return node name size limit
     */
    @Override
    public int getNodeNameLimit() {
        return delegate.getNodeNameLimit();
    }

    /**
     * Return the {@link Throttler} for the underlying store
     * Default is no throttling
     *
     * @return throttler for document store
     */
    @Override
    public Throttler throttler() {
        return delegate.throttler();
    }

    // invoke operation with lease check before/after
    private <T> T leaseChecking(Supplier<T> operation) {
        performLeaseCheck(false);
        T result = operation.get();
        performLeaseCheck(true);
        return result;
    }

    // invoke operation with lease check before/after
    private void leaseChecking(Runnable operation) {
        performLeaseCheck(false);
        operation.run();
        performLeaseCheck(true);
    }
}