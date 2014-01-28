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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernelException;

/**
 * The interface for the backend storage for documents.
 */
public interface DocumentStore {

    /**
     * Get a document.
     * <p>
     * The returned document is immutable.
     * 
     * @param <T> the document type
     * @param collection the collection
     * @param key the key
     * @return the document, or null if not found
     */
    @CheckForNull
    <T extends Document> T find(Collection<T> collection, String key);

    /**
     * Get a document, ignoring the cache if the cached entry is older than the
     * specified time.
     * <p>
     * The returned document is immutable.
     * 
     * @param <T> the document type
     * @param collection the collection
     * @param key the key
     * @param maxCacheAge the maximum age of the cached document
     * @return the document, or null if not found
     */
    @CheckForNull
    <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge);

    /**
     * Get a list of documents where the key is greater than a start value and
     * less than an end value, sorted by the key.
     * <p>
     * The returned documents are immutable.
     * 
     * @param <T> the document type
     * @param collection the collection
     * @param fromKey the start value (excluding)
     * @param toKey the end value (excluding)
     * @param limit the maximum number of entries to return (starting with the lowest key)
     * @return the list (possibly empty)
     */
    @Nonnull
    <T extends Document> List<T> query(Collection<T> collection,
                                       String fromKey,
                                       String toKey,
                                       int limit);

    /**
     * Get a list of documents where the key is greater than a start value and
     * less than an end value. The returned documents are immutable.
     * 
     * @param <T> the document type
     * @param collection the collection
     * @param fromKey the start value (excluding)
     * @param toKey the end value (excluding)
     * @param indexedProperty the name of the indexed property (optional)
     * @param startValue the minimum value of the indexed property
     * @param limit the maximum number of entries to return
     * @return the list (possibly empty)
     */
    @Nonnull
    <T extends Document> List<T> query(Collection<T> collection,
                                       String fromKey,
                                       String toKey,
                                       String indexedProperty,
                                       long startValue,
                                       int limit);

    /**
     * Remove a document.
     *
     * @param <T> the document type
     * @param collection the collection
     * @param key the key
     */
    <T extends Document> void remove(Collection<T> collection, String key);

    /**
     * Try to create a list of documents.
     * 
     * @param <T> the document type
     * @param collection the collection
     * @param updateOps the list of documents to add
     * @return true if this worked (if none of the documents already existed)
     */
    <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps);

    /**
     * Update documents with the given keys. Only existing documents are
     * updated.
     *
     * @param <T> the document type.
     * @param collection the collection.
     * @param keys the keys of the documents to update.
     * @param updateOp the update operation to apply to each of the documents.
     */
    <T extends Document> void update(Collection<T> collection,
                                     List<String> keys,
                                     UpdateOp updateOp);
    
    /**
     * Create or update a document. For MongoDb, this is using "findAndModify" with
     * the "upsert" flag (insert or update). The returned document is immutable.
     *
     * @param <T> the document type
     * @param collection the collection
     * @param update the update operation
     * @return the old document or <code>null</code> if it didn't exist before.
     * @throws MicroKernelException if the operation failed.
     */
    @CheckForNull
    <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update)
            throws MicroKernelException;

    /**
     * Performs a conditional update (e.g. using
     * {@link UpdateOp.Operation.Type#CONTAINS_MAP_ENTRY} and only updates the
     * document if the condition is <code>true</code>. The returned document is
     * immutable.
     *
     * @param <T> the document type
     * @param collection the collection
     * @param update the update operation with the condition
     * @return the old document or <code>null</code> if the condition is not met or
     *         if the document wasn't found
     * @throws MicroKernelException if the operation failed.
     */
    @CheckForNull
    <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update)
            throws MicroKernelException;

    /**
     * Invalidate the document cache.
     */
    void invalidateCache();

    /**
     * Invalidate the document cache for the given key.
     * 
     * @param <T> the document type
     * @param collection the collection
     * @param key the key
     */
    <T extends Document> void invalidateCache(Collection<T> collection, String key);

    /**
     * Dispose this instance.
     */
    void dispose();

    /**
     * Fetches the cached document. If document is not present in cache <code>null</code> would be returned
     *
     * @param <T> the document type
     * @param collection the collection
     * @param key the key
     * @return cached document if present. Otherwise null
     */
    @CheckForNull
    <T extends Document> T getIfCached(Collection<T> collection, String key);

    /**
     * Set the level of guarantee for read and write operations, if supported by this backend.
     * 
     * @param readWriteMode the read/write mode
     */
    void setReadWriteMode(String readWriteMode);
    
}
