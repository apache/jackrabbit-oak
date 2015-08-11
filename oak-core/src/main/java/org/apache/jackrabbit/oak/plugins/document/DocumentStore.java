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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;

/**
 * The interface for the backend storage for documents.
 * <p>
 * In general atomicity of operations on a DocumentStore are limited to a single
 * document. That is, an implementation does not have to guarantee atomicity of
 * the entire effect of a method call. A method that fails with an exception may
 * have modified just some documents and then abort. However, an implementation
 * must not modify a document partially. Either the complete update operation
 * is applied to a document or no modification is done at all.
 * <p>
 * For keys, the maximum length is 512 bytes in the UTF-8 representation.
 */
public interface DocumentStore {

    /**
     * Get the document with the given {@code key}. This is a convenience method
     * and equivalent to {@link #find(Collection, String, int)} with a
     * {@code maxCacheAge} of {@code Integer.MAX_VALUE}.
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
     * Get the document with the {@code key}. The implementation may serve the
     * document from a cache, but the cached document must not be older than
     * the given {@code maxCacheAge} in milliseconds. An implementation must
     * invalidate a cached document when it detects it is outdated. That is, a
     * subsequent call to {@link #find(Collection, String)} must return the
     * newer version of the document.
     * <p>
     * The returned document is immutable.
     *
     * @param <T> the document type
     * @param collection the collection
     * @param key the key
     * @param maxCacheAge the maximum age of the cached document (in ms)
     * @return the document, or null if not found
     */
    @CheckForNull
    <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge);

    /**
     * Get a list of documents where the key is greater than a start value and
     * less than an end value.
     * <p>
     * The returned documents are sorted by key and are immutable.
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
     * less than an end value <em>and</em> the given "indexed property" is greater
     * or equals the specified value.
     * <p>
     * The indexed property can either be a {@link Long} value, in which case numeric
     * comparison applies, or a {@link Boolean} value, in which case "false" is mapped
     * to "0" and "true" is mapped to "1".
     * <p>
     * The returned documents are sorted by key and are immutable.
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
     * Remove a document. This method does nothing if there is no document
     * with the given key.
     *
     * @param <T> the document type
     * @param collection the collection
     * @param key the key
     */
    <T extends Document> void remove(Collection<T> collection, String key);

    /**
     * Batch remove documents with given keys. Keys for documents that do not
     * exist are simply ignored. If this method fails with an exception, then
     * only some of the documents identified by {@code keys} may have been
     * removed.
     *
     * @param <T> the document type
     * @param collection the collection
     * @param keys list of keys
     */
    <T extends Document> void remove(Collection<T> collection, List<String> keys);

    /**
     * Batch remove documents with given keys and corresponding conditions. Keys
     * for documents that do not exist are simply ignored. A document is only
     * removed if the corresponding conditions are met. If this method fails
     * with an exception, then only some of the documents may have been removed.
     *
     * @param <T> the document type
     * @param collection the collection.
     * @param toRemove the keys of the documents to remove with the
     *                 corresponding conditions.
     * @return the number of removed documents.
     */
    <T extends Document> int remove(Collection<T> collection,
                                     Map<String, Map<UpdateOp.Key, UpdateOp.Condition>> toRemove);

    /**
     * Try to create a list of documents. This method returns {@code true} iff
     * none of the documents existed before and the create was successful. This
     * method will return {@code false} if one of the documents already exists
     * in the store. Some documents may still have been created in the store.
     * An implementation does not have to guarantee an atomic create of all the
     * documents described in the {@code updateOps}. It is the responsibility of
     * the caller to check, which documents were created and take appropriate
     * action. The same is true when this method throws an exception (e.g. when
     * a communication error occurs). In this case only some documents may have
     * been created.
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
     * Create or update a document. For MongoDB, this is using "findAndModify" with
     * the "upsert" flag (insert or update). The returned document is immutable.
     *
     * @param <T> the document type
     * @param collection the collection
     * @param update the update operation
     * @return the old document or <code>null</code> if it didn't exist before.
     */
    @CheckForNull
    <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update);

    /**
     * Performs a conditional update (e.g. using
     * {@link UpdateOp.Condition.Type#EXISTS} and only updates the
     * document if the condition is <code>true</code>. The returned document is
     * immutable.
     *
     * @param <T> the document type
     * @param collection the collection
     * @param update the update operation with the condition
     * @return the old document or <code>null</code> if the condition is not met or
     *         if the document wasn't found
     */
    @CheckForNull
    <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update);

    /**
     * Invalidate the document cache.
     */
    @CheckForNull
    CacheInvalidationStats invalidateCache();

    /**
     * Invalidate the document cache but only with entries that match one
     * of the keys provided.
     */
    @CheckForNull
    CacheInvalidationStats invalidateCache(Iterable<String> keys);

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

    /**
     * @return status information about the cache
     */
    @CheckForNull
    CacheStats getCacheStats();

    /**
     * @return description of the underlying storage.
     */
    Map<String, String> getMetadata();
}
