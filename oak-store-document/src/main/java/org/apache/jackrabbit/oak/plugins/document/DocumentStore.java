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
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;

/**
 * The interface for the backend storage for documents.
 * <p>
 * In general atomicity of operations on a DocumentStore are limited to a single
 * document. That is, an implementation does not have to guarantee atomicity of
 * the entire effect of a method call. A method that fails with an exception may
 * have modified just some documents and then abort. However, an implementation
 * must not modify a document partially. Either the complete update operation is
 * applied to a document or no modification is done at all.
 * <p>
 * The key is the id of a document. Keys are opaque strings. All characters are
 * allowed. Leading and trailing whitespace is allowed. For keys, the maximum
 * length is 512 bytes in the UTF-8 representation.
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
     * @throws DocumentStoreException if the operation failed. E.g. because of
     *          an I/O error.
     */
    @CheckForNull
    <T extends Document> T find(Collection<T> collection, String key)
            throws DocumentStoreException;

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
     * @throws DocumentStoreException if the operation failed. E.g. because of
     *          an I/O error.
     */
    @CheckForNull
    <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge)
            throws DocumentStoreException;

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
     * @throws DocumentStoreException if the operation failed. E.g. because of
     *          an I/O error.
     */
    @Nonnull
    <T extends Document> List<T> query(Collection<T> collection,
                                       String fromKey,
                                       String toKey,
                                       int limit) throws DocumentStoreException;

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
     * @throws DocumentStoreException if the operation failed. E.g. because of
     *          an I/O error.
     */
    @Nonnull
    <T extends Document> List<T> query(Collection<T> collection,
                                       String fromKey,
                                       String toKey,
                                       String indexedProperty,
                                       long startValue,
                                       int limit) throws DocumentStoreException;

    /**
     * Remove a document. This method does nothing if there is no document
     * with the given key.
     * <p>
     * In case of a {@code DocumentStoreException}, the document with the given
     * key may or may not have been removed from the store. It is the
     * responsibility of the caller to check whether it still exists. The
     * implementation however ensures that the result of the operation is
     * properly reflected in the document cache. That is, an implementation
     * could simply evict the document with the given key.
     *
     * @param <T> the document type
     * @param collection the collection
     * @param key the key
     * @throws DocumentStoreException if the operation failed. E.g. because of
     *          an I/O error.
     */
    <T extends Document> void remove(Collection<T> collection, String key)
            throws DocumentStoreException;

    /**
     * Batch remove documents with given keys. Keys for documents that do not
     * exist are simply ignored. If this method fails with an exception, then
     * only some of the documents identified by {@code keys} may have been
     * removed.
     * <p>
     * In case of a {@code DocumentStoreException}, the documents with the given
     * keys may or may not have been removed from the store. It may also be
     * possible that only some have been removed from the store. It is the
     * responsibility of the caller to check which documents still exist. The
     * implementation however ensures that the result of the operation is
     * properly reflected in the document cache. That is, an implementation
     * could simply evict documents with the given keys from the cache.
     *
     * @param <T> the document type
     * @param collection the collection
     * @param keys list of keys
     * @throws DocumentStoreException if the operation failed. E.g. because of
     *          an I/O error.
     */
    <T extends Document> void remove(Collection<T> collection, List<String> keys)
            throws DocumentStoreException;

    /**
     * Batch remove documents with given keys and corresponding equal conditions
     * on {@link NodeDocument#MODIFIED_IN_SECS} values. Keys for documents that
     * do not exist are simply ignored. A document is only removed if the
     * corresponding condition is met.
     * <p>
     * In case of a {@code DocumentStoreException}, the documents with the given
     * keys may or may not have been removed from the store. It may also be
     * possible that only some have been removed from the store. It is the
     * responsibility of the caller to check which documents still exist. The
     * implementation however ensures that the result of the operation is
     * properly reflected in the document cache. That is, an implementation
     * could simply evict documents with the given keys from the cache.
     *
     * @param <T>
     *            the document type
     * @param collection
     *            the collection.
     * @param toRemove
     *            the keys of the documents to remove with the corresponding
     *            timestamps.
     * @return the number of removed documents.
     * @throws DocumentStoreException
     *             if the operation failed. E.g. because of an I/O error.
     */
    <T extends Document> int remove(Collection<T> collection, Map<String, Long> toRemove)
            throws DocumentStoreException;


    /**
     * Batch remove documents where the given "indexed property" is within the given
     * range (exclusive) - {@code (startValue, endValue)}.
     * <p>
     * The indexed property is a {@link Long} value and numeric comparison applies.
     * <p>
     * In case of a {@code DocumentStoreException}, the documents with the given
     * keys may or may not have been removed from the store. It may also be
     * possible that only some have been removed from the store. It is the
     * responsibility of the caller to check which documents still exist. The
     * implementation however ensures that the result of the operation is
     * properly reflected in the document cache. That is, an implementation
     * could simply evict documents with the given keys from the cache.
     *
     * @param <T> the document type
     * @param collection the collection.
     * @param indexedProperty the name of the indexed property
     * @param startValue the minimum value of the indexed property (exclusive)
     * @param endValue the maximum value of the indexed property (exclusive)
     * @return the number of removed documents.
     * @throws DocumentStoreException if the operation failed. E.g. because of
     *          an I/O error.
     */
    <T extends Document> int remove(Collection<T> collection,
                                    String indexedProperty, long startValue, long endValue)
            throws DocumentStoreException;

    /**
     * Try to create a list of documents. This method returns {@code true} iff
     * none of the documents existed before and the create was successful. This
     * method will return {@code false} if one of the documents already exists
     * in the store. Some documents may still have been created in the store.
     * An implementation does not have to guarantee an atomic create of all the
     * documents described in the {@code updateOps}. It is the responsibility of
     * the caller to check, which documents were created and take appropriate
     * action. The same is true when this method throws
     * {@code DocumentStoreException} (e.g. when a communication error occurs).
     * In this case only some documents may have been created.
     *
     * @param <T> the document type
     * @param collection the collection
     * @param updateOps the list of documents to add (where {@link Condition}s are not allowed)
     * @return true if this worked (if none of the documents already existed)
     * @throws IllegalArgumentException when at least one of the {@linkplain UpdateOp}s is conditional
     * @throws DocumentStoreException if the operation failed. E.g. because of
     *          an I/O error.
     */
    <T extends Document> boolean create(Collection<T> collection,
                                        List<UpdateOp> updateOps)
            throws IllegalArgumentException, DocumentStoreException;

    /**
     * Atomically checks if the document exists and updates it, otherwise the
     * document is created (aka upsert). The returned document is immutable.
     * <p>
     * If this method fails with a {@code DocumentStoreException}, then the
     * document may or may not have been created or updated. It is the
     * responsibility of the caller to check the result e.g. by calling
     * {@link #find(Collection, String)}. The implementation however ensures
     * that the result of the operation is properly reflected in the document
     * cache. That is, an implementation could simply evict documents with the
     * given keys from the cache.
     *
     * @param <T> the document type
     * @param collection the collection
     * @param update the update operation (where {@link Condition}s are not allowed)
     * @return the old document or <code>null</code> if it didn't exist before.
     * @throws IllegalArgumentException when the {@linkplain UpdateOp} is conditional
     * @throws DocumentStoreException if the operation failed. E.g. because of
     *          an I/O error.
     */
    @CheckForNull
    <T extends Document> T createOrUpdate(Collection<T> collection,
                                          UpdateOp update)
            throws IllegalArgumentException, DocumentStoreException;

    /**
     * Create or unconditionally update a number of documents. An implementation
     * does not have to guarantee that all changes are applied atomically,
     * together.
     * <p>
     * In case of a {@code DocumentStoreException} (e.g. when a communication
     * error occurs) only some changes may have been applied. In this case it is
     * the responsibility of the caller to check which {@linkplain UpdateOp}s
     * were applied and take appropriate action. The implementation however
     * ensures that the result of the operations are properly reflected in the
     * document cache. That is, an implementation could simply evict documents
     * related to the given update operations from the cache.
     *
     * @param <T> the document type
     * @param collection the collection
     * @param updateOps the update operation list
     * @return the list containing old documents or <code>null</code> values if they didn't exist
     *         before (see {@linkplain #createOrUpdate(Collection, UpdateOp)}), where the order
     *         reflects the order in the "updateOps" parameter
     * @throws DocumentStoreException if the operation failed. E.g. because of
     *          an I/O error.
     */
    <T extends Document> List<T> createOrUpdate(Collection<T> collection,
                                                List<UpdateOp> updateOps)
            throws DocumentStoreException;

    /**
     * Performs a conditional update (e.g. using
     * {@link UpdateOp.Condition.Type#EXISTS} and only updates the
     * document if the condition is <code>true</code>. The returned document is
     * immutable.
     * <p>
     * In case of a {@code DocumentStoreException} (e.g. when a communication
     * error occurs) the update may or may not have been applied. In this case
     * it is the responsibility of the caller to check whether the update was
     * applied and take appropriate action. The implementation however ensures
     * that the result of the operation is properly reflected in the document
     * cache. That is, an implementation could simply evict the document related
     * to the given update operation from the cache.
     *
     * @param <T> the document type
     * @param collection the collection
     * @param update the update operation with the condition
     * @return the old document or <code>null</code> if the condition is not met or
     *         if the document wasn't found
     * @throws DocumentStoreException if the operation failed. E.g. because of
     *          an I/O error.
     */
    @CheckForNull
    <T extends Document> T findAndUpdate(Collection<T> collection,
                                         UpdateOp update)
            throws DocumentStoreException;

    /**
     * Invalidate the document cache. Calling this method instructs the
     * implementation to invalidate each document from the cache, which is not
     * up to date with the underlying storage at the time this method is called.
     * A document is considered in the cache if {@link #getIfCached(Collection, String)}
     * returns a non-null value for a key.
     * <p>
     * An implementation is allowed to perform lazy invalidation and only check
     * whether a document is up-to-date when it is accessed after this method
     * is called. However, this also includes a call to {@link #getIfCached(Collection, String)},
     * which must only return the document if it was up-to-date at the time
     * this method was called. Similarly, a call to {@link #find(Collection, String)}
     * must guarantee the returned document reflects all the changes done up to
     * when {@code invalidateCache()} was called.
     * <p>
     * In some implementations this method can be a NOP because documents can
     * only be modified through a single instance of a {@code DocumentStore}.
     *
     * @return cache invalidation statistics or {@code null} if none are
     *          available.
     */
    @CheckForNull
    CacheInvalidationStats invalidateCache();

    /**
     * Invalidate the document cache but only with entries that match one
     * of the keys provided.
     *
     * See {@link #invalidateCache()} for the general contract of cache
     * invalidation.
     *
     * @param keys the keys of the documents to invalidate.
     * @return cache invalidation statistics or {@code null} if none are
     *          available.
     */
    @CheckForNull
    CacheInvalidationStats invalidateCache(Iterable<String> keys);

    /**
     * Invalidate the document cache for the given key.
     *
     * See {@link #invalidateCache()} for the general contract of cache
     * invalidation.
     *
     * @param collection the collection
     * @param key the key
     */
    <T extends Document> void invalidateCache(Collection<T> collection, String key);

    /**
     * Dispose this instance.
     */
    void dispose();

    /**
     * Fetches the cached document. If the document is not present in the cache
     * {@code null} will be returned. This method is consistent with other find
     * methods that may return cached documents and will return {@code null}
     * even when the implementation has a negative cache for documents that
     * do not exist. This method will never return {@link NodeDocument#NULL}.
     *
     * @param <T> the document type
     * @param collection the collection
     * @param key the key
     * @return cached document if present. Otherwise {@code null}.
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
    Iterable<CacheStats> getCacheStats();

    /**
     * @return description of the underlying storage.
     */
    Map<String, String> getMetadata();

    /**
     * Returns statistics about the underlying storage. The information and
     * keys returned by this method are implementation specific, may change
     * between releases or may even depend on deployment aspects. E.g. depending
     * on access rights, the method may return more or less information from
     * the underlying store. This method should only be used for informational
     * or debug purposes.
     *
     * @return statistics about this document store.
     */
    @Nonnull
    Map<String, String> getStats();

    /**
     * @return the estimated time difference in milliseconds between the local
     * instance and the (typically common, shared) document server system. The
     * value can be zero if the times are estimated to be equal, positive when
     * the local instance is ahead of the remote server and negative when the
     * local instance is behind the remote server. An invocation is not cached
     * and typically requires a round-trip to the server (but that is not a
     * requirement).
     * @throws UnsupportedOperationException if this DocumentStore does not
     *                                       support this method
     * @throws DocumentStoreException if an I/O error occurs.
     */
    long determineServerTimeDifferenceMillis()
            throws UnsupportedOperationException, DocumentStoreException;
}
