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

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;

import com.google.common.base.Objects;
import com.google.common.cache.Cache;

/**
 * Cache for the NodeDocuments. This class is not thread-safe and requires
 * external locking (see {@link NodeDocumentLocks}).
 */
public class NodeDocumentCache implements Closeable {

    /**
     * Comparator for maps with {@link Revision} keys. The maps are ordered
     * descending, newest revisions first!
     */
    private final Comparator<Revision> comparator = StableRevisionComparator.REVERSE;

    private final Cache<CacheValue, NodeDocument> nodesCache;

    private final CacheStats cacheStats;

    private final DocumentStore docStore;

    public NodeDocumentCache(@Nonnull DocumentMK.Builder builder, @Nonnull DocumentStore docStore) {
        this.docStore = docStore;
        nodesCache = builder.buildDocumentCache(docStore);
        cacheStats = new CacheStats(nodesCache, "Document-Documents", builder.getWeigher(),
                builder.getDocumentCacheSize());
    }

    /**
     * Invalidate document with given key.
     *
     * @param key to invalidate
     */
    public void invalidate(@Nonnull String key) {
        nodesCache.invalidate(new StringValue(key));
    }

    /**
     * Invalidate document with given keys iff their mod counts are the same as
     * passed in the map.
     *
     * @param modCounts map where key is the document id and the value is the mod count
     * @return number of invalidated entries
     */
    @Nonnegative
    public int invalidateOutdated(@Nonnull Map<String, Number> modCounts) {
        int invalidatedCount = 0;
        for (Entry<String, Number> e : modCounts.entrySet()) {
            String id = e.getKey();
            Number modCount = e.getValue();
            NodeDocument doc = getIfPresent(id);
            if (doc == null) {
                continue;
            }
            if (!Objects.equal(modCount, doc.getModCount())) {
                invalidate(id);
                invalidatedCount++;
            }
        }
        return invalidatedCount;
    }

    /**
     * Return the cached value or null.
     *
     * @param key document key
     * @return cached value of null if there's no document with given key cached
     */
    @CheckForNull
    public NodeDocument getIfPresent(@Nonnull String key) {
        return nodesCache.getIfPresent(new StringValue(key));
    }

    /**
     * Return the document matching given key, optionally loading it from an
     * external source.
     *
     * @see Cache#get(Object, Callable)
     * @param key document key
     * @param valueLoader object used to retrieve the document
     * @return document matching given key
     */
    @Nonnull
    public NodeDocument get(@Nonnull String key, @Nonnull Callable<NodeDocument> valueLoader) throws ExecutionException {
        return nodesCache.get(new StringValue(key), valueLoader);
    }

    /**
     * Puts document into cache.
     *
     * @param doc document to put
     */
    public void put(@Nonnull NodeDocument doc) {
        if (doc != NodeDocument.NULL) {
            nodesCache.put(new StringValue(doc.getId()), doc);
        }
    }

    /**
     * Creates a new document, applying {@code updateOp} on {@code oldDoc} and
     * puts it into cache.
     *
     * @param oldDoc initial document
     * @param updateOp to apply on the {@code oldDoc}
     */
    public void putWithUpdate(@Nonnull NodeDocument oldDoc, @Nonnull UpdateOp updateOp) {
        NodeDocument newDoc = Collection.NODES.newDocument(docStore);
        oldDoc.deepCopy(newDoc);
        UpdateUtils.applyChanges(newDoc, updateOp, comparator);
        newDoc.seal();
        put(newDoc);
    }

    /**
     * Puts document into cache iff no entry with the given key is cached
     * already. This operations is atomic.
     *
     * @param doc the document to add to the cache.
     * @return either the given <code>doc</code> or the document already present
     *         in the cache.
     */
    @Nonnull
    public NodeDocument putIfAbsent(@Nonnull final NodeDocument doc) {
        if (doc == NodeDocument.NULL) {
            throw new IllegalArgumentException("doc must not be NULL document");
        }
        doc.seal();
        // make sure we only cache the document if it wasn't
        // changed and cached by some other thread in the
        // meantime. That is, use get() with a Callable,
        // which is only used when the document isn't there
        try {
            String id = doc.getId();
            for (;;) {
                NodeDocument cached = get(id, new Callable<NodeDocument>() {
                    @Override
                    public NodeDocument call() {
                        return doc;
                    }
                });
                if (cached != NodeDocument.NULL) {
                    return cached;
                } else {
                    invalidate(id);
                }
            }
        } catch (ExecutionException e) {
            // will never happen because call() just returns
            // the already available doc
            throw new IllegalStateException(e);
        }
    }

    /**
     * Applies an update on the cached value. If the {@code oldDocument} is not
     * cached, nothing will happen.
     *
     * @param oldDoc the old document
     * @param updateOp the update operation
     */
    public void applyUpdateOnCachedDocument(@Nonnull NodeDocument oldDocument, @Nonnull UpdateOp updateOp) {
        // we can only update the cache based on the oldDoc if we
        // still have the oldDoc in the cache, otherwise we may
        // update the cache with an outdated document
        String id = updateOp.getId();
        NodeDocument cached = nodesCache.getIfPresent(id);
        if (cached == null) {
            // cannot use oldDoc to update cache
            return;
        }

        // check if the currently cached document matches oldDoc
        if (Objects.equal(cached.getModCount(), oldDocument.getModCount())) {
            NodeDocument newDoc = Collection.NODES.newDocument(docStore);
            oldDocument.deepCopy(newDoc);

            UpdateUtils.applyChanges(newDoc, updateOp, comparator);
            newDoc.seal();

            put(newDoc);
        } else {
            // the cache entry was modified by some other thread in
            // the meantime. the updated cache entry may or may not
            // include this update. we cannot just apply our update
            // on top of the cached entry.
            // therefore we must invalidate the cache entry
            invalidate(id);
        }
    }

    /**
     * Replaces the cached value. If the {@code oldDocument} is not cached, nothing will happen.
     *
     * @param oldDoc the old document
     * @param newDoc the replacement
     */
    @Nonnull
    public void replaceCachedDocument(@Nonnull final NodeDocument oldDocument, @Nonnull final NodeDocument newDoc) {
        NodeDocument cached = putIfAbsent(newDoc);
        if (cached == newDoc) {
            // successful
            return;
        } else if (oldDocument == null) {
            // this is an insert and some other thread was quicker
            // loading it into the cache -> return now
            return;
        } else {
            // this is an update (oldDoc != null)
            if (Objects.equal(cached.getModCount(), oldDocument.getModCount())) {
                put(newDoc);
            } else {
                // the cache entry was modified by some other thread in
                // the meantime. the updated cache entry may or may not
                // include this update. we cannot just apply our update
                // on top of the cached entry.
                // therefore we must invalidate the cache entry
                invalidate(newDoc.getId());
            }
        }
    }

    /**
     * Returns a view of the entries stored in this cache as a thread-safe map.
     * Modifications made to the map directly affect the cache.
     */
    public Map<CacheValue, NodeDocument> asMap() {
        return nodesCache.asMap();
    }

    @Override
    public void close() throws IOException {
        if (nodesCache instanceof Closeable) {
            ((Closeable) nodesCache).close();
        }
    }

    public CacheStats getCacheStats() {
        return cacheStats;
    }
}
