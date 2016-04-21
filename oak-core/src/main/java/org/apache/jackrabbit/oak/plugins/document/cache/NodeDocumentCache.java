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
package org.apache.jackrabbit.oak.plugins.document.cache;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.locks.NodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;

import com.google.common.base.Objects;
import com.google.common.cache.Cache;

import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isLeafPreviousDocId;

/**
 * Cache for the NodeDocuments. This class is thread-safe and uses the provided NodeDocumentLock.
 */
public class NodeDocumentCache implements Closeable {

    private final Cache<CacheValue, NodeDocument> nodeDocumentsCache;

    private final CacheStats nodeDocumentsCacheStats;

    /**
     * The previous documents cache
     *
     * Key: StringValue, value: NodeDocument
     */
    private final Cache<StringValue, NodeDocument> prevDocumentsCache;
    private final CacheStats prevDocumentsCacheStats;

    private final NodeDocumentLocks locks;

    public NodeDocumentCache(@Nonnull Cache<CacheValue, NodeDocument> nodeDocumentsCache,
                             @Nonnull CacheStats nodeDocumentsCacheStats,
                             @Nonnull Cache<StringValue, NodeDocument> prevDocumentsCache,
                             @Nonnull CacheStats prevDocumentsCacheStats,
                             @Nonnull NodeDocumentLocks locks) {
        this.nodeDocumentsCache = nodeDocumentsCache;
        this.nodeDocumentsCacheStats = nodeDocumentsCacheStats;
        this.prevDocumentsCache = prevDocumentsCache;
        this.prevDocumentsCacheStats = prevDocumentsCacheStats;
        this.locks = locks;
    }

    /**
     * Invalidate document with given key.
     *
     * @param key to invalidate
     */
    public void invalidate(@Nonnull String key) {
        Lock lock = locks.acquire(key);
        try {
            if (isLeafPreviousDocId(key)) {
                prevDocumentsCache.invalidate(new StringValue(key));
            } else {
                nodeDocumentsCache.invalidate(new StringValue(key));
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Invalidate document with given keys iff their mod counts are different as
     * passed in the map.
     *
     * @param modCounts map where key is the document id and the value is the mod count
     * @return number of invalidated entries
     */
    @Nonnegative
    public int invalidateOutdated(@Nonnull Map<String, Long> modCounts) {
        int invalidatedCount = 0;
        for (Entry<String, Long> e : modCounts.entrySet()) {
            String id = e.getKey();
            Long modCount = e.getValue();
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
        if (isLeafPreviousDocId(key)) {
            return prevDocumentsCache.getIfPresent(new StringValue(key));
        } else {
            return nodeDocumentsCache.getIfPresent(new StringValue(key));
        }
    }

    /**
     * Return the document matching given key, optionally loading it from an
     * external source.
     * <p>
     * This method can modify the cache, so it's synchronized. The {@link #getIfPresent(String)}
     * is not synchronized and will be faster if you need to get the cached value
     * outside the critical section.
     *
     * @see Cache#get(Object, Callable)
     * @param key document key
     * @param valueLoader object used to retrieve the document
     * @return document matching given key
     */
    @Nonnull
    public NodeDocument get(@Nonnull String key, @Nonnull Callable<NodeDocument> valueLoader)
            throws ExecutionException {
        Lock lock = locks.acquire(key);
        try {
            if (isLeafPreviousDocId(key)) {
                return prevDocumentsCache.get(new StringValue(key), valueLoader);
            } else {
                return nodeDocumentsCache.get(new StringValue(key), valueLoader);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Puts document into cache.
     *
     * @param doc document to put
     */
    public void put(@Nonnull NodeDocument doc) {
        if (doc != NodeDocument.NULL) {
            Lock lock = locks.acquire(doc.getId());
            try {
                putInternal(doc);
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Puts document into cache iff no entry with the given key is cached
     * already or the cached document is older (has smaller {@link Document#MOD_COUNT}).
     *
     * @param doc the document to add to the cache
     * @return either the given <code>doc</code> or the document already present
     *         in the cache if it's newer
     */
    @Nonnull
    public NodeDocument putIfNewer(@Nonnull final NodeDocument doc) {
        if (doc == NodeDocument.NULL) {
            throw new IllegalArgumentException("doc must not be NULL document");
        }
        doc.seal();

        NodeDocument newerDoc;

        String id = doc.getId();
        Lock lock = locks.acquire(id);
        try {
            NodeDocument cachedDoc = getIfPresent(id);
            if (cachedDoc == null || cachedDoc == NodeDocument.NULL) {
                newerDoc = doc;
                putInternal(doc);
            } else {
                Long cachedModCount = cachedDoc.getModCount();
                Long modCount = doc.getModCount();

                if (cachedModCount == null || modCount == null) {
                    throw new IllegalStateException("Missing " + Document.MOD_COUNT);
                }

                if (modCount > cachedModCount) {
                    newerDoc = doc;
                    putInternal(doc);
                } else {
                    newerDoc = cachedDoc;
                }
            }
        } finally {
            lock.unlock();
        }
        return newerDoc;
    }

    /**
     * Puts document into cache iff no entry with the given key is cached
     * already. This operation is atomic.
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

        String id = doc.getId();

        // make sure we only cache the document if it wasn't
        // changed and cached by some other thread in the
        // meantime. That is, use get() with a Callable,
        // which is only used when the document isn't there
        Lock lock = locks.acquire(id);
        try {
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
        } finally {
            lock.unlock();
        }
    }

    /**
     * Replaces the cached value if the old document is currently present in
     * the cache. If the {@code oldDoc} is not cached, nothing will happen. If
     * {@code oldDoc} does not match the document currently in the cache, then
     * the cached document is invalidated.
     *
     * @param oldDoc the old document
     * @param newDoc the replacement
     */
    public void replaceCachedDocument(@Nonnull final NodeDocument oldDoc,
                                      @Nonnull final NodeDocument newDoc) {
        if (newDoc == NodeDocument.NULL) {
            throw new IllegalArgumentException("doc must not be NULL document");
        }
        String key = oldDoc.getId();
        Lock lock = locks.acquire(key);
        try {
            NodeDocument cached = getIfPresent(key);
            if (cached != null) {
                if (Objects.equal(cached.getModCount(), oldDoc.getModCount())) {
                    putInternal(newDoc);
                } else {
                    // the cache entry was modified by some other thread in
                    // the meantime. the updated cache entry may or may not
                    // include this update. we cannot just apply our update
                    // on top of the cached entry.
                    // therefore we must invalidate the cache entry
                    invalidate(key);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return keys stored in cache
     */
    public Iterable<CacheValue> keys() {
        return Iterables.concat(nodeDocumentsCache.asMap().keySet(), prevDocumentsCache.asMap().keySet());
    }

    /**
     * @return values stored in cache
     */
    public Iterable<NodeDocument> values() {
        return Iterables.concat(nodeDocumentsCache.asMap().values(), prevDocumentsCache.asMap().values());
    }

    public Iterable<CacheStats> getCacheStats() {
        return Lists.newArrayList(nodeDocumentsCacheStats, prevDocumentsCacheStats);
    }

    @Override
    public void close() throws IOException {
        if (prevDocumentsCache instanceof Closeable) {
            ((Closeable) prevDocumentsCache).close();
        }
        if (nodeDocumentsCache instanceof Closeable) {
            ((Closeable) nodeDocumentsCache).close();
        }
    }

    //----------------------------< internal >----------------------------------

    /**
     * Puts a document into the cache without acquiring a lock.
     *
     * @param doc the document to put into the cache.
     */
    protected final void putInternal(@Nonnull NodeDocument doc) {
        if (isLeafPreviousDocId(doc.getId())) {
            prevDocumentsCache.put(new StringValue(doc.getId()), doc);
        } else {
            nodeDocumentsCache.put(new StringValue(doc.getId()), doc);
        }
    }
}
