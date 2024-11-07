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

import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isLeafPreviousDocId;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.function.Predicate;

import org.apache.jackrabbit.guava.common.cache.Cache;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.locks.NodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.jackrabbit.guava.common.collect.Iterables;

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

    private final List<CacheChangesTracker> changeTrackers;

    public NodeDocumentCache(@NotNull Cache<CacheValue, NodeDocument> nodeDocumentsCache,
                             @NotNull CacheStats nodeDocumentsCacheStats,
                             @NotNull Cache<StringValue, NodeDocument> prevDocumentsCache,
                             @NotNull CacheStats prevDocumentsCacheStats,
                             @NotNull NodeDocumentLocks locks) {
        this.nodeDocumentsCache = nodeDocumentsCache;
        this.nodeDocumentsCacheStats = nodeDocumentsCacheStats;
        this.prevDocumentsCache = prevDocumentsCache;
        this.prevDocumentsCacheStats = prevDocumentsCacheStats;
        this.locks = locks;
        this.changeTrackers = new CopyOnWriteArrayList<>();
    }

    /**
     * Invalidate document with given key.
     *
     * @param key to invalidate
     */
    public void invalidate(@NotNull String key) {
        Lock lock = locks.acquire(key);
        try {
            if (isLeafPreviousDocId(key)) {
                prevDocumentsCache.invalidate(new StringValue(key));
            } else {
                nodeDocumentsCache.invalidate(new StringValue(key));
            }

            internalMarkChanged(key, null);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Mark that the document with the given key is being changed.
     *
     * @param key to mark
     */
    public void markChanged(@NotNull String key) {
        Lock lock = locks.acquire(key);
        try {
            internalMarkChanged(key, null);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Invalidate document with given keys iff their modification stamps are
     * different as passed in the map.
     *
     * @param modStamps map where key is the document id and the value is the
     *                  modification stamps.
     * @return number of invalidated entries
     */
    public int invalidateOutdated(@NotNull Map<String, ModificationStamp> modStamps) {
        int invalidatedCount = 0;
        for (Entry<String, ModificationStamp> e : modStamps.entrySet()) {
            String id = e.getKey();
            ModificationStamp stamp = e.getValue();
            NodeDocument doc = getIfPresent(id);
            if (doc == null) {
                continue;
            }
            if (!Objects.equals(stamp.modCount, doc.getModCount())
                    || !Objects.equals(stamp.modified, doc.getModified())) {
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
    @Nullable
    public NodeDocument getIfPresent(@NotNull String key) {
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
    @NotNull
    public NodeDocument get(@NotNull final String key, @NotNull final Callable<NodeDocument> valueLoader)
            throws ExecutionException {
        Callable<NodeDocument> wrappedLoader = new Callable<NodeDocument>() {
            @Override
            public NodeDocument call() throws Exception {
                for (CacheChangesTracker tracker : changeTrackers) {
                    tracker.invalidateDocument(key);
                }
                return valueLoader.call();
            }
        };
        Lock lock = locks.acquire(key);
        try {
            if (isLeafPreviousDocId(key)) {
                return prevDocumentsCache.get(new StringValue(key), wrappedLoader);
            } else {
                return nodeDocumentsCache.get(new StringValue(key), wrappedLoader);
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
    public void put(@NotNull NodeDocument doc) {
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
    @NotNull
    public NodeDocument putIfNewer(@NotNull final NodeDocument doc) {
        if (doc == NodeDocument.NULL) {
            throw new IllegalArgumentException("doc must not be NULL document");
        }
        doc.seal();

        NodeDocument newerDoc;

        String id = doc.getId();
        Lock lock = locks.acquire(id);
        try {
            NodeDocument cachedDoc = getIfPresent(id);
            if (isNewer(cachedDoc, doc)) {
                newerDoc = doc;
                putInternal(doc);
            } else {
                newerDoc = cachedDoc;
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
    @NotNull
    public NodeDocument putIfAbsent(@NotNull final NodeDocument doc) {
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
    public void replaceCachedDocument(@NotNull final NodeDocument oldDoc,
                                      @NotNull final NodeDocument newDoc) {
        if (newDoc == NodeDocument.NULL) {
            throw new IllegalArgumentException("doc must not be NULL document");
        }
        String key = oldDoc.getId();
        Lock lock = locks.acquire(key);
        try {
            NodeDocument cached = getIfPresent(key);
            if (cached != null) {
                if (Objects.equals(cached.getModCount(), oldDoc.getModCount())) {
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
        return Arrays.asList(nodeDocumentsCacheStats, prevDocumentsCacheStats);
    }

    @Override
    public void close() throws IOException {
        prevDocumentsCache.invalidateAll();
        if (prevDocumentsCache instanceof Closeable) {
            ((Closeable) prevDocumentsCache).close();
        }
        nodeDocumentsCache.invalidateAll();
        if (nodeDocumentsCache instanceof Closeable) {
            ((Closeable) nodeDocumentsCache).close();
        }
    }

    /**
     * Registers a new CacheChangesTracker that records all puts and
     * invalidations related to children of the given parent.
     *
     * @param fromKey only keys larger than this key will be tracked
     * @param toKey only keys smaller than this key will be tracked
     * @return new tracker
     */
    public CacheChangesTracker registerTracker(final String fromKey, final String toKey) {
        final int bloomFilterSize;
        if (toKey.equals(NodeDocument.MAX_ID_VALUE)) {
            bloomFilterSize = CacheChangesTracker.ENTRIES_OPEN;
        } else {
            bloomFilterSize = CacheChangesTracker.ENTRIES_SCOPED;
        }
        return new CacheChangesTracker(new Predicate<String>() {
            @Override
            public boolean test(@Nullable String input) {
                return input != null && fromKey.compareTo(input) < 0 && toKey.compareTo(input) > 0;
            }

            @Override
            public String toString() {
                return String.format("key range: <%s, %s>", fromKey, toKey);
            }
        }, changeTrackers, bloomFilterSize);
    }

    /**
     * Registers a new CacheChangesTracker that records all puts and
     * invalidations related to the given documents
     *
     * @param keys these documents will be tracked
     * @return new tracker
     */
    public CacheChangesTracker registerTracker(final Set<String> keys) {
        return new CacheChangesTracker(new Predicate<String>() {
            @Override
            public boolean test(@Nullable String input) {
                return input != null && keys.contains(input);
            }

            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder("key set [");
                Iterator<String> it = keys.iterator();
                int i = 0;
                while (it.hasNext() && i++ < 3) {
                    builder.append(it.next());
                    if (it.hasNext()) {
                        builder.append(", ");
                    }
                }
                if (it.hasNext()) {
                    builder.append("...");
                }
                builder.append("]").append(" (").append(keys.size()).append(" elements)");
                return builder.toString();
            }
        }, changeTrackers, CacheChangesTracker.ENTRIES_SCOPED);
    }

    /**
     * Updates the cache with all the documents that:
     *
     * (1) currently have their older versions in the cache or
     * (2) have been neither put nor invalidated during the tracker lifetime.
     *
     * We can't cache documents that has been invalidated during the tracker
     * lifetime, as it's possible that the invalidated version was newer than
     * the one passed in the docs parameter.
     *
     * If the document has been added during the tracker lifetime, but it is not
     * present in the cache anymore, it means it may have been evicted, so we
     * can't re-add it for the same reason as above.
     *
     * @param tracker
     *            used to decide whether the docs should be put into cache
     * @param docs
     *            to put into cache
     */
    public void putNonConflictingDocs(CacheChangesTracker tracker, Iterable<NodeDocument> docs) {
        for (NodeDocument d : docs) {
            if (d == null || d == NodeDocument.NULL) {
                continue;
            }
            String id = d.getId();
            Lock lock = locks.acquire(id);
            try {
                NodeDocument cachedDoc = getIfPresent(id);
                if (cachedDoc != null && isNewer(cachedDoc, d)) {
                    // if an old document is present in the cache, we can simply update it
                    putInternal(d, tracker);
                } else if (cachedDoc == null && !tracker.mightBeenAffected(id)) {
                    // if the document hasn't been invalidated or added during the tracker lifetime,
                    // we can put it as well
                    putInternal(d, tracker);
                } else {
                    // in all other cases we don't know if the current document
                    // is up-to-date. notify the other trackers and invalidate
                    // the cache
                    internalMarkChanged(id, tracker);
                    if (isLeafPreviousDocId(id)) {
                        prevDocumentsCache.invalidate(new StringValue(id));
                    } else {
                        nodeDocumentsCache.invalidate(new StringValue(id));
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    //----------------------------< internal >----------------------------------

    /**
     * Marks the document as potentially changed.
     *
     * @param key the document to be marked
     * @param trackerToSkip this tracker won't be updated. pass
     *         {@code null} to update all trackers.
     */
    private void internalMarkChanged(@NotNull String key,
                                     @Nullable CacheChangesTracker trackerToSkip) {
        for (CacheChangesTracker tracker : changeTrackers) {
            if (tracker == trackerToSkip) {
                continue;
            }
            tracker.invalidateDocument(key);
        }
    }

    /**
     * Puts a document into the cache without acquiring a lock. All trackers will
     * be updated.
     *
     * @param doc the document to put into the cache.
     */
    protected final void putInternal(@NotNull NodeDocument doc) {
        putInternal(doc, null);
    }

    /**
     * Puts a document into the cache without acquiring a lock. All trackers will
     * be updated, apart from the {@code trackerToSkip}.
     *
     * @param doc the document to put into the cache.
     * @param trackerToSkip this tracker won't be updated. pass {@code null} to update
     *                      all trackers.
     */
    protected final void putInternal(@NotNull NodeDocument doc, @Nullable CacheChangesTracker trackerToSkip) {
        if (isLeafPreviousDocId(doc.getId())) {
            prevDocumentsCache.put(new StringValue(doc.getId()), doc);
        } else {
            nodeDocumentsCache.put(new StringValue(doc.getId()), doc);
        }
        internalMarkChanged(doc.getId(), trackerToSkip);
    }

    /**
     * Check if the doc is more recent than the cachedDoc. If the cachedDoc
     * is {@code null} or {@code NodeDocument.NULL}, the doc will be considered
     * as more recent as well.
     *
     * @param cachedDoc the document already present in cache
     * @param doc the tested document
     * @return {@code true} iff the cacheDoc is null or older than the doc
     */
    private boolean isNewer(@Nullable NodeDocument cachedDoc, @NotNull NodeDocument doc) {
        if (cachedDoc == null || cachedDoc == NodeDocument.NULL) {
            return true;
        }

        Long cachedModCount = cachedDoc.getModCount();
        Long modCount = doc.getModCount();

        if (cachedModCount == null || modCount == null) {
            throw new IllegalStateException("Missing " + Document.MOD_COUNT);
        }

        return modCount > cachedModCount;
    }
}
