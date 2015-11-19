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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

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

    public NodeDocumentCache(DocumentMK.Builder builder, DocumentStore docStore) {
        this.docStore = docStore;
        nodesCache = builder.buildDocumentCache(docStore);
        cacheStats = new CacheStats(nodesCache, "Document-Documents", builder.getWeigher(),
                builder.getDocumentCacheSize());
    }

    public void invalidate(String key) {
        nodesCache.invalidate(new StringValue(key));
    }

    public int invalidateOutdated(Map<String, Number> modCounts) {
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

    public NodeDocument getIfPresent(String key) {
        return nodesCache.getIfPresent(new StringValue(key));
    }

    public NodeDocument get(String key, Callable<NodeDocument> valueLoader) throws ExecutionException {
        return nodesCache.get(new StringValue(key), valueLoader);
    }

    public void put(NodeDocument doc) {
        if (doc != NodeDocument.NULL) {
            nodesCache.put(new StringValue(doc.getId()), doc);
        }
    }

    /**
     * Unconditionally puts a document into the cache. The document put into the
     * cache is {@code oldDoc} with the {@code updateOp} applied.
     *
     * @param oldDoc how the document looked before the update.
     * @param updateOp the update just applied to the document.
     */
    public void putToCache(@Nonnull NodeDocument oldDoc,
                           @Nonnull UpdateOp updateOp) {
        NodeDocument newDoc = Collection.NODES.newDocument(docStore);
        oldDoc.deepCopy(newDoc);
        UpdateUtils.applyChanges(newDoc, updateOp, comparator);
        newDoc.seal();
        put(newDoc);
    }

    /**
     * Applies an update to the nodes cache. This method does not acquire
     * a lock for the document.
     *
     * @param oldDoc the old document.
     * @param updateOp the update operation.
     */
    public void updateCache(@Nonnull NodeDocument oldDoc,
                            @Nonnull UpdateOp updateOp) {
        // cache the new document
        checkNotNull(oldDoc);
        checkNotNull(updateOp);
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
        if (Objects.equal(cached.getModCount(), oldDoc.getModCount())) {
            NodeDocument newDoc = Collection.NODES.newDocument(docStore);
            oldDoc.deepCopy(newDoc);

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
     * Adds a document to the {@link #nodesCache} iff there is no document
     * in the cache with the document key.
     *
     * @param doc the document to add to the cache.
     * @return either the given <code>doc</code> or the document already present
     *          in the cache.
     */
    @Nonnull
    public NodeDocument addToCache(@Nonnull final NodeDocument doc) {
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
