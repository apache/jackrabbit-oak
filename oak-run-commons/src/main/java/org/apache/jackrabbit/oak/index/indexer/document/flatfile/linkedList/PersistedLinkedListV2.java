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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.linkedList;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.guava.common.base.Preconditions;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreTool;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A persistent linked list that internally uses the MVStore. This list keeps an in-memory cache, writing to the
 * persistent store the nodes only when the cache is full. The in-memory cache is limited by two parameters:
 *
 * <ul>
 *     <li>maxCacheSize: the maximum number of elements to keep in the in-memory cache</li>
 *     <li>maxCacheSizeMB: the maximum size of the in-memory cache in MB</li>
 * </ul>
 * <p>
 * The recommended configuration is to rely on the total memory usage to limit the cache, giving as much memory as
 * available in the JVM, and setting a very high limit for the number of elements. A cache miss has a very high cost,
 * so it should be avoided as much as possible.
 * <p>
 * <p>
 * Each element is stored either in the cache or in the persistent store, but not in both. And elements are not moved
 * between the two tiers, so even if there is a cache miss, that element will remain in the persistent store.
 * For the access pattern of the indexer, this policy has a lower rate of cache misses than if we move to the cache an
 * element after a miss.
 * <p>
 * Let's assume we want to traverse the children of a node P that is at line/position 100 in the FFS. When we call
 * getChildren on P, this creates an iterator that scans from position 100 for all the children. If we call recursively
 * getChildren on a child C of P, this will also create a new iterator that will start also at position 100. Therefore
 * the iterators will frequently scan from 100 down. Let's assume the cache can only hold 10 nodes and that we use a
 * policy of moving to the cache the last node that was accessed. Then if an iterator scans from 100 to, let's say 150,
 * it will finish with the nodes 141 to 150 in the cache. The next iterator that is scanning from 100 will have cache
 * misses for all the nodes until 140. And this will repeat for every new iterator. On the other hand, if we keep in the
 * cache the nodes from 100 to 109,
 */
public class PersistedLinkedListV2 implements NodeStateEntryList {

    private final static Logger LOG = LoggerFactory.getLogger(PersistedLinkedListV2.class);

    private static final String COMPACT_STORE_MILLIS_NAME = "oak.indexer.linkedList.compactMillis";

    private final HashMap<Long, NodeStateEntry> cache = new HashMap<>(1024);
    private final int compactStoreMillis = Integer.getInteger(COMPACT_STORE_MILLIS_NAME, 60 * 1000);
    private final NodeStateEntryWriter writer;
    private final NodeStateEntryReader reader;
    private final String storeFileName;
    private final long cacheSizeLimitBytes;
    private final long cacheSizeLimit;

    private MVStore store;
    private MVMap<Long, String> map;
    private long headIndex;
    private long tailIndex;
    private long size;
    private long lastLog;
    private long lastCompact;

    // Metrics
    private long cacheHits;
    private long cacheMisses;
    private long storeWrites;
    private long cacheSizeEstimationBytes;
    private long peakCacheSizeBytes;
    private long peakCacheSize;

    /**
     * @param maxCacheSize   the maximum number of elements to keep in the in-memory cache
     * @param maxCacheSizeMB the maximum size of the in-memory cache in MB
     */
    public PersistedLinkedListV2(String fileName, NodeStateEntryWriter writer, NodeStateEntryReader reader, int maxCacheSize, int maxCacheSizeMB) {
        this.cacheSizeLimit = maxCacheSize;
        this.cacheSizeLimitBytes = ((long) maxCacheSizeMB) * 1024 * 1024;
        this.storeFileName = fileName;
        LOG.info("Opening store {}", fileName);
        File oldFile = new File(fileName);
        if (oldFile.exists()) {
            LOG.info("Deleting {}", fileName);
            try {
                FileUtils.forceDelete(oldFile);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        openStore();
        this.writer = writer;
        this.reader = reader;
        lastCompact = System.currentTimeMillis();
    }

    private void openStore() {
        store = MVStore.open(storeFileName);
        map = store.openMap("list");
    }

    @Override
    public void add(@NotNull NodeStateEntry item) {
        Preconditions.checkArgument(item != null, "Can't add null to the list");
        Long index = tailIndex++;
        addEntryToCache(index, item);
        long sizeBytes = store.getFileStore().size();
        long now = System.currentTimeMillis();
        if (now >= lastLog + 10000) {
            LOG.info("Entries: {} map size: {} file size: {} bytes", size, map.sizeAsLong(), sizeBytes);
            lastLog = now;
        }
        boolean compactNow = now >= lastCompact + compactStoreMillis;
        if (compactNow && sizeBytes > 10L * 1000 * 1000) {
            // compact once a minute, if larger than 10 MB
            LOG.info("Compacting...");
            store.close();
            MVStoreTool.compact(storeFileName, true);
            openStore();
            lastCompact = System.currentTimeMillis();
            LOG.info("New size={} bytes", store.getFileStore().size());
        }
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public Iterator<NodeStateEntry> iterator() {
        return new NodeIterator(headIndex);
    }

    @Override
    public NodeStateEntry remove() {
        Preconditions.checkState(!isEmpty(), "Cannot remove item from empty list");
        Long boxedHeadIndex = headIndex;
        NodeStateEntry ret = get(boxedHeadIndex);
        NodeStateEntry cacheEntry = cache.remove(boxedHeadIndex);
        if (cacheEntry == null) {
            String mapEntry = map.remove(boxedHeadIndex);
            if (mapEntry == null) {
                throw new IllegalStateException("Cache entry found but not in map");
            }
        } else {
            cacheSizeEstimationBytes -= cacheEntry.estimatedMemUsage();
        }

        headIndex++;
        size--;
        if (size == 0) {
            map.clear();
            cache.clear();
        }
        return ret;
    }

    private NodeStateEntry get(Long index) {
//        LOG.info("Getting index {}", index);
        NodeStateEntry result = cache.get(index);
        if (result == null) {
            cacheMisses++;
            String s = map.get(index);
            result = reader.read(s);
//            LOG.info("Cache miss: {}={}", index, result.getPath());
        } else {
//            LOG.info("Cache hit: {}={}", index, result.getPath());
            cacheHits++;
        }
        return result;
    }

    private void addEntryToCache(Long index, NodeStateEntry entry) {
        if (index % (1024 * 1000) == 0) { // Print every million entries
            LOG.info("Metrics: {}", formatMetrics());
        }
        long newCacheSizeBytes = cacheSizeEstimationBytes + entry.estimatedMemUsage();
        if (cache.size() == cacheSizeLimit || newCacheSizeBytes > cacheSizeLimitBytes) {
            storeWrites++;
            LOG.debug("Mem cache size {}/{} or byte size {}/{} would exceed maximum. Writing to persistent map: {} = {}, entry size: {}",
                    newCacheSizeBytes, cacheSizeLimitBytes, cache.size() + 1, cacheSizeLimit, index, entry.getPath(), entry.estimatedMemUsage());
            map.put(index, writer.toString(entry));
        } else {
            cache.put(index, entry);
            cacheSizeEstimationBytes = newCacheSizeBytes;
            if (cacheSizeEstimationBytes > peakCacheSizeBytes) {
                peakCacheSizeBytes = cacheSizeEstimationBytes;
            }
            if (cache.size() > peakCacheSize) {
                peakCacheSize = cache.size();
            }
            if (cache.size() % 1024 == 0) {
                LOG.info("Entries added: {}, Entries in cache: {}, cacheSizeBytes: {}, map size: {} file size: {} bytes",
                        index, cache.size(), cacheSizeEstimationBytes, map.sizeAsLong(), store.getFileStore().size());
            }
        }
        size++;
    }

    @Override
    public int size() {
        return (int) size;
    }

    @Override
    public void close() {
        store.close();
        LOG.info("Closing. Metrics: {}", formatMetrics());
    }

    public String formatMetrics() {
        return String.format("cacheHits: %d, cacheMisses %d, storeWrites: %d, peakCacheSize: %d, peakCacheSizeMB: %d",
                cacheHits, cacheMisses, storeWrites, peakCacheSize, peakCacheSizeBytes);
    }

    @Override
    public long estimatedMemoryUsage() {
        return cacheSizeEstimationBytes;
    }

    /**
     * A node iterator over this list.
     */
    final class NodeIterator implements Iterator<NodeStateEntry> {

        private long index;

        NodeIterator(long index) {
//            LOG.info("New iterator from index {}", index);
            this.index = index;
        }

        @Override
        public boolean hasNext() {
            return index < tailIndex;
        }

        @Override
        public NodeStateEntry next() {
            if (index < headIndex || index >= tailIndex) {
                throw new IllegalStateException();
            }
//            LOG.info("{} next index {}", System.identityHashCode(this), index);
            return get(index++);
        }
    }
}
