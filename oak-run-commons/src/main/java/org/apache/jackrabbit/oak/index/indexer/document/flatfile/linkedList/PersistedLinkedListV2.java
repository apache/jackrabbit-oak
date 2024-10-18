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
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
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
 *     <li>cacheSize: the maximum number of elements to keep in the in-memory cache</li>
 *     <li>cacheSizeMB: the maximum size of the in-memory cache in MB</li>
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
 * To understand why, let's assume we want to traverse the children of a node P that is at line/position 100 in the FFS.
 * When we call getChildren on P, this creates an iterator that scans from position 100 for all the children.
 * If we call recursively getChildren on a child C of P, this will also create a new iterator that will start also at
 * position 100. Therefore, the iterators will frequently scan from 100 down. Let's assume the cache can only hold 10
 * nodes and that we use a policy of moving to the cache the last node that was accessed. Then, if an iterator scans
 * from 100 to, let's say 150, when it finishes iterating, the nodes 141 to 150 will be the only ones in the cache.
 * The next iterator that is scanning from 100 will have cache misses for all the nodes until 140. And this will repeat
 * for every new iterator. On the other hand, if we keep in the cache the nodes from 100 to 109, every iterator starting
 * from 100 will at least have 10 cache hits, which is better than having a cache miss for all elements.
 */
public class PersistedLinkedListV2 implements NodeStateEntryList {

    private final static Logger LOG = LoggerFactory.getLogger(PersistedLinkedListV2.class);

    private static final String COMPACT_STORE_MILLIS_NAME = "oak.indexer.linkedList.compactMillis";

    private final HashMap<Long, NodeStateEntry> cache = new HashMap<>(512);
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
    // Total entries in the list
    private long totalEntries;
    private long lastLog;
    private long lastCompact;

    // Estimation of the cache size
    private long cacheSizeEstimationBytes;
    // If the sanity check on the cache size estimation triggers, log it only once to avoid filling up the logs with
    // the same message
    private boolean loggedCacheSizeEstimationMismatch = false;

    // Metrics
    private long cacheHits;
    private long cacheMisses;
    private long storeWrites; // Each cache miss is a read from the store, so no need for a storeRead counter
    private long peakCacheSizeBytes;
    private long peakCacheSize;

    /**
     * @param cacheSize   the maximum number of elements to keep in the in-memory cache
     * @param cacheSizeMB the maximum size of the in-memory cache in MB
     */
    public PersistedLinkedListV2(String fileName, NodeStateEntryWriter writer, NodeStateEntryReader reader, int cacheSize, int cacheSizeMB) {
        this.cacheSizeLimit = cacheSize;
        this.cacheSizeLimitBytes = ((long) cacheSizeMB) * 1024 * 1024;
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
        Validate.checkArgument(item != null, "Can't add null to the list");
        long index = tailIndex++;
        addEntryToCache(index, item);
    }

    @Override
    public boolean isEmpty() {
        return totalEntries == 0;
    }

    @Override
    public Iterator<NodeStateEntry> iterator() {
        return new NodeIterator(headIndex);
    }

    @Override
    public NodeStateEntry remove() {
        Validate.checkState(!isEmpty(), "Cannot remove item from empty list");
        Long boxedHeadIndex = headIndex;
        NodeStateEntry entryRemoved = cache.remove(boxedHeadIndex);
        if (entryRemoved == null) {
            String mapEntry = map.remove(boxedHeadIndex);
            if (mapEntry == null) {
                throw new IllegalStateException("Entry not found in cache or in store: " + boxedHeadIndex);
            }
            cacheMisses++;
            entryRemoved = reader.read(mapEntry);
        } else {
            cacheHits++;
            cacheSizeEstimationBytes -= entryRemoved.estimatedMemUsage();
        }

        headIndex++;
        totalEntries--;
        if (totalEntries == 0) {
            if (cacheSizeEstimationBytes != 0 && !loggedCacheSizeEstimationMismatch) {
                loggedCacheSizeEstimationMismatch = true;
                LOG.warn("Total entries is 0, but cache size estimation is not zero: {}. Metrics: {}", cacheSizeEstimationBytes, formatMetrics());
            }
            map.clear();
            cache.clear();
        }
        return entryRemoved;
    }

    private NodeStateEntry get(Long index) {
        NodeStateEntry result = cache.get(index);
        if (result == null) {
            cacheMisses++;
            String s = map.get(index);
            result = reader.read(s);
            LOG.trace("Cache miss: {}={}", index, result.getPath());
        } else {
            cacheHits++;
        }
        return result;
    }

    private void addEntryToCache(long index, NodeStateEntry entry) {
        long now = System.currentTimeMillis();
        long newCacheSizeBytes = cacheSizeEstimationBytes + entry.estimatedMemUsage();
        if (cache.size() >= cacheSizeLimit || newCacheSizeBytes > cacheSizeLimitBytes) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Mem cache size {}/{} or byte size {}/{} would exceed maximum. Writing to persistent map: {} = {}, entry size: {}",
                        newCacheSizeBytes, cacheSizeLimitBytes, cache.size() + 1, cacheSizeLimit, index, entry.getPath(), entry.estimatedMemUsage());
            }
            storeWrites++;
            map.put(index, writer.toString(entry));
            long storeSizeBytes = store.getFileStore().size();
            boolean compactNow = now >= lastCompact + compactStoreMillis;
            if (compactNow && storeSizeBytes > 10L * 1000 * 1000) {
                // compact once a minute, if larger than 10 MB
                LOG.info("Compacting. Current size: {}", storeSizeBytes);
                store.close();
                MVStoreTool.compact(storeFileName, true);
                openStore();
                lastCompact = System.currentTimeMillis();
                LOG.info("Finished compaction. Previous size: {}, new size={} bytes", storeSizeBytes, store.getFileStore().size());
            }
        } else {
            // new element fits in the in-memory cache. Do not write it to the persistent store.
            cache.put(index, entry);
            cacheSizeEstimationBytes = newCacheSizeBytes;
            if (cacheSizeEstimationBytes > peakCacheSizeBytes) {
                peakCacheSizeBytes = cacheSizeEstimationBytes;
            }
            if (cache.size() > peakCacheSize) {
                peakCacheSize = cache.size();
            }
        }
        totalEntries++;
        if (index % (1024 * 1000) == 0 || now >= lastLog + 10000) { // Print every million entries or every 10 seconds
            LOG.info("Metrics: {}", formatMetrics());
            lastLog = now;
        }
    }

    @Override
    public int size() {
        return (int) totalEntries;
    }

    @Override
    public void close() {
        store.close();
        LOG.info("Closing. Metrics: {}", formatMetrics());
    }

    public String formatMetrics() {
        return "totalEntriesAdded: " + headIndex + ", totalEntriesInList: " + totalEntries + ", mapSize: " + map.sizeAsLong() +
                ", mapSizeBytes: " + store.getFileStore().size() + " (" + IOUtils.humanReadableByteCountBin(store.getFileStore().size()) + ")" +
                ", cacheHits: " + cacheHits + ", cacheMisses: " + cacheMisses + ", storeWrites: " + storeWrites +
                ", peakCacheSize: " + peakCacheSize +
                ", peakCacheSizeBytes: " + peakCacheSizeBytes + " (" + IOUtils.humanReadableByteCountBin(peakCacheSizeBytes) + ")";
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
            return get(index++);
        }
    }

    public long getCacheHits() {
        return cacheHits;
    }

    public long getCacheMisses() {
        return cacheMisses;
    }

    public long getStoreWrites() {
        return storeWrites;
    }

    public long getPeakCacheSizeBytes() {
        return peakCacheSizeBytes;
    }

    public long getPeakCacheSize() {
        return peakCacheSize;
    }
}
