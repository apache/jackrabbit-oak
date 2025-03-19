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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
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

/**
 * A persistent linked list that internally uses the MVStore.
 */
public class PersistedLinkedList implements NodeStateEntryList {

    private final static Logger LOG = LoggerFactory.getLogger(PersistedLinkedList.class);

    private static final String COMPACT_STORE_MILLIS_NAME = "oak.indexer.linkedList.compactMillis";

    private final LinkedHashMap<Long, NodeStateEntry> cache;

    private final NodeStateEntryWriter writer;
    private final NodeStateEntryReader reader;
    private final String storeFileName;
    private final int compactStoreMillis = Integer.getInteger(COMPACT_STORE_MILLIS_NAME, 60 * 1000);

    private MVStore store;
    private MVMap<Long, String> map;

    private long headIndex;
    private long tailIndex;
    private long size;
    private long lastLog;
    private long lastCompact;
    private long cacheHits, cacheMisses;

    public PersistedLinkedList(String fileName, NodeStateEntryWriter writer, NodeStateEntryReader reader, int cacheSize) {
        LOG.info("Opening store {}", fileName);
        this.storeFileName = fileName;
        this.cache = new LinkedHashMap<>(cacheSize + 1, .75F, true) {
            @Override
            public boolean removeEldestEntry(Map.Entry<Long, NodeStateEntry> eldest) {
                return size() > cacheSize;
            }
        };
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
        String s = writer.toString(item);
        long index = tailIndex++;
        map.put(index, s);
        cache.put(index, item);
        size++;
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
        Validate.checkState(!isEmpty(), "Cannot remove item from empty list");
        NodeStateEntry ret = get(headIndex);
        map.remove(headIndex);
        cache.remove(headIndex);
        headIndex++;
        size--;
        if (size == 0) {
            map.clear();
            cache.clear();
        }
        return ret;
    }

    private NodeStateEntry get(long index) {
        NodeStateEntry result = cache.get(index);
        if (result == null) {
            cacheMisses++;
            String s = map.get(index);
            result = reader.read(s);
            cache.put(index, result);
        } else {
            cacheHits++;
        }
        return result;
    }

    @Override
    public int size() {
        return (int) size;
    }

    @Override
    public void close() {
        store.close();
        LOG.info("Cache hits {} misses {}", cacheHits, cacheMisses);
    }

    @Override
    public long estimatedMemoryUsage() {
        return 0;
    }

    /**
     * A node iterator over this list.
     */
    class NodeIterator implements Iterator<NodeStateEntry> {

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

}
