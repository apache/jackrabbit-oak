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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.jackrabbit.guava.common.collect.AbstractIterator;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.linkedList.FlatFileBufferLinkedList;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.linkedList.NodeStateEntryList;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.linkedList.PersistedLinkedList;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.linkedList.PersistedLinkedListV2;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.ConfigHelper;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Set;

import static org.apache.jackrabbit.guava.common.collect.Iterators.concat;
import static org.apache.jackrabbit.guava.common.collect.Iterators.singletonIterator;

class FlatFileStoreIterator extends AbstractIterator<NodeStateEntry> implements Iterator<NodeStateEntry>, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(FlatFileStoreIterator.class);

    static final String BUFFER_MEM_LIMIT_CONFIG_NAME = "oak.indexer.memLimitInMB";
    // by default, use the PersistedLinkedList
    private static final int DEFAULT_BUFFER_MEM_LIMIT_IN_MB = 0;

    public static final String PERSISTED_LINKED_LIST_CACHE_SIZE = "oak.indexer.persistedLinkedList.cacheSize";
    public static final int DEFAULT_PERSISTED_LINKED_LIST_CACHE_SIZE = 1000;

    public static final String PERSISTED_LINKED_LIST_V2_CACHE_SIZE = "oak.indexer.persistedLinkedListV2.cacheSize";
    public static final int DEFAULT_PERSISTED_LINKED_LIST_V2_CACHE_SIZE = 10000;

    public static final String PERSISTED_LINKED_LIST_V2_MEMORY_CACHE_SIZE_MB = "oak.indexer.persistedLinkedListV2.cacheMaxSizeMB";
    public static final int DEFAULT_PERSISTED_LINKED_LIST_V2_MEMORY_CACHE_SIZE_MB = 8;

    public static final String PERSISTED_LINKED_LIST_USE_V2 = "oak.indexer.persistedLinkedList.useV2";
    public static final boolean DEFAULT_PERSISTED_LINKED_LIST_USE_V2 = false;

    private final Iterator<NodeStateEntry> baseItr;
    private final NodeStateEntryList buffer;
    private final Set<String> preferredPathElements;

    private NodeStateEntry current;
    private int maxBufferSize;
    private long maxBufferSizeBytes;

    public FlatFileStoreIterator(BlobStore blobStore, String fileName, Iterator<NodeStateEntry> baseItr, Set<String> preferredPathElements) {
        this(blobStore, fileName, baseItr, preferredPathElements,
                Integer.getInteger(BUFFER_MEM_LIMIT_CONFIG_NAME, DEFAULT_BUFFER_MEM_LIMIT_IN_MB));
    }

    public FlatFileStoreIterator(BlobStore blobStore, String fileName, Iterator<NodeStateEntry> baseItr, Set<String> preferredPathElements, int memLimitConfig) {
        this.baseItr = baseItr;
        this.preferredPathElements = preferredPathElements;

        if (memLimitConfig == 0) {
            LOG.info("Using a key-value store buffer: {}", fileName);
            NodeStateEntryReader reader = new NodeStateEntryReader(blobStore);
            NodeStateEntryWriter writer = new NodeStateEntryWriter(blobStore);
            boolean usePersistedLinkedListV2 = ConfigHelper.getSystemPropertyAsBoolean(PERSISTED_LINKED_LIST_USE_V2, DEFAULT_PERSISTED_LINKED_LIST_USE_V2);
            if (usePersistedLinkedListV2) {
                int cacheSizeMB = ConfigHelper.getSystemPropertyAsInt(PERSISTED_LINKED_LIST_V2_MEMORY_CACHE_SIZE_MB, DEFAULT_PERSISTED_LINKED_LIST_V2_MEMORY_CACHE_SIZE_MB);
                int cacheSize = ConfigHelper.getSystemPropertyAsInt(PERSISTED_LINKED_LIST_V2_CACHE_SIZE, DEFAULT_PERSISTED_LINKED_LIST_V2_CACHE_SIZE);
                this.buffer = new PersistedLinkedListV2(fileName, writer, reader, cacheSize, cacheSizeMB);
            } else {
                int cacheSize = ConfigHelper.getSystemPropertyAsInt(PERSISTED_LINKED_LIST_CACHE_SIZE, DEFAULT_PERSISTED_LINKED_LIST_CACHE_SIZE);
                this.buffer = new PersistedLinkedList(fileName, writer, reader, cacheSize);
            }
        } else if (memLimitConfig < 0) {
            LOG.info("Setting buffer memory limit unbounded");
            this.buffer = new FlatFileBufferLinkedList();
        } else {
            LOG.info("Setting buffer memory limit to {} MBs", memLimitConfig);
            this.buffer = new FlatFileBufferLinkedList(memLimitConfig * 1024L * 1024L);
        }
    }

    int getBufferSize() {
        return buffer.size();
    }

    long getBufferMemoryUsage() {
        return buffer.estimatedMemoryUsage();
    }

    @Override
    protected NodeStateEntry computeNext() {
        //TODO Add some checks on expected ordering
        current = computeNextEntry();
        if (current == null) {
            LOG.info("Max buffer size in complete traversal is {} / {} bytes ({})",
                    maxBufferSize, maxBufferSizeBytes, IOUtils.humanReadableByteCountBin(maxBufferSizeBytes));
            return endOfData();
        } else {
            return current;
        }
    }

    private NodeStateEntry computeNextEntry() {
        if (!buffer.isEmpty()) {
            if (buffer.size() > maxBufferSize || buffer.estimatedMemoryUsage() > maxBufferSizeBytes) {
                // We need to take the max of the current max and of the current value, because the two metrics above
                // can and will increase and decrease independently. For instance, consider the following
                //
                // buffer.size() = 10, buffer.estimatedMemoryUsage() = 1000  - 10 elements using 1000 bytes of memory.
                // buffer.size() = 1, buffer.estimatedMemoryUsage() = 2000   - 1 element using 2000 bytes of memory.
                //
                // So in this case, we want to update the max memory estimate but leave the max size at 10.
                maxBufferSize = Math.max(buffer.size(), maxBufferSize);
                maxBufferSizeBytes = Math.max(buffer.estimatedMemoryUsage(), maxBufferSizeBytes);
                LOG.info("Max buffer size changed {} (estimated memory usage: {} bytes) for path {}",
                        maxBufferSize, maxBufferSizeBytes, current.getPath());
            }
            NodeStateEntry e = buffer.remove();
            return wrapIfNeeded(e);
        }
        if (baseItr.hasNext()) {
            return wrap(baseItr.next());
        }
        return null;
    }

    private NodeStateEntry wrap(NodeStateEntry baseEntry) {
        NodeState state = new LazyChildrenNodeState(baseEntry.getNodeState(),
                new ChildNodeStateProvider(getEntries(), baseEntry.getPath(), preferredPathElements));
        return new NodeStateEntry(state, baseEntry.getPath(), baseEntry.estimatedMemUsage(), 0, "");
    }

    private Iterable<NodeStateEntry> getEntries() {
        return () -> concat(singletonIterator(current), queueIterator());
    }

    private Iterator<NodeStateEntry> queueIterator() {
        Iterator<NodeStateEntry> qitr = buffer.iterator();
        return new AbstractIterator<>() {
            @Override
            protected NodeStateEntry computeNext() {
                //If queue is empty try to append by getting entry from base
                if (!qitr.hasNext() && baseItr.hasNext()) {
                    buffer.add(wrap(baseItr.next()));
                }
                if (qitr.hasNext()) {
                    return wrapIfNeeded(qitr.next());
                }
                return endOfData();
            }
        };
    }

    private NodeStateEntry wrapIfNeeded(NodeStateEntry e) {
        if (buffer instanceof PersistedLinkedList || buffer instanceof PersistedLinkedListV2) {
            // for the PersistedLinkedList, the entries from the iterators are
            // de-serialized and don't contain the LazyChildrenNodeState -
            // so we need to wrap them
            return wrap(e);
        }
        // if not a PersistedLinkedList, no wrapping is needed, as the in-memory linked list
        // already contains the LazyChildrenNodeState
        // (actually wrapping would work just fine - it's just not needed)
        return e;
    }

    @Override
    public void close() {
        buffer.close();
    }
}
