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

import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.singletonIterator;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Set;

import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry.NodeStateEntryBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.linkedList.FlatFileBufferLinkedList;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.linkedList.NodeStateEntryList;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.linkedList.PersistedLinkedList;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;

class FlatFileStoreIterator extends AbstractIterator<NodeStateEntry> implements Iterator<NodeStateEntry>, Closeable {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Iterator<NodeStateEntry> baseItr;
    private final NodeStateEntryList buffer;
    private NodeStateEntry current;
    private final Set<String> preferredPathElements;
    private int maxBufferSize;
    static final String BUFFER_MEM_LIMIT_CONFIG_NAME = "oak.indexer.memLimitInMB";

    // by default, use the PersistedLinkedList
    private static final int DEFAULT_BUFFER_MEM_LIMIT_IN_MB = 0;

    public FlatFileStoreIterator(BlobStore blobStore, String fileName, Iterator<NodeStateEntry> baseItr, Set<String> preferredPathElements) {
        this(blobStore, fileName, baseItr, preferredPathElements,
                Integer.getInteger(BUFFER_MEM_LIMIT_CONFIG_NAME, DEFAULT_BUFFER_MEM_LIMIT_IN_MB));
    }

    public FlatFileStoreIterator(BlobStore blobStore, String fileName, Iterator<NodeStateEntry> baseItr, Set<String> preferredPathElements, int memLimitConfig) {
        this.baseItr = baseItr;
        this.preferredPathElements = preferredPathElements;

        if (memLimitConfig == 0) {
            log.info("Using a key-value store buffer: {}", fileName);
            NodeStateEntryReader reader = new NodeStateEntryReader(blobStore);
            NodeStateEntryWriter writer = new NodeStateEntryWriter(blobStore);
            this.buffer = new PersistedLinkedList(fileName, writer, reader, 1000);
        } else if (memLimitConfig < 0) {
            log.info("Setting buffer memory limit unbounded");
            this.buffer = new FlatFileBufferLinkedList();
        } else {
            log.info("Setting buffer memory limit to {} MBs", memLimitConfig);
            this.buffer = new FlatFileBufferLinkedList(memLimitConfig * 1024L * 1024L);
        }
    }

    int getBufferSize(){
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
            log.info("Max buffer size in complete traversal is [{}]", maxBufferSize);
            return endOfData();
        } else {
            return current;
        }
    }

    private NodeStateEntry computeNextEntry() {
        if (buffer.size() > maxBufferSize) {
            maxBufferSize = buffer.size();
            log.info("Max buffer size changed {} (estimated memory usage: {} bytes) for path {}",
                    maxBufferSize, buffer.estimatedMemoryUsage(), current.getPath());
        }
        if (!buffer.isEmpty()) {
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
        return new NodeStateEntryBuilder(state, baseEntry.getPath()).withMemUsage(baseEntry.estimatedMemUsage()).build();
    }

    private Iterable<NodeStateEntry> getEntries() {
        return () -> concat(singletonIterator(current), queueIterator());
    }

    private Iterator<NodeStateEntry> queueIterator() {
        Iterator<NodeStateEntry> qitr = buffer.iterator();
        return new AbstractIterator<NodeStateEntry>() {
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
        if (buffer instanceof PersistedLinkedList) {
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
