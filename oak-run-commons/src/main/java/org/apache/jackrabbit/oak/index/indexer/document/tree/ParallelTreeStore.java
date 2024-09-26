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
package org.apache.jackrabbit.oak.index.indexer.document.tree;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStore;

/**
 * A wrapper around the tree store that only iterates over a subset of the
 * nodes. Each parallel tree store reads the next block (a block is a tiny
 * subset of the nodes). Once it has finished iterating over the block, it asks
 * the backend for the next block. The result is that the whole range is
 * covered, but it doesn't matter much which thread is how fast.
 */
public class ParallelTreeStore implements IndexStore {

    private final TreeStore backend;
    private final AtomicInteger openCount;

    ParallelTreeStore(TreeStore backend, AtomicInteger openCount) {
        this.backend = backend;
        this.openCount = openCount;
    }

    @Override
    public Iterator<NodeStateEntry> iterator() {
        return new Iterator<NodeStateEntry>() {

            private Iterator<NodeStateEntry> currentIterator = backend.nextSubsetIterator();

            @Override
            public boolean hasNext() {
                while (currentIterator != null && !currentIterator.hasNext()) {
                    currentIterator = backend.nextSubsetIterator();
                }
                return currentIterator != null;
            }

            @Override
            public NodeStateEntry next() {
                // hasNext() is needed to ensure currentIterator is changed when needed.
                // It might not have a next entry still.
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return currentIterator.next();
            }

        };
    }

    @Override
    public String getStorePath() {
        return backend.getStorePath();
    }

    @Override
    public long getEntryCount() {
        return backend.getEntryCount();
    }

    @Override
    public void setEntryCount(long entryCount) {
        backend.setEntryCount(entryCount);
    }

    @Override
    public void close() throws IOException {
        if (openCount.decrementAndGet() == 0) {
            backend.close();
        }
    }

    @Override
    public String getIndexStoreType() {
        return backend.getIndexStoreType();
    }

    @Override
    public boolean isIncremental() {
        return backend.isIncremental();
    }

}
