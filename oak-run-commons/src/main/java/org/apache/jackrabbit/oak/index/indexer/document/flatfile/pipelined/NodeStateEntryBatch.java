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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class NodeStateEntryBatch {
    // Must be large enough to hold a full node state entry
    static final int MIN_BUFFER_SIZE = 256 * 1024;
    public static NodeStateEntryBatch createNodeStateEntryBatch(int bufferSize, int maxNumEntries) {
        if (bufferSize < MIN_BUFFER_SIZE) {
            throw new IllegalArgumentException("Buffer size must be at least " + MIN_BUFFER_SIZE + " bytes");
        }
        if (maxNumEntries < 1) {
            throw new IllegalArgumentException("Max number of entries must be at least 1");
        }
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        return new NodeStateEntryBatch(buffer, maxNumEntries);
    }

    private final ByteBuffer buffer;
    private final ArrayList<SortKey> sortBuffer;
    private final int maxEntries;

    public NodeStateEntryBatch(ByteBuffer buffer, int maxEntries) {
        this.buffer = buffer;
        this.maxEntries = maxEntries;
        this.sortBuffer = new ArrayList<>(maxEntries);
    }

    public ArrayList<SortKey> getSortBuffer() {
        return sortBuffer;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void addEntry(String path, byte[] entryData) {
        if (numberOfEntries() == maxEntries) {
            throw new IllegalStateException("Sort buffer size exceeded max entries: " + sortBuffer.size() + " > " + maxEntries);
        }
        int bufferPos = buffer.position();
        buffer.putInt(entryData.length);
        buffer.put(entryData);
        String[] key = SortKey.genSortKeyPathElements(path);
        sortBuffer.add(new SortKey(key, bufferPos));
    }

    public boolean isAtMaxEntries() {
        if (sortBuffer.size() > maxEntries) {
            throw new AssertionError("Sort buffer size exceeded max entries: " + sortBuffer.size() + " > " + maxEntries);
        }
        return sortBuffer.size() == maxEntries;
    }

    public boolean hasSpaceForEntry(byte[] entryData) {
        return !isAtMaxEntries() && entryData.length + 4 <= buffer.remaining();
    }

    public void flip() {
        buffer.flip();
    }

    public void reset() {
        buffer.clear();
        sortBuffer.clear();
    }

    public int sizeOfEntries() {
        return buffer.position();
    }

    public int numberOfEntries() {
        return sortBuffer.size();
    }

    public int capacity() {
        return buffer.capacity();
    }
}