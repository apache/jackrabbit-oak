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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class NodeStateEntryBatch {

    public static class BufferFullException extends RuntimeException {
        public BufferFullException(String message) {
            super(message);
        }

        public BufferFullException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // Must be large enough to hold a full node state entry
    static final int MIN_BUFFER_SIZE = 256 * 1024;

    public static NodeStateEntryBatch createNodeStateEntryBatch(int bufferSizeBytes, int maxNumEntries) {
        if (bufferSizeBytes < MIN_BUFFER_SIZE) {
            throw new IllegalArgumentException("Buffer size must be at least " + MIN_BUFFER_SIZE + " bytes");
        }
        if (maxNumEntries < 1) {
            throw new IllegalArgumentException("Max number of entries must be at least 1");
        }
        ByteBuffer buffer = ByteBuffer.allocate(bufferSizeBytes);
        return new NodeStateEntryBatch(buffer, maxNumEntries);
    }

    private final ByteBuffer buffer;
    private final int maxEntries;
    private int numberOfEntries;
    private int sizeOfEntries;


    public NodeStateEntryBatch(ByteBuffer buffer, int maxEntries) {
        this.buffer = buffer;
        this.maxEntries = maxEntries;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int addEntry(String path, byte[] entryData) throws BufferFullException {
        if (numberOfEntries == maxEntries) {
            throw new BufferFullException("Sort buffer size is full, reached max entries: " + numberOfEntries);
        }
        int bufferPos = buffer.position();
        byte[] pathBytes = path.getBytes(StandardCharsets.UTF_8);
        int totalSize = 4 + pathBytes.length + 4 + entryData.length;
        try {
            buffer.putInt(pathBytes.length);
            buffer.put(pathBytes);
            buffer.putInt(entryData.length);
            buffer.put(entryData);
            numberOfEntries++;
            sizeOfEntries += totalSize;
            return totalSize;
        } catch (BufferOverflowException e) {
            buffer.position(bufferPos);
            throw new BufferFullException(" while adding entry " + path + " of size: " + totalSize, e);
        }
    }

    public boolean isAtMaxEntries() {
        if (numberOfEntries > maxEntries) {
            throw new AssertionError("Sort buffer size exceeded max entries: " + numberOfEntries + " > " + maxEntries);
        }
        return numberOfEntries == maxEntries;
    }

    public void flip() {
        buffer.flip();
    }

    public void reset() {
        buffer.clear();
        numberOfEntries = 0;
        sizeOfEntries = 0;
    }

    public int sizeOfEntries() {
        return sizeOfEntries;
    }

    public int numberOfEntries() {
        return numberOfEntries;
    }

    public int capacity() {
        return buffer.capacity();
    }
}