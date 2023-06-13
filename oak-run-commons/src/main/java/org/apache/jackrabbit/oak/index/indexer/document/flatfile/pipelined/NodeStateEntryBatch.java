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

    public boolean isAtMaxEntries() {
        if (sortBuffer.size() > maxEntries) {
            throw new AssertionError("Sort buffer size exceeded max entries: " + sortBuffer.size() + " > " + maxEntries);
        }
        return sortBuffer.size() == maxEntries;
    }

    public void reset() {
        buffer.clear();
        sortBuffer.clear();
    }

    public static NodeStateEntryBatch createNodeStateEntryBatch(int bufferSize, int maxNumEntries) {
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        return new NodeStateEntryBatch(buffer, maxNumEntries);
    }
}