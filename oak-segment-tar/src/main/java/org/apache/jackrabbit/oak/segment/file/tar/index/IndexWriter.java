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

package org.apache.jackrabbit.oak.segment.file.tar.index;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

public class IndexWriter {

    private static class Entry {

        long msb;

        long lsb;

        int offset;

        int size;

        int fullGeneration;

        int tailGeneration;

        boolean isTail;

    }

    public static IndexWriter newIndexWriter(int blockSize) {
        checkArgument(blockSize > 0, "Invalid block size");
        return new IndexWriter(blockSize);
    }

    private final int blockSize;

    private final List<Entry> entries = new ArrayList<>();

    private IndexWriter(int blockSize) {
        this.blockSize = blockSize;
    }

    public void addEntry(long msb, long lsb, int offset, int size, int fullGeneration, int tailGeneration, boolean isTail) {
        Entry entry = new Entry();
        entry.msb = msb;
        entry.lsb = lsb;
        entry.offset = offset;
        entry.size = size;
        entry.fullGeneration = fullGeneration;
        entry.tailGeneration = tailGeneration;
        entry.isTail = isTail;
        entries.add(entry);
    }

    public byte[] write() {
        int dataSize = entries.size() * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE;
        int totalSize = ((dataSize + blockSize - 1) / blockSize) * blockSize;

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.position(totalSize - dataSize);

        entries.sort((a, b) -> {
            if (a.msb < b.msb) {
                return -1;
            }
            if (a.msb > b.msb) {
                return 1;
            }
            if (a.lsb < b.lsb) {
                return -1;
            }
            if (a.lsb > b.lsb) {
                return 1;
            }
            return 0;
        });

        for (Entry entry : entries) {
            buffer.putLong(entry.msb);
            buffer.putLong(entry.lsb);
            buffer.putInt(entry.offset);
            buffer.putInt(entry.size);
            buffer.putInt(entry.fullGeneration);
            buffer.putInt(entry.tailGeneration);
            buffer.put((byte) (entry.isTail ? 1 : 0));
        }

        CRC32 checksum = new CRC32();
        checksum.update(buffer.array(), totalSize - dataSize, dataSize - IndexV2.FOOTER_SIZE);

        buffer.putInt((int) checksum.getValue());
        buffer.putInt(entries.size());
        buffer.putInt(totalSize);
        buffer.putInt(IndexLoaderV2.MAGIC);

        return buffer.array();
    }

}
