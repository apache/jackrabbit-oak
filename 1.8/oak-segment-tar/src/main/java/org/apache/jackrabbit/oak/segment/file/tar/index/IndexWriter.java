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

/**
 * Builds an index incrementally in memory, and serializes its contents into a
 * sequence of bytes.
 */
public class IndexWriter {

    private static class Entry {

        long msb;

        long lsb;

        int offset;

        int size;

        int generation;

        int fullGeneration;

        boolean isCompacted;

    }

    /**
     * Create a new {@link IndexWriter} for the specified block size. The block
     * size is needed to ensure that the data produced by the returned {@link
     * IndexWriter} is aligned to a specified boundary, i.e. is a multiple of
     * the block size.
     *
     * @param blockSize The block size. It must be strictly positive.
     * @return An index of {@link IndexWriter}.
     */
    public static IndexWriter newIndexWriter(int blockSize) {
        checkArgument(blockSize > 0, "Invalid block size");
        return new IndexWriter(blockSize);
    }

    private final int blockSize;

    private final List<Entry> entries = new ArrayList<>();

    private IndexWriter(int blockSize) {
        this.blockSize = blockSize;
    }

    /**
     * Add an entry to this index.
     *
     * @param msb            The most significant bits of the entry identifier.
     * @param lsb            The least significant bits of the entry
     *                       identifier.
     * @param offset         The position of the entry in the file.
     * @param size           The size of the entry.
     * @param generation     The generation of the entry.
     * @param fullGeneration The full generation of the entry.
     * @param isCompacted    Whether the entry is generated as part of a
     *                       compaction operation.
     */
    public void addEntry(long msb, long lsb, int offset, int size, int generation, int fullGeneration, boolean isCompacted) {
        Entry entry = new Entry();
        entry.msb = msb;
        entry.lsb = lsb;
        entry.offset = offset;
        entry.size = size;
        entry.generation = generation;
        entry.fullGeneration = fullGeneration;
        entry.isCompacted = isCompacted;
        entries.add(entry);
    }

    /**
     * Serializes the content of the index. The returned array of bytes is
     * always a multiple of the block size specified when this {@link
     * IndexWriter} was created.
     *
     * @return the serialized content of the index.
     */
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
            buffer.putInt(entry.generation);
            buffer.putInt(entry.fullGeneration);
            buffer.put((byte) (entry.isCompacted ? 1 : 0));
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
