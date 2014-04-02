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
package org.apache.jackrabbit.oak.plugins.segment.file;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TarWriter {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(TarWriter.class);

    /** Magic byte sequence at the end of the index block. */
    static final int INDEX_MAGIC =
            ('\n' << 24) + ('0' << 16) + ('K' << 8) + '\n';

    /** The tar file block size. */
    static final int BLOCK_SIZE = 512;

    private static final byte[] ZERO_BYTES = new byte[BLOCK_SIZE];

    static final int getPaddingSize(int size) {
        int remainder = size % BLOCK_SIZE;
        if (remainder > 0) {
            return BLOCK_SIZE - remainder;
        } else {
            return 0;
        }
    }

    private final File file;

    private RandomAccessFile access = null;

    private final Map<UUID, TarEntry> index = newHashMap();

    TarWriter(File file) {
        this.file = file;
    }

    synchronized Set<UUID> getUUIDs() {
        return newHashSet(index.keySet());
    }

    synchronized boolean containsEntry(long msb, long lsb) {
        return index.containsKey(new UUID(msb, lsb));
    }

    synchronized ByteBuffer readEntry(long msb, long lsb) {
        TarEntry entry = index.get(new UUID(msb, lsb));
        if (entry != null) {
            checkState(access != null); // implied by entry != null
            try {
                try {
                    byte[] data = new byte[entry.size()];
                    access.seek(entry.offset());
                    access.readFully(data);
                    return ByteBuffer.wrap(data);
                } finally {
                    access.seek(access.length());
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        "Unable to read from tar file " + file, e);
            }
        } else {
            return null;
        }
    }

    long writeEntry(
            long msb, long lsb, byte[] data, int offset, int size)
            throws IOException {
        checkNotNull(data);
        checkPositionIndexes(offset, offset + size, data.length);

        UUID uuid = new UUID(msb, lsb);
        CRC32 checksum = new CRC32();
        checksum.update(data, offset, size);
        String entryName = String.format("%s.%08x", uuid, checksum.getValue());
        byte[] header = newEntryHeader(entryName, size);

        log.debug("Writing segment {} to {}", uuid, file);
        return writeEntry(uuid, header, data, offset, size);
    }

    private synchronized long writeEntry(
            UUID uuid, byte[] header, byte[] data, int offset, int size)
            throws IOException {
        if (access == null) {
            access = new RandomAccessFile(file, "rw");
        }

        access.write(header);
        access.write(data, offset, size);
        int padding = getPaddingSize(size);
        if (padding > 0) {
            access.write(ZERO_BYTES, 0, padding);
        }

        long length = access.getFilePointer();
        checkState(length <= Integer.MAX_VALUE);
        TarEntry entry = new TarEntry(
                uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(),
                (int) (length - size - padding), size);
        index.put(uuid, entry);

        return length;
    }

    synchronized void flush() throws IOException {
        if (access != null) {
            access.getFD().sync();
        }
    }

    synchronized void close() throws IOException {
        if (access != null) {
            int indexSize = index.size() * 24 + 16;
            int padding = getPaddingSize(indexSize);

            String indexName = file.getName() + ".idx";
            byte[] header = newEntryHeader(indexName, indexSize);

            ByteBuffer buffer = ByteBuffer.allocate(indexSize);
            TarEntry[] sorted = index.values().toArray(new TarEntry[index.size()]);
            Arrays.sort(sorted, TarEntry.IDENTIFIER_ORDER);
            for (TarEntry entry : sorted) {
                buffer.putLong(entry.msb());
                buffer.putLong(entry.lsb());
                buffer.putInt(entry.offset());
                buffer.putInt(entry.size());
            }

            CRC32 checksum = new CRC32();
            checksum.update(buffer.array(), 0, buffer.position());
            buffer.putInt((int) checksum.getValue());
            buffer.putInt(index.size());
            buffer.putInt(padding + indexSize);
            buffer.putInt(INDEX_MAGIC);

            access.write(header);
            if (padding > 0) {
                // padding comes *before* the index!
                access.write(ZERO_BYTES, 0, padding);
            }
            access.write(buffer.array());
            access.write(ZERO_BYTES);
            access.write(ZERO_BYTES);
            access.close();

            access = null;
        }
    }

    private byte[] newEntryHeader(String name, int size) throws IOException {
        byte[] header = new byte[BLOCK_SIZE];

        // File name
        byte[] nameBytes = name.getBytes(UTF_8);
        System.arraycopy(
                nameBytes, 0, header, 0, Math.min(nameBytes.length, 100));

        // File mode
        System.arraycopy(
                String.format("%07o", 0400).getBytes(UTF_8), 0,
                header, 100, 7);

        // User's numeric user ID
        System.arraycopy(
                String.format("%07o", 0).getBytes(UTF_8), 0,
                header, 108, 7);

        // Group's numeric user ID
        System.arraycopy(
                String.format("%07o", 0).getBytes(UTF_8), 0,
                header, 116, 7);

        // File size in bytes (octal basis)
        System.arraycopy(
                String.format("%011o", size).getBytes(UTF_8), 0,
                header, 124, 11);

        // Last modification time in numeric Unix time format (octal)
        long time = System.currentTimeMillis() / 1000;
        System.arraycopy(
                String.format("%011o", time).getBytes(UTF_8), 0,
                header, 136, 11);

        // Checksum for header record
        System.arraycopy(
                new byte[] { ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ' }, 0,
                header, 148, 8);

        // Type flag
        header[156] = '0';

        // Compute checksum
        int checksum = 0;
        for (int i = 0; i < header.length; i++) {
            checksum += header[i] & 0xff;
        }
        System.arraycopy(
                String.format("%06o", checksum).getBytes(UTF_8), 0,
                header, 148, 6);
        header[154] = 0;

        return header;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return file.toString();
    }

}
