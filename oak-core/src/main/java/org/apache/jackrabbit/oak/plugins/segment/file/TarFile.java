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
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Maps.newTreeMap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;

class TarFile {

    /** The tar file block size. */
    private static final int BLOCK_SIZE = 512;

    private static final byte[] ZERO_BYTES = new byte[BLOCK_SIZE];

    private final File file;

    private final FileAccess access;

    private int position = 0;

    private final int maxFileSize;

    private final byte[] indexEntryName;

    private final Map<UUID, TarEntry> entries = newConcurrentMap();

    TarFile(File file, int maxFileSize, boolean memoryMapping)
            throws IOException {
        long len = file.length();
        checkState(len <= Integer.MAX_VALUE);
        this.maxFileSize = Math.max((int) len, maxFileSize);
        checkState(maxFileSize % BLOCK_SIZE == 0);
        checkState(maxFileSize > 5 * BLOCK_SIZE);
        this.indexEntryName = (file.getName() + ".idx").getBytes(UTF_8);

        this.file = file;
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        if (memoryMapping) {
            this.access = new MappedAccess(f, this.maxFileSize);
        } else {
            this.access = new RandomAccess(f);
        }

        this.position = 0;
        if (len == 0) {
            // allocate the full file by writing one big index entry
            writeEntryHeader(indexEntryName, maxFileSize - 3 * BLOCK_SIZE);
            // zero-out the last four bytes to indicate an empty index
            access.write(
                    maxFileSize - (ZERO_BYTES.length * 2 + 4),
                    ZERO_BYTES, 0, 4);
            // tar format expects the last two blocks to be zero
            access.write(
                    maxFileSize - ZERO_BYTES.length * 2,
                    ZERO_BYTES, 0, ZERO_BYTES.length);
            access.write(
                    maxFileSize - ZERO_BYTES.length,
                    ZERO_BYTES, 0, ZERO_BYTES.length);
        } else {
            readIndex(len);
        }
    }

    Set<UUID> getUUIDs() {
        return entries.keySet();
    }

    ByteBuffer readEntry(UUID uuid) throws IOException {
        TarEntry entry = entries.get(uuid);
        if (entry != null) {
            return access.read(entry.offset(), entry.size());
        } else {
            return null;
        }
    }

    private final int getEntrySize(int size) {
        return BLOCK_SIZE + (size + BLOCK_SIZE - 1) & ~(BLOCK_SIZE - 1);
    }

    synchronized boolean writeEntry(
            UUID uuid, byte[] b, int offset, int size) throws IOException {
        int indexSize = entries.size() * 24 + 4;
        if (position
                + getEntrySize(size)      // this entry
                + getEntrySize(indexSize) // index entry
                + 2 * BLOCK_SIZE          // two zero blocks at the end
                > maxFileSize) {
            writeEntryHeader(
                    indexEntryName, maxFileSize - 3 * BLOCK_SIZE - position);
            ByteBuffer index = ByteBuffer.allocate(indexSize);
            SortedMap<UUID, TarEntry> sorted = newTreeMap();
            sorted.putAll(entries);
            for (Map.Entry<UUID, TarEntry> entry : sorted.entrySet()) {
                index.putLong(entry.getKey().getMostSignificantBits());
                index.putLong(entry.getKey().getLeastSignificantBits());
                index.putInt(entry.getValue().offset());
                index.putInt(entry.getValue().size());
            }
            index.putInt(sorted.size());
            access.write(
                    maxFileSize - 2 * BLOCK_SIZE - indexSize,
                    index.array(), 0, indexSize);
            position = maxFileSize - 2 * BLOCK_SIZE;
            return false;
        }

        writeEntryHeader(uuid.toString().getBytes(UTF_8), size);
        position += BLOCK_SIZE;

        access.write(position, b, offset, size);
        entries.put(uuid, new TarEntry(position, size));
        position += size;

        int padding = BLOCK_SIZE - position % BLOCK_SIZE;
        if (padding < BLOCK_SIZE) {
            access.write(position, ZERO_BYTES, 0, padding);
            position += padding;
        }

        return true;
    }

    protected void writeEntryHeader(byte[] name, int size) throws IOException {
        byte[] header = new byte[BLOCK_SIZE];

        // File name
        System.arraycopy(name, 0, header, 0, name.length);

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

        checkState(position % BLOCK_SIZE == 0);
        access.write(position, header, 0, BLOCK_SIZE);
    }

    public boolean flush() throws IOException {
        try {
            access.flush();
            return true;
        } catch (IOException e) {
            StackTraceElement[] trace = e.getStackTrace();
            if (trace.length > 2
                    && "java.nio.MappedByteBuffer".equals(trace[0].getClassName())
                    && "force0".equals(trace[0].getMethodName())
                    && "java.nio.MappedByteBuffer".equals(trace[1].getClassName())
                    && "force".equals(trace[1].getMethodName())) {
                return false;
            } else {
                throw e;
            }
        }
    }


    void close() throws IOException {
        access.close();
    }

    private void readIndex(long len) throws IOException {
        while (position + BLOCK_SIZE <= len) {
            // read the tar header block
            ByteBuffer buffer = this.access.read(position, BLOCK_SIZE);
            String name = readString(buffer, 100);
            buffer.position(124);
            int size = readNumber(buffer, 12);
            // TODO: verify the checksum, magic, etc.?

            if (name.isEmpty() && size == 0) {
                break; // no more entries in this file
            } else if (Arrays.equals(name.getBytes(UTF_8), indexEntryName)) {
                break; // index entry encountered, so stop here
            } else if (position + BLOCK_SIZE + size > len) {
                break; // invalid entry, truncate the file at this point
            }

            try {
                UUID id = UUID.fromString(name);
                entries.put(id, new TarEntry(position + BLOCK_SIZE, size));
            } catch (IllegalArgumentException e) {
                throw new IOException("Unexpected tar entry: " + name);
            }

            position += (1 + (size + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
        }
    }

    private static String readString(ByteBuffer buffer, int fieldSize) {
        byte[] b = new byte[fieldSize];
        buffer.get(b);
        int n = 0;
        while (n < fieldSize && b[n] != 0) {
            n++;
        }
        return new String(b, 0, n, UTF_8);
    }

    private static int readNumber(ByteBuffer buffer, int fieldSize) {
        byte[] b = new byte[fieldSize];
        buffer.get(b);
        int number = 0;
        for (int i = 0; i < fieldSize; i++) {
            int digit = b[i] & 0xff;
            if ('0' <= digit && digit <= '7') {
                number = number * 8 + digit - '0';
            } else {
                break;
            }
        }
        return number;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return file.toString();
    }

}
