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
import java.util.concurrent.ConcurrentMap;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TarFile {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(TarFile.class);

    /** Magic byte sequence at the end of the index block. */
    private static final int INDEX_MAGIC =
            '\n' << 24 + '0' << 16 + 'K' << 8 + '\n';

    /** The tar file block size. */
    private static final int BLOCK_SIZE = 512;

    private static final byte[] ZERO_BYTES = new byte[BLOCK_SIZE];

    private final File file;

    private final FileAccess access;

    private int position = 0;

    private final byte[] indexEntryName;

    private final ByteBuffer index;

    private final ConcurrentMap<UUID, TarEntry> entries;

    TarFile(File file, int maxFileSize, boolean memoryMapping)
            throws IOException {
        long len = file.length();
        checkState(len <= Integer.MAX_VALUE);
        maxFileSize = Math.max((int) len, maxFileSize);
        checkState(maxFileSize % BLOCK_SIZE == 0);
        checkState(maxFileSize > 5 * BLOCK_SIZE);
        this.indexEntryName = (file.getName() + ".idx").getBytes(UTF_8);

        this.file = file;
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        if (memoryMapping) {
            this.access = new MappedAccess(f, maxFileSize);
        } else {
            this.access = new RandomAccess(f);
        }

        this.position = 0;
        if (len == 0) {
            // allocate the full file by writing one big index entry
            writeEntryHeader(indexEntryName, maxFileSize - 3 * BLOCK_SIZE);
            // zero-out the last 16 bytes to indicate an empty index
            access.write(
                    maxFileSize - (ZERO_BYTES.length * 2 + 16),
                    ZERO_BYTES, 0, 16);
            // tar format expects the last two blocks to be zero
            access.write(
                    maxFileSize - ZERO_BYTES.length * 2,
                    ZERO_BYTES, 0, ZERO_BYTES.length);
            access.write(
                    maxFileSize - ZERO_BYTES.length,
                    ZERO_BYTES, 0, ZERO_BYTES.length);

            this.index = null;
            this.entries = newConcurrentMap();
        } else {
            this.index = loadAndValidateIndex();
            if (index == null) {
                this.entries = loadEntryMap();
            } else {
                this.entries = null;
            }
        }
    }

    Set<UUID> getUUIDs() {
        return entries.keySet();
    }

    ByteBuffer readEntry(long msb, long lsb) throws IOException {
        TarEntry entry = indexLookup(msb, lsb);
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
        if (entries == null) {
            return false;
        }

        int length = access.length();
        int indexSize = entries.size() * 24 + 16;
        if (position
                + getEntrySize(size)           // this entry
                + getEntrySize(indexSize + 24) // index with one extra entry
                + 2 * BLOCK_SIZE               // two zero blocks at the end
                > length) {
            int bytes = length - position - 3 * BLOCK_SIZE;
            writeEntryHeader(indexEntryName, bytes);

            ByteBuffer index = ByteBuffer.allocate(indexSize);

            SortedMap<UUID, TarEntry> sorted = newTreeMap();
            sorted.putAll(entries);
            for (Map.Entry<UUID, TarEntry> entry : sorted.entrySet()) {
                index.putLong(entry.getKey().getMostSignificantBits());
                index.putLong(entry.getKey().getLeastSignificantBits());
                index.putInt(entry.getValue().offset());
                index.putInt(entry.getValue().size());
            }

            CRC32 checksum = new CRC32();
            checksum.update(index.array(), 0, index.position());
            index.putInt((int) checksum.getValue());
            index.putInt(entries.size());
            index.putInt(bytes);
            index.putInt(INDEX_MAGIC);

            access.write(
                    length - 2 * BLOCK_SIZE - indexSize,
                    index.array(), 0, indexSize);
            position = length - 2 * BLOCK_SIZE;
            return false;
        }

        writeEntryHeader(uuid.toString().getBytes(UTF_8), size);
        access.write(position + BLOCK_SIZE, b, offset, size);
        entries.put(uuid, new TarEntry(position + BLOCK_SIZE, size));
        position += getEntrySize(size);

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

    //-----------------------------------------------------------< private >--

    private TarEntry indexLookup(long msb, long lsb) {
        if (entries != null) {
            return entries.get(new UUID(msb, lsb));
        }

        // The segment identifiers are randomly generated with uniform
        // distribution, so we can use interpolation search to find the
        // matching entry in the index. The average runtime is O(log log n).

        int lowIndex = 0;
        int highIndex = index.remaining() / 24 - 1;
        float lowValue = Long.MIN_VALUE;
        float highValue = Long.MAX_VALUE;
        float targetValue = msb;

        while (lowIndex <= highIndex) {
            int guessIndex = lowIndex + Math.round(
                    (highIndex - lowIndex)
                    * (targetValue - lowValue)
                    / (highValue - lowValue));
            int position = index.position() + guessIndex * 24;
            long guess = index.getLong(position);
            if (msb < guess) {
                highIndex = guessIndex - 1;
                highValue = guess;
            } else if (msb > guess) {
                lowIndex = guessIndex + 1;
                lowValue = guess;
            } else {
                // getting close...
                guess = index.getLong(position + 8);
                if (lsb < guess) {
                    highIndex = guessIndex - 1;
                    highValue = guess;
                } else if (lsb > guess) {
                    lowIndex = guessIndex + 1;
                    lowValue = guess;
                } else {
                    // found it!
                    return new TarEntry(
                            index.getInt(position + 16),
                            index.getInt(position + 20));
                }
            }
        }

        // not found
        return null;
    }

    /**
     * Tries to read an existing index from the tar file. The index is
     * returned if it is found and looks valid (correct checksum, passes
     * sanity checks).
     *
     * @return tar index, or {@code null} if not found or not valid
     * @throws IOException if the tar file could not be read
     */
    private ByteBuffer loadAndValidateIndex() throws IOException {
        int length = access.length();
        if (length % BLOCK_SIZE != 0 || length < 6 * BLOCK_SIZE) {
            log.warn("Unexpected size {} of tar file {}", length, file);
            return null; // unexpected file size
        }

        // read the index metadata just before the two final zero blocks
        ByteBuffer meta = access.read(length - 2 * BLOCK_SIZE - 16, 16);
        int crc32 = meta.getInt();
        int count = meta.getInt();
        int bytes = meta.getInt();
        int magic = meta.getInt();

        if (magic != INDEX_MAGIC) {
            // no warning here, as the index has probably not yet been written
            return null; // magic byte mismatch
        }

        if (count < 1 || bytes < count * 24 + 16 || bytes % BLOCK_SIZE != 0) {
            log.warn("Invalid index footer in tar file {}", file);
            return null; // impossible entry and/or byte counts
        }

        int position = length - 2 * BLOCK_SIZE - 16 - count * 24;
        ByteBuffer index = access.read(position, count * 24);
        index.mark();

        CRC32 checksum = new CRC32();
        int limit = length - 2 * BLOCK_SIZE - bytes - BLOCK_SIZE;
        long lastmsb = Long.MIN_VALUE;
        long lastlsb = Long.MIN_VALUE;
        byte[] buffer = new byte[24];
        for (int i = 0; i < count; i++) {
            index.get(buffer);
            checksum.update(buffer);

            ByteBuffer entry = ByteBuffer.wrap(buffer);
            long msb   = entry.getLong();
            long lsb   = entry.getLong();
            int offset = entry.getInt();
            int size   = entry.getInt();

            if (lastmsb > msb || (lastmsb == msb && lastlsb > lsb)) {
                log.warn("Incorrect index ordering in tar file {}", file);
                return null;
            } else if (lastmsb == msb && lastlsb == lsb && i > 0) {
                log.warn("Duplicate entry in the index of tar file {}", file);
                return null;
            } else if (offset < 0 || offset % BLOCK_SIZE != 0) {
                log.warn("Invalid entry offset in the index of tar file {}", file);
                return null;
            } else if (size < 1 || offset + size > limit) {
                log.warn("Invalid entry size in the index of tar file {}", file);
                return null;
            }

            lastmsb = msb;
            lastlsb = lsb;
        }

        if (crc32 != (int) checksum.getValue()) {
            log.warn("Invalid index checksum in tar file {}", file);
            return null; // checksum mismatch
        }

        index.reset();
        return index;
    }

    /**
     * Scans through the tar file, looking for all segment entries. Used on
     * tar files that don't already contain an index.
     *
     * @return map of all segment entries in this tar file
     * @throws IOException if the tar file could not be read
     */
    private ConcurrentMap<UUID, TarEntry> loadEntryMap() throws IOException {
        ConcurrentMap<UUID, TarEntry> entries = newConcurrentMap();

        int limit = access.length() - 2 * BLOCK_SIZE;
        while (position + 2 * BLOCK_SIZE <= limit) {
            // read the tar header block
            ByteBuffer header = access.read(position, BLOCK_SIZE);
            String name = readString(header, 100);
            header.position(124);
            int size = readNumber(header, 12);

            // TODO: verify the checksum, magic, etc.?

            if (name.isEmpty() && size == 0) {
                break; // no more entries in this file
            } else if (Arrays.equals(name.getBytes(UTF_8), indexEntryName)) {
                break; // index entry encountered, so stop here
            } else if (position + BLOCK_SIZE + size > limit) {
                break; // invalid entry, truncate the file at this point
            }

            try {
                UUID id = UUID.fromString(name);
                entries.put(id, new TarEntry(position + BLOCK_SIZE, size));
            } catch (IllegalArgumentException e) {
                break; // unexpected entry, truncate the file at this point
            }

            position += getEntrySize(size);
        }

        return entries;
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
