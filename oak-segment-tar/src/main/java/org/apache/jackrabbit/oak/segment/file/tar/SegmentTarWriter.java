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
package org.apache.jackrabbit.oak.segment.file.tar;

import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.BLOCK_SIZE;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import org.apache.jackrabbit.guava.common.base.Stopwatch;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.segment.file.tar.index.IndexEntry;
import org.apache.jackrabbit.oak.segment.file.tar.index.IndexWriter;
import org.apache.jackrabbit.oak.segment.file.tar.index.SimpleIndexEntry;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentTarWriter implements SegmentArchiveWriter {

    private static final Logger log = LoggerFactory.getLogger(SegmentTarWriter.class);

    private static final byte[] ZERO_BYTES = new byte[BLOCK_SIZE];

    private final FileStoreMonitor monitor;

    /**
     * The file being written. This instance is also used as an additional
     * synchronization point by {@link #flush()} and {@link #close()} to
     * allow {@link #flush()} to work concurrently with normal reads and
     * writes, but not with a concurrent {@link #close()}.
     */
    private final File file;

    private final IOMonitor ioMonitor;

    /**
     * Map of the entries that have already been written. Used by the
     * {@link #containsSegment(long, long)} and {@link #readSegment(long, long)}
     * methods to retrieve data from this file while it's still being written,
     * and finally by the {@link #close()} method to generate the tar index.
     * The map is ordered in the order that entries have been written.
     * <p>
     * The MutableIndex implementation is thread-safe.
     */
    private final Map<UUID, IndexEntry> index = Collections.synchronizedMap(new LinkedHashMap<>());

    /**
     * File handle. Initialized lazily in {@link #writeSegment(long, long, byte[], int, int, int, int, boolean)}
     * to avoid creating an extra empty file when just reading from the repository.
     * Should only be accessed from synchronized code.
     */
    private RandomAccessFile access = null;

    private FileChannel channel = null;

    private volatile long length;

    public SegmentTarWriter(File file, FileStoreMonitor monitor, IOMonitor ioMonitor) {
        this.file = file;
        this.monitor = monitor;
        this.ioMonitor = ioMonitor;
    }

    @Override
    public void writeSegment(long msb, long lsb, byte[] data, int offset, int size, int generation, int fullGeneration, boolean compacted) throws IOException {
        UUID uuid = new UUID(msb, lsb);
        CRC32 checksum = new CRC32();
        checksum.update(data, offset, size);
        String entryName = String.format("%s.%08x", uuid, checksum.getValue());
        byte[] header = newEntryHeader(entryName, size);

        log.debug("Writing segment {} to {}", uuid, file);

        if (access == null) {
            access = new RandomAccessFile(file, "rw");
            channel = access.getChannel();
        }

        int padding = getPaddingSize(size);

        long initialLength = access.getFilePointer();

        access.write(header);

        long dataOffset = access.getFilePointer();

        ioMonitor.beforeSegmentWrite(file, msb, lsb, size);
        Stopwatch stopwatch = Stopwatch.createStarted();
        access.write(data, offset, size);
        ioMonitor.afterSegmentWrite(file, msb, lsb, size, stopwatch.elapsed(TimeUnit.NANOSECONDS));

        if (padding > 0) {
            access.write(ZERO_BYTES, 0, padding);
        }

        long currentLength = access.getFilePointer();
        monitor.written(currentLength - initialLength);

        length = currentLength;

        index.put(new UUID(msb, lsb), new SimpleIndexEntry(msb, lsb, (int) dataOffset, size, generation, fullGeneration, compacted));
    }

    @Override
    public Buffer readSegment(long msb, long lsb) throws IOException {
        IndexEntry indexEntry = index.get(new UUID(msb, lsb));
        if (indexEntry == null) {
            return null;
        }
        Validate.checkState(channel != null); // implied by entry != null
        Buffer data = Buffer.allocate(indexEntry.getLength());
        if (data.readFully(channel, indexEntry.getPosition()) < indexEntry.getLength()) {
            throw new EOFException();
        }
        data.rewind();
        return data;
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        return index.containsKey(new UUID(msb, lsb));
    }

    @Override
    public void writeGraph(byte[] data) throws IOException {
        int paddingSize = getPaddingSize(data.length);
        byte[] header = newEntryHeader(file.getName() + ".gph", data.length + paddingSize);
        access.write(header);
        if (paddingSize > 0) {
            access.write(ZERO_BYTES, 0, paddingSize);
        }
        access.write(data);
        monitor.written(header.length + paddingSize + data.length);

        length = access.getFilePointer();
    }

    @Override
    public void writeBinaryReferences(byte[] data) throws IOException {
        int paddingSize = getPaddingSize(data.length);
        byte[] header = newEntryHeader(file.getName() + ".brf", data.length + paddingSize);
        access.write(header);
        if (paddingSize > 0) {
            access.write(ZERO_BYTES, 0, paddingSize);
        }
        access.write(data);
        monitor.written(header.length + paddingSize + data.length);

        length = access.getFilePointer();
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public int getEntryCount() {
        return index.size();
    }

    private void writeIndex() throws IOException {
        IndexWriter writer = IndexWriter.newIndexWriter(BLOCK_SIZE);

        for (IndexEntry entry : index.values()) {
            writer.addEntry(
                    entry.getMsb(),
                    entry.getLsb(),
                    entry.getPosition(),
                    entry.getLength(),
                    entry.getGeneration(),
                    entry.getFullGeneration(),
                    entry.isCompacted()
            );
        }

        byte[] data = writer.write();

        byte[] header = newEntryHeader(file.getName() + ".idx", data.length);
        access.write(header);
        access.write(data);
        monitor.written(header.length + data.length);

        length = access.getFilePointer();
    }

    @Override
    public void close() throws IOException {
        writeIndex();

        access.write(ZERO_BYTES);
        access.write(ZERO_BYTES);
        access.close();

        monitor.written(BLOCK_SIZE * 2);
    }

    @Override
    public boolean isCreated() {
        return access != null;
    }

    @Override
    public void flush() throws IOException {
        access.getFD().sync();
    }

    @Override
    public String getName() {
        return file.getName();
    }

    @Override
    public boolean isRemote() {
        return false;
    }

    @Override
    public int getMaxEntryCount() {
        return Integer.MAX_VALUE;
    }

    private static byte[] newEntryHeader(String name, int size) {
        byte[] header = new byte[BLOCK_SIZE];

        // File name
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(
                nameBytes, 0, header, 0, Math.min(nameBytes.length, 100));

        // File mode
        System.arraycopy(
                String.format("%07o", 0400).getBytes(StandardCharsets.UTF_8), 0,
                header, 100, 7);

        // User's numeric user ID
        System.arraycopy(
                String.format("%07o", 0).getBytes(StandardCharsets.UTF_8), 0,
                header, 108, 7);

        // Group's numeric user ID
        System.arraycopy(
                String.format("%07o", 0).getBytes(StandardCharsets.UTF_8), 0,
                header, 116, 7);

        // File size in bytes (octal basis)
        System.arraycopy(
                String.format("%011o", size).getBytes(StandardCharsets.UTF_8), 0,
                header, 124, 11);

        // Last modification time in numeric Unix time format (octal)
        long time = System.currentTimeMillis() / 1000;
        System.arraycopy(
                String.format("%011o", time).getBytes(StandardCharsets.UTF_8), 0,
                header, 136, 11);

        // Checksum for header record
        System.arraycopy(
                new byte[] {' ', ' ', ' ', ' ', ' ', ' ', ' ', ' '}, 0,
                header, 148, 8);

        // Type flag
        header[156] = '0';

        // Compute checksum
        int checksum = 0;
        for (byte aHeader : header) {
            checksum += aHeader & 0xff;
        }
        System.arraycopy(
                String.format("%06o\0 ", checksum).getBytes(StandardCharsets.UTF_8), 0,
                header, 148, 8);

        return header;
    }

    static int getPaddingSize(int size) {
        int remainder = size % BLOCK_SIZE;
        if (remainder > 0) {
            return BLOCK_SIZE - remainder;
        } else {
            return 0;
        }
    }
}
