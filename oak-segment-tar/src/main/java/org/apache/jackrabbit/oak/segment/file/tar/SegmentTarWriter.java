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

import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.oak.segment.SegmentArchiveManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.BLOCK_SIZE;

public class SegmentTarWriter implements SegmentArchiveManager.SegmentArchiveWriter {

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
     * File handle. Initialized lazily in {@link #writeSegment(long, long, byte[], int, int, GCGeneration)}
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
    public TarEntry writeSegment(long msb, long lsb, byte[] data, int offset, int size, GCGeneration generation) throws IOException {
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

        return new TarEntry(msb, lsb, (int) dataOffset, size, generation);
    }

    @Override
    public ByteBuffer readSegment(TarEntry tarEntry) throws IOException {
        checkState(channel != null); // implied by entry != null
        ByteBuffer data = ByteBuffer.allocate(tarEntry.size());
        channel.read(data, tarEntry.offset());
        data.rewind();
        return data;
    }

    @Override
    public void writeIndex(byte[] data) throws IOException {
        byte[] header = newEntryHeader(file.getName() + ".idx", data.length);
        access.write(header);
        access.write(data);
        monitor.written(header.length + data.length);

        length = access.getFilePointer();
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
    public void close() throws IOException {
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

    private static byte[] newEntryHeader(String name, int size) {
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
                String.format("%06o\0 ", checksum).getBytes(UTF_8), 0,
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
