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

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.BLOCK_SIZE;
import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.FILE_NAME_FORMAT;
import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.GRAPH_MAGIC;
import static org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexWriter.newBinaryReferencesIndexWriter;

import java.io.Closeable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexWriter;
import org.apache.jackrabbit.oak.segment.file.tar.index.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A writer for tar files. It is also used to read entries while the file is
 * still open.
 */
class TarWriter implements Closeable {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(TarWriter.class);

    private static final byte[] ZERO_BYTES = new byte[BLOCK_SIZE];

    static int getPaddingSize(int size) {
        int remainder = size % BLOCK_SIZE;
        if (remainder > 0) {
            return BLOCK_SIZE - remainder;
        } else {
            return 0;
        }
    }

    private final int writeIndex;

    /**
     * The file being written. This instance is also used as an additional
     * synchronization point by {@link #flush()} and {@link #close()} to
     * allow {@link #flush()} to work concurrently with normal reads and
     * writes, but not with a concurrent {@link #close()}.
     */
    private final File file;

    private final FileStoreMonitor monitor;

    /**
     * File handle. Initialized lazily in {@link #writeEntry(UUID, byte[],
     * byte[], int, int, GCGeneration)} to avoid creating an extra empty file
     * when just reading from the repository. Should only be accessed from
     * synchronized code.
     */
    private RandomAccessFile access = null;

    private FileChannel channel = null;

    /**
     * Flag to indicate a closed writer. Accessing a closed writer is illegal.
     * Should only be accessed from synchronized code.
     */
    private boolean closed = false;

    /**
     * Map of the entries that have already been written. Used by the
     * {@link #containsEntry(long, long)} and {@link #readEntry(long, long)}
     * methods to retrieve data from this file while it's still being written,
     * and finally by the {@link #close()} method to generate the tar index.
     * The map is ordered in the order that entries have been written.
     * <p>
     * Should only be accessed from synchronized code.
     */
    private final Map<UUID, TarEntry> index = newLinkedHashMap();

    /**
     * List of binary references contained in this TAR file.
     */
    private final BinaryReferencesIndexWriter binaryReferences = newBinaryReferencesIndexWriter();

    /**
     * Graph of references between segments.
     */
    private final Map<UUID, Set<UUID>> graph = newHashMap();

    private final IOMonitor ioMonitor;

    /**
     * Used for maintenance operations (GC or recovery) via the TarReader and
     * tests
     */
    TarWriter(File file, IOMonitor ioMonitor) {
        this.file = file;
        this.monitor = new FileStoreMonitorAdapter();
        this.writeIndex = -1;
        this.ioMonitor = ioMonitor;
    }

    TarWriter(File directory, FileStoreMonitor monitor, int writeIndex, IOMonitor ioMonitor) {
        this.file = new File(directory, format(FILE_NAME_FORMAT, writeIndex, "a"));
        this.monitor = monitor;
        this.writeIndex = writeIndex;
        this.ioMonitor = ioMonitor;
    }

    synchronized boolean containsEntry(long msb, long lsb) {
        checkState(!closed);
        return index.containsKey(new UUID(msb, lsb));
    }

    /**
     * If the given segment is in this file, get the byte buffer that allows
     * reading it.
     * 
     * @param msb the most significant bits of the segment id
     * @param lsb the least significant bits of the segment id
     * @return the byte buffer, or null if not in this file
     */
    ByteBuffer readEntry(long msb, long lsb) throws IOException {
        checkState(!closed);
        
        TarEntry entry;
        synchronized (this) {
            entry = index.get(new UUID(msb, lsb));
        }
        if (entry != null) {
            checkState(channel != null); // implied by entry != null
            ByteBuffer data = ByteBuffer.allocate(entry.size());
            channel.read(data, entry.offset());
            data.rewind();
            return data;
        } else {
            return null;
        }
    }

    long writeEntry(long msb, long lsb, byte[] data, int offset, int size, GCGeneration generation) throws IOException {
        checkNotNull(data);
        checkPositionIndexes(offset, offset + size, data.length);

        UUID uuid = new UUID(msb, lsb);
        CRC32 checksum = new CRC32();
        checksum.update(data, offset, size);
        String entryName = String.format("%s.%08x", uuid, checksum.getValue());
        byte[] header = newEntryHeader(entryName, size);

        log.debug("Writing segment {} to {}", uuid, file);
        return writeEntry(uuid, header, data, offset, size, generation);
    }

    private synchronized long writeEntry(UUID uuid, byte[] header, byte[] data, int offset, int size, GCGeneration generation) throws IOException {
        checkState(!closed);

        if (access == null) {
            access = new RandomAccessFile(file, "rw");
            channel = access.getChannel();
        }

        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();

        int padding = getPaddingSize(size);

        long initialLength = access.getFilePointer();

        access.write(header);

        ioMonitor.beforeSegmentWrite(file, msb, lsb, size);
        Stopwatch stopwatch = Stopwatch.createStarted();
        access.write(data, offset, size);
        ioMonitor.afterSegmentWrite(file, msb, lsb, size, stopwatch.elapsed(TimeUnit.NANOSECONDS));

        if (padding > 0) {
            access.write(ZERO_BYTES, 0, padding);
        }

        long currentLength = access.getFilePointer();
        monitor.written(currentLength - initialLength);

        checkState(currentLength <= Integer.MAX_VALUE);
        TarEntry entry = new TarEntry(msb, lsb, (int) (currentLength - size - padding), size, generation);
        index.put(uuid, entry);

        return currentLength;
    }

    void addBinaryReference(GCGeneration generation, UUID segmentId, String reference) {
        binaryReferences.addEntry(
            generation.getGeneration(),
            generation.getFullGeneration(),
            generation.isCompacted(),
            segmentId,
            reference
        );
    }

    void addGraphEdge(UUID from, UUID to) {
        graph.computeIfAbsent(from, k -> newHashSet()).add(to);
    }

    /**
     * Flushes the entries that have so far been written to the disk.
     * This method is <em>not</em> synchronized to allow concurrent reads
     * and writes to proceed while the file is being flushed. However,
     * this method <em>is</em> carefully synchronized with {@link #close()}
     * to prevent accidental flushing of an already closed file.
     *
     * @throws IOException if the tar file could not be flushed
     */
    void flush() throws IOException {
        synchronized (file) {
            FileDescriptor descriptor = null;

            synchronized (this) {
                if (access != null && !closed) {
                    descriptor = access.getFD();
                }
            }

            if (descriptor != null) {
                descriptor.sync();
            }
        }
    }

    /**
     * Closes this tar file.
     *
     * @throws IOException if the tar file could not be closed
     */
    @Override
    public void close() throws IOException {
        // Mark this writer as closed. Note that we only need to synchronize
        // this part, as no other synchronized methods should get invoked
        // once close() has been initiated (see related checkState calls).
        synchronized (this) {
            checkState(!closed);
            closed = true;
        }

        // If nothing was written to this file, then we're already done.
        if (access == null) {
            return;
        }

        // Complete the tar file by adding the graph, the index and the
        // trailing two zero blocks. This code is synchronized on the file
        // instance to  ensure that no concurrent thread is still flushing
        // the file when we close the file handle.
        long initialPosition, currentPosition;
        synchronized (file) {
            initialPosition = access.getFilePointer();
            writeBinaryReferences();
            writeGraph();
            writeIndex();
            access.write(ZERO_BYTES);
            access.write(ZERO_BYTES);

            currentPosition = access.getFilePointer();
            access.close();
        }

        monitor.written(currentPosition - initialPosition);
    }

    /**
     * If the current instance is dirty, this will return a new TarWriter based
     * on the next generation of the file being written to by incrementing the
     * internal {@link #writeIndex} counter. Otherwise it will return the
     * current instance.
     */
    TarWriter createNextGeneration() throws IOException {
        checkState(writeIndex >= 0);
        // If nothing was written to this file, then we're already done.
        synchronized (this) {
            if (access == null) {
                return this;
            }
        }
        close();
        int newIndex = writeIndex + 1;
        return new TarWriter(file.getParentFile(), monitor, newIndex, ioMonitor);
    }

    private void writeBinaryReferences() throws IOException {
        byte[] data = binaryReferences.write();
        int paddingSize = getPaddingSize(data.length);
        byte[] header = newEntryHeader(file.getName() + ".brf", data.length + paddingSize);
        access.write(header);
        if (paddingSize > 0) {
            access.write(ZERO_BYTES, 0, paddingSize);
        }
        access.write(data);
    }

    private void writeGraph() throws IOException {
        int graphSize = 0;

        // The following information are stored in the footer as meta-
        // information about the entry.

        // 4 bytes to store a magic number identifying this entry as containing
        // references to binary values.
        graphSize += 4;

        // 4 bytes to store the CRC32 checksum of the data in this entry.
        graphSize += 4;

        // 4 bytes to store the length of this entry, without including the
        // optional padding.
        graphSize += 4;

        // 4 bytes to store the number of entries in the graph map.
        graphSize += 4;

        // The following information are stored as part of the main content of
        // this entry, after the optional padding.

        for (Entry<UUID, Set<UUID>> entry : graph.entrySet()) {
            // 16 bytes to store the key of the map.
            graphSize += 16;

            // 4 bytes for the number of entries in the adjacency list.
            graphSize += 4;

            // 16 bytes for every element in the adjacency list.
            graphSize += 16 * entry.getValue().size();
        }

        ByteBuffer buffer = ByteBuffer.allocate(graphSize);

        for (Entry<UUID, Set<UUID>> entry : graph.entrySet()) {
            UUID from = entry.getKey();

            buffer.putLong(from.getMostSignificantBits());
            buffer.putLong(from.getLeastSignificantBits());

            Set<UUID> adj = entry.getValue();

            buffer.putInt(adj.size());

            for (UUID to : adj) {
                buffer.putLong(to.getMostSignificantBits());
                buffer.putLong(to.getLeastSignificantBits());
            }
        }

        CRC32 checksum = new CRC32();
        checksum.update(buffer.array(), 0, buffer.position());

        buffer.putInt((int) checksum.getValue());
        buffer.putInt(graph.size());
        buffer.putInt(graphSize);
        buffer.putInt(GRAPH_MAGIC);

        int padding = getPaddingSize(graphSize);

        access.write(newEntryHeader(file.getName() + ".gph", graphSize + padding));

        if (padding > 0) {
            access.write(ZERO_BYTES, 0, padding);
        }

        access.write(buffer.array());
    }

    private void writeIndex() throws IOException {
        IndexWriter writer = IndexWriter.newIndexWriter(BLOCK_SIZE);

        for (TarEntry entry : index.values()) {
            writer.addEntry(
                    entry.msb(),
                    entry.lsb(),
                    entry.offset(),
                    entry.size(),
                    entry.generation().getGeneration(),
                    entry.generation().getFullGeneration(),
                    entry.generation().isCompacted()
            );
        }

        byte[] index = writer.write();
        access.write(newEntryHeader(file.getName() + ".idx", index.length));
        access.write(index);
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

    synchronized long fileLength() {
        return file.length();
    }

    synchronized File getFile() {
        return file;
    }

    synchronized boolean isClosed() {
        return closed;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return file.toString();
    }

}
