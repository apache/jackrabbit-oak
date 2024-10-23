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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.guava.common.base.Preconditions.checkPositionIndexes;

import static java.lang.String.format;
import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.FILE_NAME_FORMAT;
import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.GRAPH_MAGIC;
import static org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexWriter.newBinaryReferencesIndexWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.segment.file.UnrecoverableArchiveException;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.NoopStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A writer for tar files. It is also used to read entries while the file is
 * still open.
 */
class TarWriter implements Closeable {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(TarWriter.class);

    private final int writeIndex;

    /**
     * Flag to indicate a closed writer. Accessing a closed writer is illegal.
     * Should only be accessed from synchronized code.
     */
    private boolean closed = false;

    /**
     * List of binary references contained in this TAR file.
     */
    private final BinaryReferencesIndexWriter binaryReferences = newBinaryReferencesIndexWriter();

    /**
     * Graph of references between segments.
     */
    private final Map<UUID, Set<UUID>> graph = new HashMap<>();

    private final SegmentArchiveManager archiveManager;

    private final SegmentArchiveWriter archive;

    /** This object is used as an additional
     *  synchronization point by {@link #flush()} and {@link #close()} to
     *  allow {@link #flush()} to work concurrently with normal reads and
     *  writes, but not with a concurrent {@link #close()}. */
    private final Object closeMonitor = new Object();

    /**
     * Counter exposing the number of segments.
     */
    private final CounterStats segmentCount;

    /**
     * Used for maintenance operations (GC or recovery) via the TarReader and
     * tests
     */
    TarWriter(SegmentArchiveManager archiveManager, String archiveName) throws IOException {
        this.archiveManager = archiveManager;
        this.archive = archiveManager.create(archiveName);
        this.writeIndex = -1;
        this.segmentCount = NoopStats.INSTANCE;
    }

    TarWriter(SegmentArchiveManager archiveManager, int writeIndex, CounterStats segmentCountStats)
    throws IOException {
        this.archiveManager = archiveManager;
        this.archive = archiveManager.create(format(FILE_NAME_FORMAT, writeIndex, "a"));
        this.writeIndex = writeIndex;
        this.segmentCount = segmentCountStats;
    }

    synchronized boolean containsEntry(long msb, long lsb) {
        Validate.checkState(!closed);
        return archive.containsSegment(msb, lsb);
    }

    /**
     * @return  the number of entries currently in this writer
     */
    int getEntryCount() {
        return archive.getEntryCount();
    }

    /**
     * @return the maximum number of entries supported by this writer
     */
    int getMaxEntryCount() {
        return archive.getMaxEntryCount();
    }

    /**
     * If the given segment is in this file, get the byte buffer that allows
     * reading it.
     * 
     * @param msb the most significant bits of the segment id
     * @param lsb the least significant bits of the segment id
     * @return the byte buffer, or null if not in this file
     */
    Buffer readEntry(long msb, long lsb) throws IOException {
        synchronized (this) {
            Validate.checkState(!closed);
        }
        return archive.readSegment(msb, lsb);
    }

    long writeEntry(long msb, long lsb, byte[] data, int offset, int size, GCGeneration generation) throws IOException {
        requireNonNull(data);
        checkPositionIndexes(offset, offset + size, data.length);

        synchronized (this) {
            Validate.checkState(!closed);

            archive.writeSegment(msb, lsb, data, offset, size, generation.getGeneration(), generation.getFullGeneration(), generation.isCompacted());
            segmentCount.inc();
            long currentLength = archive.getLength();
            int currentEntryCount = archive.getEntryCount();

            Validate.checkState(currentLength <= Integer.MAX_VALUE);
            Validate.checkState(currentEntryCount <= archive.getMaxEntryCount());

            return currentLength;
        }
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
        graph.computeIfAbsent(from, k -> new HashSet<>()).add(to);
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
        synchronized (closeMonitor) {
            boolean doFlush;

            synchronized (this) {
                doFlush = archive.isCreated() && !closed;
            }

            if (doFlush) {
                archive.flush();
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
            if (closed) {
                return;
            }
            closed = true;
        }

        // If nothing was written to this file, then we're already done.
        if (!archive.isCreated()) {
            return;
        }

        // Complete the tar file by adding the graph, the index and the
        // trailing two zero blocks. This code is synchronized on the closeMonitor
        // to ensure that no concurrent thread is still flushing
        // the file when we close the file handle.
        synchronized (closeMonitor) {
            try {
                writeBinaryReferences();
                writeGraph();

                archive.close();
            } catch (IOException e) {
                throw new UnrecoverableArchiveException("Failed to close tar archive", e);
            }
        }
    }

    /**
     * If the current instance is dirty, this will return a new TarWriter based
     * on the next generation of the file being written to by incrementing the
     * internal {@link #writeIndex} counter. Otherwise it will return the
     * current instance.
     */
    TarWriter createNextGeneration() throws IOException {
        Validate.checkState(writeIndex >= 0);
        // If nothing was written to this file, then we're already done.
        synchronized (this) {
            if (!archive.isCreated()) {
                return this;
            }
        }
        close();
        int newIndex = writeIndex + 1;
        return new TarWriter(archiveManager, newIndex, segmentCount);
    }

    private void writeBinaryReferences() throws IOException {
        archive.writeBinaryReferences(binaryReferences.write());
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

        Buffer buffer = Buffer.allocate(graphSize);

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

        archive.writeGraph(buffer.array());
    }

    synchronized long fileLength() {
        return archive.getLength();
    }

    synchronized String getFileName() {
        return archive.getName();
    }

    synchronized boolean isClosed() {
        return closed;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return getFileName();
    }

}
