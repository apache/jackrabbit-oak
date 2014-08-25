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
import static com.google.common.collect.Maps.newTreeMap;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.REF_COUNT_OFFSET;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentId.isDataSegmentId;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

class TarWriter {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(TarWriter.class);

    /** Magic byte sequence at the end of the index block. */
    static final int INDEX_MAGIC =
            ('\n' << 24) + ('0' << 16) + ('K' << 8) + '\n';

    /** Magic byte sequence at the end of the graph block. */
    static final int GRAPH_MAGIC =
            ('\n' << 24) + ('0' << 16) + ('G' << 8) + '\n';

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

    /**
     * The file being written. This instance is also used as an additional
     * synchronization point by {@link #flush()} and {@link #close()} to
     * allow {@link #flush()} to work concurrently with normal reads and
     * writes, but not with a concurrent {@link #close()}.
     */
    private final File file;

    /**
     * File handle. Initialized lazily in
     * {@link #writeEntry(long, long, byte[], int, int)} to avoid creating
     * an extra empty file when just reading from the repository.
     * Should only be accessed from synchronized code.
     */
    private RandomAccessFile access = null;

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
     * Should only be accessed from synchronized code;
     */
    private final Map<UUID, TarEntry> index = newHashMap();

    private final Set<UUID> references = newHashSet();

    private final SortedMap<UUID, List<UUID>> graph = newTreeMap();

    TarWriter(File file) {
        this.file = file;
    }

    /**
     * Returns the number of segments written so far to this tar file.
     *
     * @return number of segments written so far
     */
    synchronized int count() {
        return index.size();
    }

    synchronized Set<UUID> getUUIDs() {
        return newHashSet(index.keySet());
    }

    synchronized boolean containsEntry(long msb, long lsb) {
        checkState(!closed);
        return index.containsKey(new UUID(msb, lsb));
    }

    synchronized ByteBuffer readEntry(long msb, long lsb) throws IOException {
        checkState(!closed);
        TarEntry entry = index.get(new UUID(msb, lsb));
        if (entry != null) {
            checkState(access != null); // implied by entry != null
            ByteBuffer data = ByteBuffer.allocate(entry.size());
            access.seek(entry.offset());
            access.readFully(data.array());
            access.seek(access.length());
            return data;
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
        checkState(!closed);
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

        if (isDataSegmentId(uuid.getLeastSignificantBits())) {
            ByteBuffer segment = ByteBuffer.wrap(data, offset, size);
            int pos = segment.position();
            int refcount = segment.get(pos + REF_COUNT_OFFSET) & 0xff;
            if (refcount != 0) {
                int refend = pos + 16 * (refcount + 1);
                List<UUID> list = Lists.newArrayListWithCapacity(refcount);
                for (int refpos = pos + 16; refpos < refend; refpos += 16) {
                    UUID refid = new UUID(
                            segment.getLong(refpos),
                            segment.getLong(refpos + 8));
                    if (!index.containsKey(refid)) {
                        references.add(refid);
                    }
                    list.add(refid);
                }
                Collections.sort(list);
                graph.put(uuid, list);
            }
        }

        return length;
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
    void close() throws IOException {
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
        synchronized (file) {
            writeGraph();
            writeIndex();
            access.write(ZERO_BYTES);
            access.write(ZERO_BYTES);
            access.close();
        }
    }

    private void writeGraph() throws IOException {
        List<UUID> uuids = Lists.newArrayListWithCapacity(
                index.size() + references.size());
        uuids.addAll(index.keySet());
        uuids.addAll(references);
        Collections.sort(uuids);

        int graphSize = uuids.size() * 16 + 16;
        for (List<UUID> list : graph.values()) {
            graphSize += 4 + list.size() * 4 + 4;
        }
        int padding = getPaddingSize(graphSize);

        String graphName = file.getName() + ".gph";
        byte[] header = newEntryHeader(graphName, graphSize + padding);

        ByteBuffer buffer = ByteBuffer.allocate(graphSize);

        Map<UUID, Integer> refmap = newHashMap();

        int index = 0;
        for (UUID uuid : uuids) {
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
            refmap.put(uuid, index++);
        }

        for (Map.Entry<UUID, List<UUID>> entry : graph.entrySet()) {
            buffer.putInt(refmap.get(entry.getKey()));
            for (UUID refid : entry.getValue()) {
                buffer.putInt(refmap.get(refid));
            }
            buffer.putInt(-1);
        }

        CRC32 checksum = new CRC32();
        checksum.update(buffer.array(), 0, buffer.position());
        buffer.putInt((int) checksum.getValue());
        buffer.putInt(uuids.size());
        buffer.putInt(graphSize);
        buffer.putInt(GRAPH_MAGIC);

        access.write(header);
        if (padding > 0) {
            // padding comes *before* the graph!
            access.write(ZERO_BYTES, 0, padding);
        }
        access.write(buffer.array());
    }

    private void writeIndex() throws IOException {
        int indexSize = index.size() * 24 + 16;
        int padding = getPaddingSize(indexSize);

        String indexName = file.getName() + ".idx";
        byte[] header = newEntryHeader(indexName, indexSize + padding);

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
                String.format("%06o\0 ", checksum).getBytes(UTF_8), 0,
                header, 148, 8);

        return header;
    }

    synchronized void cleanup(Set<UUID> referencedIds) throws IOException {
        referencedIds.removeAll(index.keySet());
        referencedIds.addAll(references);
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return file.toString();
    }

}
