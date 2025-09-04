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
package org.apache.jackrabbit.oak.segment.remote;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.remote.queue.SegmentWriteAction;
import org.apache.jackrabbit.oak.segment.remote.queue.SegmentWriteQueue;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public abstract class AbstractRemoteSegmentArchiveWriter implements SegmentArchiveWriter {
    protected final IOMonitor ioMonitor;

    protected final FileStoreMonitor monitor;

    protected final Optional<SegmentWriteQueue> queue;

    protected Map<UUID, RemoteSegmentArchiveEntry> index = Collections.synchronizedMap(new LinkedHashMap<>());

    protected int entries;

    protected long totalLength;

    protected volatile boolean created = false;

    protected WriteAccessController writeAccessController = null;

    public AbstractRemoteSegmentArchiveWriter(IOMonitor ioMonitor, FileStoreMonitor monitor) {
        this.ioMonitor = ioMonitor;
        this.monitor = monitor;
        this.queue = SegmentWriteQueue.THREADS > 0 ? Optional.of(new SegmentWriteQueue(this::doWriteArchiveEntry))
                : Optional.empty();
    }

    @Override
    public void writeSegment(long msb, long lsb, @NotNull byte[] data, int offset, int size, int generation,
            int fullGeneration, boolean compacted) throws IOException {
        created = true;

        RemoteSegmentArchiveEntry entry = new RemoteSegmentArchiveEntry(msb, lsb, entries++, size, generation, fullGeneration, compacted);
        if (queue.isPresent()) {
            queue.get().addToQueue(entry, data, offset, size);
        } else {
            doWriteArchiveEntry(entry, data, offset, size);
        }
        index.put(new UUID(msb, lsb), entry);

        totalLength += size;
        monitor.written(size);
    }

    @Override
    public Buffer readSegment(long msb, long lsb) throws IOException {
        UUID uuid = new UUID(msb, lsb);
        Optional<SegmentWriteAction> segment = queue.map(q -> q.read(uuid));
        if (segment.isPresent()) {
            return segment.get().toBuffer();
        }

        RemoteSegmentArchiveEntry indexEntry = index.get(new UUID(msb, lsb));
        if (indexEntry == null) {
            return null;
        }

        return doReadArchiveEntry(indexEntry);
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        UUID uuid = new UUID(msb, lsb);
        Optional<SegmentWriteAction> segment = queue.map(q -> q.read(uuid));
        if (segment.isPresent()) {
            return true;
        }
        return index.containsKey(new UUID(msb, lsb));
    }

    @Override
    public void writeGraph(@NotNull byte[] data) throws IOException {
        writeDataFile(data, ".gph");
    }

    @Override
    public void writeBinaryReferences(@NotNull byte[] data) throws IOException {
        writeDataFile(data, ".brf");
    }

    public void writeDataFile(byte[] data, String extension) throws IOException {
        doWriteDataFile(data, extension);
        totalLength += data.length;
        monitor.written(data.length);
    }

    @Override
    public long getLength() {
        return totalLength;
    }

    @Override
    public int getEntryCount() {
        return index.size();
    }

    @Override
    public void close() throws IOException {
        if (queue.isPresent()) { // required to handle IOException
            SegmentWriteQueue q = queue.get();
            q.flush();
            q.close();
        }

        afterQueueClosed();
    }

    @Override
    public boolean isCreated() {
        return created || !queueIsEmpty();
    }

    private boolean queueIsEmpty() {
        return queue.map(SegmentWriteQueue::isEmpty).orElse(true);
    }

    @Override
    public void flush() throws IOException {
        if (queue.isPresent()) { // required to handle IOException
            queue.get().flush();
            afterQueueFlushed();
        }
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public int getMaxEntryCount() {
        return RemoteUtilities.MAX_ENTRY_COUNT;
    }

    /**
     * Writes a segment to the remote storage.
     * @param indexEntry, the archive index entry to write
     * @param data, the actual bytes in the entry
     * @param offset,  the start offset in the data.
     * @param size, the number of bytes to write.
     */
    protected abstract void doWriteArchiveEntry(RemoteSegmentArchiveEntry indexEntry, byte[] data, int offset, int size) throws IOException;

    /**
     * Reads a segment from remote storage into a buffer.
     * @param indexEntry, the archive index entry to read
     * @return th buffer containing the segment bytes
     */
    protected abstract Buffer doReadArchiveEntry(RemoteSegmentArchiveEntry indexEntry) throws IOException;

    /**
     * Writes a data file inside the archive. This entry is not a segment. Its full name is given by archive name + extension.
     * @param data, bytes to write
     * @param extension, the extension of the data file
     */
    protected abstract void doWriteDataFile(byte[] data, String extension) throws IOException;

    /**
     * Hook for executing additional actions after the segment write queue is closed.
     */
    protected abstract void afterQueueClosed() throws IOException;

    /**
     * Hook for executing additional actions after the segment write queue is flushed.
     */
    protected abstract void afterQueueFlushed() throws IOException;
}