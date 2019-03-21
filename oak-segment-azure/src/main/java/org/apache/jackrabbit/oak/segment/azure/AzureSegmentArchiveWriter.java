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
package org.apache.jackrabbit.oak.segment.azure;

import static org.apache.jackrabbit.oak.segment.azure.AzureSegmentArchiveReader.OFF_HEAP;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.getSegmentFileName;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.readBufferFully;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.apache.jackrabbit.oak.segment.azure.queue.SegmentWriteAction;
import org.apache.jackrabbit.oak.segment.azure.queue.SegmentWriteQueue;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;

public class AzureSegmentArchiveWriter implements SegmentArchiveWriter {

    private final CloudBlobDirectory archiveDirectory;

    private final IOMonitor ioMonitor;

    private final FileStoreMonitor monitor;

    private final Optional<SegmentWriteQueue> queue;

    private Map<UUID, AzureSegmentArchiveEntry> index = Collections.synchronizedMap(new LinkedHashMap<>());

    private int entries;

    private long totalLength;

    private volatile boolean created = false;

    public AzureSegmentArchiveWriter(CloudBlobDirectory archiveDirectory, IOMonitor ioMonitor, FileStoreMonitor monitor) {
        this.archiveDirectory = archiveDirectory;
        this.ioMonitor = ioMonitor;
        this.monitor = monitor;
        this.queue = SegmentWriteQueue.THREADS > 0 ? Optional.of(new SegmentWriteQueue(this::doWriteEntry)) : Optional.empty();
    }

    @Override
    public void writeSegment(long msb, long lsb, byte[] data, int offset, int size, int generation, int fullGeneration, boolean compacted) throws IOException {
        created = true;

        AzureSegmentArchiveEntry entry = new AzureSegmentArchiveEntry(msb, lsb, entries++, size, generation, fullGeneration, compacted);
        if (queue.isPresent()) {
            queue.get().addToQueue(entry, data, offset, size);
        } else {
            doWriteEntry(entry, data, offset, size);
        }
        index.put(new UUID(msb, lsb), entry);

        totalLength += size;
        monitor.written(size);
    }

    private void doWriteEntry(AzureSegmentArchiveEntry indexEntry, byte[] data, int offset, int size) throws IOException {
        long msb = indexEntry.getMsb();
        long lsb = indexEntry.getLsb();
        ioMonitor.beforeSegmentWrite(pathAsFile(), msb, lsb, size);
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            CloudBlockBlob blob = getBlob(getSegmentFileName(indexEntry));
            blob.setMetadata(AzureBlobMetadata.toSegmentMetadata(indexEntry));
            blob.uploadFromByteArray(data, offset, size);
            blob.uploadMetadata();
        } catch (StorageException e) {
            throw new IOException(e);
        }
        ioMonitor.afterSegmentWrite(pathAsFile(), msb, lsb, size, stopwatch.elapsed(TimeUnit.NANOSECONDS));
    }

    @Override
    public Buffer readSegment(long msb, long lsb) throws IOException {
        UUID uuid = new UUID(msb, lsb);
        Optional<SegmentWriteAction> segment = queue.map(q -> q.read(uuid));
        if (segment.isPresent()) {
            return segment.get().toBuffer();
        }
        AzureSegmentArchiveEntry indexEntry = index.get(new UUID(msb, lsb));
        if (indexEntry == null) {
            return null;
        }

        Buffer buffer;
        if (OFF_HEAP) {
            buffer = Buffer.allocateDirect(indexEntry.getLength());
        } else {
            buffer = Buffer.allocate(indexEntry.getLength());
        }
        readBufferFully(getBlob(getSegmentFileName(indexEntry)), buffer);
        return buffer;
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
    public void writeGraph(byte[] data) throws IOException {
        writeDataFile(data, ".gph");
    }

    @Override
    public void writeBinaryReferences(byte[] data) throws IOException {
        writeDataFile(data, ".brf");
    }

    private void writeDataFile(byte[] data, String extension) throws IOException {
        try {
            getBlob(getName() + extension).uploadFromByteArray(data, 0, data.length);
        } catch (StorageException e) {
            throw new IOException(e);
        }
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
        try {
            getBlob("closed").uploadFromByteArray(new byte[0], 0, 0);
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean isCreated() {
        return created || !queueIsEmpty();
    }

    @Override
    public void flush() throws IOException {
        if (queue.isPresent()) { // required to handle IOException
            queue.get().flush();
        }
    }

    private boolean queueIsEmpty() {
        return queue.map(SegmentWriteQueue::isEmpty).orElse(true);
    }

    @Override
    public String getName() {
        return AzureUtilities.getName(archiveDirectory);
    }

    private File pathAsFile() {
        return new File(archiveDirectory.getUri().getPath());
    }

    private CloudBlockBlob getBlob(String name) throws IOException {
        try {
            return archiveDirectory.getBlockBlobReference(name);
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }
}
