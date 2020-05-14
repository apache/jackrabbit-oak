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
package org.apache.jackrabbit.oak.segment.aws;

import static org.apache.jackrabbit.oak.segment.aws.AwsSegmentArchiveReader.OFF_HEAP;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.aws.queue.SegmentWriteAction;
import org.apache.jackrabbit.oak.segment.aws.queue.SegmentWriteQueue;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;

public class AwsSegmentArchiveWriter implements SegmentArchiveWriter {

    private final AwsContext directoryContext;

    private final String archiveName;

    private final IOMonitor ioMonitor;

    private final FileStoreMonitor monitor;

    private final Optional<SegmentWriteQueue> queue;

    private Map<UUID, AwsSegmentArchiveEntry> index = Collections.synchronizedMap(new LinkedHashMap<>());

    private int entries;

    private long totalLength;

    private volatile boolean created = false;

    public AwsSegmentArchiveWriter(AwsContext directoryContext, String archiveName, IOMonitor ioMonitor,
            FileStoreMonitor monitor) {
        this.directoryContext = directoryContext;
        this.archiveName = archiveName;
        this.ioMonitor = ioMonitor;
        this.monitor = monitor;
        this.queue = SegmentWriteQueue.THREADS > 0 ? Optional.of(new SegmentWriteQueue(this::doWriteEntry))
                : Optional.empty();
    }

    @Override
    public void writeSegment(long msb, long lsb, byte[] data, int offset, int size, int generation, int fullGeneration,
            boolean compacted) throws IOException {
        created = true;

        AwsSegmentArchiveEntry entry = new AwsSegmentArchiveEntry(msb, lsb, entries++, size, generation, fullGeneration,
                compacted);
        if (queue.isPresent()) {
            queue.get().addToQueue(entry, data, offset, size);
        } else {
            doWriteEntry(entry, data, offset, size);
        }
        index.put(new UUID(msb, lsb), entry);

        totalLength += size;
        monitor.written(size);
    }

    private void doWriteEntry(AwsSegmentArchiveEntry indexEntry, byte[] data, int offset, int size) throws IOException {
        long msb = indexEntry.getMsb();
        long lsb = indexEntry.getLsb();
        String segmentName = indexEntry.getFileName();
        String fullName = directoryContext.getPath() + segmentName;
        ioMonitor.beforeSegmentWrite(new File(fullName), msb, lsb, size);
        Stopwatch stopwatch = Stopwatch.createStarted();
        directoryContext.writeObject(segmentName, data, AwsBlobMetadata.toSegmentMetadata(indexEntry));
        ioMonitor.afterSegmentWrite(new File(fullName), msb, lsb, size, stopwatch.elapsed(TimeUnit.NANOSECONDS));
    }

    @Override
    public Buffer readSegment(long msb, long lsb) throws IOException {
        UUID uuid = new UUID(msb, lsb);
        Optional<SegmentWriteAction> segment = queue.map(q -> q.read(uuid));
        if (segment.isPresent()) {
            return segment.get().toBuffer();
        }
        AwsSegmentArchiveEntry indexEntry = index.get(new UUID(msb, lsb));
        if (indexEntry == null) {
            return null;
        }
        return directoryContext.readObjectToBuffer(indexEntry.getFileName(), OFF_HEAP);
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
        directoryContext.writeObject(getName() + extension, data);
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
        directoryContext.writeObject("closed", new byte[0]);
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
        return archiveName;
    }
}
