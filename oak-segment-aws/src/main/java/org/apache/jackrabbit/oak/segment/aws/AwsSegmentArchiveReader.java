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

import static java.lang.Boolean.getBoolean;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;

public class AwsSegmentArchiveReader implements SegmentArchiveReader {
    static final boolean OFF_HEAP = getBoolean("access.off.heap");

    private final S3Directory directory;

    private final String archiveName;

    private final IOMonitor ioMonitor;

    private final long length;

    private final Map<UUID, AwsSegmentArchiveEntry> index = new LinkedHashMap<>();

    private Boolean hasGraph;

    AwsSegmentArchiveReader(S3Directory directory, String archiveName, IOMonitor ioMonitor) throws IOException {
        this.directory = directory;
        this.archiveName = archiveName;
        this.ioMonitor = ioMonitor;
        this.length = readIndex();
    }

    private long readIndex() throws IOException {
        long length = 0;
        Buffer buffer = directory.readObjectToBuffer(archiveName + ".idx", OFF_HEAP);
        while (buffer.hasRemaining()) {
            long msb = buffer.getLong();
            long lsb = buffer.getLong();
            int position = buffer.getInt();
            int contentLength = buffer.getInt();
            int generation = buffer.getInt();
            int fullGeneration = buffer.getInt();
            boolean compacted = buffer.get() != 0;

            AwsSegmentArchiveEntry indexEntry = new AwsSegmentArchiveEntry(msb, lsb, position, contentLength,
                    generation, fullGeneration, compacted);
            index.put(new UUID(indexEntry.getMsb(), indexEntry.getLsb()), indexEntry);
            length += contentLength;
        }

        return length;
    }

    @Override
    public Buffer readSegment(long msb, long lsb) throws IOException {
        AwsSegmentArchiveEntry indexEntry = index.get(new UUID(msb, lsb));
        if (indexEntry == null) {
            return null;
        }

        ioMonitor.beforeSegmentRead(pathAsFile(), msb, lsb, indexEntry.getLength());
        Stopwatch stopwatch = Stopwatch.createStarted();
        Buffer buffer = directory.readObjectToBuffer(indexEntry.getFileName(), OFF_HEAP);
        long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
        ioMonitor.afterSegmentRead(pathAsFile(), msb, lsb, indexEntry.getLength(), elapsed);
        return buffer;
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        return index.containsKey(new UUID(msb, lsb));
    }

    @Override
    public List<SegmentArchiveEntry> listSegments() {
        return new ArrayList<>(index.values());
    }

    @Override
    public Buffer getGraph() throws IOException {
        Buffer graph = readObjectToBuffer(getName() + ".gph");
        hasGraph = graph != null;
        return graph;
    }

    @Override
    public boolean hasGraph() {
        if (hasGraph == null) {
            try {
                getGraph();
            } catch (IOException ignore) {
            }
        }
        return hasGraph;
    }

    @Override
    public Buffer getBinaryReferences() throws IOException {
        return readObjectToBuffer(getName() + ".brf");
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public String getName() {
        return archiveName;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public int getEntrySize(int size) {
        return size;
    }

    private Buffer readObjectToBuffer(String name) throws IOException {
        if (directory.doesObjectExist(name)) {
            return directory.readObjectToBuffer(name, false);
        }

        return null;
    }

    private File pathAsFile() {
        return new File(directory.getPath());
    }
}
