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

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Stopwatch;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;

public class AwsSegmentArchiveReader implements SegmentArchiveReader {
    static final boolean OFF_HEAP = getBoolean("access.off.heap");

    private final AwsContext directoryContext;

    private final String archiveName;

    private final IOMonitor ioMonitor;

    private final long length;

    private final Map<UUID, AwsSegmentArchiveEntry> index = new LinkedHashMap<>();

    private Boolean hasGraph;

    AwsSegmentArchiveReader(AwsContext directoryContext, String archiveName, IOMonitor ioMonitor) throws IOException {
        this.directoryContext = directoryContext;
        this.archiveName = archiveName;
        this.ioMonitor = ioMonitor;
        long length = 0;
        for (S3ObjectSummary blob : directoryContext.listObjects("")) {
            ObjectMetadata allMetadata = directoryContext.getObjectMetadata(blob.getKey());
            Map<String, String> metadata = allMetadata.getUserMetadata();
            if (AwsBlobMetadata.isSegment(metadata)) {
                AwsSegmentArchiveEntry indexEntry = AwsBlobMetadata.toIndexEntry(metadata,
                        (int) allMetadata.getContentLength());
                index.put(new UUID(indexEntry.getMsb(), indexEntry.getLsb()), indexEntry);
            }
            length += allMetadata.getContentLength();
        }
        this.length = length;
    }

    @Override
    public Buffer readSegment(long msb, long lsb) throws IOException {
        AwsSegmentArchiveEntry indexEntry = index.get(new UUID(msb, lsb));
        if (indexEntry == null) {
            return null;
        }

        ioMonitor.beforeSegmentRead(pathAsFile(), msb, lsb, indexEntry.getLength());
        Stopwatch stopwatch = Stopwatch.createStarted();
        Buffer buffer = directoryContext.readObjectToBuffer(indexEntry.getFileName(), OFF_HEAP);
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
        if (directoryContext.doesObjectExist(name)) {
            return directoryContext.readObjectToBuffer(name, false);
        }

        return null;
    }

    private File pathAsFile() {
        return new File(directoryContext.getPath());
    }
}
