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

import static org.apache.jackrabbit.oak.segment.remote.RemoteUtilities.getSegmentFileName;
import static org.apache.jackrabbit.oak.segment.remote.RemoteUtilities.OFF_HEAP;

import com.google.common.base.Stopwatch;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public abstract class AbstractRemoteSegmentArchiveReader implements SegmentArchiveReader {
    protected final IOMonitor ioMonitor;

    protected final Map<UUID, RemoteSegmentArchiveEntry> index = new LinkedHashMap<>();

    protected Boolean hasGraph;

    public AbstractRemoteSegmentArchiveReader(IOMonitor ioMonitor) throws IOException {
        this.ioMonitor = ioMonitor;
    }

    @Override
    public Buffer readSegment(long msb, long lsb) throws IOException {
        RemoteSegmentArchiveEntry indexEntry = index.get(new UUID(msb, lsb));
        if (indexEntry == null) {
            return null;
        }

        Buffer buffer;
        if (OFF_HEAP) {
            buffer = Buffer.allocateDirect(indexEntry.getLength());
        } else {
            buffer = Buffer.allocate(indexEntry.getLength());
        }
        ioMonitor.beforeSegmentRead(archivePathAsFile(), msb, lsb, indexEntry.getLength());
        Stopwatch stopwatch = Stopwatch.createStarted();
        String segmentFileName = getSegmentFileName(indexEntry);
        doReadSegmentToBuffer(segmentFileName, buffer);
        long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
        ioMonitor.afterSegmentRead(archivePathAsFile(), msb, lsb, indexEntry.getLength(), elapsed);
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
        Buffer graph = doReadDataFile(".gph");
        hasGraph = graph != null;
        return graph;
    }

    @Override
    public boolean hasGraph() {
        if (hasGraph == null) {
            try {
                getGraph();
            } catch (IOException ignore) { }
        }
        return hasGraph != null ? hasGraph : false;
    }

    @Override
    public Buffer getBinaryReferences() throws IOException {
        return doReadDataFile(".brf");
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public int getEntrySize(int size) {
        return size;
    }

    /**
     * Populates the archive index, summing up each entry's length.
     * @return length, the total length of the archive
     */
    protected abstract long computeArchiveIndexAndLength() throws IOException;

    /**
     * Reads the segment from the remote storage.
     * @param segmentFileName, the name of the segment (msb + lsb) prefixed by its position in the archive
     * @param buffer, the buffer to which to read
     */
    protected abstract void doReadSegmentToBuffer(String segmentFileName, Buffer buffer) throws IOException;

    /**
     * Reads a data file inside the archive. This entry is not a segment. Its full name is given by archive name + extension.
     * @param extension, extension of the file
     * @return the buffer containing the data file bytes
     */
    protected abstract Buffer doReadDataFile(String extension) throws IOException;

    /**
     * Returns the decoded file component of this archive.
     * @return the decoded file component of this archive.
     */
    protected abstract File archivePathAsFile();
}
