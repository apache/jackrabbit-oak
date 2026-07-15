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

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.commons.time.Stopwatch;
import org.apache.jackrabbit.oak.segment.file.tar.SegmentGraph;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class AbstractRemoteSegmentArchiveReader implements SegmentArchiveReader {

    protected final IOMonitor ioMonitor;

    /**
     * Unordered immutable map of segment UUIDs to their corresponding archive entries.
     */
    private final Map<UUID, RemoteSegmentArchiveEntry> index;

    /**
     * The name of the archive.
     */
    private final String archiveName;

    /**
     * The total size of the archive in bytes.
     */
    private final long length;

    protected AbstractRemoteSegmentArchiveReader(IOMonitor ioMonitor, String archiveName, Iterable<ArchiveEntry> entries) {
        this.ioMonitor = ioMonitor;
        this.archiveName = archiveName;

        IndexBuilder indexBuilder = new IndexBuilder();
        entries.forEach(indexBuilder::addEntry);
        this.index = indexBuilder.createIndex();
        this.length = indexBuilder.getLength();
    }

    @Override
    public @NotNull String getName() {
        return archiveName;
    }

    @Override
    public long length() {
        return length;
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
    public Set<UUID> getSegmentUUIDs() {
        return Collections.unmodifiableSet(index.keySet());
    }

    @Override
    public List<SegmentArchiveEntry> listSegments() {
        return index.values().stream()
                .sorted(Comparator.comparing(RemoteSegmentArchiveEntry::getPosition))
                .collect(Collectors.toList());
    }

    @Override
    public @NotNull SegmentGraph getGraph() throws IOException {
        Buffer buffer = doReadDataFile(".gph");
        if (buffer != null) {
            return SegmentGraph.parse(buffer);
        }
        return SegmentGraph.compute(this);
    }

    @Override
    public @Nullable Buffer getBinaryReferences() throws IOException {
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

    @Override
    public boolean isRemote() {
        return true;
    }

    protected static final class ArchiveEntry {

        private final RemoteSegmentArchiveEntry entry;

        private final int length;

        public ArchiveEntry(RemoteSegmentArchiveEntry entry) {
            this.entry = entry;
            this.length = entry.getLength();
        }

        public ArchiveEntry(int length) {
            this.entry = null;
            this.length = length;
        }

        int getLength() {
            return length;
        }

        RemoteSegmentArchiveEntry getRemoteSegmentArchiveEntry() {
            return entry;
        }
    }

    private static final class IndexBuilder {

        private final List<Map.Entry<UUID, RemoteSegmentArchiveEntry>> entries = new LinkedList<>();

        private long length = 0;

        private void addEntry(ArchiveEntry entry) {
            RemoteSegmentArchiveEntry archiveEntry = entry.getRemoteSegmentArchiveEntry();
            if (archiveEntry != null) {
                this.entries.add(Map.entry(archiveEntry.getUuid(), archiveEntry));
            }
            this.length += entry.getLength();
        }

        @SuppressWarnings("unchecked")
        private Map<UUID, RemoteSegmentArchiveEntry> createIndex() {
            return Map.ofEntries(entries.toArray(Map.Entry[]::new));
        }

        private long getLength() {
            return length;
        }
    }
}
