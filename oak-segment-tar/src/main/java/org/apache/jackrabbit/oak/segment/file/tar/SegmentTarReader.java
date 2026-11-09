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

import static org.apache.jackrabbit.oak.segment.file.tar.SegmentTarWriter.getPaddingSize;
import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.BLOCK_SIZE;
import static org.apache.jackrabbit.oak.segment.file.tar.index.IndexLoader.newIndexLoader;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.commons.time.Stopwatch;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexLoader;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.InvalidBinaryReferencesIndexException;
import org.apache.jackrabbit.oak.segment.file.tar.index.Index;
import org.apache.jackrabbit.oak.segment.file.tar.index.IndexEntry;
import org.apache.jackrabbit.oak.segment.file.tar.index.IndexLoader;
import org.apache.jackrabbit.oak.segment.file.tar.index.InvalidIndexException;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.util.ReaderAtEnd;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentTarReader implements SegmentArchiveReader {

    private static final Logger log = LoggerFactory.getLogger(SegmentTarReader.class);

    private static final IndexLoader indexLoader = newIndexLoader(BLOCK_SIZE);

    private final IOReadAccess io;

    private final IOMonitor ioMonitor;

    private final Index index;

    static class FileIOReadAccess implements IOReadAccess {

        protected final File file;

        protected FileAccess readAccess;

        protected FileIOReadAccess(File file) {
            this.file = file;
        }

        private FileIOReadAccess(File file, FileAccess access) {
            this(file);
            this.readAccess = access;
        }

        private void initIfNeeded() throws IOException {
            if (readAccess == null) {
                readAccess = new FileAccess.Random(new RandomAccessFile(file, "r"));
            }
        }

        @Override
        public String getName() {
            return file.getName();
        }

        @Override
        public Buffer read(int position, int length) throws IOException {
            initIfNeeded();
            return readAccess.read(position, length);
        }

        @Override
        public int length() {
            try {
                initIfNeeded();
                return readAccess.length();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            if (readAccess != null) {
                readAccess.close();
                readAccess = null;
            }
        }
    }

    static SegmentArchiveReader create(File file, FileAccess fileAccess, IOMonitor ioMonitor) throws IOException {
        return create(new FileIOReadAccess(file, fileAccess), ioMonitor);
    }

    public static @Nullable SegmentArchiveReader create(IOReadAccess io, IOMonitor ioMonitor) throws IOException {
        Index index = loadAndValidateIndex(io);
        if (index == null) {
            log.info("No index found in tar file {}, skipping...", io.getName());
            return null;
        }
        return new SegmentTarReader(io, index, ioMonitor);
    }

    private SegmentTarReader(IOReadAccess io, Index index, IOMonitor ioMonitor) {
        this.io = io;
        this.index = index;
        this.ioMonitor = ioMonitor;
    }

    @Override
    public Buffer readSegment(long msb, long lsb) throws IOException {
        int i = index.findEntry(msb, lsb);
        if (i == -1) {
            return null;
        }
        IndexEntry indexEntry = index.entry(i);
        File file = new File(io.getName());
        ioMonitor.beforeSegmentRead(file, msb, lsb, indexEntry.getLength());
        Stopwatch stopwatch = Stopwatch.createStarted();
        Buffer buffer = io.read(indexEntry.getPosition(), indexEntry.getLength());
        long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
        ioMonitor.afterSegmentRead(file, msb, lsb, indexEntry.getLength(), elapsed);
        return buffer;
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        return index.findEntry(msb, lsb) != -1;
    }

    @Override
    public List<SegmentArchiveEntry> listSegments() {
        IndexEntry[] entries = new IndexEntry[index.count()];
        for (int i = 0; i < index.count(); i++) {
            entries[i] = index.entry(i);
        }
        Arrays.sort(entries, IndexEntry.POSITION_ORDER);
        return Arrays.asList(entries);
    }

    public static Index loadAndValidateIndex(IOReadAccess io) throws IOException {
        int length = io.length();
        if (length % BLOCK_SIZE != 0) {
            log.warn("Unable to load index of file {}: Invalid alignment", io.getName());
            return null;
        }
        if (length < 6 * BLOCK_SIZE) {
            log.warn("Unable to load index of file {}: File too short", io.getName());
            return null;
        }
        ReaderAtEnd r = (whence, size) -> {
            int offset = length - 2 * BLOCK_SIZE - whence;
            return io.read(offset, size);
        };
        try {
            return indexLoader.loadIndex(r);
        } catch (InvalidIndexException e) {
            log.warn("Unable to load index of file {}: {}", io.getName(), e.getMessage());
        }
        return null;
    }

    @Override
    public @NotNull SegmentGraph getGraph() throws IOException {
        int end = io.length() - 2 * BLOCK_SIZE - getIndexEntrySize();
        SegmentGraph graph = SegmentGraph.load((whence, amount) -> io.read(end - whence, amount));
        if (graph == null) {
            log.warn("Recomputing missing graph for {}.", io.getName());
            graph = SegmentGraph.compute(this);
        }
        return graph;
    }

    @Override
    public @NotNull Buffer getBinaryReferences() throws IOException {
        try {
            int end = io.length() - 2 * BLOCK_SIZE - getIndexEntrySize() - getGraphEntrySize();
            return BinaryReferencesIndexLoader.loadBinaryReferencesIndex((whence, amount) -> io.read(end - whence, amount));
        } catch (InvalidBinaryReferencesIndexException e) {
            throw new IOException(e);
        }
    }

    @Override
    public long length() {
        return io.length();
    }

    @Override
    public @NotNull String getName() {
        return io.getName();
    }

    @Override
    public void close() throws IOException {
        io.close();
    }

    @Override
    public int getEntrySize(int size) {
        return BLOCK_SIZE + size + getPaddingSize(size);
    }

    private int getIndexEntrySize() {
        return getEntrySize(index.size());
    }

    private int getGraphEntrySize() {
        try {
            return getEntrySize(getGraph().size());
        } catch (IOException e) {
            log.warn("Exception while loading pre-compiled tar graph", e);
            return 0;
        }
    }

    @Override
    public boolean isRemote() {
        return false;
    }
}
