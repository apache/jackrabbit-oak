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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexLoader;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.InvalidBinaryReferencesIndexException;
import org.apache.jackrabbit.oak.segment.file.tar.index.Index;
import org.apache.jackrabbit.oak.segment.file.tar.index.IndexEntry;
import org.apache.jackrabbit.oak.segment.file.tar.index.IndexLoader;
import org.apache.jackrabbit.oak.segment.file.tar.index.InvalidIndexException;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;
import org.apache.jackrabbit.oak.segment.util.ReaderAtEnd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentTarReader implements SegmentArchiveReader {

    private static final Logger log = LoggerFactory.getLogger(SegmentTarReader.class);

    private static final IndexLoader indexLoader = newIndexLoader(BLOCK_SIZE);

    private final FileAccess access;

    private final File file;

    private final IOMonitor ioMonitor;

    private final String name;

    private final Index index;

    private volatile Boolean hasGraph;

    public SegmentTarReader(File file, FileAccess access, Index index, IOMonitor ioMonitor) {
        this.access = access;
        this.file = file;
        this.index = index;
        this.name = file.getName();
        this.ioMonitor = ioMonitor;
    }

    @Override
    public Buffer readSegment(long msb, long lsb) throws IOException {
        int i = index.findEntry(msb, lsb);
        if (i == -1) {
            return null;
        }
        IndexEntry indexEntry = index.entry(i);
        ioMonitor.beforeSegmentRead(file, msb, lsb, indexEntry.getLength());
        Stopwatch stopwatch = Stopwatch.createStarted();
        Buffer buffer = access.read(indexEntry.getPosition(), indexEntry.getLength());
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

    public static Index loadAndValidateIndex(RandomAccessFile file, String name) throws IOException {
        long length = file.length();
        if (length % BLOCK_SIZE != 0) {
            log.warn("Unable to load index of file {}: Invalid alignment", name);
            return null;
        }
        if (length < 6 * BLOCK_SIZE) {
            log.warn("Unable to load index of file {}: File too short", name);
            return null;
        }
        if (length > Integer.MAX_VALUE) {
            log.warn("Unable to load index of file {}: File too long", name);
            return null;
        }
        ReaderAtEnd r = (whence, size) -> {
            Buffer buffer = Buffer.allocate(size);
            file.seek(length - 2 * BLOCK_SIZE - whence);
            file.readFully(buffer.array());
            return buffer;
        };
        try {
            return indexLoader.loadIndex(r);
        } catch (InvalidIndexException e) {
            log.warn("Unable to load index of file {}: {}", name, e.getMessage());
        }
        return null;
    }

    @Override
    public Buffer getGraph() throws IOException {
        int end = access.length() - 2 * BLOCK_SIZE - getIndexEntrySize();
        Buffer graph = GraphLoader.loadGraph((whence, amount) -> access.read(end - whence, amount));
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
        return hasGraph;
    }

    @Override
    public Buffer getBinaryReferences() throws IOException {
        try {
            int end = access.length() - 2 * BLOCK_SIZE - getIndexEntrySize() - getGraphEntrySize();
            return BinaryReferencesIndexLoader.loadBinaryReferencesIndex((whence, amount) -> access.read(end - whence, amount));
        } catch (InvalidBinaryReferencesIndexException e) {
            throw new IOException(e);
        }
    }

    @Override
    public long length() {
        return file.length();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() throws IOException {
        access.close();
    }

    @Override
    public int getEntrySize(int size) {
        return BLOCK_SIZE + size + getPaddingSize(size);
    }

    private int getIndexEntrySize() {
        return getEntrySize(index.size());
    }

    private int getGraphEntrySize() {
        Buffer buffer;

        try {
            buffer = getGraph();
        } catch (IOException e) {
            log.warn("Exception while loading pre-compiled tar graph", e);
            return 0;
        }

        if (buffer == null) {
            return 0;
        }

        return getEntrySize(buffer.getInt(buffer.limit() - 8));
    }
}
