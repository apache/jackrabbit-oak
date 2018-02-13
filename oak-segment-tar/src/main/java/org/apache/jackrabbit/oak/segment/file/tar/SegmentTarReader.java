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

import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.oak.segment.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndex;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexLoader;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.InvalidBinaryReferencesIndexException;
import org.apache.jackrabbit.oak.segment.file.tar.index.Index;
import org.apache.jackrabbit.oak.segment.file.tar.index.IndexEntry;
import org.apache.jackrabbit.oak.segment.file.tar.index.IndexLoader;
import org.apache.jackrabbit.oak.segment.file.tar.index.InvalidIndexException;
import org.apache.jackrabbit.oak.segment.util.ReaderAtEnd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.segment.file.tar.SegmentTarWriter.getPaddingSize;
import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.BLOCK_SIZE;
import static org.apache.jackrabbit.oak.segment.file.tar.index.IndexLoader.newIndexLoader;

public class SegmentTarReader implements SegmentArchiveManager.SegmentArchiveReader {

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
    public ByteBuffer readSegment(long msb, long lsb) throws IOException {
        int i = index.findEntry(msb, lsb);
        if (i == -1) {
            return null;
        }
        IndexEntry indexEntry = index.entry(i);
        ioMonitor.beforeSegmentRead(file, msb, lsb, indexEntry.getLength());
        Stopwatch stopwatch = Stopwatch.createStarted();
        ByteBuffer buffer = access.read(indexEntry.getPosition(), indexEntry.getLength());
        long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
        ioMonitor.afterSegmentRead(file, msb, lsb, indexEntry.getLength(), elapsed);
        return buffer;
    }

    @Override
    public Index getIndex() {
        return index;
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
            ByteBuffer buffer = ByteBuffer.allocate(size);
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
    public Map<UUID, List<UUID>> getGraph() throws IOException {
        ByteBuffer graph = loadGraph();
        if (graph == null) {
            return null;
        } else {
            return GraphLoader.parseGraph(graph);
        }
    }

    @Override
    public boolean hasGraph() {
        if (hasGraph == null) {
            try {
                loadGraph();
            } catch (IOException ignore) { }
        }
        return hasGraph;
    }

    private ByteBuffer loadGraph() throws IOException {
        int end = access.length() - 2 * BLOCK_SIZE - getIndexEntrySize();
        ByteBuffer graph = GraphLoader.loadGraph((whence, amount) -> access.read(end - whence, amount));
        hasGraph = graph != null;
        return graph;
    }

    @Override
    public BinaryReferencesIndex getBinaryReferences() throws IOException, InvalidBinaryReferencesIndexException {
        int end = access.length() - 2 * BLOCK_SIZE - getIndexEntrySize() - getGraphEntrySize();
        return BinaryReferencesIndexLoader.loadBinaryReferencesIndex((whence, size) -> access.read(end - whence, size));
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
        ByteBuffer buffer;

        try {
            buffer = loadGraph();
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
