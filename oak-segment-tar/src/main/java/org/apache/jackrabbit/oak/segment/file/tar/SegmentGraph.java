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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.data.SegmentData;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.util.ReaderAtEnd;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SegmentGraph {

    private static final Logger log = LoggerFactory.getLogger(SegmentGraph.class);

    private static final int FOOTER_SIZE = 16;

    /**
     * Magic byte sequence at the end of the graph block.
     * <p>
     * The file is read from the end (the tar file is read from the end: the
     * last entry is the index, then the graph). File format:
     * <ul>
     * <li>0 padding to make the footer end at a 512 byte boundary</li>
     * <li>The list of UUIDs (segments included the graph; this includes
     * segments in this tar file, and referenced segments in tar files with a
     * lower sequence number). 16 bytes each.</li>
     * <li>The graph data. The index of the source segment UUID (in the above
     * list, 4 bytes), then the list of referenced segments (the indexes of
     * those; 4 bytes each). Then the list is terminated by -1.</li>
     * <li>The last part is the footer, which contains metadata of the graph
     * (size, checksum, the number of UUIDs).</li>
     * </ul>
     */
    private static final int MAGIC = ('\n' << 24) + ('0' << 16) + ('G' << 8) + '\n';

    private final @NotNull Map<UUID, Set<UUID>> edgeMap;

    public SegmentGraph() {
        this.edgeMap = new HashMap<>();
    }

    private SegmentGraph(@NotNull Map<UUID, Set<UUID>> edgeMap) {
        this.edgeMap = edgeMap;
    }

    public void addEdge(@NotNull UUID from, @NotNull UUID to) {
        edgeMap.computeIfAbsent(from, k -> new HashSet<>()).add(to);
    }

    public @NotNull Map<UUID, Set<UUID>> getEdges() {
        return Collections.unmodifiableMap(edgeMap);
    }

    public @NotNull Set<UUID> getEdges(@NotNull UUID from) {
        Set<UUID> set = edgeMap.getOrDefault(from, Collections.emptySet());
        return Collections.unmodifiableSet(set);
    }

    /**
     * Loads the optional pre-compiled graph entry from the given tar file.
     *
     * @return the graph or {@code null} if one was not found
     * @throws IOException if the tar file could not be read
     */
    public static @Nullable SegmentGraph load(ReaderAtEnd readerAtEnd) throws IOException {
        Buffer meta = readerAtEnd.readAtEnd(FOOTER_SIZE, FOOTER_SIZE);

        int crc32 = meta.getInt();
        int count = meta.getInt();
        int bytes = meta.getInt();
        int magic = meta.getInt();

        if (magic != MAGIC) {
            log.warn("Invalid graph magic number");
            return null;
        }

        if (count < 0) {
            log.warn("Invalid number of entries");
            return null;
        }

        if (bytes < 4 + count * 34) {
            log.warn("Invalid entry size");
            return null;
        }

        Buffer buffer = readerAtEnd.readAtEnd(bytes, bytes);
        byte[] b = new byte[bytes - FOOTER_SIZE];

        buffer.mark();
        buffer.get(b);
        buffer.reset();

        CRC32 checksum = new CRC32();
        checksum.update(b);

        if (crc32 != (int) checksum.getValue()) {
            log.warn("Invalid graph checksum in tar file");
            return null;
        }

        return SegmentGraph.parse(buffer);
    }

    /**
     * Computes the graph from a segment archive
     *
     * @return the computed segment graph.
     */
    public static @NotNull SegmentGraph compute(SegmentArchiveReader archiveReader) throws IOException {
        SegmentGraph graph = new SegmentGraph();
        for (SegmentArchiveEntry entry : archiveReader.listSegments()) {
            if (!SegmentId.isDataSegmentId(entry.getLsb())) {
                continue;
            }
            UUID from = new UUID(entry.getMsb(), entry.getLsb());
            Buffer buffer = archiveReader.readSegment(entry.getMsb(), entry.getLsb());
            SegmentData data = SegmentData.newSegmentData(buffer);
            for (int i = 0; i < data.getSegmentReferencesCount(); i++) {
                UUID to = new UUID(data.getSegmentReferenceMsb(i), data.getSegmentReferenceLsb(i));
                graph.addEdge(from, to);
            }
        }
        return graph;
    }

    public static @NotNull SegmentGraph parse(@NotNull Buffer buffer) {
        int nEntries = buffer.getInt(buffer.limit() - 12);
        Map<UUID, Set<UUID>> edgeMap = new HashMap<>(nEntries);

        for (int i = 0; i < nEntries; i++) {
            long msb = buffer.getLong();
            long lsb = buffer.getLong();
            int nVertices = buffer.getInt();

            Set<UUID> vertices = new HashSet<>(nVertices);

            for (int j = 0; j < nVertices; j++) {
                long vMsb = buffer.getLong();
                long vLsb = buffer.getLong();
                vertices.add(new UUID(vMsb, vLsb));
            }

            edgeMap.put(new UUID(msb, lsb), vertices);
        }

        return new SegmentGraph(edgeMap);
    }

    public byte[] write() {
        int graphSize = size();
        Buffer buffer = Buffer.allocate(graphSize);

        for (Map.Entry<UUID, Set<UUID>> entry : edgeMap.entrySet()) {
            UUID from = entry.getKey();
            buffer.putLong(from.getMostSignificantBits());
            buffer.putLong(from.getLeastSignificantBits());

            Set<UUID> adj = entry.getValue();
            buffer.putInt(adj.size());
            for (UUID to : adj) {
                buffer.putLong(to.getMostSignificantBits());
                buffer.putLong(to.getLeastSignificantBits());
            }
        }

        CRC32 checksum = new CRC32();
        checksum.update(buffer.array(), 0, buffer.position());

        buffer.putInt((int) checksum.getValue());
        buffer.putInt(edgeMap.size());
        buffer.putInt(graphSize);
        buffer.putInt(MAGIC);

        return buffer.array();
    }

    public int size() {
        // The following information is stored in the footer as meta information about the entry.
        // 4 bytes to store a magic number identifying this entry as containing references to binary values.
        // 4 bytes to store the CRC32 checksum of the data in this entry.
        // 4 bytes to store the length of this entry, without including the optional padding.
        // 4 bytes to store the number of entries in the graph map.
        int graphSize = FOOTER_SIZE;
        // The following information is stored as part of the main content of
        // this entry, after the optional padding.
        for (Map.Entry<UUID, Set<UUID>> entry : edgeMap.entrySet()) {
            // 16 bytes to store the key of the map.
            graphSize += 16;
            // 4 bytes for the number of entries in the adjacency list.
            graphSize += 4;
            // 16 bytes for every element in the adjacency list.
            graphSize += 16 * entry.getValue().size();
        }
        return graphSize;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other instanceof SegmentGraph) {
            return edgeMap.equals(((SegmentGraph) other).edgeMap);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return edgeMap.hashCode();
    }
}
