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
package org.apache.jackrabbit.oak.segment.file.tar.binaries;

import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexWriter.newBinaryReferencesIndexWriter;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.jetbrains.annotations.NotNull;

/**
 * Rebuilds the binary references index of an archive from its segments.
 * <p>
 * The value of a long blob id can be stored in a segment of another archive. Such a segment is
 * read from the fallback store, which is requested only when it is needed.
 */
public class BinaryReferencesCollector {

    private final Supplier<SegmentStore> fallback;

    private final Map<SegmentId, Segment> segments = new HashMap<>();

    private final SegmentStore store = new SegmentStore() {

        @Override
        public boolean containsSegment(SegmentId id) {
            return segments.containsKey(id) || fallback.get().containsSegment(id);
        }

        @Override
        public @NotNull Segment readSegment(SegmentId id) {
            Segment segment = segments.get(id);
            return segment != null ? segment : fallback.get().readSegment(id);
        }

        @Override
        public void writeSegment(SegmentId id, byte[] bytes, int offset, int length) throws IOException {
            throw new IOException("This store is read only");
        }
    };

    private final SegmentTracker tracker = new SegmentTracker((msb, lsb) -> new SegmentId(store, msb, lsb));

    public BinaryReferencesCollector(@NotNull Supplier<SegmentStore> fallback) {
        this.fallback = fallback;
    }

    public void addSegment(@NotNull SegmentArchiveEntry entry, @NotNull Buffer data) {
        SegmentId id = tracker.newSegmentId(entry.getMsb(), entry.getLsb());
        segments.put(id, new Segment(tracker, id, data));
    }

    public byte[] write() {
        BinaryReferencesIndexWriter indexWriter = newBinaryReferencesIndexWriter();
        for (Segment segment : segments.values()) {
            if (segment.getSegmentId().isDataSegmentId()) {
                GCGeneration generation = segment.getGcGeneration();
                UUID id = segment.getSegmentId().asUUID();
                for (String reference : readBinaryReferences(segment, emptyMap())) {
                    indexWriter.addEntry(generation.getGeneration(), generation.getFullGeneration(),
                            generation.isCompacted(), id, reference);
                }
            }
        }
        return indexWriter.write();
    }

    /**
     * Reads the blob ids referenced by the {@link RecordType#BLOB_ID} records of a segment.
     *
     * @param segment           the segment to read
     * @param recoveredSegments segments in which the value of a long blob id is looked up before
     *                          the segment store. TAR recovery passes the segments recovered so far,
     *                          because it runs before the file store can read segments (OAK-10636).
     *                          Can be empty.
     * @return the blob ids referenced by {@code segment}
     */
    public static @NotNull Set<String> readBinaryReferences(@NotNull Segment segment,
                                                            @NotNull Map<SegmentId, Segment> recoveredSegments) {
        Set<String> binaryReferences = new HashSet<>();
        segment.forEachRecord((number, type, offset) -> {
            if (type == RecordType.BLOB_ID) {
                binaryReferences.add(SegmentBlob.readBlobId(segment, number, recoveredSegments));
            }
        });
        return binaryReferences;
    }
}
