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
package org.apache.jackrabbit.oak.segment.data;

import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.jetbrains.annotations.NotNull;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Contains the state of a partial segment, i.e. a segment that is currently being written to by {@link SegmentWriter}
 * and has not been flushed yet.
 *
 * <p>
 * The data contained in the partial segment is permanent, i.e. instances of {@code PartialSegmentState} will never
 * return data which is incoherent with earlier instances of {@code PartialSegmentState} for the same segment.
 * For instance, {@link #version()} will always return the same value for the same segment and will never be changed.
 * However, the data may be incomplete. For instance, the segment may not contain all the records that will be written
 * to it before it is flushed.
 *
 * <p>
 * Instances of this class are <em>immutable</em> and thus <em>thread-safe</em>.
 *
 * @link <a href="https://jackrabbit.apache.org/oak/docs/nodestore/segment/records.html">Documentation of the segment structure</a>
 */
public final class PartialSegmentState {
    private final byte version;
    private final int generation;
    private final int fullGeneration;
    private final boolean isCompacted;
    private final @NotNull List<@NotNull SegmentReference> segmentReferences;
    private final @NotNull List<@NotNull Record> records;

    public PartialSegmentState(byte version, int generation, int fullGeneration, boolean isCompacted, @NotNull List<@NotNull SegmentReference> segmentReferences, @NotNull List<@NotNull Record> records) {
        this.version = version;
        this.generation = generation;
        this.fullGeneration = fullGeneration;
        this.isCompacted = isCompacted;
        this.segmentReferences = List.copyOf(segmentReferences);
        this.records = List.copyOf(records);
    }

    public byte version() {
        return version;
    }

    public int generation() {
        return generation;
    }

    public int fullGeneration() {
        return fullGeneration;
    }

    public boolean isCompacted() {
        return isCompacted;
    }

    /**
     * The references to other segments, in the order of their appearance in the segment header
     * (segment number = index of a reference in {@code segmentReferences} + 1).
     *
     * <p>
     * New elements might be added to this list before the segment is flushed in newer instances of
     * {@code PartialSegmentState} for the same segment, but the elements that are already present will never be changed.
     */
    public @NotNull List<@NotNull SegmentReference> segmentReferences() {
        return segmentReferences;
    }

    /**
     * The records in the segment, sorted by offset in descending order (highest offset first, which is the order of
     * record numbers in the segment header).
     *
     * <p>
     * This list may be incomplete if new records are added before the segment is flushed.
     * Also, existing records may themselves be incomplete (see {@link Record}).
     */
    public @NotNull List<@NotNull Record> records() {
        return records;
    }

    public static final class SegmentReference {
        private final long msb;
        private final long lsb;

        public SegmentReference(long msb, long lsb) {
            this.msb = msb;
            this.lsb = lsb;
        }

        public long msb() {
            return msb;
        }

        public long lsb() {
            return lsb;
        }
    }

    public static final class Record {
        /** The reference number of the record (= record number) */
        private final int refNumber;

        /** The type of the record */
        private final RecordType recordType;

        /**
         * The offset of the record in the segment (smaller than {@link Segment#MAX_SEGMENT_SIZE}).
         *
         * <p>
         * This offset is relative to a
         * <a href="https://jackrabbit.apache.org/oak/docs/nodestore/segment/records.html#record-numbers-and-offsets">theoretical 256-KiB segment</a>.
         */
        private final int offset;

        /**
         * The known contents of the segment starting at its {@link #offset}.
         *
         * <p>
         * This array can be smaller than the actual size of the record if the contents of this record are only
         * partially known.
         */
        private final byte @NotNull [] contents;

        public Record(int refNumber, @NotNull RecordType recordType, int offset, byte @NotNull [] contents) {
            this.refNumber = refNumber;
            this.recordType = requireNonNull(recordType);
            this.offset = offset;
            this.contents = requireNonNull(contents);
        }

        /** @see #refNumber */
        public int refNumber() {
            return refNumber;
        }

        /** @see #recordType */
        public @NotNull RecordType recordType() {
            return recordType;
        }

        /** @see #offset */
        public int offset() {
            return offset;
        }

        /** @see #contents */
        public byte @NotNull [] contents() {
            return contents;
        }
    }
}
