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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.System.arraycopy;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.identityHashCode;
import static org.apache.jackrabbit.oak.segment.Segment.GC_FULL_GENERATION_OFFSET;
import static org.apache.jackrabbit.oak.segment.Segment.GC_GENERATION_OFFSET;
import static org.apache.jackrabbit.oak.segment.Segment.HEADER_SIZE;
import static org.apache.jackrabbit.oak.segment.Segment.RECORD_ID_BYTES;
import static org.apache.jackrabbit.oak.segment.Segment.RECORD_SIZE;
import static org.apache.jackrabbit.oak.segment.Segment.SEGMENT_REFERENCE_SIZE;
import static org.apache.jackrabbit.oak.segment.Segment.align;
import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.LATEST_VERSION;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.RecordNumbers.Entry;
import org.apache.jackrabbit.oak.segment.data.SegmentData;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates the state of a segment being written. It provides methods
 * for writing primitive data types and for pre-allocating buffer space in the current
 * segment. Should the current segment not have enough space left the current segment
 * is flushed and a fresh one is allocated.
 * <p>
 * The common usage pattern is:
 * <pre>
 *    SegmentBufferWriter writer = ...
 *    writer.prepare(...)  // allocate buffer
 *    writer.writeXYZ(...)
 * </pre>
 * The behaviour of this class is undefined should the pre-allocated buffer be
 * overrun be calling any of the write methods.
 * <p>
 * Instances of this class are <em>not thread safe</em>
 */
public class SegmentBufferWriter implements WriteOperationHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentBufferWriter.class);

    private static final class Statistics {

        int segmentIdCount;

        int recordIdCount;

        int recordCount;

        int size;

        SegmentId id;

        @Override
        public String toString() {
            return "id=" + id +
                    ",size=" + size +
                    ",segmentIdCount=" + segmentIdCount +
                    ",recordIdCount=" + recordIdCount +
                    ",recordCount=" + recordCount;
        }
    }

    private MutableRecordNumbers recordNumbers = new MutableRecordNumbers();

    private MutableSegmentReferences segmentReferences = new MutableSegmentReferences();

    @Nonnull
    private final SegmentIdProvider idProvider;

    @Nonnull
    private final SegmentReader reader;

    /**
     * Id of this writer.
     */
    @Nonnull
    private final String wid;

    @Nonnull
    private final GCGeneration gcGeneration;

    /**
     * The segment write buffer, filled from the end to the beginning
     * (see OAK-629).
     */
    private byte[] buffer;

    private Segment segment;

    /**
     * The number of bytes already written (or allocated). Counted from
     * the <em>end</em> of the buffer.
     */
    private int length;

    /**
     * Current write position within the buffer. Grows up when raw data
     * is written, but shifted downwards by the prepare methods.
     */
    private int position;

    private Statistics statistics;

    /**
     * Mark this buffer as dirty. A dirty buffer needs to be flushed to disk
     * regularly to avoid data loss.
     */
    private boolean dirty;

    public SegmentBufferWriter(@Nonnull SegmentIdProvider idProvider,
                               @Nonnull SegmentReader reader,
                               @CheckForNull String wid,
                               @Nonnull GCGeneration gcGeneration) {
        this.idProvider = checkNotNull(idProvider);
        this.reader = checkNotNull(reader);
        this.wid = (wid == null
                ? "w-" + identityHashCode(this)
                : wid);

        this.gcGeneration = checkNotNull(gcGeneration);
    }

    @Nonnull
    @Override
    public RecordId execute(@Nonnull WriteOperation writeOperation) throws IOException {
        return writeOperation.execute(this);
    }

    @Nonnull
    GCGeneration getGCGeneration() {
        return gcGeneration;
    }

    /**
     * Allocate a new segment and write the segment meta data.
     * The segment meta data is a string of the format {@code "{wid=W,sno=S,t=T}"}
     * where:
     * <ul>
     * <li>{@code W} is the writer id {@code wid}, </li>
     * <li>{@code S} is a unique, increasing sequence number corresponding to the allocation order
     * of the segments in this store, </li>
     * <li>{@code T} is a time stamp according to {@link System#currentTimeMillis()}.</li>
     * </ul>
     * The segment meta data is guaranteed to be the first string record in a segment.
     */
    private void newSegment(SegmentStore store) throws IOException {
        buffer = new byte[Segment.MAX_SEGMENT_SIZE];
        buffer[0] = '0';
        buffer[1] = 'a';
        buffer[2] = 'K';
        buffer[3] = SegmentVersion.asByte(LATEST_VERSION);
        buffer[4] = 0; // reserved
        buffer[5] = 0; // reserved

        int generation = gcGeneration.getGeneration();
        buffer[GC_GENERATION_OFFSET] = (byte) (generation >> 24);
        buffer[GC_GENERATION_OFFSET + 1] = (byte) (generation >> 16);
        buffer[GC_GENERATION_OFFSET + 2] = (byte) (generation >> 8);
        buffer[GC_GENERATION_OFFSET + 3] = (byte) generation;

        int fullGeneration = gcGeneration.getFullGeneration();
        if (gcGeneration.isCompacted()) {
            // Set highest order bit to mark segment created by compaction
            fullGeneration |= 0x80000000;
        }
        buffer[GC_FULL_GENERATION_OFFSET] = (byte) (fullGeneration >> 24);
        buffer[GC_FULL_GENERATION_OFFSET + 1] = (byte) (fullGeneration >> 16);
        buffer[GC_FULL_GENERATION_OFFSET + 2] = (byte) (fullGeneration >> 8);
        buffer[GC_FULL_GENERATION_OFFSET + 3] = (byte) fullGeneration;

        length = 0;
        position = buffer.length;
        recordNumbers = new MutableRecordNumbers();
        segmentReferences = new MutableSegmentReferences();

        String metaInfo =
            "{\"wid\":\"" + wid + '"' +
            ",\"sno\":" + idProvider.getSegmentIdCount() +
            ",\"t\":" + currentTimeMillis() + "}";
        segment = new Segment(idProvider.newDataSegmentId(), reader, buffer, recordNumbers, segmentReferences, metaInfo);

        statistics = new Statistics();
        statistics.id = segment.getSegmentId();

        byte[] data = metaInfo.getBytes(UTF_8);
        RecordWriters.newValueWriter(data.length, data).write(this, store);

        dirty = false;
    }

    public void writeByte(byte value) {
        position = BinaryUtils.writeByte(buffer, position, value);
        dirty = true;
    }

    public void writeShort(short value) {
        position = BinaryUtils.writeShort(buffer, position, value);
        dirty = true;
    }

    public void writeInt(int value) {
        position = BinaryUtils.writeInt(buffer, position, value);
        dirty = true;
    }

    public void writeLong(long value) {
        position = BinaryUtils.writeLong(buffer, position, value);
        dirty = true;
    }

    /**
     * Write a record ID.
     *
     * @param recordId  the record ID.
     */
    public void writeRecordId(RecordId recordId) {
        checkNotNull(recordId);
        checkState(segmentReferences.size() + 1 < 0xffff,
                "Segment cannot have more than 0xffff references");

        writeShort(toShort(writeSegmentIdReference(recordId.getSegmentId())));
        writeInt(recordId.getRecordNumber());

        statistics.recordIdCount++;

        dirty = true;
    }

    private static short toShort(int value) {
        return (short) value;
    }

    private int writeSegmentIdReference(SegmentId id) {
        if (id.equals(segment.getSegmentId())) {
            return 0;
        }

        return segmentReferences.addOrReference(id);
    }

    private static String info(Segment segment) {
        String info = segment.getSegmentId().toString();
        if (isDataSegmentId(segment.getSegmentId().getLeastSignificantBits())) {
            info += (" " + segment.getSegmentInfo());
        }
        return info;
    }

    public void writeBytes(byte[] data, int offset, int length) {
        arraycopy(data, offset, buffer, position, length);
        position += length;
        dirty = true;
    }

    /**
     * Adds a segment header to the buffer and writes a segment to the segment
     * store. This is done automatically (called from prepare) when there is not
     * enough space for a record. It can also be called explicitly.
     */
    @Override
    public void flush(@Nonnull SegmentStore store) throws IOException {
        if (dirty) {
            int referencedSegmentIdCount = segmentReferences.size();
            BinaryUtils.writeInt(buffer, Segment.REFERENCED_SEGMENT_ID_COUNT_OFFSET, referencedSegmentIdCount);

            statistics.segmentIdCount = referencedSegmentIdCount;

            int recordNumberCount = recordNumbers.size();
            BinaryUtils.writeInt(buffer, Segment.RECORD_NUMBER_COUNT_OFFSET, recordNumberCount);

            int totalLength = align(HEADER_SIZE + referencedSegmentIdCount * SEGMENT_REFERENCE_SIZE + recordNumberCount * RECORD_SIZE + length, 16);

            if (totalLength > buffer.length) {
                throw new IllegalStateException(String.format(
                        "Too much data for a segment %s (referencedSegmentIdCount=%d, recordNumberCount=%d, length=%d, totalLength=%d)",
                        segment.getSegmentId(), referencedSegmentIdCount, recordNumberCount, length, totalLength));
            }

            statistics.size = length = totalLength;

            int pos = HEADER_SIZE;
            if (pos + length <= buffer.length) {
                // the whole segment fits to the space *after* the referenced
                // segment identifiers we've already written, so we can safely
                // copy those bits ahead even if concurrent code is still
                // reading from that part of the buffer
                arraycopy(buffer, 0, buffer, buffer.length - length, pos);
                pos += buffer.length - length;
            } else {
                // this might leave some empty space between the header and
                // the record data, but this case only occurs when the
                // segment is >252kB in size and the maximum overhead is <<4kB,
                // which is acceptable
                length = buffer.length;
            }

            for (SegmentId segmentId : segmentReferences) {
                pos = BinaryUtils.writeLong(buffer, pos, segmentId.getMostSignificantBits());
                pos = BinaryUtils.writeLong(buffer, pos, segmentId.getLeastSignificantBits());
            }

            for (Entry entry : recordNumbers) {
                pos = BinaryUtils.writeInt(buffer, pos, entry.getRecordNumber());
                pos = BinaryUtils.writeByte(buffer, pos, (byte) entry.getType().ordinal());
                pos = BinaryUtils.writeInt(buffer, pos, entry.getOffset());
            }

            SegmentId segmentId = segment.getSegmentId();
            LOG.debug("Writing data segment: {} ", statistics);
            store.writeSegment(segmentId, buffer, buffer.length - length, length);
            newSegment(store);
        }
    }

    /**
     * Before writing a record (which are written backwards, from the end of the
     * file to the beginning), this method is called, to ensure there is enough
     * space. A new segment is also created if there is not enough space in the
     * segment lookup table or elsewhere.
     * <p>
     * This method does not actually write into the segment, just allocates the
     * space (flushing the segment if needed and starting a new one), and sets
     * the write position (records are written from the end to the beginning,
     * but within a record from left to right).
     *
     * @param type  the record type (only used for root records)
     * @param size  the size of the record, excluding the size used for the
     *              record ids
     * @param ids   the record ids
     * @param store the {@code SegmentStore} instance to write full segments to
     * @return a new record id
     */
    public RecordId prepare(RecordType type, int size, Collection<RecordId> ids, SegmentStore store) throws IOException {
        checkArgument(size >= 0);
        checkNotNull(ids);

        if (segment == null) {
            // Create a segment first if this is the first time this segment buffer writer is used.
            newSegment(store);
        }

        int idCount = ids.size();
        int recordSize = align(size + idCount * RECORD_ID_BYTES, 1 << Segment.RECORD_ALIGN_BITS);

        // First compute the header and segment sizes based on the assumption
        // that *all* identifiers stored in this record point to previously
        // unreferenced segments.

        int recordNumbersCount = recordNumbers.size() + 1;
        int referencedIdCount = segmentReferences.size() + ids.size();
        int headerSize = HEADER_SIZE + referencedIdCount * SEGMENT_REFERENCE_SIZE + recordNumbersCount * RECORD_SIZE;
        int segmentSize = align(headerSize + recordSize + length, 16);

        // If the size estimate looks too big, recompute it with a more
        // accurate refCount value. We skip doing this when possible to
        // avoid the somewhat expensive list and set traversals.

        if (segmentSize > buffer.length) {

            // Collect the newly referenced segment ids
            Set<SegmentId> segmentIds = newHashSet();
            for (RecordId recordId : ids) {
                SegmentId segmentId = recordId.getSegmentId();
                if (!segmentReferences.contains(segmentId)) {
                    segmentIds.add(segmentId);
                }
            }

            // Adjust the estimation of the new referenced segment ID count.
            referencedIdCount =  segmentReferences.size() + segmentIds.size();

            headerSize = HEADER_SIZE + referencedIdCount * SEGMENT_REFERENCE_SIZE + recordNumbersCount * RECORD_SIZE;
            segmentSize = align(headerSize + recordSize + length, 16);
        }

        if (segmentSize > buffer.length) {
            LOG.debug("Flushing full segment {} (headerSize={}, recordSize={}, length={}, segmentSize={})",
                    segment.getSegmentId(), headerSize, recordSize, length, segmentSize);
            flush(store);
        }

        statistics.recordCount++;

        length += recordSize;
        position = buffer.length - length;
        checkState(position >= 0);

        int recordNumber = recordNumbers.addRecord(type, position);
        return new RecordId(segment.getSegmentId(), recordNumber);
    }

}
