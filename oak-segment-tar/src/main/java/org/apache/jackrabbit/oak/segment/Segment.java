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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.segment.CacheWeights.OBJECT_HEADER_SIZE;
import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;
import static org.apache.jackrabbit.oak.segment.SegmentStream.BLOCK_SIZE;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.LATEST_VERSION;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.isValid;
import static org.apache.jackrabbit.oak.segment.data.SegmentData.newRawSegmentData;
import static org.apache.jackrabbit.oak.segment.data.SegmentData.newSegmentData;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.commons.io.HexDump;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.segment.RecordNumbers.Entry;
import org.apache.jackrabbit.oak.segment.data.RecordIdData;
import org.apache.jackrabbit.oak.segment.data.SegmentData;
import org.apache.jackrabbit.oak.segment.data.StringData;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A list of records.
 * <p>
 * Record data is not kept in memory, but some entries are cached (templates,
 * all strings in the segment).
 * <p>
 * This class includes method to read records from the raw bytes.
 */
public class Segment {

    static final int HEADER_SIZE = 32;

    /**
     * Size of a line in the table of references to external segments.
     */
    static final int SEGMENT_REFERENCE_SIZE = 16;

    /**
     * Size of a line in the table mapping record numbers to their type and
     * offset.
     */
    static final int RECORD_SIZE = 9;

    /**
     * Number of bytes used for storing a record identifier. Two bytes
     * are used for identifying the segment and four for the record offset
     * within that segment.
     */
    static final int RECORD_ID_BYTES = 2 + 4;

    /**
     * The number of bytes (or bits of address space) to use for the
     * alignment boundary of segment records.
     */
    static final int RECORD_ALIGN_BITS = 2; // align at the four-byte boundary

    /**
     * Maximum segment size (in bytes)
     */
    public static final int MAX_SEGMENT_SIZE = 1 << 18; // 256 KiB

    /**
     * The size limit for small values. The variable length of small values
     * is encoded as a single byte with the high bit as zero, which gives us
     * seven bits for encoding the length of the value.
     */
    static final int SMALL_LIMIT = 1 << 7;

    /**
     * The size limit for medium values. The variable length of medium values
     * is encoded as two bytes with the highest bits of the first byte set to
     * one and zero, which gives us 14 bits for encoding the length of the
     * value. And since small values are never stored as medium ones, we can
     * extend the size range to cover that many longer values.
     */
    public static final int MEDIUM_LIMIT = (1 << (16 - 2)) + SMALL_LIMIT;
    
    /**
     * Maximum size of small blob IDs. A small blob ID is stored in a value
     * record whose length field contains the pattern "1110" in its most
     * significant bits. Since two bytes are used to store both the bit pattern
     * and the actual length of the blob ID, a maximum of 2^12 values can be
     * stored in the length field.
     */
    static final int BLOB_ID_SMALL_LIMIT = 1 << 12;

    static final int GC_FULL_GENERATION_OFFSET = 4;

    static final int GC_GENERATION_OFFSET = 10;

    static final int REFERENCED_SEGMENT_ID_COUNT_OFFSET = 14;

    static final int RECORD_NUMBER_COUNT_OFFSET = 18;

    @NotNull
    private final SegmentId id;

    private final SegmentData data;

    /**
     * Version of the segment storage format.
     */
    @NotNull
    private final SegmentVersion version;

    /**
     * The table translating record numbers to offsets.
     */
    private final RecordNumbers recordNumbers;

    /**
     * The table translating references to segment IDs.
     */
    private final SegmentReferences segmentReferences;

    /**
     * Align an {@code address} on the given {@code boundary}
     *
     * @param address     address to align
     * @param boundary    boundary to align to
     * @return  {@code n = address + a} such that {@code n % boundary == 0} and
     *          {@code 0 <= a < boundary}.
     */
    static int align(int address, int boundary) {
        return (address + boundary - 1) & ~(boundary - 1);
    }

    Segment(
        @NotNull SegmentId id,
        byte @NotNull [] buffer,
        @NotNull RecordNumbers recordNumbers,
        @NotNull SegmentReferences segmentReferences,
        @NotNull String info
    ) {
        this.id = requireNonNull(id);
        this.info = requireNonNull(info);
        if (id.isDataSegmentId()) {
            this.data = newSegmentData(Buffer.wrap(buffer));
        } else {
            this.data = newRawSegmentData(Buffer.wrap(buffer));
        }
        this.version = SegmentVersion.fromByte(buffer[3]);
        this.recordNumbers = recordNumbers;
        this.segmentReferences = segmentReferences;
        id.loaded(this);
    }

    public Segment(@NotNull SegmentIdProvider idProvider,
                   @NotNull final SegmentId id,
        @NotNull final Buffer data) {
        this.id = requireNonNull(id);
        if (id.isDataSegmentId()) {
            this.data = newSegmentData(requireNonNull(data).slice());
            byte segmentVersion = this.data.getVersion();
            Validate.checkState(this.data.getSignature().equals("0aK") && isValid(segmentVersion), new Object() {

                    @Override
                    public String toString() {
                        return String.format("Invalid segment format. Dumping segment %s\n%s", id, toHex(data.array()));
                    }

            });
            this.version = SegmentVersion.fromByte(segmentVersion);
            this.recordNumbers = RecordNumbers.fromSegmentData(this.data);
            this.segmentReferences = SegmentReferences.fromSegmentData(this.data, idProvider);
        } else {
            this.data = newRawSegmentData(requireNonNull(data).slice());
            this.version = LATEST_VERSION;
            this.recordNumbers = new IdentityRecordNumbers();
            this.segmentReferences = new IllegalSegmentReferences();
        }
    }

    public Segment(
            @NotNull SegmentId id,
            @NotNull SegmentData data,
            @NotNull RecordNumbers recordNumbers,
            @NotNull SegmentReferences segmentReferences
    ) {
        this.id = requireNonNull(id);
        this.data = requireNonNull(data);
        this.recordNumbers = requireNonNull(recordNumbers);
        this.segmentReferences = requireNonNull(segmentReferences);
        this.version = SegmentVersion.fromByte(data.getVersion());
    }

    private static String toHex(byte[] bytes) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            HexDump.dump(bytes, 0, out, 0);
            return out.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            return "Error dumping segment: " + e.getMessage();
        } finally {
            closeQuietly(out);
        }
    }

    public SegmentVersion getSegmentVersion() {
        return version;
    }

    public SegmentId getSegmentId() {
        return id;
    }

    public int getReferencedSegmentIdCount() {
        return data.getSegmentReferencesCount();
    }

    private int getRecordNumberCount() {
        return data.getRecordReferencesCount();
    }

    public UUID getReferencedSegmentId(int index) {
        return segmentReferences.getSegmentId(index + 1).asUUID();
    }

    /**
     * Determine the gc generation a segment from its data. Note that bulk
     * segments don't have generations (i.e. stay at 0).
     *
     * @param data      the data of the segment
     * @param segmentId the id of the segment
     * @return the gc generation of this segment or {@link GCGeneration#NULL} if
     * this is bulk segment.
     */
    public static GCGeneration getGcGeneration(SegmentData data, UUID segmentId) {
        if (isDataSegmentId(segmentId.getLeastSignificantBits())) {
            return newGCGeneration(data.getGeneration(), data.getFullGeneration(), data.isCompacted());
        }
        return GCGeneration.NULL;
    }

    /**
     * Determine the gc generation of this segment. Note that bulk segments don't have
     * generations (i.e. stay at 0).
     * @return  the gc generation of this segment or 0 if this is bulk segment.
     */
    @NotNull
    public GCGeneration getGcGeneration() {
        return getGcGeneration(data, id.asUUID());
    }

    private volatile String info;

    /**
     * Returns the segment meta data of this segment or {@code null} if none is present.
     * <p>
     * The segment meta data is a string of the format {@code "{wid=W,sno=S,gc=G,t=T}"}
     * where:
     * <ul>
     * <li>{@code W} is the writer id {@code wid}, </li>
     * <li>{@code S} is a unique, increasing sequence number corresponding to the allocation order
     * of the segments in this store, </li>
     * <li>{@code G} is the garbage collection generation (i.e. the number of compaction cycles
     * that have been run),</li>
     * <li>{@code T} is a time stamp according to {@link System#currentTimeMillis()}.</li>
     * </ul>
     * @return the segment meta data
     */
    @Nullable
    public String getSegmentInfo() {
        if (info == null && id.isDataSegmentId()) {
            info = readString(recordNumbers.iterator().next().getRecordNumber());
        }
        return info;
    }

    public int size() {
        return data.size();
    }

    public byte readByte(int recordNumber) {
        return data.readByte(recordNumbers.getOffset(recordNumber));
    }

    public byte readByte(int recordNumber, int offset) {
        return data.readByte(recordNumbers.getOffset(recordNumber) + offset);
    }

    short readShort(int recordNumber) {
        return data.readShort(recordNumbers.getOffset(recordNumber));
    }

    int readInt(int recordNumber) {
        return data.readInt(recordNumbers.getOffset(recordNumber));
    }

    int readInt(int recordNumber, int offset) {
        return data.readInt(recordNumbers.getOffset(recordNumber) + offset);
    }

    long readLong(int recordNumber) {
        return data.readLong(recordNumbers.getOffset(recordNumber));
    }

    public void readBytes(int recordNumber, int position, byte[] buffer, int offset, int length) {
        readBytes(recordNumber, position, length).get(buffer, offset, length);
    }

    public Buffer readBytes(int recordNumber, int position, int length) {
        return data.readBytes(recordNumbers.getOffset(recordNumber) + position, length);
    }

    @NotNull
    RecordId readRecordId(int recordNumber, int rawOffset, int recordIdOffset) {
        int offset = recordNumbers.getOffset(recordNumber) + rawOffset + recordIdOffset * RecordIdData.BYTES;
        RecordIdData recordIdData = data.readRecordId(offset);
        return new RecordId(dereferenceSegmentId(recordIdData.getSegmentReference()), recordIdData.getRecordNumber());
    }

    RecordId readRecordId(int recordNumber, int rawOffset) {
        return readRecordId(recordNumber, rawOffset, 0);
    }

    RecordId readRecordId(int recordNumber) {
        return readRecordId(recordNumber, 0, 0);
    }

    @NotNull
    private SegmentId dereferenceSegmentId(int reference) {
        if (reference == 0) {
            return id;
        }

        SegmentId id = segmentReferences.getSegmentId(reference);

        if (id == null) {
            throw new IllegalStateException("Referenced segment not found");
        }

        return id;
    }

    @NotNull
    String readString(int recordNumber) {
        StringData data = this.data.readString(recordNumbers.getOffset(recordNumber));

        if (data.isString()) {
            return data.getString();
        }

        if (data.isRecordId()) {
            SegmentId segmentId = dereferenceSegmentId(data.getRecordId().getSegmentReference());
            RecordId recordId = new RecordId(segmentId, data.getRecordId().getRecordNumber());
            ListRecord list = new ListRecord(recordId, (data.getLength() + BLOCK_SIZE - 1) / BLOCK_SIZE);
            try (SegmentStream stream = new SegmentStream(new RecordId(id, recordNumber), list, data.getLength())) {
                return stream.getString();
            }
        }

        throw new IllegalStateException("Invalid return value");
    }

    static long readLength(RecordId id) {
        return id.getSegment().readLength(id.getRecordNumber());
    }

    long readLength(int recordNumber) {
        return data.readLength(recordNumbers.getOffset(recordNumber));
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return SegmentDump.dumpSegment(
            id,
            data.size(),
            info,
            getGcGeneration(),
            segmentReferences,
            recordNumbers,
            stream -> {
                try {
                    data.hexDump(stream);
                } catch (IOException e) {
                    e.printStackTrace(new PrintStream(stream));
                }
            }
        );
    }

    public void writeTo(OutputStream stream) throws IOException {
        data.binDump(stream);
    }

    /**
     * Convert an offset into an address.
     * @param offset
     * @return  the address corresponding the {@code offset}
     */
    public int getAddress(int offset) {
        return data.size() - (MAX_SEGMENT_SIZE - offset);
    }

    /**
     * A consumer of record data.
     */
    public interface RecordConsumer {

        /**
         * Consume data about a record.
         *
         * @param number the record number.
         * @param type   the record type.
         * @param offset the offset where the record is stored.
         */
        void consume(int number, RecordType type, int offset);

    }

    /**
     * Iterate over the records contained in this segment.
     *
     * @param consumer an instance of {@link RecordConsumer}.
     */
    public void forEachRecord(RecordConsumer consumer) {
        for (Entry entry : recordNumbers) {
            consumer.consume(entry.getRecordNumber(), entry.getType(), entry.getOffset());
        }
    }

    /**
     * Estimate of how much memory this instance would occupy in the segment
     * cache.
     */
    int estimateMemoryUsage() {
        int size = OBJECT_HEADER_SIZE + 76;
        size += 56; // 7 refs x 8 bytes

        if (id.isDataSegmentId()) {
            int recordNumberCount = getRecordNumberCount();
            size += 5 * recordNumberCount;

            int referencedSegmentIdCount = getReferencedSegmentIdCount();
            size += 8 * referencedSegmentIdCount;

            size += StringUtils.estimateMemoryUsage(info);
        }
        size += data.estimateMemoryUsage();
        size += id.estimateMemoryUsage();
        return size;
    }

}
