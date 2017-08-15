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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.fill;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.segment.CacheWeights.OBJECT_HEADER_SIZE;
import static org.apache.jackrabbit.oak.segment.RecordNumbers.EMPTY_RECORD_NUMBERS;
import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;
import static org.apache.jackrabbit.oak.segment.SegmentStream.BLOCK_SIZE;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.LATEST_VERSION;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.isValid;
import static org.apache.jackrabbit.oak.segment.data.SegmentData.newRawSegmentData;
import static org.apache.jackrabbit.oak.segment.data.SegmentData.newSegmentData;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Charsets;
import com.google.common.collect.AbstractIterator;
import org.apache.commons.io.HexDump;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.io.output.WriterOutputStream;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.segment.RecordNumbers.Entry;
import org.apache.jackrabbit.oak.segment.data.RecordIdData;
import org.apache.jackrabbit.oak.segment.data.SegmentData;
import org.apache.jackrabbit.oak.segment.data.StringData;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;

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
     * Maximum segment size
     */
    static final int MAX_SEGMENT_SIZE = 1 << 18; // 256kB

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
    static final int MEDIUM_LIMIT = (1 << (16 - 2)) + SMALL_LIMIT;

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

    @Nonnull
    private final SegmentReader reader;

    @Nonnull
    private final SegmentId id;

    private final SegmentData data;

    /**
     * Version of the segment storage format.
     */
    @Nonnull
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
        @Nonnull SegmentId id,
        @Nonnull SegmentReader reader,
        @Nonnull byte[] buffer,
        @Nonnull RecordNumbers recordNumbers,
        @Nonnull SegmentReferences segmentReferences,
        @Nonnull String info
    ) {
        this.id = checkNotNull(id);
        this.reader = checkNotNull(reader);
        this.info = checkNotNull(info);
        if (id.isDataSegmentId()) {
            this.data = newSegmentData(ByteBuffer.wrap(buffer));
        } else {
            this.data = newRawSegmentData(ByteBuffer.wrap(buffer));
        }
        this.version = SegmentVersion.fromByte(buffer[3]);
        this.recordNumbers = recordNumbers;
        this.segmentReferences = segmentReferences;
        id.loaded(this);
    }

    public Segment(@Nonnull SegmentIdProvider idProvider,
                   @Nonnull SegmentReader reader,
                   @Nonnull final SegmentId id,
                   @Nonnull final ByteBuffer data) {
        this.reader = checkNotNull(reader);
        this.id = checkNotNull(id);
        if (id.isDataSegmentId()) {
            this.data = newSegmentData(checkNotNull(data).slice());
            byte segmentVersion = this.data.getVersion();
            checkState(this.data.getSignature().equals("0aK") && isValid(segmentVersion), new Object() {

                    @Override
                    public String toString() {
                        return String.format("Invalid segment format. Dumping segment %s\n%s", id, toHex(data.array()));
                    }

            });
            this.version = SegmentVersion.fromByte(segmentVersion);
            this.recordNumbers = readRecordNumberOffsets();
            this.segmentReferences = readReferencedSegments(idProvider);
        } else {
            this.data = newRawSegmentData(checkNotNull(data).slice());
            this.version = LATEST_VERSION;
            this.recordNumbers = new IdentityRecordNumbers();
            this.segmentReferences = new IllegalSegmentReferences();
        }
    }

    private static String toHex(byte[] bytes) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            HexDump.dump(bytes, 0, out, 0);
            return out.toString(Charsets.UTF_8.name());
        } catch (IOException e) {
            return "Error dumping segment: " + e.getMessage();
        } finally {
            closeQuietly(out);
        }
    }

    /**
     * Read the serialized table mapping record numbers to offsets.
     *
     * @return An instance of {@link RecordNumbers}, never {@code null}.
     */
    private RecordNumbers readRecordNumberOffsets() {
        int recordNumberCount = data.getRecordReferencesCount();

        if (recordNumberCount == 0) {
            return EMPTY_RECORD_NUMBERS;
        }

        int maxIndex = data.getRecordReferenceNumber(recordNumberCount - 1);

        byte[] types = new byte[maxIndex + 1];
        int[] offsets = new int[maxIndex + 1];
        fill(offsets, -1);

        for (int i = 0; i < recordNumberCount; i++) {
            int recordNumber = data.getRecordReferenceNumber(i);
            types[recordNumber] = data.getRecordReferenceType(i);
            offsets[recordNumber] = data.getRecordReferenceOffset(i);
        }

        return new ImmutableRecordNumbers(offsets, types);
    }

    private SegmentReferences readReferencedSegments(final SegmentIdProvider idProvider) {
        checkState(getReferencedSegmentIdCount() + 1 < 0xffff, "Segment cannot have more than 0xffff references");

        final int referencedSegmentIdCount = getReferencedSegmentIdCount();

        // We need to keep SegmentId references (as opposed to e.g. UUIDs)
        // here as frequently resolving the segment ids via the segment id
        // tables is prohibitively expensive.
        // These SegmentId references are not a problem wrt. heap usage as
        // their individual memoised references to their underlying segment
        // is managed via the SegmentCache. It is the size of that cache that
        // keeps overall heap usage by Segment instances bounded.
        // See OAK-6106.

        final SegmentId[] refIds = new SegmentId[referencedSegmentIdCount];

        return new SegmentReferences() {

            @Override
            public SegmentId getSegmentId(int reference) {
                checkArgument(reference <= referencedSegmentIdCount, "Segment reference out of bounds");
                SegmentId id = refIds[reference - 1];
                if (id == null) {
                    synchronized (refIds) {
                        id = refIds[reference - 1];
                        if (id == null) {
                            long msb = data.getSegmentReferenceMsb(reference - 1);
                            long lsb = data.getSegmentReferenceLsb(reference - 1);
                            id = idProvider.newSegmentId(msb, lsb);
                            refIds[reference - 1] = id;
                        }
                    }
                }
                return id;
            }

            @Nonnull
            @Override
            public Iterator<SegmentId> iterator() {
                return new AbstractIterator<SegmentId>() {

                    private int reference = 1;

                    @Override
                    protected SegmentId computeNext() {
                        if (reference <= referencedSegmentIdCount) {
                            return getSegmentId(reference++);
                        } else {
                            return endOfData();
                        }
                    }

                };
            }

        };
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
    @Nonnull
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
    @CheckForNull
    String getSegmentInfo() {
        if (info == null && id.isDataSegmentId()) {
            info = readString(recordNumbers.iterator().next().getRecordNumber());
        }
        return info;
    }

    public int size() {
        return data.size();
    }

    byte readByte(int recordNumber) {
        return data.readByte(recordNumbers.getOffset(recordNumber));
    }

    byte readByte(int recordNumber, int offset) {
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

    void readBytes(int recordNumber, int position, byte[] buffer, int offset, int length) {
        readBytes(recordNumber, position, length).get(buffer, offset, length);
    }

    ByteBuffer readBytes(int recordNumber, int position, int length) {
        return data.readBytes(recordNumbers.getOffset(recordNumber) + position, length);
    }

    @Nonnull
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

    @Nonnull
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

    @Nonnull
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

    @Nonnull
    Template readTemplate(int recordNumber) {
        int head = readInt(recordNumber);
        boolean hasPrimaryType = (head & (1 << 31)) != 0;
        boolean hasMixinTypes = (head & (1 << 30)) != 0;
        boolean zeroChildNodes = (head & (1 << 29)) != 0;
        boolean manyChildNodes = (head & (1 << 28)) != 0;
        int mixinCount = (head >> 18) & ((1 << 10) - 1);
        int propertyCount = head & ((1 << 18) - 1);

        int offset = 4;

        PropertyState primaryType = null;
        if (hasPrimaryType) {
            RecordId primaryId = readRecordId(recordNumber, offset);
            primaryType = PropertyStates.createProperty(
                    "jcr:primaryType", reader.readString(primaryId), Type.NAME);
            offset += RECORD_ID_BYTES;
        }

        PropertyState mixinTypes = null;
        if (hasMixinTypes) {
            String[] mixins = new String[mixinCount];
            for (int i = 0; i < mixins.length; i++) {
                RecordId mixinId = readRecordId(recordNumber, offset);
                mixins[i] =  reader.readString(mixinId);
                offset += RECORD_ID_BYTES;
            }
            mixinTypes = PropertyStates.createProperty(
                    "jcr:mixinTypes", Arrays.asList(mixins), Type.NAMES);
        }

        String childName = Template.ZERO_CHILD_NODES;
        if (manyChildNodes) {
            childName = Template.MANY_CHILD_NODES;
        } else if (!zeroChildNodes) {
            RecordId childNameId = readRecordId(recordNumber, offset);
            childName = reader.readString(childNameId);
            offset += RECORD_ID_BYTES;
        }

        PropertyTemplate[] properties;
        properties = readProps(propertyCount, recordNumber, offset);
        return new Template(reader, primaryType, mixinTypes, properties, childName);
    }

    private PropertyTemplate[] readProps(int propertyCount, int recordNumber, int offset) {
        PropertyTemplate[] properties = new PropertyTemplate[propertyCount];
        if (propertyCount > 0) {
            RecordId id = readRecordId(recordNumber, offset);
            ListRecord propertyNames = new ListRecord(id, properties.length);
            offset += RECORD_ID_BYTES;
            for (int i = 0; i < propertyCount; i++) {
                byte type = readByte(recordNumber, offset++);
                properties[i] = new PropertyTemplate(i,
                        reader.readString(propertyNames.getEntry(i)), Type.fromTag(
                                Math.abs(type), type < 0));
            }
        }
        return properties;
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
        StringWriter string = new StringWriter();
        try (PrintWriter writer = new PrintWriter(string)) {
            writer.format("Segment %s (%d bytes)%n", id, data.size());
            String segmentInfo = getSegmentInfo();
            if (segmentInfo != null) {
                writer.format("Info: %s, Generation: %s%n", segmentInfo, getGcGeneration());
            }
            if (id.isDataSegmentId()) {
                writer.println("--------------------------------------------------------------------------");
                int i = 1;
                for (SegmentId segmentId : segmentReferences) {
                    writer.format("reference %02x: %s%n", i++, segmentId);
                }
                for (Entry entry : recordNumbers) {
                    writer.format("%10s record %08x: %08x%n", entry.getType(), entry.getRecordNumber(), entry.getOffset());
                }
            }
            writer.println("--------------------------------------------------------------------------");
            try {
                data.hexDump(new WriterOutputStream(writer, Charsets.UTF_8));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            writer.println("--------------------------------------------------------------------------");
        }
        return string.toString();
    }

    public void writeTo(OutputStream stream) throws IOException {
        data.binDump(stream);
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
