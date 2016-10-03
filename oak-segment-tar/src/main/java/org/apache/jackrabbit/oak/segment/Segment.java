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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.LATEST_VERSION;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.isValid;
import static org.apache.jackrabbit.oak.segment.SegmentWriter.BLOCK_SIZE;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Charsets;
import org.apache.commons.io.HexDump;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.segment.RecordNumbers.Entry;

/**
 * A list of records.
 * <p>
 * Record data is not kept in memory, but some entries are cached (templates,
 * all strings in the segment).
 * <p>
 * This class includes method to read records from the raw bytes.
 */
public class Segment {

    static final int HEADER_SIZE = 22;

    /**
     * Number of bytes used for storing a record identifier. One byte
     * is used for identifying the segment and two for the record offset
     * within that segment.
     */
    static final int RECORD_ID_BYTES = 4 + 4;

    /**
     * The limit on segment references within one segment. Since record
     * identifiers use one byte to indicate the referenced segment, a single
     * segment can hold references to up to 255 segments plus itself.
     */
    static final int SEGMENT_REFERENCE_LIMIT = (1 << 8) - 1; // 255

    /**
     * The number of bytes (or bits of address space) to use for the
     * alignment boundary of segment records.
     */
    public static final int RECORD_ALIGN_BITS = 2; // align at the four-byte boundary

    /**
     * Maximum segment size. Record identifiers are stored as three-byte
     * sequences with the first byte indicating the segment and the next
     * two the offset within that segment. Since all records are aligned
     * at four-byte boundaries, the two bytes can address up to 256kB of
     * record data.
     */
    public static final int MAX_SEGMENT_SIZE = 1 << (16 + RECORD_ALIGN_BITS); // 256kB

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
    public static final int BLOB_ID_SMALL_LIMIT = 1 << 12;

    public static final int GC_GENERATION_OFFSET = 10;

    public static final int REFERENCED_SEGMENT_ID_COUNT_OFFSET = 14;

    public static final int RECORD_NUMBER_COUNT_OFFSET = 18;

    @Nonnull
    private final SegmentStore store;

    @Nonnull
    private final SegmentReader reader;

    @Nonnull
    private final SegmentId id;

    @Nonnull
    private final ByteBuffer data;

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
    public static int align(int address, int boundary) {
        return (address + boundary - 1) & ~(boundary - 1);
    }

    public Segment(@Nonnull SegmentStore store,
                   @Nonnull SegmentReader reader,
                   @Nonnull final SegmentId id,
                   @Nonnull final ByteBuffer data) {
        this.store = checkNotNull(store);
        this.reader = checkNotNull(reader);
        this.id = checkNotNull(id);
        this.data = checkNotNull(data);
        if (id.isDataSegmentId()) {
            byte segmentVersion = data.get(3);
            checkState(data.get(0) == '0'
                    && data.get(1) == 'a'
                    && data.get(2) == 'K'
                    && isValid(segmentVersion),
                new Object() {  // Defer evaluation of error message
                    @Override
                    public String toString() {
                        return "Invalid segment format. Dumping segment " + id + "\n"
                            + toHex(data.array());
                    }
            });
            this.version = SegmentVersion.fromByte(segmentVersion);
            this.recordNumbers = readRecordNumberOffsets();
            this.segmentReferences = readReferencedSegments();
        } else {
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
        Map<Integer, RecordEntry> recordNumberOffsets = newLinkedHashMap();

        int position = data.position();

        position += HEADER_SIZE;
        position += getReferencedSegmentIdCount() * 16;

        for (int i = 0; i < getRecordNumberCount(); i++) {
            int recordNumber = data.getInt(position);
            position += 4;
            int type = data.getInt(position);
            position += 4;
            int offset = data.getInt(position);
            position += 4;
            recordNumberOffsets.put(recordNumber, new RecordEntry(RecordType.values()[type], offset));
        }

        return new ImmutableRecordNumbers(recordNumberOffsets);
    }

    private SegmentReferences readReferencedSegments() {
        List<SegmentId> referencedSegments = newArrayListWithCapacity(getReferencedSegmentIdCount());

        int position = data.position();

        position += HEADER_SIZE;

        for (int i = 0; i < getReferencedSegmentIdCount(); i++) {
            long msb = data.getLong(position);
            position += 8;
            long lsb = data.getLong(position);
            position += 8;
            referencedSegments.add(store.newSegmentId(msb, lsb));
        }

        return new ImmutableSegmentReferences(referencedSegments);
    }

    Segment(@Nonnull SegmentStore store,
            @Nonnull SegmentReader reader,
            @Nonnull byte[] buffer,
            @Nonnull RecordNumbers recordNumbers,
            @Nonnull SegmentReferences segmentReferences,
            @Nonnull String info
    ) {
        this.store = checkNotNull(store);
        this.reader = checkNotNull(reader);
        this.id = store.newDataSegmentId();
        this.info = checkNotNull(info);
        this.data = ByteBuffer.wrap(checkNotNull(buffer));
        this.version = SegmentVersion.fromByte(buffer[3]);
        this.recordNumbers = recordNumbers;
        this.segmentReferences = segmentReferences;
        id.loaded(this);
    }

    public SegmentVersion getSegmentVersion() {
        return version;
    }

    private int pos(int recordNumber, int length) {
        return pos(recordNumber, 0, 0, length);
    }

    private int pos(int recordNumber, int rawOffset, int length) {
        return pos(recordNumber, rawOffset, 0, length);
    }

    /**
     * Maps the given record number to the respective position within the
     * internal {@link #data} array. The validity of a record with the given
     * length at the given record number is also verified.
     *
     * @param recordNumber   record number
     * @param rawOffset      offset to add to the base position of the record
     * @param recordIdOffset offset to add to to the base position of the
     *                       record, multiplied by the length of a record ID
     * @param length         record length
     * @return position within the data array
     */
    private int pos(int recordNumber, int rawOffset, int recordIdOffset, int length) {
        int offset = recordNumbers.getOffset(recordNumber);

        if (offset == -1) {
            throw new IllegalStateException("invalid record number");
        }

        int base = offset + rawOffset + recordIdOffset * RECORD_ID_BYTES;
        checkPositionIndexes(base, base + length, MAX_SEGMENT_SIZE);
        int pos = data.limit() - MAX_SEGMENT_SIZE + base;
        checkState(pos >= data.position());
        return pos;
    }

    public SegmentId getSegmentId() {
        return id;
    }

    public int getReferencedSegmentIdCount() {
        return data.getInt(REFERENCED_SEGMENT_ID_COUNT_OFFSET);
    }

    public int getRecordNumberCount() {
        return data.getInt(RECORD_NUMBER_COUNT_OFFSET);
    }

    public UUID getReferencedSegmentId(int index) {
        return segmentReferences.getSegmentId(index + 1).asUUID();
    }

    /**
     * Determine the gc generation a segment from its data. Note that bulk segments don't have
     * generations (i.e. stay at 0).
     *
     * @param data         the date of the segment
     * @param segmentId    the id of the segment
     * @return  the gc generation of this segment or 0 if this is bulk segment.
     */
    public static int getGcGeneration(ByteBuffer data, UUID segmentId) {
        return isDataSegmentId(segmentId.getLeastSignificantBits())
            ? data.getInt(GC_GENERATION_OFFSET)
            : 0;
    }

    /**
     * Determine the gc generation of this segment. Note that bulk segments don't have
     * generations (i.e. stay at 0).
     * @return  the gc generation of this segment or 0 if this is bulk segment.
     */
    public int getGcGeneration() {
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
    public String getSegmentInfo() {
        if (info == null && id.isDataSegmentId()) {
            info = readString(recordNumbers.iterator().next().getRecordNumber());
        }
        return info;
    }

    public int size() {
        return data.remaining();
    }

    byte readByte(int recordNumber) {
        return readByte(recordNumber, 0);
    }

    byte readByte(int recordNumber, int offset) {
        return data.get(pos(recordNumber, offset, 1));
    }

    short readShort(int recordNumber) {
        return data.getShort(pos(recordNumber, 2));
    }

    int readInt(int recordNumber) {
        return data.getInt(pos(recordNumber, 4));
    }

    int readInt(int recordNumber, int offset) {
        return data.getInt(pos(recordNumber, offset, 4));
    }

    long readLong(int recordNumber) {
        return data.getLong(pos(recordNumber, 8));
    }

    /**
     * Reads the given number of bytes starting from the given position
     * in this segment.
     *
     * @param recordNumber position within segment
     * @param buffer target buffer
     * @param offset offset within target buffer
     * @param length number of bytes to read
     */
    void readBytes(int recordNumber, byte[] buffer, int offset, int length) {
        readBytes(recordNumber, 0, buffer, offset, length);
    }

    void readBytes(int recordNumber, int position, byte[] buffer, int offset, int length) {
        checkNotNull(buffer);
        checkPositionIndexes(offset, offset + length, buffer.length);
        ByteBuffer d = data.duplicate();
        d.position(pos(recordNumber, position, length));
        d.get(buffer, offset, length);
    }

    RecordId readRecordId(int recordNumber, int rawOffset, int recordIdOffset) {
        return internalReadRecordId(pos(recordNumber, rawOffset, recordIdOffset, RECORD_ID_BYTES));
    }

    RecordId readRecordId(int recordNumber, int rawOffset) {
        return readRecordId(recordNumber, rawOffset, 0);
    }

    RecordId readRecordId(int recordNumber) {
        return readRecordId(recordNumber, 0, 0);
    }

    private RecordId internalReadRecordId(int pos) {
        SegmentId segmentId = dereferenceSegmentId(data.getInt(pos));
        return new RecordId(segmentId, data.getInt(pos + 4));
    }

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
    String readString(int offset) {
        int pos = pos(offset, 1);
        long length = internalReadLength(pos);
        if (length < SMALL_LIMIT) {
            byte[] bytes = new byte[(int) length];
            ByteBuffer buffer = data.duplicate();
            buffer.position(pos + 1);
            buffer.get(bytes);
            return new String(bytes, Charsets.UTF_8);
        } else if (length < MEDIUM_LIMIT) {
            byte[] bytes = new byte[(int) length];
            ByteBuffer buffer = data.duplicate();
            buffer.position(pos + 2);
            buffer.get(bytes);
            return new String(bytes, Charsets.UTF_8);
        } else if (length < Integer.MAX_VALUE) {
            int size = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            ListRecord list = new ListRecord(internalReadRecordId(pos + 8), size);
            try (SegmentStream stream = new SegmentStream(new RecordId(id, offset), list, length)) {
                return stream.getString();
            }
        } else {
            throw new IllegalStateException("String is too long: " + length);
        }
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

    long readLength(RecordId id) {
        return id.getSegment().readLength(id.getRecordNumber());
    }

    long readLength(int recordNumber) {
        return internalReadLength(pos(recordNumber, 1));
    }

    private long internalReadLength(int pos) {
        int length = data.get(pos++) & 0xff;
        if ((length & 0x80) == 0) {
            return length;
        } else if ((length & 0x40) == 0) {
            return ((length & 0x3f) << 8
                    | data.get(pos) & 0xff)
                    + SMALL_LIMIT;
        } else {
            return (((long) length & 0x3f) << 56
                    | ((long) (data.get(pos++) & 0xff)) << 48
                    | ((long) (data.get(pos++) & 0xff)) << 40
                    | ((long) (data.get(pos++) & 0xff)) << 32
                    | ((long) (data.get(pos++) & 0xff)) << 24
                    | ((long) (data.get(pos++) & 0xff)) << 16
                    | ((long) (data.get(pos++) & 0xff)) << 8
                    | ((long) (data.get(pos) & 0xff)))
                    + MEDIUM_LIMIT;
        }
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        StringWriter string = new StringWriter();
        try (PrintWriter writer = new PrintWriter(string)) {
            int length = data.remaining();

            writer.format("Segment %s (%d bytes)%n", id, length);
            String segmentInfo = getSegmentInfo();
            if (segmentInfo != null) {
                writer.format("Info: %s, Generation: %d%n", segmentInfo, getGcGeneration());
            }
            if (id.isDataSegmentId()) {
                writer.println("--------------------------------------------------------------------------");

                int i = 1;

                for (SegmentId segmentId : segmentReferences) {
                    writer.format("reference %02x: %s%n", i++, segmentId);
                }

                for (Entry entry : recordNumbers) {
                    writer.format("record number %08x: %08x", entry.getRecordNumber(), entry.getOffset());
                }
            }
            writer.println("--------------------------------------------------------------------------");
            int pos = data.limit() - ((length + 15) & ~15);
            while (pos < data.limit()) {
                writer.format("%04x: ", (MAX_SEGMENT_SIZE - data.limit() + pos) >> RECORD_ALIGN_BITS);
                for (int i = 0; i < 16; i++) {
                    if (i > 0 && i % 4 == 0) {
                        writer.append(' ');
                    }
                    if (pos + i >= data.position()) {
                        byte b = data.get(pos + i);
                        writer.format("%02x ", b & 0xff);
                    } else {
                        writer.append("   ");
                    }
                }
                writer.append(' ');
                for (int i = 0; i < 16; i++) {
                    if (pos + i >= data.position()) {
                        byte b = data.get(pos + i);
                        if (b >= ' ' && b < 127) {
                            writer.append((char) b);
                        } else {
                            writer.append('.');
                        }
                    } else {
                        writer.append(' ');
                    }
                }
                writer.println();
                pos += 16;
            }
            writer.println("--------------------------------------------------------------------------");
            return string.toString();
        }
    }

    public void writeTo(OutputStream stream) throws IOException {
        ByteBuffer buffer = data.duplicate();
        WritableByteChannel channel = Channels.newChannel(stream);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    public interface RecordConsumer {

        void consume(int number, RecordType type, int offset);

    }

    public void forEachRecord(RecordConsumer consumer) {
        for (Entry entry : recordNumbers) {
            consumer.consume(entry.getRecordNumber(), entry.getType(), entry.getOffset());
        }
    }

}
