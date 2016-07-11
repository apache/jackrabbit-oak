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
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newConcurrentMap;
import static java.lang.Boolean.getBoolean;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.LATEST_VERSION;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.isValid;
import static org.apache.jackrabbit.oak.segment.SegmentWriter.BLOCK_SIZE;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Charsets;
import org.apache.commons.io.HexDump;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;

/**
 * A list of records.
 * <p>
 * Record data is not kept in memory, but some entries are cached (templates,
 * all strings in the segment).
 * <p>
 * This class includes method to read records from the raw bytes.
 */
public class Segment {

    /**
     * Number of bytes used for storing a record identifier. One byte
     * is used for identifying the segment and two for the record offset
     * within that segment.
     */
    static final int RECORD_ID_BYTES = 1 + 2;

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

    public static final int REF_COUNT_OFFSET = 5;

    static final int ROOT_COUNT_OFFSET = 6;

    public static final int GC_GENERATION_OFFSET = 10;

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
     * Referenced segment identifiers. Entries are initialized lazily in
     * {@link #getRefId(int)}. Set to {@code null} for bulk segments.
     */
    @CheckForNull
    private final SegmentId[] refids;

    /**
     * Template records read from segment. Used to avoid duplicate
     * copies and repeated parsing of the same templates.
     * FIXME OAK-4451 move the template cache to the segment reader along side with the string cache
     */
    @CheckForNull
    final ConcurrentMap<Integer, Template> templates;

    private static final boolean DISABLE_TEMPLATE_CACHE = getBoolean("oak.segment.disableTemplateCache");

    /**
     * Unpacks a 4 byte aligned segment offset.
     * @param offset  4 byte aligned segment offset
     * @return unpacked segment offset
     */
    public static int unpack(short offset) {
        return (offset & 0xffff) << RECORD_ALIGN_BITS;
    }

    /**
     * Packs a segment offset into a 4 byte aligned address packed into a {@code short}.
     * @param offset  segment offset
     * @return  encoded segment offset packed into a {@code short}
     */
    public static short pack(int offset) {
        return (short) (offset >> RECORD_ALIGN_BITS);
    }

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

        if (DISABLE_TEMPLATE_CACHE) {
            templates = null;
        } else {
            templates = newConcurrentMap();
        }
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
            this.refids = new SegmentId[getRefCount()];
            this.refids[0] = id;
            this.version = SegmentVersion.fromByte(segmentVersion);
        } else {
            this.refids = null;
            this.version = LATEST_VERSION;
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

    Segment(@Nonnull SegmentStore store,
            @Nonnull SegmentReader reader,
            @Nonnull byte[] buffer,
            @Nonnull String info) {
        this.store = checkNotNull(store);
        this.reader = checkNotNull(reader);
        this.id = store.newDataSegmentId();
        this.info = checkNotNull(info);
        if (DISABLE_TEMPLATE_CACHE) {
            templates = null;
        } else {
            templates = newConcurrentMap();
        }

        this.data = ByteBuffer.wrap(checkNotNull(buffer));
        this.refids = new SegmentId[SEGMENT_REFERENCE_LIMIT + 1];
        this.refids[0] = id;
        this.version = SegmentVersion.fromByte(buffer[3]);
        id.loaded(this);
    }

    public SegmentVersion getSegmentVersion() {
        return version;
    }

    /**
     * Maps the given record offset to the respective position within the
     * internal {@link #data} array. The validity of a record with the given
     * length at the given offset is also verified.
     *
     * @param offset record offset
     * @param length record length
     * @return position within the data array
     */
    private int pos(int offset, int length) {
        checkPositionIndexes(offset, offset + length, MAX_SEGMENT_SIZE);
        int pos = data.limit() - MAX_SEGMENT_SIZE + offset;
        checkState(pos >= data.position());
        return pos;
    }

    public SegmentId getSegmentId() {
        return id;
    }

    int getRefCount() {
        return (data.get(REF_COUNT_OFFSET) & 0xff) + 1;
    }

    public int getRootCount() {
        return data.getShort(ROOT_COUNT_OFFSET) & 0xffff;
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

    public RecordType getRootType(int index) {
        int refCount = getRefCount();
        checkArgument(index < getRootCount());
        return RecordType.values()[data.get(data.position() + refCount * 16 + index * 3) & 0xff];
    }

    public int getRootOffset(int index) {
        int refCount = getRefCount();
        checkArgument(index < getRootCount());
        return (data.getShort(data.position() + refCount * 16 + index * 3 + 1) & 0xffff)
                << RECORD_ALIGN_BITS;
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
        if (info == null && getRefCount() != 0) {
            info = readString(getRootOffset(0));
        }
        return info;
    }

    SegmentId getRefId(int index) {
        if (refids == null || index >= refids.length) {
            String type = "data";
            if (!id.isDataSegmentId()) {
                type = "bulk";
            }
            long delta = System.currentTimeMillis() - id.getCreationTime();
            throw new IllegalStateException("RefId '" + index
                    + "' doesn't exist in " + type + " segment " + id
                    + ". Creation date delta is " + delta + " ms.");
        }
        SegmentId refid = refids[index];
        if (refid == null) {
            synchronized (this) {
                refid = refids[index];
                if (refid == null) {
                    int refpos = data.position() + index * 16;
                    long msb = data.getLong(refpos);
                    long lsb = data.getLong(refpos + 8);
                    refid = store.newSegmentId(msb, lsb);
                    refids[index] = refid;
                }
            }
        }
        return refid;
    }

    public List<SegmentId> getReferencedIds() {
        int refcount = getRefCount();
        List<SegmentId> ids = newArrayListWithCapacity(refcount);
        for (int refid = 0; refid < refcount; refid++) {
            ids.add(getRefId(refid));
        }
        return ids;
    }

    public int size() {
        return data.remaining();
    }

    byte readByte(int offset) {
        return data.get(pos(offset, 1));
    }

    short readShort(int offset) {
        return data.getShort(pos(offset, 2));
    }

    int readInt(int offset) {
        return data.getInt(pos(offset, 4));
    }

    long readLong(int offset) {
        return data.getLong(pos(offset, 8));
    }

    /**
     * Reads the given number of bytes starting from the given position
     * in this segment.
     *
     * @param position position within segment
     * @param buffer target buffer
     * @param offset offset within target buffer
     * @param length number of bytes to read
     */
    void readBytes(int position, byte[] buffer, int offset, int length) {
        checkNotNull(buffer);
        checkPositionIndexes(offset, offset + length, buffer.length);
        ByteBuffer d = data.duplicate();
        d.position(pos(position, length));
        d.get(buffer, offset, length);
    }

    RecordId readRecordId(int offset) {
        int pos = pos(offset, RECORD_ID_BYTES);
        return internalReadRecordId(pos);
    }

    private RecordId internalReadRecordId(int pos) {
        SegmentId refid = getRefId(data.get(pos) & 0xff);
        int offset = ((data.get(pos + 1) & 0xff) << 8) | (data.get(pos + 2) & 0xff);
        return new RecordId(refid, offset << RECORD_ALIGN_BITS);
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
    Template readTemplate(int offset) {
        int head = readInt(offset);
        boolean hasPrimaryType = (head & (1 << 31)) != 0;
        boolean hasMixinTypes = (head & (1 << 30)) != 0;
        boolean zeroChildNodes = (head & (1 << 29)) != 0;
        boolean manyChildNodes = (head & (1 << 28)) != 0;
        int mixinCount = (head >> 18) & ((1 << 10) - 1);
        int propertyCount = head & ((1 << 18) - 1);
        offset += 4;

        PropertyState primaryType = null;
        if (hasPrimaryType) {
            RecordId primaryId = readRecordId(offset);
            primaryType = PropertyStates.createProperty(
                    "jcr:primaryType", reader.readString(primaryId), Type.NAME);
            offset += RECORD_ID_BYTES;
        }

        PropertyState mixinTypes = null;
        if (hasMixinTypes) {
            String[] mixins = new String[mixinCount];
            for (int i = 0; i < mixins.length; i++) {
                RecordId mixinId = readRecordId(offset);
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
            RecordId childNameId = readRecordId(offset);
            childName = reader.readString(childNameId);
            offset += RECORD_ID_BYTES;
        }

        PropertyTemplate[] properties;
        properties = readProps(propertyCount, offset);
        return new Template(reader, primaryType, mixinTypes, properties, childName);
    }

    private PropertyTemplate[] readProps(int propertyCount, int offset) {
        PropertyTemplate[] properties = new PropertyTemplate[propertyCount];
        if (propertyCount > 0) {
            RecordId id = readRecordId(offset);
            ListRecord propertyNames = new ListRecord(id, properties.length);
            offset += RECORD_ID_BYTES;
            for (int i = 0; i < propertyCount; i++) {
                byte type = readByte(offset++);
                properties[i] = new PropertyTemplate(i,
                        reader.readString(propertyNames.getEntry(i)), Type.fromTag(
                                Math.abs(type), type < 0));
            }
        }
        return properties;
    }

    long readLength(RecordId id) {
        return id.getSegment().readLength(id.getOffset());
    }

    long readLength(int offset) {
        return internalReadLength(pos(offset, 1));
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
                int refcount = getRefCount();
                for (int refid = 0; refid < refcount; refid++) {
                    writer.format("reference %02x: %s%n", refid, getRefId(refid));
                }
                int rootcount = data.getShort(ROOT_COUNT_OFFSET) & 0xffff;
                int pos = data.position() + refcount * 16;
                for (int rootid = 0; rootid < rootcount; rootid++) {
                    writer.format(
                            "root %d: %s at %04x%n", rootid,
                            RecordType.values()[data.get(pos + rootid * 3) & 0xff],
                            data.getShort(pos + rootid * 3 + 1) & 0xffff);
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

}
