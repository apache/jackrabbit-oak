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
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newConcurrentMap;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.V_11;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentWriter.BLOCK_SIZE;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;

import com.google.common.base.Charsets;

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

    public static int REF_COUNT_OFFSET = 5;

    static int ROOT_COUNT_OFFSET = 6;

    static int BLOBREF_COUNT_OFFSET = 8;

    private final SegmentTracker tracker;

    private final SegmentId id;

    private final ByteBuffer data;

    /**
     * Version of the segment storage format.
     */
    private final SegmentVersion version;

    /**
     * Referenced segment identifiers. Entries are initialized lazily in
     * {@link #getRefId(int)}. Set to {@code null} for bulk segments.
     */
    private final SegmentId[] refids;

    /**
     * String records read from segment. Used to avoid duplicate
     * copies and repeated parsing of the same strings.
     */
    private final ConcurrentMap<Integer, String> strings = newConcurrentMap();

    /**
     * Template records read from segment. Used to avoid duplicate
     * copies and repeated parsing of the same templates.
     */
    private final ConcurrentMap<Integer, Template> templates = newConcurrentMap();

    private volatile long accessed = 0;

    public Segment(SegmentTracker tracker, SegmentId id, ByteBuffer data) {
        this(tracker, id, data, V_11);
    }

    public Segment(SegmentTracker tracker, SegmentId id, ByteBuffer data, SegmentVersion version) {
        this.tracker = checkNotNull(tracker);
        this.id = checkNotNull(id);
        this.data = checkNotNull(data);
        if (id.isDataSegmentId()) {
            byte segmentVersion = data.get(3);
            checkState(data.get(0) == '0'
                    && data.get(1) == 'a'
                    && data.get(2) == 'K'
                    && SegmentVersion.isValid(segmentVersion));
            this.refids = new SegmentId[getRefCount()];
            this.refids[0] = id;
            this.version = SegmentVersion.fromByte(segmentVersion);
        } else {
            this.refids = null;
            this.version = version;
        }
    }

    Segment(SegmentTracker tracker, byte[] buffer) {
        this.tracker = checkNotNull(tracker);
        this.id = tracker.newDataSegmentId();
        this.data = ByteBuffer.wrap(checkNotNull(buffer));
        this.refids = new SegmentId[SEGMENT_REFERENCE_LIMIT + 1];
        this.refids[0] = id;
        this.version = SegmentVersion.fromByte(buffer[3]);
        this.id.setSegment(this);
    }

    void access() {
        accessed++;
    }

    boolean accessed() {
        accessed >>>= 1;
        return accessed != 0;
    }

    SegmentVersion getSegmentVersion() {
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
                    refid = tracker.getSegmentId(msb, lsb);
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

    public long getCacheSize() {
        int size = 1024;
        if (!data.isDirect()) {
            size += size();
        }
        if (id.isDataSegmentId()) {
            size += size();
        }
        return size;
    }

    /**
     * Writes this segment to the given output stream.
     *
     * @param stream stream to which this segment will be written
     * @throws IOException on an IO error
     */
    public void writeTo(OutputStream stream) throws IOException {
        ByteBuffer buffer = data.duplicate();
        WritableByteChannel channel = Channels.newChannel(stream);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    void collectBlobReferences(ReferenceCollector collector) {
        int refcount = getRefCount();
        int rootcount =
                data.getShort(data.position() + ROOT_COUNT_OFFSET) & 0xffff;
        int blobrefcount =
                data.getShort(data.position() + BLOBREF_COUNT_OFFSET) & 0xffff;
        int blobrefpos = data.position() + refcount * 16 + rootcount * 3;

        for (int i = 0; i < blobrefcount; i++) {
            int offset = (data.getShort(blobrefpos + i * 2) & 0xffff) << 2;
            SegmentBlob blob = new SegmentBlob(new RecordId(id, offset));
            collector.addReference(blob.getBlobId());
        }
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

    String readString(final RecordId id) {
        return id.getSegmentId().getSegment().readString(id.getOffset());
    }

    private String readString(int offset) {
        String string = strings.get(offset);
        if (string == null) {
            string = loadString(offset);
            strings.putIfAbsent(offset, string); // only keep the first copy
        }
        return string;
    }

    private String loadString(int offset) {
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
            ListRecord list =
                    new ListRecord(internalReadRecordId(pos + 8), size);
            SegmentStream stream = new SegmentStream(
                    new RecordId(id, offset), list, length);
            try {
                return stream.getString();
            } finally {
                stream.close();
            }
        } else {
            throw new IllegalStateException("String is too long: " + length);
        }
    }

    MapRecord readMap(RecordId id) {
        return new MapRecord(id);
    }

    Template readTemplate(final RecordId id) {
        return id.getSegment().readTemplate(id.getOffset());
    }

    private Template readTemplate(int offset) {
        Template template = templates.get(offset);
        if (template == null) {
            template = loadTemplate(offset);
            templates.putIfAbsent(offset, template); // only keep the first copy
        }
        return template;
    }

    private Template loadTemplate(int offset) {
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
                    "jcr:primaryType", readString(primaryId), Type.NAME);
            offset += Segment.RECORD_ID_BYTES;
        }

        PropertyState mixinTypes = null;
        if (hasMixinTypes) {
            String[] mixins = new String[mixinCount];
            for (int i = 0; i < mixins.length; i++) {
                RecordId mixinId = readRecordId(offset);
                mixins[i] =  readString(mixinId);
                offset += Segment.RECORD_ID_BYTES;
            }
            mixinTypes = PropertyStates.createProperty(
                    "jcr:mixinTypes", Arrays.asList(mixins), Type.NAMES);
        }

        String childName = Template.ZERO_CHILD_NODES;
        if (manyChildNodes) {
            childName = Template.MANY_CHILD_NODES;
        } else if (!zeroChildNodes) {
            RecordId childNameId = readRecordId(offset);
            childName = readString(childNameId);
            offset += Segment.RECORD_ID_BYTES;
        }

        PropertyTemplate[] properties;
        if (version.onOrAfter(V_11)) {
            properties = readPropsV11(propertyCount, offset);
        } else {
            properties = readPropsV10(propertyCount, offset);
        }
        return new Template(primaryType, mixinTypes, properties, childName);
    }

    private PropertyTemplate[] readPropsV10(int propertyCount, int offset) {
        PropertyTemplate[] properties = new PropertyTemplate[propertyCount];
        for (int i = 0; i < propertyCount; i++) {
            RecordId propertyNameId = readRecordId(offset);
            offset += Segment.RECORD_ID_BYTES;
            byte type = readByte(offset++);
            properties[i] = new PropertyTemplate(i, readString(propertyNameId),
                    Type.fromTag(Math.abs(type), type < 0));
        }
        return properties;
    }

    private PropertyTemplate[] readPropsV11(int propertyCount, int offset) {
        PropertyTemplate[] properties = new PropertyTemplate[propertyCount];
        if (propertyCount > 0) {
            RecordId id = readRecordId(offset);
            ListRecord propertyNames = new ListRecord(id, properties.length);
            offset += Segment.RECORD_ID_BYTES;
            for (int i = 0; i < propertyCount; i++) {
                byte type = readByte(offset++);
                properties[i] = new PropertyTemplate(i,
                        readString(propertyNames.getEntry(i)), Type.fromTag(
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
                    | data.get(pos++) & 0xff)
                    + SMALL_LIMIT;
        } else {
            return (((long) length & 0x3f) << 56
                    | ((long) (data.get(pos++) & 0xff)) << 48
                    | ((long) (data.get(pos++) & 0xff)) << 40
                    | ((long) (data.get(pos++) & 0xff)) << 32
                    | ((long) (data.get(pos++) & 0xff)) << 24
                    | ((long) (data.get(pos++) & 0xff)) << 16
                    | ((long) (data.get(pos++) & 0xff)) << 8
                    | ((long) (data.get(pos++) & 0xff)))
                    + MEDIUM_LIMIT;
        }
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        StringWriter string = new StringWriter();
        PrintWriter writer = new PrintWriter(string);

        int length = data.remaining();

        writer.format("Segment %s (%d bytes)%n", id, length);
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
            int blobrefcount = data.getShort(BLOBREF_COUNT_OFFSET) & 0xffff;
            pos += rootcount * 3;
            for (int blobrefid = 0; blobrefid < blobrefcount; blobrefid++) {
                int offset = data.getShort(pos + blobrefid * 2) & 0xffff;
                SegmentBlob blob = new SegmentBlob(
                        new RecordId(id, offset << RECORD_ALIGN_BITS));
                writer.format(
                        "blobref %d: %s at %04x%n", blobrefid,
                        blob.getBlobId(), offset);
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

        writer.close();
        return string.toString();
    }

}
