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
import static org.apache.jackrabbit.oak.plugins.segment.SegmentWriter.BLOCK_SIZE;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;

import com.google.common.base.Charsets;
import com.google.common.cache.Weigher;

class Segment {

    /**
     * Number of bytes used for storing a record identifier. One byte
     * is used for identifying the segment and two for the record offset
     * within that segment.
     */
    static final int RECORD_ID_BYTES = 1 + 2;

    /**
     * The limit on segment references within one segment. Since record
     * identifiers use one byte to indicate the referenced segment, a single
     * segment can hold references to up to 256 segments.
     */
    static final int SEGMENT_REFERENCE_LIMIT = 1 << 8; // 256

    /**
     * The number of bytes (or bits of address space) to use for the
     * alignment boundary of segment records.
     */
    static final int RECORD_ALIGN_BITS = 2;
    static final int RECORD_ALIGN_BYTES = 1 << RECORD_ALIGN_BITS; // 4

    /**
     * Maximum segment size. Record identifiers are stored as three-byte
     * sequences with the first byte indicating the segment and the next
     * two the offset within that segment. Since all records are aligned
     * at four-byte boundaries, the two bytes can address up to 256kB of
     * record data.
     */
    static final int MAX_SEGMENT_SIZE = 1 << (16 + RECORD_ALIGN_BITS); // 256kB

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
    static final int MEDIUM_LIMIT = 1 << (16-2) + SMALL_LIMIT;

    static final Weigher<UUID, Segment> WEIGHER =
            new Weigher<UUID, Segment>() {
                @Override
                public int weigh(UUID key, Segment value) {
                    return value.size();
                }
            };

    private static final UUID[] NO_UUIDS = new UUID[0];

    private final SegmentStore store;

    private final UUID uuid;

    private final ByteBuffer data;

    private final UUID[] uuids;

    private final OffsetCache<String> strings;

    private final OffsetCache<Template> templates;

    Segment(SegmentStore store,
            UUID uuid, ByteBuffer data, Collection<UUID> uuids,
            Map<String, RecordId> strings, Map<Template, RecordId> templates) {
        this.store = checkNotNull(store);
        this.uuid = checkNotNull(uuid);
        this.data = checkNotNull(data);
        this.uuids = checkNotNull(uuids).toArray(NO_UUIDS);
        this.strings = new OffsetCache<String>(strings) {
            @Override
            protected String load(int offset) {
                return loadString(offset);
            }
        };
        this.templates = new OffsetCache<Template>(templates) {
            @Override
            protected Template load(int offset) {
                return loadTemplate(offset);
            }
        };
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
        int pos = offset - (MAX_SEGMENT_SIZE - size());
        checkPositionIndexes(pos, pos + length, size());
        return pos;
    }

    public UUID getSegmentId() {
        return uuid;
    }

    public ByteBuffer getData() {
        return data;
    }

    public UUID[] getUUIDs() {
        return uuids;
    }

    public int size() {
        return data.limit();
    }

    byte readByte(int offset) {
        return data.get(pos(offset, 1));
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
        return new RecordId(
                uuids[data.get(pos) & 0xff],
                (data.get(pos + 1) & 0xff) << (8 + Segment.RECORD_ALIGN_BITS)
                | (data.get(pos + 2) & 0xff) << Segment.RECORD_ALIGN_BITS);
    }

    int readInt(int offset) {
        int pos = pos(offset, 4);
        return (data.get(pos) & 0xff) << 24
                | (data.get(pos + 1) & 0xff) << 16
                | (data.get(pos + 2) & 0xff) << 8
                | (data.get(pos + 3) & 0xff);
    }

    String readString(int offset) {
        return strings.get(offset);
    }

    String readString(RecordId id) {
        checkNotNull(id);
        Segment segment = this;
        if (!uuid.equals(id.getSegmentId())) {
            segment = store.readSegment(id.getSegmentId());
        }
        return segment.readString(id.getOffset());
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
                    store, new RecordId(uuid, offset), list, length);
            try {
                return stream.getString();
            } finally {
                stream.close();
            }
        } else {
            throw new IllegalStateException("String is too long: " + length);
        }
    }

    Template readTemplate(int offset) {
        return templates.get(offset);
    }

    Template readTemplate(RecordId id) {
        checkNotNull(id);
        Segment segment = this;
        if (!uuid.equals(id.getSegmentId())) {
            segment = store.readSegment(id.getSegmentId());
        }
        return segment.readTemplate(id.getOffset());
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

        PropertyTemplate[] properties =
                new PropertyTemplate[propertyCount];
        for (int i = 0; i < properties.length; i++) {
            RecordId propertyNameId = readRecordId(offset);
            offset += Segment.RECORD_ID_BYTES;
            byte type = readByte(offset++);
            properties[i] = new PropertyTemplate(
                    readString(propertyNameId),
                    Type.fromTag(Math.abs(type), type < 0));
        }

        return new Template(
                primaryType, mixinTypes, properties, childName);
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

    SegmentStream readStream(int offset) {
        RecordId id = new RecordId(uuid, offset);
        int pos = pos(offset, 1);
        long length = internalReadLength(pos);
        if (length < Segment.MEDIUM_LIMIT) {
            byte[] inline = new byte[(int) length];
            ByteBuffer buffer = data.duplicate();
            if (length < Segment.SMALL_LIMIT) {
                buffer.position(pos + 1);
            } else {
                buffer.position(pos + 2);
            }
            buffer.get(inline);
            return new SegmentStream(id, inline);
        } else {
            int size = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            ListRecord list = new ListRecord(internalReadRecordId(pos + 8), size);
            return new SegmentStream(store, id, list, length);
        }
    }

}
