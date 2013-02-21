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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentWriter.BLOCK_SIZE;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;

public class SegmentReader {

    private final SegmentStore store;

    private final LoadingCache<RecordId, String> strings =
            CacheBuilder.newBuilder()
            .maximumWeight(1 << 20) // 1 MB
            .weigher(newStringWeigher())
            .build(newStringLoader());

    private final LoadingCache<RecordId, Template> templates =
            CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build(newTemplateLoader());

    public SegmentReader(SegmentStore store) {
        this.store = store;
    }

    private static Weigher<RecordId, String> newStringWeigher() {
        return new Weigher<RecordId, String>() {
            @Override
            public int weigh(RecordId key, String value) {
                return 32 + value.length() * 2;
            }
        };
    }

    private CacheLoader<RecordId, String> newStringLoader() {
        return new CacheLoader<RecordId, String>() {
            @Override
            public String load(RecordId key) throws Exception {
                SegmentStream stream = readStream(key);
                try {
                    return stream.getString();
                } finally {
                    stream.close();
                }
            }
        };
    }

    private CacheLoader<RecordId, Template> newTemplateLoader() {
        return new CacheLoader<RecordId, Template>() {
            @Override
            public Template load(RecordId key) throws Exception {
                Segment segment = store.readSegment(key.getSegmentId());
                int offset = key.getOffset();

                int head = segment.readInt(offset);
                boolean hasPrimaryType = (head & (1 << 31)) != 0;
                boolean hasMixinTypes = (head & (1 << 30)) != 0;
                boolean zeroChildNodes = (head & (1 << 29)) != 0;
                boolean manyChildNodes = (head & (1 << 28)) != 0;
                int mixinCount = (head >> 18) & ((1 << 10) - 1);
                int propertyCount = head & ((1 << 18) - 1);
                offset += 4;

                PropertyState primaryType = null;
                if (hasPrimaryType) {
                    RecordId primaryId = segment.readRecordId(offset);
                    primaryType = PropertyStates.createProperty(
                            "jcr:primaryType", readString(primaryId), Type.NAME);
                    offset += Segment.RECORD_ID_BYTES;
                }

                PropertyState mixinTypes = null;
                if (hasMixinTypes) {
                    String[] mixins = new String[mixinCount];
                    for (int i = 0; i < mixins.length; i++) {
                        RecordId mixinId = segment.readRecordId(offset);
                        mixins[i] = readString(mixinId);
                        offset += Segment.RECORD_ID_BYTES;
                    }
                    mixinTypes = PropertyStates.createProperty(
                            "jcr:mixinTypes", Arrays.asList(mixins), Type.NAMES);
                }

                String childName = Template.ZERO_CHILD_NODES;
                if (manyChildNodes) {
                    childName = Template.MANY_CHILD_NODES;
                } else if (!zeroChildNodes) {
                    RecordId childNameId = segment.readRecordId(offset);
                    childName = readString(childNameId);
                    offset += Segment.RECORD_ID_BYTES;
                }

                PropertyTemplate[] properties =
                        new PropertyTemplate[propertyCount];
                for (int i = 0; i < properties.length; i++) {
                    RecordId propertyNameId = segment.readRecordId(offset);
                    offset += Segment.RECORD_ID_BYTES;
                    byte type = segment.readByte(offset++);
                    properties[i] = new PropertyTemplate(
                            readString(propertyNameId),
                            Type.fromTag(Math.abs(type), type < 0));
                }

                return new Template(
                        primaryType, mixinTypes, properties, childName);
            }
        };
    }

    public Template readTemplate(RecordId recordId) {
        try {
            return templates.get(recordId);
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                    "Unable to access template record " + recordId, e);
        }
    }

    public String readString(RecordId recordId) {
        try {
            return strings.get(recordId);
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                    "Unable to access string record " + recordId, e);
        }
    }

    public long readLength(RecordId recordId) {
        checkNotNull(recordId);
        Segment segment = store.readSegment(recordId.getSegmentId());
        return readLength(segment, recordId.getOffset());
    }

    private long readLength(Segment segment, int offset) {
        int length = segment.readByte(offset++) & 0xff;
        if ((length & 0x80) == 0) {
            return length;
        } else if ((length & 0x40) == 0) {
            return ((length & 0x3f) << 8
                    | segment.readByte(offset) & 0xff)
                    + Segment.SMALL_LIMIT;
        } else {
            return (((long) length & 0x3f) << 56
                    | ((long) (segment.readByte(offset++) & 0xff)) << 48
                    | ((long) (segment.readByte(offset++) & 0xff)) << 40
                    | ((long) (segment.readByte(offset++) & 0xff)) << 32
                    | ((long) (segment.readByte(offset++) & 0xff)) << 24
                    | ((long) (segment.readByte(offset++) & 0xff)) << 16
                    | ((long) (segment.readByte(offset++) & 0xff)) << 8
                    | ((long) (segment.readByte(offset) & 0xff)))
                    + Segment.MEDIUM_LIMIT;
        }
    }

    public SegmentStream readStream(RecordId recordId) {
        Segment segment = store.readSegment(recordId.getSegmentId());
        int offset = recordId.getOffset();
        long length = readLength(segment, offset);
        if (length < Segment.MEDIUM_LIMIT) {
            if (length < Segment.SMALL_LIMIT) {
                offset += 1;
            } else {
                offset += 2;
            }
            byte[] data = new byte[(int) length];
            segment.readBytes(offset, data, 0, data.length);
            return new SegmentStream(recordId, data);
        } else {
            int size = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            ListRecord list =
                    new ListRecord(segment.readRecordId(offset + 8), size);
            return new SegmentStream(this, recordId, list, length);
        }
    }

    public byte readByte(RecordId recordId, int position) {
        checkNotNull(recordId);
        checkArgument(position >= 0);
        Segment segment = store.readSegment(recordId.getSegmentId());
        return segment.readByte(recordId.getOffset() + position);
    }

    public int readInt(RecordId recordId, int position) {
        checkNotNull(recordId);
        checkArgument(position >= 0);
        Segment segment = store.readSegment(recordId.getSegmentId());
        return segment.readInt(recordId.getOffset() + position);
    }

    public long readLong(RecordId recordId, int position) {
        checkNotNull(recordId);
        checkArgument(position >= 0);
        Segment segment = store.readSegment(recordId.getSegmentId());
        return segment.readLong(recordId.getOffset() + position);
    }

    public void readBytes(
            RecordId recordId, int position,
            byte[] buffer, int offset, int length) {
        checkNotNull(recordId);
        checkArgument(position >= 0);
        checkNotNull(buffer);
        checkPositionIndexes(offset, offset + length, buffer.length);

        Segment segment = store.readSegment(recordId.getSegmentId());
        segment.readBytes(
                recordId.getOffset() + position, buffer, offset, length);
    }

    public RecordId readRecordId(RecordId recordId, int position) {
        checkNotNull(recordId);
        checkArgument(position >= 0);

        Segment segment = store.readSegment(recordId.getSegmentId());
        return segment.readRecordId(recordId.getOffset() + position);
    }

    public ListRecord readList(RecordId recordId, int numberOfEntries) {
        checkNotNull(recordId);
        checkArgument(numberOfEntries >= 0);

        if (numberOfEntries > 0) {
            Segment segment = store.readSegment(recordId.getSegmentId());
            RecordId id = segment.readRecordId(recordId.getOffset());
            return new ListRecord(id, numberOfEntries);
        } else {
            return new ListRecord(recordId, numberOfEntries);
        }
    }

    public BlockRecord readBlock(RecordId recordId, int size) {
        checkNotNull(recordId);
        checkArgument(size > 0);
        return new BlockRecord(recordId, size);
    }

}
