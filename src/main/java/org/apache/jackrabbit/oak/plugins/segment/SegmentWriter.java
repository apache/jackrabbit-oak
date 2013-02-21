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
import static com.google.common.base.Preconditions.checkState;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;

public class SegmentWriter {

    static final int BLOCK_SIZE = 1 << 12; // 4kB

    private final SegmentStore store;

    private final SegmentReader reader;

    private final Map<String, RecordId> strings = Maps.newHashMap();

    private final Map<Template, RecordId> templates = Maps.newHashMap();

    private UUID uuid = UUID.randomUUID();

    private final List<UUID> uuids =
            Lists.newArrayListWithCapacity(Segment.SEGMENT_REFERENCE_LIMIT);

    /**
     * The segment write buffer, filled from the end to the beginning
     * (see OAK-629). Note that we currently allocate the entire buffer
     * right from the beginning. It might turn out that a better approach
     * would be to start with a smaller buffer that grows automatically,
     * or to use a pool of pre-allocated buffers.
     */
    private final byte[] buffer = new byte[Segment.MAX_SEGMENT_SIZE];

    /**
     * The number of bytes already written (or allocated). Counted from
     * the <em>end</em> of the buffer.
     */
    private int length = 0;

    /**
     * Current write position within the buffer. Grows up when raw data
     * is written, but shifted downwards by the prepare methods.
     */
    private int position;

    public SegmentWriter(SegmentStore store, SegmentReader reader) {
        this.store = store;
        this.reader = reader;
    }

    public synchronized void flush() {
        if (length > 0) {
            byte[] data = buffer;
            if (length < buffer.length) {
                data = new byte[length];
                int start = buffer.length - length;
                System.arraycopy(buffer, start, data, 0, data.length);
            }

            store.createSegment(new Segment(
                    uuid, data, uuids.toArray(new UUID[uuids.size()])));

            uuid = UUID.randomUUID();
            length = 0;
            uuids.clear();
        }
    }

    private RecordId prepare(int size) {
        return prepare(size, Collections.<RecordId>emptyList());
    }

    private synchronized RecordId prepare(int size, Collection<RecordId> ids) {
        checkArgument(size >= 0);

        Set<UUID> segmentIds = new HashSet<UUID>();
        for (RecordId id : checkNotNull(ids)) {
            UUID segmentId = id.getSegmentId();
            if (!uuids.contains(segmentId)) {
                segmentIds.add(segmentId);
            }
        }

        int fullSize = size + ids.size() * Segment.RECORD_ID_BYTES;
        checkArgument(fullSize > 0);

        int alignment = Segment.RECORD_ALIGN_BYTES - 1;
        int alignedSize = (fullSize + alignment) & ~alignment;
        if (length + alignedSize > buffer.length
                || uuids.size() + segmentIds.size() > 0x100) {
            flush();
        }

        length += alignedSize;
        position = buffer.length - length;
        return new RecordId(uuid, position);
    }

    private synchronized void writeRecordId(RecordId id) {
        checkNotNull(id);

        UUID segmentId = id.getSegmentId();
        int index = uuids.indexOf(segmentId);
        if (index == -1) {
            index = uuids.size();
            uuids.add(segmentId);
        }
        int offset = id.getOffset();

        checkState(index < Segment.SEGMENT_REFERENCE_LIMIT);
        checkState(0 <= offset && offset < buffer.length);
        checkState((offset & (Segment.RECORD_ALIGN_BYTES - 1)) == 0);

        buffer[position++] = (byte) index;
        buffer[position++] = (byte) (offset >> (8 + Segment.RECORD_ALIGN_BITS));
        buffer[position++] = (byte) (offset >> Segment.RECORD_ALIGN_BITS);
    }

    private synchronized void writeInt(int value) {
        buffer[position++] = (byte) (value >> 24);
        buffer[position++] = (byte) (value >> 16);
        buffer[position++] = (byte) (value >> 8);
        buffer[position++] = (byte) value;
    }

    private synchronized void writeLong(long value) {
        writeInt((int) (value >> 32));
        writeInt((int) value);
    }

    private synchronized RecordId writeListBucket(List<RecordId> bucket) {
        RecordId bucketId = prepare(0, bucket);
        for (RecordId id : bucket) {
            writeRecordId(id);
        }
        return bucketId;
    }

    private class MapEntry implements Comparable<MapEntry> {
        private final int hashCode;
        private final String string;
        private final RecordId key;
        private final RecordId value;

        MapEntry(String key, RecordId value) {
            this.hashCode = key.hashCode();
            this.string = key;
            this.key = writeString(key);
            this.value = value;
        }

        @Override
        public int compareTo(MapEntry that) {
            // int diff = Integer.compare(hashCode, that.hashCode);
            int diff = hashCode == that.hashCode ? 0 : hashCode < that.hashCode ? -1 : 1;
            if (diff == 0) {
                diff = key.compareTo(that.key);
            }
            if (diff == 0) {
                diff = value.compareTo(that.value);
            }
            return diff;
        }

        public boolean equals(Object object) {
            if (this == object) {
                return true;
            } else if (object instanceof MapEntry) {
                MapEntry that = (MapEntry) object;
                return hashCode == that.hashCode
                        && key.equals(that.key)
                        && value.equals(that.value);
            } else {
                return false;
            }
        }

    }

    private static class BucketInfo {
        RecordId id;
        int size;
        BucketInfo(RecordId id, int size) {
            this.id = id;
            this.size = size;
        }
    }

    private synchronized BucketInfo writeMapBucket(
            RecordId baseId, List<MapEntry> entries, int level) {
        int size = 1 << MapRecord.LEVEL_BITS;
        int mask = size - 1;
        int shift = level * MapRecord.LEVEL_BITS;

        if (entries.isEmpty()) {
            if (baseId != null) {
                return new BucketInfo(baseId, new MapRecord(baseId).size(reader));
            } else if (level == 0) {
                RecordId id = prepare(4);
                writeInt(0);
                return new BucketInfo(id, 0);
            } else {
                return new BucketInfo(null, 0);
            }
        } else if (baseId != null) {
            // FIXME: messy code with lots of duplication
            MapRecord base = new MapRecord(baseId);
            int baseSize = base.size(reader);
            if (baseSize <= size) {
                Map<String, RecordId> update = Maps.newHashMap();
                for (MapRecord.Entry entry : base.getEntries(reader)) {
                    update.put(entry.getKey(), entry.getValue());
                }
                for (MapEntry entry : entries) {
                    if (entry.value != null) {
                        update.put(entry.string, entry.value);
                    } else {
                        update.remove(entry.string);
                    }
                }
                entries = Lists.newArrayListWithCapacity(update.size());
                for (Map.Entry<String, RecordId> entry : update.entrySet()) {
                    entries.add(new MapEntry(entry.getKey(), entry.getValue()));
                }
                return writeMapBucket(null, entries, level);
            } else {
                List<MapEntry>[] buckets = new List[size];
                for (MapEntry entry : entries) {
                    int bucketIndex = (entry.hashCode >> shift) & mask;
                    if (buckets[bucketIndex] == null) {
                        buckets[bucketIndex] = Lists.newArrayList();
                    }
                    buckets[bucketIndex].add(entry);
                }

                long baseMap = reader.readLong(baseId, 4);

                int newSize = 0;
                RecordId[] bucketIds = new RecordId[size];
                for (int i = 0; i < buckets.length; i++) {
                    long bucketBit = 1L << i;
                    RecordId baseBucketId = null;
                    if ((baseMap & bucketBit) != 0) {
                        int index = Long.bitCount(baseMap & (bucketBit - 1));
                        baseBucketId = reader.readRecordId(
                                baseId, 12 + index * Segment.RECORD_ID_BYTES);
                    }
                    if (buckets[i] != null) {
                        BucketInfo info = writeMapBucket(
                                baseBucketId, buckets[i], level + 1);
                        bucketIds[i] = info.id;
                        newSize += info.size;
                    } else {
                        bucketIds[i] = baseBucketId;
                        if (baseBucketId != null) {
                            newSize += new MapRecord(baseBucketId).size(reader);
                        }
                    }
                }
                
                List<RecordId> ids = Lists.newArrayList();
                long bucketMap = 0L;
                for (int i = 0; i < buckets.length; i++) {
                    if (bucketIds[i] != null) {
                        ids.add(bucketIds[i]);
                        bucketMap |= 1L << i;
                    }
                }

                RecordId bucketId = prepare(12, ids);
                writeInt(newSize);
                writeLong(bucketMap);
                for (RecordId id : ids) {
                    writeRecordId(id);
                }
                return new BucketInfo(bucketId, newSize);
            }
        } else if (entries.size() <= size) {
            Collections.sort(entries);

            List<RecordId> ids = Lists.newArrayList();
            for (MapEntry entry : entries) {
                ids.add(entry.key);
                ids.add(entry.value);
            }

            RecordId bucketId = prepare(4 + entries.size() * 4, ids);
            writeInt(entries.size());
            for (MapEntry entry : entries) {
                writeInt(entry.hashCode);
            }
            for (MapEntry entry : entries) {
                writeRecordId(entry.key);
            }
            for (MapEntry entry : entries) {
                writeRecordId(entry.value);
            }
            return new BucketInfo(bucketId, entries.size());
        } else {
            List<MapEntry>[] buckets = new List[size];
            for (MapEntry entry : entries) {
                int bucketIndex = (entry.hashCode >> shift) & mask;
                if (buckets[bucketIndex] == null) {
                    buckets[bucketIndex] = Lists.newArrayList();
                }
                buckets[bucketIndex].add(entry);
            }

            List<RecordId> bucketIds = Lists.newArrayList();
            long bucketMap = 0L;
            for (int i = 0; i < buckets.length; i++) {
                if (buckets[i] != null) {
                    bucketIds.add(writeMapBucket(null, buckets[i], level + 1).id);
                    bucketMap |= 1L << i;
                }
            }

            RecordId bucketId = prepare(12, bucketIds);
            writeInt(entries.size());
            writeLong(bucketMap);
            for (RecordId id : bucketIds) {
                writeRecordId(id);
            }
            return new BucketInfo(bucketId, entries.size());
        }
    }

    private synchronized RecordId writeValueRecord(
            long length, RecordId blocks) {
        RecordId valueId = prepare(8, Collections.singleton(blocks));
        writeLong((length - Segment.MEDIUM_LIMIT) | (0x3L << 62));
        writeRecordId(blocks);
        return valueId;
    }

    /**
     * Writes a block record containing the given block of bytes.
     *
     * @param bytes source buffer
     * @param offset offset within the source buffer
     * @param length number of bytes to write
     * @return block record identifier
     */
    public synchronized RecordId writeBlock(
            byte[] bytes, int offset, int length) {
        checkNotNull(bytes);
        checkPositionIndexes(offset, offset + length, bytes.length);

        RecordId blockId = prepare(length);
        System.arraycopy(bytes, offset, buffer, position, length);
        position += length;
        return blockId;
    }

    /**
     * Writes a list record containing the given list of record identifiers.
     *
     * @param list list of record identifiers
     * @return list record identifier
     */
    public RecordId writeList(List<RecordId> list) {
        checkNotNull(list);
        checkArgument(list.size() > 0);

        List<RecordId> thisLevel = list;
        while (thisLevel.size() > 1) {
            List<RecordId> nextLevel = Lists.newArrayList();
            for (List<RecordId> bucket :
                    Lists.partition(thisLevel, ListRecord.LEVEL_SIZE)) {
                nextLevel.add(writeListBucket(bucket));
            }
            thisLevel = nextLevel;
        }
        return thisLevel.iterator().next();
    }

    public MapRecord writeMap(MapRecord base, Map<String, RecordId> changes) {
        List<MapEntry> entries = Lists.newArrayList();
        for (Map.Entry<String, RecordId> entry : changes.entrySet()) {
            entries.add(new MapEntry(entry.getKey(), entry.getValue()));
        }
        RecordId baseId = null;
        if (base != null) {
            baseId = base.getRecordId();
        }
        return new MapRecord(writeMapBucket(baseId, entries, 0).id);
    }

    /**
     * Writes a string value record.
     *
     * @param string string to be written
     * @return value record identifier
     */
    public RecordId writeString(String string) {
        RecordId id = strings.get(string);
        if (id == null) {
            byte[] data = string.getBytes(Charsets.UTF_8);
            try {
                id = writeStream(new ByteArrayInputStream(data));
            } catch (IOException e) {
                throw new IllegalStateException("Unexpected IOException", e);
            }
            strings.put(string, id);
        }
        return id;
    }

    /**
     * Writes a stream value record. The given stream is consumed
     * <em>and closed</em> by this method.
     *
     * @param stream stream to be written
     * @return value record identifier
     * @throws IOException if the stream could not be read
     */
    public RecordId writeStream(InputStream stream) throws IOException {
        RecordId id = SegmentStream.getRecordIdIfAvailable(stream);
        if (id == null) {
            try {
                id = internalWriteStream(stream);
            } finally {
                stream.close();
            }
        }
        return id;
    }

    private synchronized RecordId internalWriteStream(InputStream stream)
            throws IOException {
        // First read the head of the stream. This covers most small
        // values and the frequently accessed head of larger ones.
        // The head gets inlined in the current segment.
        byte[] head = new byte[Segment.MEDIUM_LIMIT];
        int headLength = ByteStreams.read(stream, head, 0, head.length);

        if (headLength < Segment.SMALL_LIMIT) {
            RecordId id = prepare(1 + headLength);
            buffer[position++] = (byte) headLength;
            System.arraycopy(head, 0, buffer, position, headLength);
            position += headLength;
            return id;
        } else if (headLength < Segment.MEDIUM_LIMIT) {
            RecordId id = prepare(2 + headLength);
            int len = (headLength - Segment.SMALL_LIMIT) | 0x8000;
            buffer[position++] = (byte) (len >> 8);
            buffer[position++] = (byte) len;
            System.arraycopy(head, 0, buffer, position, headLength);
            position += headLength;
            return id;
        } else {
            // If the stream filled the full head buffer, it's likely
            // that the bulk of the data is still to come. Read it
            // in larger chunks and save in separate segments.

            long length = 0;
            List<RecordId> blockIds = new ArrayList<RecordId>();

            byte[] bulk = new byte[Segment.MAX_SEGMENT_SIZE];
            System.arraycopy(head, 0, bulk, 0, headLength);
            int bulkLength = headLength + ByteStreams.read(
                    stream, bulk, headLength, bulk.length - headLength);
            while (bulkLength > 0) {
                UUID segmentId = UUID.randomUUID();
                int align = Segment.RECORD_ALIGN_BYTES - 1;
                int bulkAlignLength = (bulkLength + align) & ~align;
                store.createSegment(segmentId, bulk, 0, bulkAlignLength);
                for (int pos = Segment.MAX_SEGMENT_SIZE - bulkAlignLength;
                        pos < Segment.MAX_SEGMENT_SIZE;
                        pos += BLOCK_SIZE) {
                    blockIds.add(new RecordId(segmentId, pos));
                }
                length += bulkLength;
                bulkLength = ByteStreams.read(stream, bulk, 0, bulk.length);
            }

            return writeValueRecord(length, writeList(blockIds));
        }
    }

    private synchronized RecordId writeProperty(PropertyState state) {
        Type<?> type = state.getType();
        int count = state.count();

        List<RecordId> valueIds = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            if (type.tag() == PropertyType.BINARY) {
                try {
                    Blob blob = state.getValue(Type.BINARY, i);
                    valueIds.add(writeStream(blob.getNewStream()));
                } catch (IOException e) {
                    throw new IllegalStateException("Unexpected IOException", e);
                }
            } else {
                valueIds.add(writeString(state.getValue(Type.STRING, i)));
            }
        }

        if (!type.isArray()) {
            return valueIds.iterator().next();
        } else if (count == 0) {
            RecordId propertyId = prepare(4);
            writeInt(0);
            return propertyId;
        } else {
            RecordId listId = writeList(valueIds);
            RecordId propertyId = prepare(4, Collections.singleton(listId));
            writeInt(count);
            writeRecordId(listId);
            return propertyId;
        }
    }

    public synchronized RecordId writeTemplate(Template template) {
        checkNotNull(template);
        RecordId id = templates.get(template);
        if (id == null) {
            Collection<RecordId> ids = Lists.newArrayList();
            int head = 0;

            RecordId primaryId = null;
            if (template.hasPrimaryType()) {
                head |= 1 << 31;
                primaryId = writeString(template.getPrimaryType());
                ids.add(primaryId);
            }

            List<RecordId> mixinIds = null;
            if (template.hasMixinTypes()) {
                head |= 1 << 30;
                mixinIds = Lists.newArrayList();
                for (String mixin : template.getMixinTypes()) {
                    mixinIds.add(writeString(mixin));
                }
                ids.addAll(mixinIds);
                checkState(mixinIds.size() < (1 << 10));
                head |= mixinIds.size() << 18;
            }

            RecordId childNameId = null;
            if (template.hasNoChildNodes()) {
                head |= 1 << 29;
            } else if (template.hasManyChildNodes()) {
                head |= 1 << 28;
            } else {
                childNameId = writeString(template.getChildName());
                ids.add(childNameId);
            }

            PropertyTemplate[] properties = template.getPropertyTemplates();
            RecordId[] propertyNames = new RecordId[properties.length];
            byte[] propertyTypes = new byte[properties.length];
            for (int i = 0; i < properties.length; i++) {
                propertyNames[i] = writeString(properties[i].getName());
                Type<?> type = properties[i].getType();
                if (type.isArray()) {
                    propertyTypes[i] = (byte) -type.tag();
                } else {
                    propertyTypes[i] = (byte) type.tag();
                }
            }
            ids.addAll(Arrays.asList(propertyNames));
            checkState(propertyNames.length < (1 << 18));
            head |= propertyNames.length;

            id = prepare(4 + propertyTypes.length, ids);
            writeInt(head);
            if (primaryId != null) {
                writeRecordId(primaryId);
            }
            if (mixinIds != null) {
                for (RecordId mixinId : mixinIds) {
                    writeRecordId(mixinId);
                }
            }
            if (childNameId != null) {
                writeRecordId(childNameId);
            }
            for (int i = 0; i < propertyNames.length; i++) {
                writeRecordId(propertyNames[i]);
                buffer[position++] = propertyTypes[i];
            }

            templates.put(template, id);
        }
        return id;
    }

    public RecordId writeNode(NodeState state) {
        RecordId nodeId = SegmentNodeState.getRecordIdIfAvailable(state);
        if (nodeId != null) {
            return nodeId;
        }

        SegmentNodeState before = null;
        ModifiedNodeState after = null;
        if (state instanceof ModifiedNodeState) {
            after = ModifiedNodeState.collapse((ModifiedNodeState) state);
            NodeState base = after.getBaseState();
            if (base instanceof SegmentNodeState) {
                before = (SegmentNodeState) base;
            }
        }

        Template template = new Template(state);

        List<RecordId> ids = Lists.newArrayList();
        ids.add(writeTemplate(template));

        if (template.hasManyChildNodes()) {
            MapRecord base;
            final Map<String, RecordId> childNodes = Maps.newHashMap();
            if (before != null
                    && before.getChildNodeCount() > 1
                    && after.getChildNodeCount() > 1) {
                base = before.getChildNodeMap();
                after.compareAgainstBaseState(before, new DefaultNodeStateDiff() {
                    @Override
                    public void childNodeAdded(String name, NodeState after) {
                        childNodes.put(name, writeNode(after));
                    }
                    @Override
                    public void childNodeChanged(
                            String name, NodeState before, NodeState after) {
                        childNodes.put(name, writeNode(after));
                    }
                    @Override
                    public void childNodeDeleted(String name, NodeState before) {
                        childNodes.put(name, null);
                    }
                });
            } else {
                base = null;
                for (ChildNodeEntry entry : state.getChildNodeEntries()) {
                    childNodes.put(entry.getName(), writeNode(entry.getNodeState()));
                }
            }
            ids.add(writeMap(base, childNodes).getRecordId());
        } else if (!template.hasNoChildNodes()) {
            ids.add(writeNode(state.getChildNode(template.getChildName())));
        }

        for (PropertyTemplate property : template.getPropertyTemplates()) {
            ids.add(writeProperty(state.getProperty(property.getName())));
        }

        RecordId recordId = prepare(0, ids);
        for (RecordId id : ids) {
            writeRecordId(id);
        }
        return recordId;
    }

}
