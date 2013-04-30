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
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.segment.MapRecord.BUCKETS_PER_LEVEL;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;

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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;

public class SegmentWriter {

    private static final byte[] EMPTY_BUFFER = new byte[0];

    static final int BLOCK_SIZE = 1 << 12; // 4kB

    private final SegmentStore store;

    private final Map<String, RecordId> strings = Maps.newHashMap();

    private final Map<Template, RecordId> templates = Maps.newHashMap();

    private UUID uuid = UUID.randomUUID();

    /**
     * Insertion-ordered map from the UUIDs of referenced segments to the
     * respective single-byte UUID index values used when serializing
     * record identifiers.
     */
    private final Map<UUID, Byte> uuids = Maps.newLinkedHashMap();

    /**
     * The segment write buffer, filled from the end to the beginning
     * (see OAK-629). The buffer grows automatically up to
     * {@link Segment#MAX_SEGMENT_SIZE}.
     */
    private byte[] buffer = EMPTY_BUFFER;

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

    public SegmentWriter(SegmentStore store) {
        this.store = store;
    }

    public synchronized void flush() {
        if (length > 0) {
            byte[] data = buffer;
            if (length < buffer.length) {
                data = new byte[length];
                System.arraycopy(
                        buffer, buffer.length - length, data, 0, data.length);
            }

            store.createSegment(new Segment(
                    store, uuid, data, uuids.keySet(), strings, templates));

            uuid = UUID.randomUUID();
            buffer = EMPTY_BUFFER;
            length = 0;
            uuids.clear();
            strings.clear();
            templates.clear();
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
            if (!uuids.containsKey(segmentId)) {
                segmentIds.add(segmentId);
            }
        }

        int fullSize = size + ids.size() * Segment.RECORD_ID_BYTES;
        checkArgument(fullSize > 0);

        int alignment = Segment.RECORD_ALIGN_BYTES - 1;
        int alignedSize = (fullSize + alignment) & ~alignment;
        if (length + alignedSize > MAX_SEGMENT_SIZE
                || uuids.size() + segmentIds.size() > 0x100) {
            flush();
        }
        if (length + alignedSize > buffer.length) {
            int newBufferLength = Math.max(2 * buffer.length, 4096);
            while (length + alignedSize > newBufferLength) {
                newBufferLength *= 2;
            }
            byte[] newBuffer = new byte[newBufferLength];
            System.arraycopy(
                    buffer, buffer.length - length,
                    newBuffer, newBuffer.length - length, length);
            buffer = newBuffer;
        }

        length += alignedSize;
        checkState(length <= MAX_SEGMENT_SIZE);
        position = buffer.length - length;
        return new RecordId(uuid, MAX_SEGMENT_SIZE - length);
    }

    private synchronized void writeRecordId(RecordId id) {
        checkNotNull(id);

        UUID segmentId = id.getSegmentId();
        Byte segmentIndex = uuids.get(segmentId);
        if (segmentIndex == null) {
            checkState(uuids.size() < Segment.SEGMENT_REFERENCE_LIMIT);
            segmentIndex = Byte.valueOf((byte) uuids.size());
            uuids.put(segmentId, segmentIndex);
        }

        int offset = id.getOffset();
        checkState(0 <= offset && offset < MAX_SEGMENT_SIZE);
        checkState((offset & (Segment.RECORD_ALIGN_BYTES - 1)) == 0);

        buffer[position++] = segmentIndex.byteValue();
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

    private synchronized MapLeaf writeMapLeaf(
            int level, Collection<MapEntry> entries) {
        checkNotNull(entries);

        int size = entries.size();
        checkElementIndex(size, MapRecord.MAX_SIZE);
        checkPositionIndex(level, MapRecord.MAX_NUMBER_OF_LEVELS);
        checkArgument(size != 0 || level == MapRecord.MAX_NUMBER_OF_LEVELS);

        List<RecordId> ids = Lists.newArrayListWithCapacity(2 * size);
        for (MapEntry entry : entries) {
            ids.add(entry.getKey());
            ids.add(entry.getValue());
        }

        // copy the entries to an array so we can sort them before writing
        MapEntry[] array = entries.toArray(new MapEntry[entries.size()]);
        Arrays.sort(array);

        RecordId id = prepare(4 + size * 4, ids);
        writeInt((level << MapRecord.SIZE_BITS) | size);
        for (MapEntry entry : array) {
            writeInt(entry.getName().hashCode());
        }
        for (MapEntry entry : array) {
            writeRecordId(entry.getKey());
        }
        for (MapEntry entry : array) {
            writeRecordId(entry.getValue());
        }
        return new MapLeaf(store, id, size, level);
    }

    private MapRecord writeMapBranch(int level, int size, RecordId[] buckets) {
        int bitmap = 0;
        List<RecordId> ids = Lists.newArrayListWithCapacity(buckets.length);
        for (int i = 0; i < buckets.length; i++) {
            if (buckets[i] != null) {
                bitmap |= 1 << i;
                ids.add(buckets[i]);
            }
        }

        RecordId mapId = prepare(8, ids);
        writeInt((level << MapRecord.SIZE_BITS) | size);
        writeInt(bitmap);
        for (RecordId id : ids) {
            writeRecordId(id);
        }
        return new MapBranch(store, mapId, size, level, bitmap);
    }


    private synchronized RecordId writeListBucket(List<RecordId> bucket) {
        RecordId bucketId = prepare(0, bucket);
        for (RecordId id : bucket) {
            writeRecordId(id);
        }
        return bucketId;
    }

    private synchronized MapRecord writeMapBucket(
            RecordId baseId, Collection<MapEntry> entries, int level) {
        int mask = MapRecord.BUCKETS_PER_LEVEL - 1;
        int shift = level * MapRecord.LEVEL_BITS;

        if (entries == null || entries.isEmpty()) {
            if (baseId != null) {
                return MapRecord.readMap(store, baseId);
            } else if (level == 0) {
                RecordId id = prepare(4);
                writeInt(0);
                return new MapLeaf(store, id, 0, 0);
            } else {
                return null;
            }
        } else if (baseId != null) {
            // FIXME: messy code with lots of duplication
            MapRecord base = MapRecord.readMap(store, baseId);
            if (base instanceof MapLeaf) {
                Map<String, MapEntry> map = ((MapLeaf) base).getMapEntries();
                for (MapEntry entry : entries) {
                    if (entry.getValue() != null) {
                        map.put(entry.getName(), entry);
                    } else {
                        map.remove(entry.getName());
                    }
                }
                if (map.isEmpty()) {
                    return null;
                } else {
                    return writeMapBucket(null, map.values(), level);
                }
            } else {
                List<Collection<MapEntry>> buckets =
                        Lists.newArrayListWithCapacity(BUCKETS_PER_LEVEL);
                buckets.addAll(Collections.nCopies(
                        BUCKETS_PER_LEVEL, (Collection<MapEntry>) null));
                for (MapEntry entry : entries) {
                    int bucketIndex = (entry.hashCode() >> shift) & mask;
                    Collection<MapEntry> bucket = buckets.get(bucketIndex);
                    if (bucket == null) {
                        bucket = Lists.newArrayList();
                        buckets.set(bucketIndex, bucket);
                    }
                    bucket.add(entry);
                }

                int newSize = 0;
                List<MapRecord> newBuckets = Lists.newArrayList();
                RecordId[] bucketIds = ((MapBranch) base).getBuckets();
                for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
                    MapRecord newBucket = writeMapBucket(
                            bucketIds[i], buckets.get(i), level + 1);
                    if (newBucket != null) {
                        newBuckets.add(newBucket);
                        bucketIds[i] = newBucket.getRecordId();
                        newSize += newBucket.size();
                    } else {
                        bucketIds[i] = null;
                    }
                }

                // OAK-654: what if the updated map is smaller?
                if (newSize > MapRecord.BUCKETS_PER_LEVEL) {
                    return writeMapBranch(level, newSize, bucketIds);
                } else if (newSize == 0) {
                    if (level == 0) {
                        RecordId id = prepare(4);
                        writeInt(0);
                        return new MapLeaf(store, id, 0, 0);
                    } else {
                        return null;
                    }
                } else if (newBuckets.size() == 1) {
                    return newBuckets.iterator().next();
                } else {
                    // FIXME: ugly hack, flush() shouldn't be needed here
                    flush();
                    List<MapEntry> list = Lists.newArrayList();
                    for (MapRecord record : newBuckets) {
                        Iterables.addAll(list, record.getEntries());
                    }
                    return writeMapLeaf(level, list);
                }
            }
        } else if (entries.size() <= MapRecord.BUCKETS_PER_LEVEL
                || level == MapRecord.MAX_NUMBER_OF_LEVELS) {
            return writeMapLeaf(level, entries);
        } else {
            List<MapEntry>[] lists = new List[MapRecord.BUCKETS_PER_LEVEL];
            for (MapEntry entry : entries) {
                int bucketIndex = (entry.hashCode() >> shift) & mask;
                if (lists[bucketIndex] == null) {
                    lists[bucketIndex] = Lists.newArrayList();
                }
                lists[bucketIndex].add(entry);
            }

            RecordId[] buckets = new RecordId[MapRecord.BUCKETS_PER_LEVEL];
            for (int i = 0; i < lists.length; i++) {
                if (lists[i] != null) {
                    buckets[i] = writeMapBucket(null, lists[i], level + 1).getRecordId();
                }
            }

            return writeMapBranch(level, entries.size(), buckets);
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
            String name = entry.getKey();
            entries.add(new MapEntry(
                    store, name, writeString(name), entry.getValue()));
        }
        RecordId baseId = null;
        if (base != null) {
            baseId = base.getRecordId();
        }
        return writeMapBucket(baseId, entries, 0);
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

    public SegmentNodeState writeNode(NodeState state) {
        if (state instanceof SegmentNodeState) {
            return (SegmentNodeState) state;
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
        RecordId templateId;
        if (before != null && template.equals(before.getTemplate())) {
            templateId = before.getTemplateId();
        } else {
            templateId = writeTemplate(template);
        }

        List<RecordId> ids = Lists.newArrayList();
        ids.add(templateId);

        if (template.hasManyChildNodes()) {
            MapRecord base;
            final Map<String, RecordId> childNodes = Maps.newHashMap();
            if (before != null
                    && before.getChildNodeCount() > 1
                    && after.getChildNodeCount() > 1) {
                base = before.getChildNodeMap();
                after.compareAgainstBaseState(before, new DefaultNodeStateDiff() {
                    @Override
                    public boolean childNodeAdded(String name, NodeState after) {
                        childNodes.put(name, writeNode(after).getRecordId());
                        return true;
                    }
                    @Override
                    public boolean childNodeChanged(
                            String name, NodeState before, NodeState after) {
                        childNodes.put(name, writeNode(after).getRecordId());
                        return true;
                    }
                    @Override
                    public boolean childNodeDeleted(String name, NodeState before) {
                        childNodes.put(name, null);
                        return true;
                    }
                });
            } else {
                base = null;
                for (ChildNodeEntry entry : state.getChildNodeEntries()) {
                    childNodes.put(
                            entry.getName(),
                            writeNode(entry.getNodeState()).getRecordId());
                }
            }
            ids.add(writeMap(base, childNodes).getRecordId());
        } else if (!template.hasNoChildNodes()) {
            ids.add(writeNode(state.getChildNode(template.getChildName())).getRecordId());
        }

        for (PropertyTemplate property : template.getPropertyTemplates()) {
            ids.add(writeProperty(state.getProperty(property.getName())));
        }

        RecordId recordId = prepare(0, ids);
        for (RecordId id : ids) {
            writeRecordId(id);
        }
        return new SegmentNodeState(store, recordId);
    }

}
