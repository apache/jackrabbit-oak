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

import static com.google.common.base.Objects.equal;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayListWithExpectedSize;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.segment.MapRecord.BUCKETS_PER_LEVEL;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.align;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import com.google.common.io.Closeables;

public class SegmentWriter {

    private enum RecordType {
        LEAF,
        BRANCH,
        BUCKET,
        LIST,
        VALUE,
        BLOCK,
        TEMPLATE,
        NODE
    }

    static final int BLOCK_SIZE = 1 << 12; // 4kB

    private final SegmentStore store;

    private final Map<String, RecordId> strings = new LinkedHashMap<String, RecordId>(1500, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Entry<String, RecordId> eldest) {
            return size() > 1000;
        }
    };

    private final Map<Template, RecordId> templates = new LinkedHashMap<Template, RecordId>(1500, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Entry<Template, RecordId> eldest) {
            return size() > 1000;
        }
    };

    private UUID uuid = UUID.randomUUID();

    /**
     * Insertion-ordered map from the UUIDs of referenced segments to the
     * respective single-byte UUID index values used when serializing
     * record identifiers.
     */
    private final Map<UUID, Byte> refids = newLinkedHashMap();

    /**
     * The set of root records (i.e. ones not referenced by other records)
     * in this segment.
     */
    private final Map<RecordId, RecordType> roots = newLinkedHashMap();

    /**
     * The segment write buffer, filled from the end to the beginning
     * (see OAK-629).
     */
    private final byte[] buffer = new byte[MAX_SEGMENT_SIZE];

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

    private Segment currentSegment = null;

    private final Segment dummySegment;

    public SegmentWriter(SegmentStore store) {
        this.store = store;
        this.dummySegment =
                new Segment(store, UUID.randomUUID(), ByteBuffer.allocate(0));
    }

    private void writeSegmentHeader(ByteBuffer b) {
        int p = b.position();

        b.put((byte) refids.size());
        b.putShort((short) roots.size());

        for (Map.Entry<RecordId, RecordType> entry : roots.entrySet()) {
            int offset = entry.getKey().getOffset();
            b.put((byte) entry.getValue().ordinal());
            b.put((byte) (offset >> (8 + Segment.RECORD_ALIGN_BITS)));
            b.put((byte) (offset >> Segment.RECORD_ALIGN_BITS));
        }

        p = b.position() - p;
        int q = Segment.align(p);
        for (int i = p; i < q; i++) {
            b.put((byte) 0);
        }

        for (UUID refid : refids.keySet()) {
            b.putLong(refid.getMostSignificantBits());
            b.putLong(refid.getLeastSignificantBits());
        }
    }

    public synchronized Segment getCurrentSegment(UUID id) {
        if (equal(id, uuid)) {
            if (currentSegment == null) {
                int header = align(3 + roots.size() * 3) + 16 * refids.size();
                ByteBuffer b = ByteBuffer.allocate(header + length);
                writeSegmentHeader(b);
                b.put(buffer, buffer.length - length, length);
                b.rewind();
                currentSegment = new Segment(store, uuid, b);
            }
            return currentSegment;
        } else {
            return null;
        }
    }

    public Segment getDummySegment() {
        return dummySegment;
    }

    public synchronized void flush() {
        if (length > 0) {
            length += align(3 + roots.size() * 3) + refids.size() * 16;

            ByteBuffer b = ByteBuffer.wrap(
                    buffer, buffer.length - length, length);
            writeSegmentHeader(b);

            store.writeSegment(uuid, buffer, buffer.length - length, length);

            uuid = UUID.randomUUID();
            refids.clear();
            roots.clear();
            length = 0;
            position = buffer.length;
            currentSegment = null;
        }
    }

    private RecordId prepare(RecordType type, int size) {
        return prepare(type, size, Collections.<RecordId>emptyList());
    }

    private RecordId prepare(
            RecordType type, int size, Collection<RecordId> ids) {
        checkArgument(size >= 0);
        checkNotNull(ids);

        Set<UUID> segmentIds = newHashSet();
        for (RecordId id : ids) {
            UUID segmentId = id.getSegmentId();
            if (!equal(uuid, segmentId) && !refids.containsKey(segmentId)) {
                segmentIds.add(segmentId);
            }
        }
        int refCount = refids.size() + segmentIds.size();

        Set<RecordId> rootIds = newHashSet(roots.keySet());
        rootIds.removeAll(ids);
        int rootCount = rootIds.size() + 1;

        int recordSize = Segment.align(size + ids.size() * Segment.RECORD_ID_BYTES);
        int headerSize = Segment.align(3 + rootCount * 3);
        int segmentSize = headerSize + refCount * 16 + recordSize + length;
        if (segmentSize > buffer.length - 1
                || rootCount > 0xffff
                || refCount > Segment.SEGMENT_REFERENCE_LIMIT) {
            flush();
        }

        length += recordSize;
        position = buffer.length - length;
        checkState(position >= 0);

        currentSegment = null;

        RecordId id = new RecordId(uuid, position);
        roots.put(id, type);
        return id;
    }

    private void writeRecordId(RecordId id) {
        checkNotNull(id);

        roots.remove(id);

        UUID segmentId = id.getSegmentId();
        if (equal(uuid, segmentId)) {
            buffer[position++] = (byte) 0xff;
        } else {
            Byte segmentIndex = refids.get(segmentId);
            if (segmentIndex == null) {
                checkState(refids.size() < Segment.SEGMENT_REFERENCE_LIMIT);
                segmentIndex = Byte.valueOf((byte) refids.size());
                refids.put(segmentId, segmentIndex);
            }
            buffer[position++] = segmentIndex.byteValue();
        }

        int offset = id.getOffset();
        checkState(0 <= offset && offset < MAX_SEGMENT_SIZE);
        checkState(offset == Segment.align(offset));

        buffer[position++] = (byte) (offset >> (8 + Segment.RECORD_ALIGN_BITS));
        buffer[position++] = (byte) (offset >> Segment.RECORD_ALIGN_BITS);
    }

    private void writeInt(int value) {
        buffer[position++] = (byte) (value >> 24);
        buffer[position++] = (byte) (value >> 16);
        buffer[position++] = (byte) (value >> 8);
        buffer[position++] = (byte) value;
    }

    private void writeLong(long value) {
        writeInt((int) (value >> 32));
        writeInt((int) value);
    }

    private MapRecord writeMapLeaf(
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

        synchronized (this) {
            RecordId id = prepare(RecordType.LEAF, 4 + size * 4, ids);
            writeInt((level << MapRecord.SIZE_BITS) | size);
            for (MapEntry entry : array) {
                writeInt(entry.getHash());
            }
            for (MapEntry entry : array) {
                writeRecordId(entry.getKey());
            }
            for (MapEntry entry : array) {
                writeRecordId(entry.getValue());
            }
            return new MapRecord(dummySegment, id);
        }
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

        synchronized (this) {
            RecordId mapId = prepare(RecordType.BRANCH, 8, ids);
            writeInt((level << MapRecord.SIZE_BITS) | size);
            writeInt(bitmap);
            for (RecordId id : ids) {
                writeRecordId(id);
            }
            return new MapRecord(dummySegment, mapId);
        }
    }


    private synchronized RecordId writeListBucket(List<RecordId> bucket) {
        RecordId bucketId = prepare(RecordType.BUCKET, 0, bucket);
        for (RecordId id : bucket) {
            writeRecordId(id);
        }
        return bucketId;
    }

    private synchronized MapRecord writeMapBucket(
            RecordId baseId, Collection<MapEntry> entries, int level) {
        int mask = MapRecord.BUCKETS_PER_LEVEL - 1;
        int shift = 32 - (level + 1) * MapRecord.LEVEL_BITS;

        if (entries == null || entries.isEmpty()) {
            if (baseId != null) {
                return dummySegment.readMap(baseId);
            } else if (level == 0) {
                synchronized (this) {
                    RecordId id = prepare(RecordType.LIST, 4);
                    writeInt(0);
                    return new MapRecord(dummySegment, id);
                }
            } else {
                return null;
            }
        } else if (baseId != null) {
            // FIXME: messy code with lots of duplication
            MapRecord base = dummySegment.readMap(baseId);
            if (base.isLeaf()) {
                Map<String, MapEntry> map = newHashMap();
                for (MapEntry entry : base.getEntries()) {
                    map.put(entry.getName(), entry);
                }
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
                    int bucketIndex = (entry.getHash() >> shift) & mask;
                    Collection<MapEntry> bucket = buckets.get(bucketIndex);
                    if (bucket == null) {
                        bucket = Lists.newArrayList();
                        buckets.set(bucketIndex, bucket);
                    }
                    bucket.add(entry);
                }

                int newSize = 0;
                List<MapRecord> newBuckets = Lists.newArrayList();
                RecordId[] bucketIds = base.getBuckets();
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
                        synchronized (this) {
                            RecordId id = prepare(RecordType.LIST, 4);
                            writeInt(0);
                            return new MapRecord(dummySegment, id);
                        }
                    } else {
                        return null;
                    }
                } else if (newBuckets.size() == 1) {
                    return newBuckets.iterator().next();
                } else {
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
                int bucketIndex = (entry.getHash() >> shift) & mask;
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
        RecordId valueId = prepare(
                RecordType.VALUE, 8, Collections.singleton(blocks));
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

        RecordId blockId = prepare(RecordType.BLOCK, length);
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
                    dummySegment, name, writeString(name), entry.getValue()));
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
        synchronized (strings) {
            RecordId id = strings.get(string);
            if (id == null) {
                byte[] data = string.getBytes(Charsets.UTF_8);
                try {
                    id = writeStream(new ByteArrayInputStream(data)).getRecordId();
                } catch (IOException e) {
                    throw new IllegalStateException("Unexpected IOException", e);
                }
                strings.put(string, id);
            }
            return id;
        }
    }

    public SegmentBlob writeBlob(Blob blob) throws IOException {
        if (blob instanceof SegmentBlob) {
            return (SegmentBlob) blob;
        } else {
            return writeStream(blob.getNewStream());
        }
    }

    /**
     * Writes a stream value record. The given stream is consumed
     * <em>and closed</em> by this method.
     *
     * @param stream stream to be written
     * @return value record identifier
     * @throws IOException if the stream could not be read
     */
    public SegmentBlob writeStream(InputStream stream) throws IOException {
        RecordId id = SegmentStream.getRecordIdIfAvailable(stream);
        if (id == null) {
            boolean threw = true;
            try {
                id = internalWriteStream(stream);
                threw = false;
            } finally {
                Closeables.close(stream, threw);
            }
        }
        return new SegmentBlob(dummySegment, id);
    }

    private RecordId internalWriteStream(InputStream stream)
            throws IOException {
        byte[] data = new byte[Segment.MAX_SEGMENT_SIZE];
        int n = ByteStreams.read(stream, data, 0, data.length);

        // Special case for short binaries (up to about 16kB):
        // store them directly as small- or medium-sized value records
        if (n < Segment.MEDIUM_LIMIT) {
            synchronized (this) {
                RecordId id;
                if (n < Segment.SMALL_LIMIT) {
                    id = prepare(RecordType.VALUE, 1 + n);
                    buffer[position++] = (byte) n;
                } else {
                    id = prepare(RecordType.VALUE, 2 + n);
                    int len = (n - Segment.SMALL_LIMIT) | 0x8000;
                    buffer[position++] = (byte) (len >> 8);
                    buffer[position++] = (byte) len;
                }
                System.arraycopy(data, 0, buffer, position, n);
                position += n;
                return id;
            }
        }

        long length = n;
        List<RecordId> blockIds = newArrayListWithExpectedSize(n / 4096);

        // Write full bulk segments
        while (n == buffer.length) {
            UUID id = UUID.randomUUID();
            store.writeSegment(id, data, 0, data.length);

            for (int i = 0; i < data.length; i += BLOCK_SIZE) {
                blockIds.add(new RecordId(id, i));
            }

            n = ByteStreams.read(stream, data, 0, data.length);
            length += n;
        }


        // Inline the remaining blocks in the current segments
        for (int p = 0; p < n; p += BLOCK_SIZE) {
            int size = Math.min(n - p, BLOCK_SIZE);
            synchronized (this) {
                blockIds.add(prepare(RecordType.BLOCK, size));
                System.arraycopy(data, p, buffer, position, size);
            }
        }

        return writeValueRecord(length, writeList(blockIds));
    }

    private RecordId writeProperty(
            PropertyState state, Map<String, RecordId> previousValues) {
        Type<?> type = state.getType();
        int count = state.count();

        List<RecordId> valueIds = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            if (type.tag() == PropertyType.BINARY) {
                try {
                    SegmentBlob blob =
                            writeBlob(state.getValue(Type.BINARY, i));
                    valueIds.add(blob.getRecordId());
                } catch (IOException e) {
                    throw new IllegalStateException("Unexpected IOException", e);
                }
            } else {
                String value = state.getValue(Type.STRING, i);
                RecordId valueId = previousValues.get(value);
                if (valueId == null) {
                    valueId = writeString(value);
                }
                valueIds.add(valueId);
            }
        }

        if (!type.isArray()) {
            return valueIds.iterator().next();
        } else if (count == 0) {
            synchronized (this) {
                RecordId propertyId = prepare(RecordType.LIST, 4);
                writeInt(0);
                return propertyId;
            }
        } else {
            RecordId listId = writeList(valueIds);
            synchronized (this) {
                RecordId propertyId = prepare(
                        RecordType.LIST, 4, Collections.singleton(listId));
                writeInt(count);
                writeRecordId(listId);
                return propertyId;
            }
        }
    }

    public RecordId writeTemplate(Template template) {
        checkNotNull(template);
        synchronized (templates) {
            RecordId id = templates.get(template);
            if (id == null) {
                Collection<RecordId> ids = Lists.newArrayList();
                int head = 0;

                RecordId primaryId = null;
                PropertyState primaryType = template.getPrimaryType();
                if (primaryType != null) {
                    head |= 1 << 31;
                    primaryId = writeString(primaryType.getValue(NAME));
                    ids.add(primaryId);
                }

                List<RecordId> mixinIds = null;
                PropertyState mixinTypes = template.getMixinTypes();
                if (mixinTypes != null) {
                    head |= 1 << 30;
                    mixinIds = Lists.newArrayList();
                    for (String mixin : mixinTypes.getValue(NAMES)) {
                        mixinIds.add(writeString(mixin));
                    }
                    ids.addAll(mixinIds);
                    checkState(mixinIds.size() < (1 << 10));
                    head |= mixinIds.size() << 18;
                }

                RecordId childNameId = null;
                String childName = template.getChildName();
                if (childName == Template.ZERO_CHILD_NODES) {
                    head |= 1 << 29;
                } else if (childName == Template.MANY_CHILD_NODES) {
                    head |= 1 << 28;
                } else {
                    childNameId = writeString(childName);
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

                synchronized (this) {
                    id = prepare(
                            RecordType.TEMPLATE, 4 + propertyTypes.length, ids);
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
                }

                templates.put(template, id);
            }
            return id;
        }
    }

    public SegmentNodeState writeNode(NodeState state) {
        if (state instanceof SegmentNodeState) {
            return (SegmentNodeState) state;
        }

        SegmentNodeState before = null;
        ModifiedNodeState after = null;
        if (state instanceof ModifiedNodeState) {
            after = (ModifiedNodeState) state;
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

        String childName = template.getChildName();
        if (childName == Template.MANY_CHILD_NODES) {
            MapRecord base;
            final Map<String, RecordId> childNodes = Maps.newHashMap();
            if (before != null
                    && before.getChildNodeCount(2) > 1
                    && after.getChildNodeCount(2) > 1) {
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
        } else if (childName != Template.ZERO_CHILD_NODES) {
            ids.add(writeNode(state.getChildNode(template.getChildName())).getRecordId());
        }

        for (PropertyTemplate pt : template.getPropertyTemplates()) {
            String name = pt.getName();
            PropertyState property = state.getProperty(name);

            if (property instanceof SegmentPropertyState) {
                ids.add(((SegmentPropertyState) property).getRecordId());
            } else {
                Map<String, RecordId> previousValues = emptyMap();
                if (before != null) {
                    // reuse previously stored property values, if possible
                    PropertyState beforeProperty = before.getProperty(name);
                    if (beforeProperty instanceof SegmentPropertyState
                            && beforeProperty.isArray()
                            && beforeProperty.getType() != Type.BINARIES) {
                        SegmentPropertyState segmentProperty =
                                (SegmentPropertyState) beforeProperty;
                        previousValues = segmentProperty.getValueRecords();
                    }
                }
                ids.add(writeProperty(property, previousValues));
            }
        }

        synchronized (this) {
            RecordId recordId = prepare(RecordType.NODE, 0, ids);
            for (RecordId id : ids) {
                writeRecordId(id);
            }
            return new SegmentNodeState(dummySegment, recordId);
        }
    }

}
