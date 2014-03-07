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
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithExpectedSize;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.nCopies;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.segment.MapRecord.BUCKETS_PER_LEVEL;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.align;

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

    private final SegmentIdFactory factory;

    /**
     * Cache of recently stored string and template records, used to
     * avoid storing duplicates of frequently occurring data.
     * Should only be accessed from synchronized blocks to prevent corruption.
     */
    private final Map<Object, RecordId> records =
        new LinkedHashMap<Object, RecordId>(15000, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Entry<Object, RecordId> e) {
                return size() > 10000;
            }
        };

    private UUID uuid;

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
    private byte[] buffer = new byte[MAX_SEGMENT_SIZE];

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

    public SegmentWriter(SegmentStore store, SegmentIdFactory factory) {
        this.store = store;
        this.factory = factory;
        this.uuid = factory.newDataSegmentId();
        this.dummySegment = new Segment(
                store, factory,
                factory.newBulkSegmentId(), ByteBuffer.allocate(0));
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
                ByteBuffer b = ByteBuffer.wrap(buffer);
                b.position(buffer.length - length);
                currentSegment = new Segment(
                        store, uuid,
                        refids.keySet().toArray(new UUID[refids.size()]),
                        b);
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

            uuid = factory.newDataSegmentId();
            buffer = new byte[MAX_SEGMENT_SIZE];
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
        int rootCount = roots.size() + 1;

        int recordSize = Segment.align(size + ids.size() * Segment.RECORD_ID_BYTES);
        int headerSize = Segment.align(3 + rootCount * 3);
        int segmentSize = headerSize + refCount * 16 + recordSize + length;
        if (segmentSize > buffer.length - 1
                || rootCount > 0xffff
                || refCount >= Segment.SEGMENT_REFERENCE_LIMIT) {
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
                writeRecordId(entry.getValue());
            }
            return new MapRecord(dummySegment, id);
        }
    }

    private MapRecord writeMapBranch(int level, int size, MapRecord[] buckets) {
        int bitmap = 0;
        List<RecordId> ids = Lists.newArrayListWithCapacity(buckets.length);
        for (int i = 0; i < buckets.length; i++) {
            if (buckets[i] != null) {
                bitmap |= 1L << i;
                ids.add(buckets[i].getRecordId());
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
        checkArgument(bucket.size() > 1);
        RecordId bucketId = prepare(RecordType.BUCKET, 0, bucket);
        for (RecordId id : bucket) {
            writeRecordId(id);
        }
        return bucketId;
    }

    private synchronized MapRecord writeMapBucket(
            MapRecord base, Collection<MapEntry> entries, int level) {
        // when no changed entries, return the base map (if any) as-is
        if (entries == null || entries.isEmpty()) {
            if (base != null) {
                return base;
            } else if (level == 0) {
                synchronized (this) {
                    RecordId id = prepare(RecordType.LEAF, 4);
                    writeInt(0);
                    return new MapRecord(dummySegment, id);
                }
            } else {
                return null;
            }
        }

        // when no base map was given, write a fresh new map
        if (base == null) {
            // use leaf records for small maps or the last map level
            if (entries.size() <= BUCKETS_PER_LEVEL
                    || level == MapRecord.MAX_NUMBER_OF_LEVELS) {
                return writeMapLeaf(level, entries);
            }

            // write a large map by dividing the entries into buckets
            MapRecord[] buckets = new MapRecord[BUCKETS_PER_LEVEL];
            List<List<MapEntry>> changes = splitToBuckets(entries, level);
            for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
                buckets[i] = writeMapBucket(null, changes.get(i), level + 1);
            }

            // combine the buckets into one big map
            return writeMapBranch(level, entries.size(), buckets);
        }

        // if the base map is small, update in memory and write as a new map
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
            return writeMapBucket(null, map.values(), level);
        }

        // finally, the if the base map is large, handle updates per bucket
        int newSize = 0;
        int newCount = 0;
        MapRecord[] buckets = base.getBuckets();
        List<List<MapEntry>> changes = splitToBuckets(entries, level);
        for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
            buckets[i] = writeMapBucket(buckets[i], changes.get(i), level + 1);
            if (buckets[i] != null) {
                newSize += buckets[i].size();
                newCount++;
            }
        }

        // OAK-654: what if the updated map is smaller?
        if (newSize > BUCKETS_PER_LEVEL) {
            return writeMapBranch(level, newSize, buckets);
        } else if (newCount <= 1) {
            // up to one bucket contains entries, so return that as the new map
            for (int i = 0; i < buckets.length; i++) {
                if (buckets[i] != null) {
                    return buckets[i];
                }
            }
            // no buckets remaining, return empty map
            return writeMapBucket(null, null, level);
        } else {
            // combine all remaining entries into a leaf record
            List<MapEntry> list = Lists.newArrayList();
            for (int i = 0; i < buckets.length; i++) {
                if (buckets[i] != null) {
                    addAll(list, buckets[i].getEntries());
                }
            }
            return writeMapLeaf(level, list);
        }
    }

    private static List<List<MapEntry>> splitToBuckets(
            Collection<MapEntry> entries, int level) {
        List<MapEntry> empty = null;
        int mask = (1 << MapRecord.BITS_PER_LEVEL) - 1;
        int shift = 32 - (level + 1) * MapRecord.BITS_PER_LEVEL;

        List<List<MapEntry>> buckets =
                newArrayList(nCopies(MapRecord.BUCKETS_PER_LEVEL, empty));
        for (MapEntry entry : entries) {
            int index = (entry.getHash() >> shift) & mask;
            List<MapEntry> bucket = buckets.get(index);
            if (bucket == null) {
                bucket = newArrayList();
                buckets.set(index, bucket);
            }
            bucket.add(entry);
        }
        return buckets;
    }

    private synchronized RecordId writeValueRecord(
            long length, RecordId blocks) {
        RecordId valueId = prepare(
                RecordType.VALUE, 8, Collections.singleton(blocks));
        writeLong((length - Segment.MEDIUM_LIMIT) | (0x3L << 62));
        writeRecordId(blocks);
        return valueId;
    }

    private synchronized RecordId writeValueRecord(int length, byte[] data) {
        checkArgument(length < Segment.MEDIUM_LIMIT);
        RecordId id;
        if (length < Segment.SMALL_LIMIT) {
            id = prepare(RecordType.VALUE, 1 + length);
            buffer[position++] = (byte) length;
        } else {
            id = prepare(RecordType.VALUE, 2 + length);
            int len = (length - Segment.SMALL_LIMIT) | 0x8000;
            buffer[position++] = (byte) (len >> 8);
            buffer[position++] = (byte) len;
        }
        System.arraycopy(data, 0, buffer, position, length);
        position += length;
        return id;
    }

    /**
     * Write a reference to an external blob.
     *
     * @param reference reference
     * @param blobLength blob length
     * @return record id
     */
    private synchronized RecordId writeValueRecord(String reference, long blobLength) {
        byte[] data = reference.getBytes(Charsets.UTF_8);
        int length = data.length;

        checkArgument(length < 8192);

        RecordId id = prepare(RecordType.VALUE, 2 + 8 + length);
        int len = length | 0xE000;
        buffer[position++] = (byte) (len >> 8);
        buffer[position++] = (byte) len;

        writeLong(blobLength);

        System.arraycopy(data, 0, buffer, position, length);
        position += length;
        return id;
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
                if (bucket.size() > 1) {
                    nextLevel.add(writeListBucket(bucket));
                } else {
                    nextLevel.add(bucket.get(0));
                }
            }
            thisLevel = nextLevel;
        }
        return thisLevel.iterator().next();
    }

    public MapRecord writeMap(MapRecord base, Map<String, RecordId> changes) {
        if (base != null && base.isDiff()) {
            Segment segment = base.getSegment();
            RecordId key = segment.readRecordId(base.getOffset(8));
            String name = segment.readString(key);
            if (!changes.containsKey(name)) {
                changes.put(name, segment.readRecordId(base.getOffset(8, 1)));
            }
            base = new MapRecord(
                    segment, segment.readRecordId(base.getOffset(8, 2)));
        }

        if (base != null && changes.size() == 1) {
            Map.Entry<String, RecordId> change =
                    changes.entrySet().iterator().next();
            RecordId value = change.getValue();
            if (value != null) {
                MapEntry entry = base.getEntry(change.getKey());
                if (entry != null) {
                    if (value.equals(entry.getValue())) {
                        return base;
                    } else {
                        synchronized (this) {
                            RecordId id = prepare(RecordType.BRANCH, 8, asList(
                                    entry.getKey(), value, base.getRecordId()));
                            writeInt(-1);
                            writeInt(entry.getHash());
                            writeRecordId(entry.getKey());
                            writeRecordId(value);
                            writeRecordId(base.getRecordId());
                            return new MapRecord(dummySegment, id);
                        }
                    }
                }
            }
        }

        List<MapEntry> entries = Lists.newArrayList();
        for (Map.Entry<String, RecordId> entry : changes.entrySet()) {
            String key = entry.getKey();

            RecordId keyId = null;
            if (base != null) {
                MapEntry e = base.getEntry(key);
                if (e != null) {
                    keyId = e.getKey();
                }
            }
            if (keyId == null) {
                keyId = writeString(key);
            }

            entries.add(new MapEntry(
                    dummySegment, key, keyId, entry.getValue()));
        }

        return writeMapBucket(base, entries, 0);
    }

    /**
     * Writes a string value record.
     *
     * @param string string to be written
     * @return value record identifier
     */
    public RecordId writeString(String string) {
        synchronized (this) {
            RecordId id = records.get(string);
            if (id != null) {
                return id; // shortcut if the same string was recently stored
            }
        }

        byte[] data = string.getBytes(Charsets.UTF_8);

        if (data.length < Segment.MEDIUM_LIMIT) {
            // only cache short strings to avoid excessive memory use
            synchronized (this) {
                RecordId id = records.get(string);
                if (id == null) {
                    id = writeValueRecord(data.length, data);
                    records.put(string, id);
                }
                return id;
            }
        }

        int pos = 0;
        List<RecordId> blockIds = newArrayListWithExpectedSize(
                data.length / BLOCK_SIZE + 1);

        // write as many full bulk segments as possible
        while (pos + MAX_SEGMENT_SIZE <= data.length) {
            UUID uuid = factory.newBulkSegmentId();
            store.writeSegment(uuid, data, pos, MAX_SEGMENT_SIZE);
            for (int i = 0; i < MAX_SEGMENT_SIZE; i += BLOCK_SIZE) {
                blockIds.add(new RecordId(uuid, i));
            }
            pos += MAX_SEGMENT_SIZE;
        }

        // inline the remaining data as block records
        while (pos < data.length) {
            int len = Math.min(BLOCK_SIZE, data.length - pos);
            blockIds.add(writeBlock(data, pos, len));
            pos += len;
        }

        return writeValueRecord(data.length, writeList(blockIds));
    }

    public SegmentBlob writeBlob(Blob blob) throws IOException {
        if (blob instanceof SegmentBlob
                && ((SegmentBlob) blob).getStore() == store) {
            return (SegmentBlob) blob;
        }

        String reference = blob.getReference();
        if (reference != null) {
            RecordId id = writeValueRecord(reference, blob.length());
            return new SegmentBlob(dummySegment, id);
        }

        return writeStream(blob.getNewStream());
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
        boolean threw = true;
        try {
            RecordId id = SegmentStream.getRecordIdIfAvailable(stream, store);
            if (id == null) {
                id = internalWriteStream(stream);
            }
            threw = false;
            return new SegmentBlob(dummySegment, id);
        } finally {
            Closeables.close(stream, threw);
        }
    }

    private RecordId internalWriteStream(InputStream stream)
            throws IOException {
        byte[] data = new byte[MAX_SEGMENT_SIZE];
        int n = ByteStreams.read(stream, data, 0, data.length);

        // Special case for short binaries (up to about 16kB):
        // store them directly as small- or medium-sized value records
        if (n < Segment.MEDIUM_LIMIT) {
            return writeValueRecord(n, data);
        }

        long length = n;
        List<RecordId> blockIds =
                newArrayListWithExpectedSize(2 * n / BLOCK_SIZE);

        // Write the data to bulk segments and collect the list of block ids
        while (n != 0) {
            UUID id = factory.newBulkSegmentId();
            int len = align(n);
            store.writeSegment(id, data, 0, len);

            for (int i = 0; i < n; i += BLOCK_SIZE) {
                blockIds.add(new RecordId(id, data.length - len + i));
            }

            n = ByteStreams.read(stream, data, 0, data.length);
            length += n;
        }

        return writeValueRecord(length, writeList(blockIds));
    }

    private RecordId writeProperty(PropertyState state) {
        Map<String, RecordId> previousValues = emptyMap();
        return writeProperty(state, previousValues);
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

    public synchronized RecordId writeTemplate(Template template) {
        checkNotNull(template);

        RecordId id = records.get(template);
        if (id != null) {
            return id; // shortcut if the same template was recently stored
        }

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

        id = prepare(RecordType.TEMPLATE, 4 + propertyTypes.length, ids);
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

        records.put(template, id);

        return id;
    }

    public SegmentNodeState writeNode(NodeState state) {
        if (state instanceof SegmentNodeState
                && ((SegmentNodeState) state).getStore() == store) {
            return (SegmentNodeState) state;
        }

        SegmentNodeState before = null;
        Template beforeTemplate = null;
        ModifiedNodeState after = null;
        if (state instanceof ModifiedNodeState) {
            after = (ModifiedNodeState) state;
            NodeState base = after.getBaseState();
            if (base instanceof SegmentNodeState
                    && ((SegmentNodeState) base).getStore() == store) {
                before = (SegmentNodeState) base;
                beforeTemplate = before.getTemplate();
            }
        }

        Template template = new Template(state);
        RecordId templateId;
        if (before != null && template.equals(beforeTemplate)) {
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

            if (property instanceof SegmentPropertyState
                    && ((SegmentPropertyState) property).getStore() == store) {
                ids.add(((SegmentPropertyState) property).getRecordId());
            } else if (!(before instanceof SegmentNodeState)
                    || ((SegmentNodeState) before).getStore() != store) {
                ids.add(writeProperty(property));
            } else {
                // reuse previously stored property, if possible
                PropertyTemplate bt = beforeTemplate.getPropertyTemplate(name);
                if (bt == null) {
                    ids.add(writeProperty(property)); // new property
                } else {
                    SegmentPropertyState bp = beforeTemplate.getProperty(
                            before.getSegment(), before.getRecordId(),
                            bt.getIndex());
                    if (property.equals(bp)) {
                        ids.add(bp.getRecordId()); // no changes
                    } else if (bp.isArray() && bp.getType() != BINARIES) {
                        // reuse entries from the previous list
                        ids.add(writeProperty(property, bp.getValueRecords()));
                    } else {
                        ids.add(writeProperty(property));
                    }
                }
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
