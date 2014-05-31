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
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithExpectedSize;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.nCopies;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.segment.MapRecord.BUCKETS_PER_LEVEL;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ID_BYTES;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentWriter {

    /** Logger instance */
    private static final Logger log =
            LoggerFactory.getLogger(SegmentWriter.class);

    static final int BLOCK_SIZE = 1 << 12; // 4kB

    private static byte[] createNewBuffer() {
        byte[] buffer = new byte[Segment.MAX_SEGMENT_SIZE];
        buffer[0] = '0';
        buffer[1] = 'a';
        buffer[2] = 'K';
        buffer[3] = '\n';
        buffer[4] = 0;
        buffer[5] = 0; // refcount
        return buffer;
    }

    private static int align(int value) {
        return align(value, 1 << Segment.RECORD_ALIGN_BITS);
    }

    private static int align(int value, int boundary) {
        return (value + boundary - 1) & ~(boundary - 1);
    }


    private final SegmentTracker tracker;

    private final SegmentStore store;

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

    /**
     * The set of root records (i.e. ones not referenced by other records)
     * in this segment.
     */
    private final Map<RecordId, RecordType> roots = newLinkedHashMap();

    /**
     * Identifiers of the external blob references stored in this segment.
     */
    private final List<RecordId> blobrefs = newArrayList();

    /**
     * The segment write buffer, filled from the end to the beginning
     * (see OAK-629).
     */
    private byte[] buffer = createNewBuffer();

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

    private Segment segment;

    public SegmentWriter(SegmentStore store, SegmentTracker tracker) {
        this.store = store;
        this.tracker = tracker;
        this.segment = new Segment(tracker, buffer);
        segment.getSegmentId().setSegment(segment);
    }

    public synchronized Segment getCurrentSegment(SegmentId id) {
        if (id == segment.getSegmentId()) {
            return segment;
        } else {
            return null;
        }
    }

    public synchronized void flush() {
        if (length > 0) {
            int refcount = segment.getRefCount();

            int rootcount = roots.size();
            buffer[Segment.ROOT_COUNT_OFFSET] = (byte) (rootcount >> 8);
            buffer[Segment.ROOT_COUNT_OFFSET + 1] = (byte) rootcount;

            int blobrefcount = blobrefs.size();
            buffer[Segment.BLOBREF_COUNT_OFFSET] = (byte) (blobrefcount >> 8);
            buffer[Segment.BLOBREF_COUNT_OFFSET + 1] = (byte) blobrefcount;

            length = align(
                    refcount * 16 + rootcount * 3 + blobrefcount * 2 + length,
                    16);

            int pos = refcount * 16;
            if (pos + length <= buffer.length) {
                // the whole segment fits to the space *after* the referenced
                // segment identifiers we've already written, so we can safely
                // copy those bits ahead even if concurrent code is still
                // reading from that part of the buffer
                System.arraycopy(buffer, 0, buffer, buffer.length-length, pos);
                pos += buffer.length - length;
            } else {
                // this might leave some empty space between the header and
                // the record data, but this case only occurs when the
                // segment is >252kB in size and the maximum overhead is <<4kB,
                // which is acceptable
                length = buffer.length;
            }

            for (Map.Entry<RecordId, RecordType> entry : roots.entrySet()) {
                int offset = entry.getKey().getOffset();
                buffer[pos++] = (byte) entry.getValue().ordinal();
                buffer[pos++] = (byte) (offset >> (8 + Segment.RECORD_ALIGN_BITS));
                buffer[pos++] = (byte) (offset >> Segment.RECORD_ALIGN_BITS);
            }

            for (RecordId blobref : blobrefs) {
                int offset = blobref.getOffset();
                buffer[pos++] = (byte) (offset >> (8 + Segment.RECORD_ALIGN_BITS));
                buffer[pos++] = (byte) (offset >> Segment.RECORD_ALIGN_BITS);
            }

            SegmentId id = segment.getSegmentId();
            log.debug("Writing data segment {} ({} bytes)", id, length);
            store.writeSegment(id, buffer, buffer.length - length, length);

            // Keep this segment in memory as it's likely to be accessed soon
            ByteBuffer data;
            if (buffer.length - length > 4096) {
                data = ByteBuffer.allocate(length);
                data.put(buffer, buffer.length - length, length);
                data.rewind();
            } else {
                data = ByteBuffer.wrap(buffer, buffer.length - length, length);
            }
            tracker.setSegment(id, new Segment(tracker, id, data));

            buffer = createNewBuffer();
            roots.clear();
            blobrefs.clear();
            length = 0;
            position = buffer.length;
            segment = new Segment(tracker, buffer);
            segment.getSegmentId().setSegment(segment);
        }
    }

    private RecordId prepare(RecordType type, int size) {
        return prepare(type, size, Collections.<RecordId>emptyList());
    }

    private RecordId prepare(
            RecordType type, int size, Collection<RecordId> ids) {
        checkArgument(size >= 0);
        checkNotNull(ids);

        int idcount = ids.size();
        int recordSize = align(size + idcount * RECORD_ID_BYTES);

        // First compute the header and segment sizes based on the assumption
        // that *all* identifiers stored in this record point to previously
        // unreferenced segments.
        int refcount = segment.getRefCount() + idcount;
        int blobrefcount = blobrefs.size() + 1;
        int rootcount = roots.size() + 1;
        int headerSize = refcount * 16 + rootcount * 3 + blobrefcount * 2;
        int segmentSize = align(headerSize + recordSize + length, 16);

        // If the size estimate looks too big, recompute it with a more
        // accurate refcount value. We skip doing this when possible to
        // avoid the somewhat expensive list and set traversals.
        if (segmentSize > buffer.length - 1
                || refcount > Segment.SEGMENT_REFERENCE_LIMIT) {
            refcount -= idcount;

            Set<SegmentId> segmentIds = newIdentityHashSet();
            for (RecordId recordId : ids) {
                SegmentId segmentId = recordId.getSegmentId();
                if (segmentId != segment.getSegmentId()) {
                    segmentIds.add(segmentId);
                } else if (roots.containsKey(recordId)) {
                    rootcount--;
                }
            }
            if (!segmentIds.isEmpty()) {
                for (int refid = 1; refid < refcount; refid++) {
                    segmentIds.remove(segment.getRefId(refid));
                }
                refcount += segmentIds.size();
            }

            headerSize = refcount * 16 + rootcount * 3 + blobrefcount * 2;
            segmentSize = align(headerSize + recordSize + length, 16);
        }

        if (segmentSize > buffer.length - 1
                || blobrefcount > 0xffff
                || rootcount > 0xffff
                || refcount > Segment.SEGMENT_REFERENCE_LIMIT) {
            flush();
        }

        length += recordSize;
        position = buffer.length - length;
        checkState(position >= 0);

        RecordId id = new RecordId(segment.getSegmentId(), position);
        roots.put(id, type);
        return id;
    }

    private synchronized int getSegmentRef(SegmentId segmentId) {
        int refcount = segment.getRefCount();
        for (int index = 0; index < refcount; index++) {
            if (segmentId == segment.getRefId(index)) {
                return index;
            }
        }

        ByteBuffer.wrap(buffer, refcount * 16, 16)
            .putLong(segmentId.getMostSignificantBits())
            .putLong(segmentId.getLeastSignificantBits());
        buffer[Segment.REF_COUNT_OFFSET] = (byte) refcount;
        return refcount;
    }

    private synchronized void writeRecordId(RecordId recordId) {
        checkNotNull(recordId);
        roots.remove(recordId);

        int offset = recordId.getOffset();
        checkState(0 <= offset && offset < MAX_SEGMENT_SIZE);
        checkState(offset == align(offset));

        buffer[position++] = (byte) getSegmentRef(recordId.getSegmentId());
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
            return new MapRecord(id);
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
            return new MapRecord(mapId);
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
                    return new MapRecord(id);
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
     * @return record id
     */
    private synchronized RecordId writeValueRecord(String reference) {
        byte[] data = reference.getBytes(Charsets.UTF_8);
        int length = data.length;

        checkArgument(length < 8192);

        RecordId id = prepare(RecordType.VALUE, 2 + length);
        int len = length | 0xE000;
        buffer[position++] = (byte) (len >> 8);
        buffer[position++] = (byte) len;

        System.arraycopy(data, 0, buffer, position, length);
        position += length;

        blobrefs.add(id);
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
            base = new MapRecord(segment.readRecordId(base.getOffset(8, 2)));
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
                            return new MapRecord(id);
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

            entries.add(new MapEntry(key, keyId, entry.getValue()));
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
            SegmentId bulkId = store.getTracker().newBulkSegmentId();
            store.writeSegment(bulkId, data, pos, MAX_SEGMENT_SIZE);
            for (int i = 0; i < MAX_SEGMENT_SIZE; i += BLOCK_SIZE) {
                blockIds.add(new RecordId(bulkId, i));
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
                && store.containsSegment(((SegmentBlob) blob).getRecordId().getSegmentId())) {
            return (SegmentBlob) blob;
        }

        String reference = blob.getReference();
        if (reference != null && store.getBlobStore() != null) {
            String blobId = store.getBlobStore().getBlobId(reference);
            if (blobId != null) {
                RecordId id = writeValueRecord(blobId);
                return new SegmentBlob(id);
            } else {
                log.debug("No blob found for reference {}, inlining...", reference);
            }
        }

        return writeStream(blob.getNewStream());
    }

    SegmentBlob writeExternalBlob(String blobId) throws IOException {
        RecordId id = writeValueRecord(blobId);
        return new SegmentBlob(id);
    }

    SegmentBlob writeLargeBlob(long length, List<RecordId> list) {
        RecordId id = writeValueRecord(length, writeList(list));
        return new SegmentBlob(id);
    }

    public synchronized void dropCache(){
        records.clear();
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
            return new SegmentBlob(id);
        } finally {
            Closeables.close(stream, threw);
        }
    }

    private RecordId internalWriteStream(InputStream stream)
            throws IOException {
        BlobStore blobStore = store.getBlobStore();
        byte[] data = new byte[MAX_SEGMENT_SIZE];
        int n = ByteStreams.read(stream, data, 0, data.length);

        // Special case for short binaries (up to about 16kB):
        // store them directly as small- or medium-sized value records
        if (n < Segment.MEDIUM_LIMIT) {
            return writeValueRecord(n, data);
        } else if (blobStore != null) {
            String blobId = blobStore.writeBlob(new SequenceInputStream(
                    new ByteArrayInputStream(data, 0, n), stream));
            return writeValueRecord(blobId);
        }

        long length = n;
        List<RecordId> blockIds =
                newArrayListWithExpectedSize(2 * n / BLOCK_SIZE);

        // Write the data to bulk segments and collect the list of block ids
        while (n != 0) {
            SegmentId bulkId = store.getTracker().newBulkSegmentId();
            int len = align(n);
            log.debug("Writing bulk segment {} ({} bytes)", bulkId, n);
            store.writeSegment(bulkId, data, 0, len);

            for (int i = 0; i < n; i += BLOCK_SIZE) {
                blockIds.add(new RecordId(bulkId, data.length - len + i));
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
                && store.containsSegment(((SegmentNodeState) state).getRecordId().getSegmentId())) {
            return (SegmentNodeState) state;
        }

        SegmentNodeState before = null;
        Template beforeTemplate = null;
        ModifiedNodeState after = null;
        if (state instanceof ModifiedNodeState) {
            after = (ModifiedNodeState) state;
            NodeState base = after.getBaseState();
            if (base instanceof SegmentNodeState
                    && store.containsSegment(((SegmentNodeState) base).getRecordId().getSegmentId())) {
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
                    && store.containsSegment(((SegmentPropertyState) property).getRecordId().getSegmentId())) {
                ids.add(((SegmentPropertyState) property).getRecordId());
            } else if (!(before instanceof SegmentNodeState)
                    || store.containsSegment(((SegmentNodeState) before).getRecordId().getSegmentId())) {
                ids.add(writeProperty(property));
            } else {
                // reuse previously stored property, if possible
                PropertyTemplate bt = beforeTemplate.getPropertyTemplate(name);
                if (bt == null) {
                    ids.add(writeProperty(property)); // new property
                } else {
                    SegmentPropertyState bp = beforeTemplate.getProperty(
                            before.getRecordId(), bt.getIndex());
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
            return new SegmentNodeState(recordId);
        }
    }

}
