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
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.nCopies;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.segment.MapRecord.BUCKETS_PER_LEVEL;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.BLOCK;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.BRANCH;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.BUCKET;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.LEAF;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.LIST;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.NODE;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.TEMPLATE;
import static org.apache.jackrabbit.oak.plugins.segment.RecordType.VALUE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.readString;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.V_11;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jcr.PropertyType;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts nodes, properties, and values to records, which are written to a
 * byte array, in order to create segments.
 * <p>
 * The same writer is used to create multiple segments (data is automatically
 * split: new segments are automatically created if and when needed).
 */
public class SegmentWriter {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentWriter.class);

    static final int BLOCK_SIZE = 1 << 12; // 4kB

    private final SegmentBuilderPool segmentBuilderPool = new SegmentBuilderPool();

    /**
     * Cache of recently stored string and template records, used to
     * avoid storing duplicates of frequently occurring data.
     */
    private final Map<Object, RecordId> records =
        new LinkedHashMap<Object, RecordId>(15000, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Object, RecordId> e) {
                return size() > 10000;
            }
            @Override
            public synchronized RecordId get(Object key) {
                return super.get(key);
            }
            @Override
            public synchronized RecordId put(Object key, RecordId value) {
                return super.put(key, value);
            }
            @Override
            public synchronized void clear() {
                super.clear();
            }
        };

    private final SegmentStore store;

    private final SegmentTracker tracker;

    /**
     * Version of the segment storage format.
     */
    private final SegmentVersion version;

    private final String wid;

    public SegmentWriter(SegmentStore store, SegmentTracker tracker, SegmentVersion version) {
        this(store, tracker, version, null);
    }

    /**
     * @param store     store to write to
     * @param tracker   segment tracker for that {@code store}
     * @param version   segment version to write
     * @param wid       id of this writer
     */
    public SegmentWriter(SegmentStore store, SegmentTracker tracker, SegmentVersion version, String wid) {
        this.store = store;
        this.tracker = tracker;
        this.version = version;
        this.wid = wid;
    }

    SegmentTracker getTracker() {
        return tracker;
    }

    public void flush() {
        segmentBuilderPool.flush();
    }

    public void dropCache() {
        records.clear();
    }

    MapRecord writeMap(MapRecord base, Map<String, RecordId> changes) {
        if (base != null && base.isDiff()) {
            Segment segment = base.getSegment();
            RecordId key = segment.readRecordId(base.getOffset(8));
            String name = readString(key);
            if (!changes.containsKey(name)) {
                changes.put(name, segment.readRecordId(base.getOffset(8, 1)));
            }
            base = new MapRecord(segment.readRecordId(base.getOffset(8, 2)));
        }

        if (base != null && changes.size() == 1) {
            Map.Entry<String, RecordId> change =
                changes.entrySet().iterator().next();
            final RecordId value = change.getValue();
            if (value != null) {
                final MapEntry entry = base.getEntry(change.getKey());
                if (entry != null) {
                    if (value.equals(entry.getValue())) {
                        return base;
                    } else {
                        final MapRecord baseMap = base;
                        RecordId mapId = writeRecord(new RecordWriter(BRANCH, 8, asList(
                            entry.getKey(), value, baseMap.getRecordId())) {
                            @Override
                            protected void write(RecordId id, SegmentBuilder builder) {
                                builder.writeInt(-1);
                                builder.writeInt(entry.getHash());
                                builder.writeRecordId(entry.getKey());
                                builder.writeRecordId(value);
                                builder.writeRecordId(baseMap.getRecordId());
                            }
                        });
                        return new MapRecord(mapId);
                    }
                }
            }
        }

        List<MapEntry> entries = newArrayList();
        for (Map.Entry<String, RecordId> entry : changes.entrySet()) {
            String key = entry.getKey();

            RecordId keyId = null;
            if (base != null) {
                MapEntry e = base.getEntry(key);
                if (e != null) {
                    keyId = e.getKey();
                }
            }
            if (keyId == null && entry.getValue() != null) {
                keyId = writeString(key);
            }

            if (keyId != null) {
                entries.add(new MapEntry(key, keyId, entry.getValue()));
            }
        }

        return writeMapBucket(base, entries, 0);
    }

    private MapRecord writeMapLeaf(final int level, Collection<MapEntry> entries) {
        checkNotNull(entries);

        final int size = entries.size();
        checkElementIndex(size, MapRecord.MAX_SIZE);
        checkPositionIndex(level, MapRecord.MAX_NUMBER_OF_LEVELS);
        checkArgument(size != 0 || level == MapRecord.MAX_NUMBER_OF_LEVELS);

        List<RecordId> ids = Lists.newArrayListWithCapacity(2 * size);
        for (MapEntry entry : entries) {
            ids.add(entry.getKey());
            ids.add(entry.getValue());
        }

        // copy the entries to an array so we can sort them before writing
        final MapEntry[] array = entries.toArray(new MapEntry[entries.size()]);
        Arrays.sort(array);

        RecordId mapId = writeRecord(new RecordWriter(LEAF, 4 + size * 4, ids) {
            @Override
            protected void write(RecordId id, SegmentBuilder builder) {
                builder.writeInt((level << MapRecord.SIZE_BITS) | size);
                for (MapEntry entry : array) {
                    builder.writeInt(entry.getHash());
                }
                for (MapEntry entry : array) {
                    builder.writeRecordId(entry.getKey());
                    builder.writeRecordId(entry.getValue());
                }
            }
        });
        return new MapRecord(mapId);
    }

    private MapRecord writeMapBranch(final int level, final int size, MapRecord[] buckets) {
        int bitmap = 0;
        final List<RecordId> bucketIds = Lists.newArrayListWithCapacity(buckets.length);
        for (int i = 0; i < buckets.length; i++) {
            if (buckets[i] != null) {
                bitmap |= 1L << i;
                bucketIds.add(buckets[i].getRecordId());
            }
        }

        final int bits = bitmap;
        RecordId mapId = writeRecord(new RecordWriter(BRANCH, 8, bucketIds) {
            @Override
            protected void write(RecordId id, SegmentBuilder builder) {
                builder.writeInt((level << MapRecord.SIZE_BITS) | size);
                builder.writeInt(bits);
                for (RecordId buckedId : bucketIds) {
                    builder.writeRecordId(buckedId);
                }
            }
        });
        return new MapRecord(mapId);
    }

    private MapRecord writeMapBucket(MapRecord base, Collection<MapEntry> entries, int level) {
        // when no changed entries, return the base map (if any) as-is
        if (entries == null || entries.isEmpty()) {
            if (base != null) {
                return base;
            } else if (level == 0) {
                RecordId mapId = writeRecord(new RecordWriter(LEAF, 4) {
                    @Override
                    protected void write(RecordId id, SegmentBuilder builder) {
                        builder.writeInt(0);
                    }
                });
                return new MapRecord(mapId);
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
            for (MapRecord bucket : buckets) {
                if (bucket != null) {
                    return bucket;
                }
            }
            // no buckets remaining, return empty map
            return writeMapBucket(null, null, level);
        } else {
            // combine all remaining entries into a leaf record
            List<MapEntry> list = newArrayList();
            for (MapRecord bucket : buckets) {
                if (bucket != null) {
                    addAll(list, bucket.getEntries());
                }
            }
            return writeMapLeaf(level, list);
        }
    }

    /**
     * Writes a list record containing the given list of record identifiers.
     *
     * @param list list of record identifiers
     * @return list record identifier
     */
    public RecordId writeList(List<RecordId> list) {
        checkNotNull(list);
        checkArgument(!list.isEmpty());
        List<RecordId> thisLevel = list;
        while (thisLevel.size() > 1) {
            List<RecordId> nextLevel = newArrayList();
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

    private RecordId writeListBucket(final List<RecordId> bucket) {
        checkArgument(bucket.size() > 1);
        return writeRecord(new RecordWriter(BUCKET, 0, bucket) {
            @Override
            protected void write(RecordId id, SegmentBuilder builder) {
                for (RecordId bucketId : bucket) {
                    builder.writeRecordId(bucketId);
                }
            }
        });
    }

    private static List<List<MapEntry>> splitToBuckets(Collection<MapEntry> entries, int level) {
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

    private RecordId writeValueRecord(final long length, final RecordId blocks) {
        return writeRecord(new RecordWriter(VALUE, 8, blocks) {
            @Override
            protected void write(RecordId id, SegmentBuilder builder) {
                builder.writeLong((length - Segment.MEDIUM_LIMIT) | (0x3L << 62));
                builder.writeRecordId(blocks);
            }
        });
    }

    private RecordId writeValueRecord(final int length, final byte[] data) {
        checkArgument(length < Segment.MEDIUM_LIMIT);
        RecordId id;
        if (length < Segment.SMALL_LIMIT) {
            return writeRecord(new RecordWriter(VALUE, 1 + length) {
                @Override
                protected void write(RecordId id, SegmentBuilder builder) {
                    builder.writeByte((byte) length);
                    builder.writeBytes(data, 0, length);
                }
            });
        } else {
            return writeRecord(new RecordWriter(VALUE, 2 + length) {
                @Override
                protected void write(RecordId id, SegmentBuilder builder) {
                    builder.writeShort((short) ((length - Segment.SMALL_LIMIT) | 0x8000));
                    builder.writeBytes(data, 0, length);
                }
            });
        }
    }

    /**
     * Writes a string value record.
     *
     * @param string string to be written
     * @return value record identifier
     */
    public RecordId writeString(String string) {
        RecordId id = records.get(string);
        if (id != null) {
            return id; // shortcut if the same string was recently stored
        }

        byte[] data = string.getBytes(Charsets.UTF_8);

        if (data.length < Segment.MEDIUM_LIMIT) {
            // only cache short strings to avoid excessive memory use
            id = writeValueRecord(data.length, data);
            records.put(string, id);
            return id;
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
                RecordId id = writeBlobId(blobId);
                return new SegmentBlob(id);
            } else {
                LOG.debug("No blob found for reference {}, inlining...", reference);
            }
        }

        return writeStream(blob.getNewStream());
    }

    /**
     * Write a reference to an external blob. This method handles blob IDs of
     * every length, but behaves differently for small and large blob IDs.
     *
     * @param blobId Blob ID.
     * @return Record ID pointing to the written blob ID.
     * @see Segment#BLOB_ID_SMALL_LIMIT
     */
    private RecordId writeBlobId(String blobId) {
        byte[] data = blobId.getBytes(Charsets.UTF_8);
        if (data.length < Segment.BLOB_ID_SMALL_LIMIT) {
            return writeSmallBlobId(data);
        } else {
            return writeLargeBlobId(blobId);
        }
    }

    /**
     * Write a large blob ID. A blob ID is considered large if the length of its
     * binary representation is equal to or greater than {@code
     * Segment.BLOB_ID_SMALL_LIMIT}.
     *
     * @param blobId Blob ID.
     * @return A record ID pointing to the written blob ID.
     */
    private RecordId writeLargeBlobId(String blobId) {
        final RecordId stringRecord = writeString(blobId);
        return writeRecord(new RecordWriter(VALUE, 1, stringRecord) {
            @Override
            protected void write(RecordId id, SegmentBuilder builder) {
                // The length uses a fake "length" field that is always equal to 0xF0.
                // This allows the code to take apart small from a large blob IDs.
                builder.writeByte((byte) 0xF0);
                builder.writeRecordId(stringRecord);
                builder.addBlobRef(id);
            }
        });
    }

    /**
     * Write a small blob ID. A blob ID is considered small if the length of its
     * binary representation is less than {@code Segment.BLOB_ID_SMALL_LIMIT}.
     *
     * @param blobId Blob ID.
     * @return A record ID pointing to the written blob ID.
     */
    private RecordId writeSmallBlobId(final byte[] blobId) {
        final int length = blobId.length;
        checkArgument(length < Segment.BLOB_ID_SMALL_LIMIT);
        return writeRecord(new RecordWriter(VALUE, 2 + length) {
            @Override
            protected void write(RecordId id, SegmentBuilder builder) {
                builder.writeShort((short) (length | 0xE000));
                builder.writeBytes(blobId, 0, length);
                builder.addBlobRef(id);
            }
        });
    }

    /**
     * Writes a block record containing the given block of bytes.
     *
     * @param bytes source buffer
     * @param offset offset within the source buffer
     * @param length number of bytes to write
     * @return block record identifier
     */
    RecordId writeBlock(final byte[] bytes, final int offset, final int length) {
        checkNotNull(bytes);
        checkPositionIndexes(offset, offset + length, bytes.length);
        return writeRecord(new RecordWriter(BLOCK, length) {
            @Override
            protected void write(RecordId id, SegmentBuilder builder) {
                builder.writeBytes(bytes, offset, length);
            }
        });
    }

    SegmentBlob writeExternalBlob(String blobId) {
        RecordId id = writeBlobId(blobId);
        return new SegmentBlob(id);
    }

    SegmentBlob writeLargeBlob(long length, List<RecordId> list) {
        RecordId id = writeValueRecord(length, writeList(list));
        return new SegmentBlob(id);
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
            return writeBlobId(blobId);
        }

        long length = n;
        List<RecordId> blockIds =
                newArrayListWithExpectedSize(2 * n / BLOCK_SIZE);

        // Write the data to bulk segments and collect the list of block ids
        while (n != 0) {
            SegmentId bulkId = store.getTracker().newBulkSegmentId();
            int len = align(n);
            LOG.debug("Writing bulk segment {} ({} bytes)", bulkId, n);
            store.writeSegment(bulkId, data, 0, len);

            for (int i = 0; i < n; i += BLOCK_SIZE) {
                blockIds.add(new RecordId(bulkId, data.length - len + i));
            }

            n = ByteStreams.read(stream, data, 0, data.length);
            length += n;
        }

        return writeValueRecord(length, writeList(blockIds));
    }

    public RecordId writeProperty(PropertyState state) {
        Map<String, RecordId> previousValues = emptyMap();
        return writeProperty(state, previousValues);
    }

    private RecordId writeProperty(PropertyState state, Map<String, RecordId> previousValues) {
        Type<?> type = state.getType();
        final int count = state.count();

        List<RecordId> valueIds = newArrayList();
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
            return writeRecord(new RecordWriter(LIST, 4) {
                @Override
                protected void write(RecordId id, SegmentBuilder builder) {
                    builder.writeInt(0);
                }
            });
        } else {
            final RecordId listId = writeList(valueIds);
            return writeRecord(new RecordWriter(LIST, 4, listId) {
                @Override
                public void write(RecordId id, SegmentBuilder builder) {
                    builder.writeInt(count);
                    builder.writeRecordId(listId);
                }
            });
        }
    }

    public RecordId writeTemplate(Template template) {
        checkNotNull(template);

        RecordId id = records.get(template);
        if (id != null) {
            return id; // shortcut if the same template was recently stored
        }

        Collection<RecordId> ids = newArrayList();
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
            mixinIds = newArrayList();
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
            // Note: if the property names are stored in more than 255 separate
            // segments, this will not work.
            propertyNames[i] = writeString(properties[i].getName());
            Type<?> type = properties[i].getType();
            if (type.isArray()) {
                propertyTypes[i] = (byte) -type.tag();
            } else {
                propertyTypes[i] = (byte) type.tag();
            }
        }

        RecordId propNamesId = null;
        if (version.onOrAfter(V_11)) {
            if (propertyNames.length > 0) {
                propNamesId = writeList(Arrays.asList(propertyNames));
                ids.add(propNamesId);
            }
        } else {
            ids.addAll(Arrays.asList(propertyNames));
        }

        checkState(propertyNames.length < (1 << 18));
        head |= propertyNames.length;
        return writeTemplate(template, ids, propertyNames, propertyTypes, head, primaryId,
                mixinIds, childNameId, propNamesId);
    }

    public RecordId writeTemplate(Template template, final Collection<RecordId> ids,
            final RecordId[] propertyNames, final byte[] propertyTypes, final int finalHead,
            final RecordId finalPrimaryId, final List<RecordId> finalMixinIds, final RecordId
            finalChildNameId, final RecordId finalPropNamesId) {
        RecordId id = writeRecord(new RecordWriter(TEMPLATE, 4 + propertyTypes.length, ids) {
            @Override
            protected void write(RecordId id, SegmentBuilder builder) {
                builder.writeInt(finalHead);
                if (finalPrimaryId != null) {
                    builder.writeRecordId(finalPrimaryId);
                }
                if (finalMixinIds != null) {
                    for (RecordId mixinId : finalMixinIds) {
                        builder.writeRecordId(mixinId);
                    }
                }
                if (finalChildNameId != null) {
                    builder.writeRecordId(finalChildNameId);
                }
                if (version.onOrAfter(V_11)) {
                    if (finalPropNamesId != null) {
                        builder.writeRecordId(finalPropNamesId);
                    }
                }
                for (int i = 0; i < propertyNames.length; i++) {
                    if (!version.onOrAfter(V_11)) {
                        // V10 only
                        builder.writeRecordId(propertyNames[i]);
                    }
                    builder.writeByte(propertyTypes[i]);
                }
            }
        });
        records.put(template, id);
        return id;
    }

    public SegmentNodeState writeNode(NodeState state) {
        if (state instanceof SegmentNodeState) {
            SegmentNodeState sns = uncompact((SegmentNodeState) state);
            if (sns != state || store.containsSegment(
                sns.getRecordId().getSegmentId())) {
                return sns;
            }
        }

        SegmentNodeState before = null;
        Template beforeTemplate = null;
        ModifiedNodeState after = null;
        if (state instanceof ModifiedNodeState) {
            after = (ModifiedNodeState) state;
            NodeState base = after.getBaseState();
            if (base instanceof SegmentNodeState) {
                SegmentNodeState sns = uncompact((SegmentNodeState) base);
                if (sns != base || store.containsSegment(
                    sns.getRecordId().getSegmentId())) {
                    before = sns;
                    beforeTemplate = before.getTemplate();
                }
            }
        }

        Template template = new Template(state);
        RecordId templateId;
        if (before != null && template.equals(beforeTemplate)) {
            templateId = before.getTemplateId();
        } else {
            templateId = writeTemplate(template);
        }

        final List<RecordId> ids = newArrayList();
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

        List<RecordId> pIds = newArrayList();
        for (PropertyTemplate pt : template.getPropertyTemplates()) {
            String name = pt.getName();
            PropertyState property = state.getProperty(name);

            if (property instanceof SegmentPropertyState
                && store.containsSegment(((SegmentPropertyState) property).getRecordId().getSegmentId())) {
                pIds.add(((SegmentPropertyState) property).getRecordId());
            } else if (before == null
                || !store.containsSegment(before.getRecordId().getSegmentId())) {
                pIds.add(writeProperty(property));
            } else {
                // reuse previously stored property, if possible
                PropertyTemplate bt = beforeTemplate.getPropertyTemplate(name);
                if (bt == null) {
                    pIds.add(writeProperty(property)); // new property
                } else {
                    SegmentPropertyState bp = beforeTemplate.getProperty(
                        before.getRecordId(), bt.getIndex());
                    if (property.equals(bp)) {
                        pIds.add(bp.getRecordId()); // no changes
                    } else if (bp.isArray() && bp.getType() != BINARIES) {
                        // reuse entries from the previous list
                        pIds.add(writeProperty(property, bp.getValueRecords()));
                    } else {
                        pIds.add(writeProperty(property));
                    }
                }
            }
        }

        if (!pIds.isEmpty()) {
            if (version.onOrAfter(V_11)) {
                ids.add(writeList(pIds));
            } else {
                ids.addAll(pIds);
            }
        }

        RecordId recordId = writeRecord(new RecordWriter(NODE, 0, ids) {
            @Override
            protected void write(RecordId id, SegmentBuilder builder) {
                for (RecordId recordId : ids) {
                    builder.writeRecordId(recordId);
                }
            }
        });
        return new SegmentNodeState(recordId);
    }

    /**
     * If the given node was compacted, return the compacted node, otherwise
     * return the passed node. This is to avoid pointing to old nodes, if they
     * have been compacted.
     *
     * @param state the node
     * @return the compacted node (if it was compacted)
     */
    private SegmentNodeState uncompact(SegmentNodeState state) {
        RecordId id = tracker.getCompactionMap().get(state.getRecordId());
        if (id != null) {
            return new SegmentNodeState(id);
        } else {
            return state;
        }
    }

    private RecordId writeRecord(RecordWriter recordWriter) {
        SegmentBuilder builder = segmentBuilderPool.borrowBuilder(currentThread());
        try {
            RecordId id = builder.prepare(recordWriter.type, recordWriter.size, recordWriter.ids);
            recordWriter.write(id, builder);
            return id;
        } finally {
            segmentBuilderPool.returnBuilder(currentThread(), builder);
        }
    }

    private abstract static class RecordWriter {
        private final RecordType type;
        private final int size;
        private final Collection<RecordId> ids;

        protected RecordWriter(RecordType type, int size, Collection<RecordId> ids) {
            this.type = type;
            this.size = size;
            this.ids = ids;
        }

        protected RecordWriter(RecordType type, int size, RecordId id) {
            this(type, size, singleton(id));
        }

        protected RecordWriter(RecordType type, int size) {
            this(type, size, Collections.<RecordId>emptyList());
        }

        protected abstract void write(RecordId id, SegmentBuilder builder);
    }

    private class SegmentBuilderPool {
        private final Set<SegmentBuilder> borrowed = newHashSet();
        private final Map<Object, SegmentBuilder> builders = newHashMap();

        public void flush() {
            ArrayList<SegmentBuilder> toFlush = Lists.newArrayList();
            synchronized (this) {
                toFlush.addAll(builders.values());
                builders.clear();
                borrowed.clear();
            }
            // Call flush from outside a synchronized context to avoid
            // deadlocks of that method calling SegmentStore.writeSegment
            for (SegmentBuilder builder : toFlush) {
                builder.flush();
            }
        }

        public synchronized SegmentBuilder borrowBuilder(Object key) {
            SegmentBuilder builder = builders.remove(key);
            if (builder == null) {
                builder = new SegmentBuilder(store, tracker, version, wid + "." + (key.hashCode() & 0xffff));
            }
            borrowed.add(builder);
            return builder;
        }

        public void returnBuilder(Object key, SegmentBuilder builder) {
            if (!tryReturn(key, builder)) {
                // Delayed flush this builder as it was borrowed while flush() was called.
                builder.flush();
            }
        }

        private synchronized boolean tryReturn(Object key, SegmentBuilder builder) {
            if (borrowed.remove(builder)) {
                builders.put(key, builder);
                return true;
            } else {
                return false;
            }
        }
    }

    private static int align(int value) {
        return align(value, 1 << Segment.RECORD_ALIGN_BITS);
    }

    private static int align(int value, int boundary) {
        return (value + boundary - 1) & ~(boundary - 1);
    }

}
