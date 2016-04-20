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

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Lists.newArrayListWithExpectedSize;
import static com.google.common.collect.Lists.partition;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.io.ByteStreams.read;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.nCopies;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.segment.MapRecord.BUCKETS_PER_LEVEL;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newBlobIdWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newBlockWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newListBucketWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newListWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newMapBranchWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newMapLeafWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newNodeStateWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newTemplateWriter;
import static org.apache.jackrabbit.oak.plugins.segment.RecordWriters.newValueWriter;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.align;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.readString;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.V_11;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.jcr.PropertyType;

import com.google.common.io.Closeables;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.plugins.segment.RecordCache.Cache;
import org.apache.jackrabbit.oak.plugins.segment.WriteOperationHandler.WriteOperation;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts nodes, properties, and values to records, which are written to segments.
 * FIXME OAK-3348 doc thread safety properties
 */
public class SegmentWriter {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentWriter.class);

    static final int BLOCK_SIZE = 1 << 12; // 4kB

    private static final int STRING_RECORDS_CACHE_SIZE = Integer.getInteger(
            "oak.segment.writer.stringsCacheSize", 15000);

    /**
     * Cache of recently stored string records, used to avoid storing duplicates
     * of frequently occurring data.
     */
    private final RecordCache<String> stringCache =
        STRING_RECORDS_CACHE_SIZE <= 0
            ? RecordCache.<String>disabled()
            : new RecordCache<String>() {
                @Override
                protected Cache<String> getCache(int generation) {
                    return new LRUCache<String>(STRING_RECORDS_CACHE_SIZE);
                }
        };

    private static final int TPL_RECORDS_CACHE_SIZE = Integer.getInteger(
            "oak.segment.writer.templatesCacheSize", 3000);

    /**
     * Cache of recently stored template records, used to avoid storing
     * duplicates of frequently occurring data.
     */
    private final RecordCache<Template> templateCache =
        TPL_RECORDS_CACHE_SIZE <= 0
            ? RecordCache.<Template>disabled()
            : new RecordCache<Template>() {
            @Override
            protected Cache<Template> getCache(int generation) {
                return new LRUCache<Template>(TPL_RECORDS_CACHE_SIZE);
            }
        };

    private final RecordCache<String> nodeCache;


    private final SegmentStore store;

    /**
     * Version of the segment storage format.
     */
    private final SegmentVersion version;

    private final WriteOperationHandler writeOperationHandler;

    public SegmentWriter(SegmentStore store, SegmentVersion version, WriteOperationHandler writeOperationHandler,
            RecordCache<String> nodeCache) {
        this.store = store;
        this.version = version;
        this.writeOperationHandler = writeOperationHandler;
        this.nodeCache = nodeCache;
    }

    /**
     * @param store     store to write to
     * @param version   segment version to write
     */
    public SegmentWriter(SegmentStore store, SegmentVersion version, WriteOperationHandler writeOperationHandler) {
        this(store, version, writeOperationHandler, new RecordCache<String>());
    }

    public void addCachedNodes(int generation, Cache<String> cache) {
        nodeCache.put(cache, generation);

        stringCache.clearUpTo(generation - 1);
        templateCache.clearUpTo(generation - 1);
        nodeCache.clearUpTo(generation - 1);
    }

    public void flush() throws IOException {
        writeOperationHandler.flush();
    }

    MapRecord writeMap(final MapRecord base, final Map<String, RecordId> changes) throws IOException {
        return new MapRecord(
            writeOperationHandler.execute(new SegmentWriteOperation() {
                @Override
                public RecordId execute(SegmentBufferWriter writer) throws IOException {
                    return with(writer).writeMap(base, changes);
                }
            }));
    }

    public RecordId writeList(final List<RecordId> list) throws IOException {
        return writeOperationHandler.execute(new SegmentWriteOperation() {
            @Override
            public RecordId execute(SegmentBufferWriter writer) throws IOException {
                return with(writer).writeList(list);
            }
        });
    }

    public RecordId writeString(final String string) throws IOException {
        return writeOperationHandler.execute(new SegmentWriteOperation() {
            @Override
            public RecordId execute(SegmentBufferWriter writer) throws IOException {
                return with(writer).writeString(string);
            }
        });
    }

    SegmentBlob writeBlob(final Blob blob) throws IOException {
        return new SegmentBlob(
            writeOperationHandler.execute(new SegmentWriteOperation() {
                @Override
                public RecordId execute(SegmentBufferWriter writer) throws IOException {
                    return with(writer).writeBlob(blob);
                }
            }));
    }

    /**
     * Writes a block record containing the given block of bytes.
     *
     * @param bytes source buffer
     * @param offset offset within the source buffer
     * @param length number of bytes to write
     * @return block record identifier
     */
    RecordId writeBlock(final byte[] bytes, final int offset, final int length) throws IOException {
        return writeOperationHandler.execute(new SegmentWriteOperation() {
            @Override
            public RecordId execute(SegmentBufferWriter writer) throws IOException {
                return with(writer).writeBlock(bytes, offset, length);
            }
        });
    }

    /**
     * Writes a stream value record. The given stream is consumed <em>and closed</em> by
     * this method.
     *
     * @param stream stream to be written
     * @return blob for the passed {@code stream}
     * @throws IOException if the input stream could not be read or the output could not be written
     */
    public SegmentBlob writeStream(final InputStream stream) throws IOException {
        return new SegmentBlob(
            writeOperationHandler.execute(new SegmentWriteOperation() {
                @Override
                public RecordId execute(SegmentBufferWriter writer) throws IOException {
                    return with(writer).writeStream(stream);
                }
            }));
    }

    SegmentPropertyState writeProperty(final PropertyState state) throws IOException {
        RecordId id = writeOperationHandler.execute(new SegmentWriteOperation() {
            @Override
            public RecordId execute(SegmentBufferWriter writer) throws IOException {
                return with(writer).writeProperty(state);
            }
        });
        return new SegmentPropertyState(id, state.getName(), state.getType());
    }

    public SegmentNodeState writeNode(final NodeState state) throws IOException {
        return new SegmentNodeState(
            writeOperationHandler.execute(new SegmentWriteOperation() {
                @Override
                public RecordId execute(SegmentBufferWriter writer) throws IOException {
                    return with(writer).writeNode(state, 0);
                }
            }));
    }

    // FIXME OAK-3348 document: not thread safe
    private abstract class SegmentWriteOperation implements WriteOperation {
        private SegmentBufferWriter writer;

        @Override
        public abstract RecordId execute(SegmentBufferWriter writer) throws IOException;

        SegmentWriteOperation with(SegmentBufferWriter writer) {
            checkState(this.writer == null);
            this.writer = writer;
            return this;
        }

        private int generation() {
            return writer.getGeneration();
        }

        private RecordId writeMap(MapRecord base, Map<String, RecordId> changes) throws IOException {
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
                RecordId value = change.getValue();
                if (value != null) {
                    MapEntry entry = base.getEntry(change.getKey());
                    if (entry != null) {
                        if (value.equals(entry.getValue())) {
                            return base.getRecordId();
                        } else {
                            return newMapBranchWriter(entry.getHash(), asList(entry.getKey(),
                                value, base.getRecordId())).write(writer);
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

        private RecordId writeMapLeaf(int level, Collection<MapEntry> entries) throws IOException {
            checkNotNull(entries);
            int size = entries.size();
            checkElementIndex(size, MapRecord.MAX_SIZE);
            checkPositionIndex(level, MapRecord.MAX_NUMBER_OF_LEVELS);
            checkArgument(size != 0 || level == MapRecord.MAX_NUMBER_OF_LEVELS);
            return newMapLeafWriter(level, entries).write(writer);
        }

        private RecordId writeMapBranch(int level, int size, MapRecord... buckets) throws IOException {
            int bitmap = 0;
            List<RecordId> bucketIds = newArrayListWithCapacity(buckets.length);
            for (int i = 0; i < buckets.length; i++) {
                if (buckets[i] != null) {
                    bitmap |= 1L << i;
                    bucketIds.add(buckets[i].getRecordId());
                }
            }
            return newMapBranchWriter(level, size, bitmap, bucketIds).write(writer);
        }

        private RecordId writeMapBucket(MapRecord base, Collection<MapEntry> entries, int level)
                throws IOException {
            // when no changed entries, return the base map (if any) as-is
            if (entries == null || entries.isEmpty()) {
                if (base != null) {
                    return base.getRecordId();
                } else if (level == 0) {
                    return newMapLeafWriter().write(writer);
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
                    buckets[i] = mapRecordOrNull(writeMapBucket(null, changes.get(i), level + 1));
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
                buckets[i] = mapRecordOrNull(writeMapBucket(buckets[i], changes.get(i), level + 1));
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
                        return bucket.getRecordId();
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

        private MapRecord mapRecordOrNull(RecordId id) {
            return id == null ? null : new MapRecord(id);
        }

        /**
         * Writes a list record containing the given list of record identifiers.
         *
         * @param list list of record identifiers
         * @return list record identifier
         */
        private RecordId writeList(List<RecordId> list) throws IOException {
            checkNotNull(list);
            checkArgument(!list.isEmpty());
            List<RecordId> thisLevel = list;
            while (thisLevel.size() > 1) {
                List<RecordId> nextLevel = newArrayList();
                for (List<RecordId> bucket :
                    partition(thisLevel, ListRecord.LEVEL_SIZE)) {
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

        private RecordId writeListBucket(List<RecordId> bucket) throws IOException {
            checkArgument(bucket.size() > 1);
            return newListBucketWriter(bucket).write(writer);
        }

        private List<List<MapEntry>> splitToBuckets(Collection<MapEntry> entries, int level) {
            int mask = (1 << MapRecord.BITS_PER_LEVEL) - 1;
            int shift = 32 - (level + 1) * MapRecord.BITS_PER_LEVEL;

            List<List<MapEntry>> buckets =
                newArrayList(nCopies(MapRecord.BUCKETS_PER_LEVEL, (List<MapEntry>) null));
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

        private RecordId writeValueRecord(long length, RecordId blocks) throws IOException {
            long len = (length - Segment.MEDIUM_LIMIT) | (0x3L << 62);
            return newValueWriter(blocks, len).write(writer);
        }

        private RecordId writeValueRecord(int length, byte... data) throws IOException {
            checkArgument(length < Segment.MEDIUM_LIMIT);
            return newValueWriter(length, data).write(writer);
        }

        /**
         * Writes a string value record.
         *
         * @param string string to be written
         * @return value record identifier
         */
        private RecordId writeString(String string) throws IOException {
            RecordId id = stringCache.generation(generation()).get(string);
            if (id != null) {
                return id; // shortcut if the same string was recently stored
            }

            byte[] data = string.getBytes(UTF_8);

            if (data.length < Segment.MEDIUM_LIMIT) {
                // only cache short strings to avoid excessive memory use
                id = writeValueRecord(data.length, data);
                stringCache.generation(generation()).put(string, id);
                return id;
            }

            int pos = 0;
            List<RecordId> blockIds = newArrayListWithExpectedSize(
                data.length / BLOCK_SIZE + 1);

            // write as many full bulk segments as possible
            while (pos + MAX_SEGMENT_SIZE <= data.length) {
                SegmentId bulkId = getTracker().newBulkSegmentId();
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

        private boolean hasSegment(Blob blob) {
            return (blob instanceof SegmentBlob)
                    && (getTracker().isTracking(((Record) blob).getRecordId().getSegmentId()));
        }

        private RecordId writeBlob(Blob blob) throws IOException {
            if (hasSegment(blob)) {
                SegmentBlob segmentBlob = (SegmentBlob) blob;
                if (!isOldGen(segmentBlob.getRecordId())) {
                    return segmentBlob.getRecordId();
                }
            }

            String reference = blob.getReference();
            if (reference != null && store.getBlobStore() != null) {
                String blobId = store.getBlobStore().getBlobId(reference);
                if (blobId != null) {
                    return writeBlobId(blobId);
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
        private RecordId writeBlobId(String blobId) throws IOException {
            byte[] data = blobId.getBytes(UTF_8);
            if (data.length < Segment.BLOB_ID_SMALL_LIMIT) {
                return newBlobIdWriter(data).write(writer);
            } else {
                return newBlobIdWriter(writeString(blobId)).write(writer);
            }
        }

        private RecordId writeBlock(byte[] bytes, int offset, int length) throws IOException {
            checkNotNull(bytes);
            checkPositionIndexes(offset, offset + length, bytes.length);
            return newBlockWriter(bytes, offset, length).write(writer);
        }

        private RecordId writeStream(InputStream stream) throws IOException {
            boolean threw = true;
            try {
                RecordId id = SegmentStream.getRecordIdIfAvailable(stream, store);
                if (id == null || isOldGen(id)) {
                    id = internalWriteStream(stream);
                }
                threw = false;
                return id;
            } finally {
                Closeables.close(stream, threw);
            }
        }

        private RecordId internalWriteStream(InputStream stream) throws IOException {
            if (stream instanceof SegmentStream) {
                SegmentStream segmentStream = (SegmentStream) stream;
                List<RecordId> blockIds = segmentStream.getBlockIds();
                if (blockIds != null) {
                    return writeValueRecord(segmentStream.getLength(), writeList(blockIds));
                }
            }

            // Special case for short binaries (up to about 16kB):
            // store them directly as small- or medium-sized value records
            byte[] data = new byte[Segment.MEDIUM_LIMIT];
            int n = read(stream, data, 0, data.length);
            if (n < Segment.MEDIUM_LIMIT) {
                return writeValueRecord(n, data);
            }

            BlobStore blobStore = store.getBlobStore();
            if (blobStore != null) {
                String blobId = blobStore.writeBlob(new SequenceInputStream(
                    new ByteArrayInputStream(data, 0, n), stream));
                return writeBlobId(blobId);
            }

            data = Arrays.copyOf(data, MAX_SEGMENT_SIZE);
            n += read(stream, data, n, MAX_SEGMENT_SIZE - n);
            long length = n;
            List<RecordId> blockIds =
                newArrayListWithExpectedSize(2 * n / BLOCK_SIZE);

            // Write the data to bulk segments and collect the list of block ids
            while (n != 0) {
                SegmentId bulkId = getTracker().newBulkSegmentId();
                int len = align(n, 1 << Segment.RECORD_ALIGN_BITS);
                LOG.debug("Writing bulk segment {} ({} bytes)", bulkId, n);
                store.writeSegment(bulkId, data, 0, len);

                for (int i = 0; i < n; i += BLOCK_SIZE) {
                    blockIds.add(new RecordId(bulkId, data.length - len + i));
                }

                n = read(stream, data, 0, data.length);
                length += n;
            }

            return writeValueRecord(length, writeList(blockIds));
        }

        private RecordId writeProperty(PropertyState state) throws IOException {
            Map<String, RecordId> previousValues = emptyMap();
            return writeProperty(state, previousValues);
        }

        private RecordId writeProperty(PropertyState state, Map<String, RecordId> previousValues)
                throws IOException {
            Type<?> type = state.getType();
            int count = state.count();

            List<RecordId> valueIds = newArrayList();
            for (int i = 0; i < count; i++) {
                if (type.tag() == PropertyType.BINARY) {
                    try {
                        valueIds.add(writeBlob(state.getValue(BINARY, i)));
                    } catch (IOException e) {
                        throw new IllegalStateException("Unexpected IOException", e);
                    }
                } else {
                    String value = state.getValue(STRING, i);
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
                return newListWriter().write(writer);
            } else {
                return newListWriter(count, writeList(valueIds)).write(writer);
            }
        }

        private RecordId writeTemplate(Template template) throws IOException {
            checkNotNull(template);

            RecordId id = templateCache.generation(generation()).get(template);
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
                    propNamesId = writeList(asList(propertyNames));
                    ids.add(propNamesId);
                }
            } else {
                ids.addAll(asList(propertyNames));
            }

            checkState(propertyNames.length < (1 << 18));
            head |= propertyNames.length;

            RecordId tid = newTemplateWriter(ids, propertyNames,
                propertyTypes, head, primaryId, mixinIds, childNameId,
                propNamesId, version).write(writer);
            templateCache.generation(generation()).put(template, tid);
            return tid;
        }

        private RecordId toCache(NodeState state, int depth, RecordId recordId) {
            if (state instanceof SegmentNodeState) {
                SegmentNodeState sns = (SegmentNodeState) state;
                nodeCache.generation(generation()).put(sns.getId(), recordId, depth);
            }
            return recordId;
        }

        private RecordId writeNode(NodeState state, int depth) throws IOException {
            if (state instanceof SegmentNodeState) {
                SegmentNodeState sns = ((SegmentNodeState) state);
                if (hasSegment(sns)) {
                    if (isOldGen(sns.getRecordId())) {
                        RecordId cachedId = nodeCache.generation(generation()).get(sns.getId());
                        if (cachedId != null) {
                            return cachedId;
                        }
                    } else {
                        return sns.getRecordId();
                    }
                }
            }

            RecordId recordId = writeNodeUncached(state, depth);
            if (state instanceof SegmentNodeState) {
                SegmentNodeState sns = (SegmentNodeState) state;
                nodeCache.generation(generation()).put(sns.getId(), recordId, depth);
            }
            return recordId;
        }

        private RecordId writeNodeUncached(NodeState state, int depth) throws IOException {
            SegmentNodeState before = null;
            Template beforeTemplate = null;
            ModifiedNodeState after = null;
            if (state instanceof ModifiedNodeState) {
                after = (ModifiedNodeState) state;
                NodeState base = after.getBaseState();
                if (base instanceof SegmentNodeState) {
                    SegmentNodeState sns = ((SegmentNodeState) base);
                    if (hasSegment(sns)) {
                        if (!isOldGen(sns.getRecordId())) {
                            before = sns;
                            beforeTemplate = before.getTemplate();
                        }
                    }
                }
            }

            List<RecordId> ids = newArrayList();
            if (state instanceof SegmentNodeState) {
                ids.add(writeString(((SegmentNodeState) state).getId()));
            } else {
                ids.add(writeString(createId()));
            }

            Template template = new Template(state);
            if (template.equals(beforeTemplate)) {
                ids.add(before.getTemplateId());
            } else {
                ids.add(writeTemplate(template));
            }

            String childName = template.getChildName();
            if (childName == Template.MANY_CHILD_NODES) {
                MapRecord base;
                Map<String, RecordId> childNodes;
                if (before != null
                    && before.getChildNodeCount(2) > 1
                    && after.getChildNodeCount(2) > 1) {
                    base = before.getChildNodeMap();
                    childNodes = new ChildNodeCollectorDiff(depth).diff(before, after);
                } else {
                    base = null;
                    childNodes = newHashMap();
                    for (ChildNodeEntry entry : state.getChildNodeEntries()) {
                        childNodes.put(
                            entry.getName(),
                            writeNode(entry.getNodeState(), depth + 1));
                    }
                }
                ids.add(writeMap(base, childNodes));
            } else if (childName != Template.ZERO_CHILD_NODES) {
                ids.add(writeNode(state.getChildNode(template.getChildName()), depth + 1));
            }

            List<RecordId> pIds = newArrayList();
            for (PropertyTemplate pt : template.getPropertyTemplates()) {
                String name = pt.getName();
                PropertyState property = state.getProperty(name);

                if (hasSegment(property)) {
                    RecordId pid = ((Record) property).getRecordId();
                    if (isOldGen(pid)) {
                        pIds.add(writeProperty(property));
                    } else {
                        pIds.add(pid);
                    }
                } else if (before == null || !hasSegment(before)) {
                    pIds.add(writeProperty(property));
                } else {
                    // reuse previously stored property, if possible
                    PropertyTemplate bt = beforeTemplate.getPropertyTemplate(name);
                    if (bt == null) {
                        pIds.add(writeProperty(property)); // new property
                    } else {
                        SegmentPropertyState bp = beforeTemplate.getProperty(before.getRecordId(), bt.getIndex());
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
            return newNodeStateWriter(ids).write(writer);
        }

        private boolean hasSegment(SegmentNodeState node) {
            return getTracker().isTracking(node.getRecordId().getSegmentId());
        }

        private boolean hasSegment(PropertyState property) {
            return (property instanceof SegmentPropertyState)
                && (getTracker().isTracking(((Record) property).getRecordId().getSegmentId()));
        }

        private boolean isOldGen(RecordId id) {
            int thatGen = id.getSegment().getGcGen();
            int thisGen = writer.getGeneration();
            return thatGen < thisGen;
        }

        private class ChildNodeCollectorDiff extends DefaultNodeStateDiff {
            private final int depth;
            private final Map<String, RecordId> childNodes = newHashMap();
            private IOException exception;

            private ChildNodeCollectorDiff(int depth) {
                this.depth = depth;
            }

            public Map<String, RecordId> diff(SegmentNodeState before, ModifiedNodeState after) throws IOException {
                after.compareAgainstBaseState(before, this);
                if (exception != null) {
                    throw new IOException(exception);
                } else {
                    return childNodes;
                }
            }

            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                try {
                    childNodes.put(name, writeNode(after, depth + 1));
                } catch (IOException e) {
                    exception = e;
                    return false;
                }
                return true;
            }

            @Override
            public boolean childNodeChanged(
                String name, NodeState before, NodeState after) {
                try {
                    childNodes.put(name, writeNode(after, depth + 1));
                } catch (IOException e) {
                    exception = e;
                    return false;
                }
                return true;
            }

            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                childNodes.put(name, null);
                return true;
            }
        }
    }

    private SegmentTracker getTracker() {
        return store.getTracker();
    }

    private static final AtomicLong NEXT_ID = new AtomicLong();
    private static String createId() {
        return valueOf(NEXT_ID.getAndIncrement());
    }

}
