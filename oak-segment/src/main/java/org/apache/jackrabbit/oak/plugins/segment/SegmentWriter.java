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
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.io.ByteStreams.read;
import static com.google.common.io.Closeables.close;
import static java.lang.String.valueOf;
import static java.lang.Thread.currentThread;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.plugins.segment.RecordWriters.RecordWriter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts nodes, properties, and values to records, which are written to segments.
 */
@Deprecated
public class SegmentWriter {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentWriter.class);

    static final int BLOCK_SIZE = 1 << 12; // 4kB

    private final SegmentBufferWriterPool segmentBufferWriterPool = new SegmentBufferWriterPool();

    private static final int STRING_RECORDS_CACHE_SIZE = Integer.getInteger(
            "oak.segment.writer.stringsCacheSize", 15000);

    /**
     * Cache of recently stored string records, used to avoid storing duplicates
     * of frequently occurring data.
     */
    final Map<String, RecordId> stringCache = newItemsCache(
            STRING_RECORDS_CACHE_SIZE);

    private static final int TPL_RECORDS_CACHE_SIZE = Integer.getInteger(
            "oak.segment.writer.templatesCacheSize", 3000);

    /**
     * Cache of recently stored template records, used to avoid storing
     * duplicates of frequently occurring data.
     */
    final Map<Template, RecordId> templateCache = newItemsCache(TPL_RECORDS_CACHE_SIZE);

    private static final <T> Map<T, RecordId> newItemsCache(final int size) {
        final boolean disabled = size <= 0;
        final int safeSize = size <= 0 ? 0 : (int) (size * 1.2);
        return new LinkedHashMap<T, RecordId>(safeSize, 0.9f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<T, RecordId> e) {
                return size() > size;
            }
            @Override
            public synchronized RecordId get(Object key) {
                if (disabled) {
                    return null;
                }
                return super.get(key);
            }
            @Override
            public synchronized RecordId put(T key, RecordId value) {
                if (disabled) {
                    return null;
                }
                return super.put(key, value);
            }
            @Override
            public synchronized void clear() {
                super.clear();
            }
        };
    }

    private final SegmentStore store;

    /**
     * Version of the segment storage format.
     */
    private final SegmentVersion version;

    private final String wid;

    /**
     * @param store     store to write to
     * @param version   segment version to write
     * @param wid       id of this writer
     */
    @Deprecated
    public SegmentWriter(SegmentStore store, SegmentVersion version, String wid) {
        this.store = store;
        this.version = version;
        this.wid = wid;
    }

    @Deprecated
    public void flush() throws IOException {
        segmentBufferWriterPool.flush();
    }

    @Deprecated
    public void dropCache() {
        stringCache.clear();
        templateCache.clear();
    }

    MapRecord writeMap(MapRecord base, Map<String, RecordId> changes) throws IOException {
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
                        return base;
                    } else {
                        return writeRecord(newMapBranchWriter(entry.getHash(),
                                asList(entry.getKey(), value, base.getRecordId())));
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

    private MapRecord writeMapLeaf(int level, Collection<MapEntry> entries) throws IOException {
        checkNotNull(entries);
        int size = entries.size();
        checkElementIndex(size, MapRecord.MAX_SIZE);
        checkPositionIndex(level, MapRecord.MAX_NUMBER_OF_LEVELS);
        checkArgument(size != 0 || level == MapRecord.MAX_NUMBER_OF_LEVELS);
        return writeRecord(newMapLeafWriter(level, entries));
    }

    private MapRecord writeMapBranch(int level, int size, MapRecord[] buckets) throws IOException {
        int bitmap = 0;
        List<RecordId> bucketIds = newArrayListWithCapacity(buckets.length);
        for (int i = 0; i < buckets.length; i++) {
            if (buckets[i] != null) {
                bitmap |= 1L << i;
                bucketIds.add(buckets[i].getRecordId());
            }
        }
        return writeRecord(newMapBranchWriter(level, size, bitmap, bucketIds));
    }

    private MapRecord writeMapBucket(MapRecord base, Collection<MapEntry> entries, int level) throws IOException {
        // when no changed entries, return the base map (if any) as-is
        if (entries == null || entries.isEmpty()) {
            if (base != null) {
                return base;
            } else if (level == 0) {
                return writeRecord(newMapLeafWriter());
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
    @Deprecated
    public RecordId writeList(List<RecordId> list) throws IOException {
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
        return writeRecord(newListBucketWriter(bucket));
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

    private RecordId writeValueRecord(long length, RecordId blocks) throws IOException {
        long len = (length - Segment.MEDIUM_LIMIT) | (0x3L << 62);
        return writeRecord(newValueWriter(blocks, len));
    }

    private RecordId writeValueRecord(int length, byte[] data) throws IOException {
        checkArgument(length < Segment.MEDIUM_LIMIT);
        return writeRecord(newValueWriter(length, data));
    }

    /**
     * Writes a string value record.
     *
     * @param string string to be written
     * @return value record identifier
     */
    @Deprecated
    public RecordId writeString(String string) throws IOException {
        RecordId id = stringCache.get(string);
        if (id != null) {
            return id; // shortcut if the same string was recently stored
        }

        byte[] data = string.getBytes(UTF_8);

        if (data.length < Segment.MEDIUM_LIMIT) {
            // only cache short strings to avoid excessive memory use
            id = writeValueRecord(data.length, data);
            stringCache.put(string, id);
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

    @Deprecated
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
    private RecordId writeBlobId(String blobId) throws IOException {
        byte[] data = blobId.getBytes(UTF_8);
        if (data.length < Segment.BLOB_ID_SMALL_LIMIT) {
            return writeRecord(newBlobIdWriter(data));
        } else {
            return writeRecord(newBlobIdWriter(writeString(blobId)));
        }
    }

    /**
     * Writes a block record containing the given block of bytes.
     *
     * @param bytes source buffer
     * @param offset offset within the source buffer
     * @param length number of bytes to write
     * @return block record identifier
     */
    RecordId writeBlock(byte[] bytes, int offset, int length) throws IOException {
        checkNotNull(bytes);
        checkPositionIndexes(offset, offset + length, bytes.length);
        return writeRecord(newBlockWriter(bytes, offset, length));
    }

    SegmentBlob writeExternalBlob(String blobId) throws IOException {
        RecordId id = writeBlobId(blobId);
        return new SegmentBlob(id);
    }

    SegmentBlob writeLargeBlob(long length, List<RecordId> list) throws IOException {
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
    @Deprecated
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
            close(stream, threw);
        }
    }

    private RecordId internalWriteStream(InputStream stream)
            throws IOException {
        BlobStore blobStore = store.getBlobStore();
        byte[] data = new byte[Segment.MEDIUM_LIMIT];
        int n = read(stream, data, 0, data.length);

        // Special case for short binaries (up to about 16kB):
        // store them directly as small- or medium-sized value records
        if (n < Segment.MEDIUM_LIMIT) {
            return writeValueRecord(n, data);
        } else if (blobStore != null) {
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
            SegmentId bulkId = store.getTracker().newBulkSegmentId();
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

    @Deprecated
    public RecordId writeProperty(PropertyState state) throws IOException {
        Map<String, RecordId> previousValues = emptyMap();
        return writeProperty(state, previousValues);
    }

    private RecordId writeProperty(PropertyState state, Map<String, RecordId> previousValues) throws IOException {
        Type<?> type = state.getType();
        int count = state.count();

        List<RecordId> valueIds = newArrayList();
        for (int i = 0; i < count; i++) {
            if (type.tag() == PropertyType.BINARY) {
                try {
                    SegmentBlob blob =
                            writeBlob(state.getValue(BINARY, i));
                    valueIds.add(blob.getRecordId());
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
            return writeRecord(newListWriter());
        } else {
            return writeRecord(newListWriter(count, writeList(valueIds)));
        }
    }

    @Deprecated
    public RecordId writeTemplate(Template template) throws IOException {
        checkNotNull(template);

        RecordId id = templateCache.get(template);
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

        RecordId tid = writeRecord(newTemplateWriter(ids, propertyNames,
                propertyTypes, head, primaryId, mixinIds, childNameId,
                propNamesId, version));
        templateCache.put(template, tid);
        return tid;
    }

    @Deprecated
    public SegmentNodeState writeNode(NodeState state) throws IOException {
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

        List<RecordId> ids = newArrayList();
        ids.add(templateId);

        String childName = template.getChildName();
        if (childName == Template.MANY_CHILD_NODES) {
            MapRecord base;
            Map<String, RecordId> childNodes;
            if (before != null
                && before.getChildNodeCount(2) > 1
                && after.getChildNodeCount(2) > 1) {
                base = before.getChildNodeMap();
                childNodes = new ChildNodeCollectorDiff().diff(before, after);
            } else {
                base = null;
                childNodes = newHashMap();
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
        return writeRecord(newNodeStateWriter(ids));
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
        RecordId id = store.getTracker().getCompactionMap().get(state.getRecordId());
        if (id != null) {
            return new SegmentNodeState(id);
        } else {
            return state;
        }
    }

    private <T> T writeRecord(RecordWriter<T> recordWriter) throws IOException {
        SegmentBufferWriter writer = segmentBufferWriterPool.borrowWriter(currentThread());
        try {
            return recordWriter.write(writer);
        } finally {
            segmentBufferWriterPool.returnWriter(currentThread(), writer);
        }
    }

    private class SegmentBufferWriterPool {
        private final Set<SegmentBufferWriter> borrowed = newHashSet();
        private final Map<Object, SegmentBufferWriter> writers = newHashMap();

        private short writerId = -1;

        public void flush() throws IOException {
            List<SegmentBufferWriter> toFlush = newArrayList();
            synchronized (this) {
                toFlush.addAll(writers.values());
                writers.clear();
                borrowed.clear();
            }
            // Call flush from outside a synchronized context to avoid
            // deadlocks of that method calling SegmentStore.writeSegment
            for (SegmentBufferWriter writer : toFlush) {
                writer.flush();
            }
        }

        public synchronized SegmentBufferWriter borrowWriter(Object key) throws IOException {
            SegmentBufferWriter writer = writers.remove(key);
            if (writer == null) {
                writer = new SegmentBufferWriter(store, version, wid + "." + getWriterId());
            }
            borrowed.add(writer);
            return writer;
        }

        public void returnWriter(Object key, SegmentBufferWriter writer) throws IOException {
            if (!tryReturn(key, writer)) {
                // Delayed flush this writer as it was borrowed while flush() was called.
                writer.flush();
            }
        }

        private synchronized boolean tryReturn(Object key, SegmentBufferWriter writer) {
            if (borrowed.remove(writer)) {
                writers.put(key, writer);
                return true;
            } else {
                return false;
            }
        }

        private synchronized String getWriterId() {
            if (++writerId > 9999) {
                writerId = 0;
            }
            // Manually padding seems to be fastest here
            if (writerId < 10) {
                return "000" + writerId;
            } else if (writerId < 100) {
                return "00" + writerId;
            } else if (writerId < 1000) {
                return "0" + writerId;
            } else {
                return valueOf(writerId);
            }
        }
    }

    private class ChildNodeCollectorDiff extends DefaultNodeStateDiff {
        private final Map<String, RecordId> childNodes = newHashMap();
        private IOException exception;

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            try {
                childNodes.put(name, writeNode(after).getRecordId());
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
                childNodes.put(name, writeNode(after).getRecordId());
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

        public Map<String, RecordId> diff(SegmentNodeState before, ModifiedNodeState after) throws IOException {
            after.compareAgainstBaseState(before, this);
            if (exception != null) {
                throw new IOException(exception);
            } else {
                return childNodes;
            }
        }
    }
}
