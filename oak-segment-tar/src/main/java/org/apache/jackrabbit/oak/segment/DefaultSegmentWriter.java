/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment;

import static java.lang.Long.numberOfLeadingZeros;
import static java.lang.Math.min;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static org.apache.jackrabbit.guava.common.base.Preconditions.checkElementIndex;
import static org.apache.jackrabbit.guava.common.base.Preconditions.checkPositionIndex;
import static org.apache.jackrabbit.guava.common.base.Preconditions.checkPositionIndexes;
import static org.apache.jackrabbit.guava.common.collect.Iterables.addAll;
import static org.apache.jackrabbit.guava.common.io.ByteStreams.read;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.compareAgainstEmptyState;
import static org.apache.jackrabbit.oak.segment.MapEntry.newModifiedMapEntry;
import static org.apache.jackrabbit.oak.segment.MapRecord.BUCKETS_PER_LEVEL;
import static org.apache.jackrabbit.oak.segment.RecordWriters.newNodeStateWriter;
import static org.apache.jackrabbit.oak.segment.SegmentNodeState.getStableId;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.guava.common.io.Closeables;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.segment.RecordWriters.RecordWriter;
import org.apache.jackrabbit.oak.segment.WriteOperationHandler.WriteOperation;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts nodes, properties, values, etc. to records and persists them with
 * the help of a {@link WriteOperationHandler}. All public methods of this class
 * are thread safe if and only if the {@link WriteOperationHandler} passed to
 * the constructor is thread safe.
 */
public class DefaultSegmentWriter implements SegmentWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSegmentWriter.class);

    /**
     * Number of updates to child nodes before changes are flushed to disk.
     */
    private static final int CHILD_NODE_UPDATE_LIMIT = Integer
            .getInteger("child.node.update.limit", 10000);

    protected static final String MAX_MAP_RECORD_SIZE_KEY = "oak.segmentNodeStore.maxMapRecordSize";

    @NotNull
    private final WriterCacheManager cacheManager;

    @NotNull
    private final SegmentStore store;

    @NotNull
    private final SegmentReader reader;

    @NotNull
    private final SegmentIdProvider idProvider;

    @Nullable
    private final BlobStore blobStore;

    @NotNull
    private final WriteOperationHandler writeOperationHandler;
    
    private final int binariesInlineThreshold;

    /**
     * Create a new instance of a {@code SegmentWriter}. Note the thread safety
     * properties pointed out in the class comment.
     *
     * @param store                   store to write to
     * @param reader                  segment reader for the {@code store}
     * @param idProvider              segment id provider for the {@code store}
     * @param blobStore               the blob store or {@code null} for inlined
     *                                blobs
     * @param cacheManager            cache manager instance for the
     *                                de-duplication caches used by this writer
     * @param writeOperationHandler   handler for write operations.
     * @param binariesInlineThreshold threshold in bytes, specifying the limit up to which
     *                                blobs will be inlined
     */
    public DefaultSegmentWriter(
            @NotNull SegmentStore store,
            @NotNull SegmentReader reader,
            @NotNull SegmentIdProvider idProvider,
            @Nullable BlobStore blobStore,
            @NotNull WriterCacheManager cacheManager,
            @NotNull WriteOperationHandler writeOperationHandler,
            int binariesInlineThreshold
    ) {
        this.store = requireNonNull(store);
        this.reader = requireNonNull(reader);
        this.idProvider = requireNonNull(idProvider);
        this.blobStore = blobStore;
        this.cacheManager = requireNonNull(cacheManager);
        this.writeOperationHandler = requireNonNull(writeOperationHandler);
        checkArgument(binariesInlineThreshold >= 0);
        checkArgument(binariesInlineThreshold <= Segment.MEDIUM_LIMIT);
        this.binariesInlineThreshold = binariesInlineThreshold;
    }

    @Override
    public void flush() throws IOException {
        writeOperationHandler.flush(store);
    }

    @NotNull
    RecordId writeMap(@Nullable final MapRecord base, @NotNull final Map<String, RecordId> changes) throws IOException {
        return new SegmentWriteOperation(writeOperationHandler.getGCGeneration())
                .writeMap(base, changes);
    }

    @NotNull
    RecordId writeList(@NotNull final List<RecordId> list) throws IOException {
        return new SegmentWriteOperation(writeOperationHandler.getGCGeneration())
                .writeList(list);
    }

    @NotNull
    RecordId writeString(@NotNull final String string) throws IOException {
        return new SegmentWriteOperation(writeOperationHandler.getGCGeneration())
                .writeString(string);
    }

    @Override
    @NotNull
    public RecordId writeBlob(@NotNull final Blob blob) throws IOException {
        return new SegmentWriteOperation(writeOperationHandler.getGCGeneration())
                .writeBlob(blob);
    }

    @NotNull
    RecordId writeBlock(@NotNull final byte[] bytes, final int offset, final int length) throws IOException {
        return new SegmentWriteOperation(writeOperationHandler.getGCGeneration())
                .writeBlock(bytes, offset, length);
    }

    @Override
    @NotNull
    public RecordId writeStream(@NotNull final InputStream stream) throws IOException {
        return new SegmentWriteOperation(writeOperationHandler.getGCGeneration())
                .writeStream(stream);
    }

    @NotNull
    RecordId writeProperty(@NotNull final PropertyState state) throws IOException {
        return new SegmentWriteOperation(writeOperationHandler.getGCGeneration())
                .writeProperty(state);
    }

    @Override
    @NotNull
    public RecordId writeNode(@NotNull final NodeState state, @Nullable final Buffer stableIdBytes) throws IOException {
        return new SegmentWriteOperation(writeOperationHandler.getGCGeneration())
                .writeNode(state, stableIdBytes);
    }

    /**
     * This {@code WriteOperation} implementation is used internally to provide
     * context to a recursive chain of calls without having pass the context
     * as a separate argument (a poor mans monad). As such it is entirely
     * <em>not thread safe</em>.
     */
    private class SegmentWriteOperation {
        private final GCGeneration gcGeneration;

        private final Cache<String, RecordId> stringCache;

        private final Cache<Template, RecordId> templateCache;

        private final Cache<String, RecordId> nodeCache;

        private long lastLogTime;

        SegmentWriteOperation(@NotNull GCGeneration gcGeneration) {
            int generation = gcGeneration.getGeneration();
            this.gcGeneration = gcGeneration;
            this.stringCache = cacheManager.getStringCache(generation);
            this.templateCache = cacheManager.getTemplateCache(generation);
            this.nodeCache = cacheManager.getNodeCache(generation);
        }

        private WriteOperation newWriteOperation(RecordWriter recordWriter) {
            return writer -> recordWriter.write(writer, store);
        }

        private boolean shouldLog() {
            long now = System.currentTimeMillis();
            if (now - lastLogTime > 1000) {
                lastLogTime = now;
                return true;
            } else {
                return false;
            }
        }

        private RecordId writeMap(@Nullable MapRecord base, @NotNull Map<String, RecordId> changes) throws IOException {
            if (base != null) {
                if (base.size() >= MapRecord.WARN_SIZE) {
                    int maxMapRecordSize = Integer.getInteger(MAX_MAP_RECORD_SIZE_KEY, 0);
                    if (base.size() > maxMapRecordSize) {
                        System.setProperty(MAX_MAP_RECORD_SIZE_KEY, String.valueOf(base.size()));
                    }

                    if (base.size() >= MapRecord.ERROR_SIZE_HARD_STOP) {
                        throw new UnsupportedOperationException("Map record has more than " + MapRecord.ERROR_SIZE_HARD_STOP
                                        + " direct entries. Writing is not allowed. Please remove entries.");
                    } else if (base.size() >= MapRecord.ERROR_SIZE_DISCARD_WRITES) {
                        if (!Boolean.getBoolean("oak.segmentNodeStore.allowWritesOnHugeMapRecord")) {
                            if (shouldLog()) {
                                LOG.error(
                                        "Map entry has more than {} entries. Writing more than {} entries (up to the hard limit of {}) is only allowed "
                                                + "if the system property \"oak.segmentNodeStore.allowWritesOnHugeMapRecord\" is set",
                                        MapRecord.ERROR_SIZE, MapRecord.ERROR_SIZE_DISCARD_WRITES, MapRecord.ERROR_SIZE_HARD_STOP);
                            }

                            throw new UnsupportedOperationException("Map record has more than " + MapRecord.ERROR_SIZE_DISCARD_WRITES
                                            + " direct entries. Writing is not allowed. Please remove entries.");
                        }
                    } else if (base.size() >=  MapRecord.ERROR_SIZE) {
                        if (shouldLog()) {
                            LOG.error("Map entry has more than {} entries. Please remove entries.", MapRecord.ERROR_SIZE);
                        }
                    } else {
                        if (shouldLog()) {
                            LOG.warn("Map entry has more than {} entries. Please remove entries.", MapRecord.WARN_SIZE);
                        }
                    }
                }
            }

            if (base != null && base.isDiff()) {
                Segment segment = base.getSegment();
                RecordId key = segment.readRecordId(base.getRecordNumber(), 8);
                String name = reader.readString(key);
                if (!changes.containsKey(name)) {
                    changes.put(name, segment.readRecordId(base.getRecordNumber(), 8, 1));
                }
                base = new MapRecord(reader, segment.readRecordId(base.getRecordNumber(), 8, 2));
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
                            return writeOperationHandler.execute(gcGeneration, newWriteOperation(
                                RecordWriters.newMapBranchWriter(
                                    entry.getHash(),
                                    asList(entry.getKey(), value, base.getRecordId()))));
                        }
                    }
                }
            }

            List<MapEntry> entries = new ArrayList<>();
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
                    entries.add(newModifiedMapEntry(reader, key, keyId, entry.getValue()));
                }
            }
            return writeMapBucket(base, entries, 0);
        }

        private RecordId writeMapLeaf(int level, Collection<MapEntry> entries) throws IOException {
            requireNonNull(entries);
            int size = entries.size();
            checkElementIndex(size, MapRecord.MAX_SIZE);
            checkPositionIndex(level, MapRecord.MAX_NUMBER_OF_LEVELS);
            checkArgument(size != 0 || level == MapRecord.MAX_NUMBER_OF_LEVELS);
            return writeOperationHandler.execute(gcGeneration, newWriteOperation(
                RecordWriters.newMapLeafWriter(level, entries)));
        }

        private RecordId writeMapBranch(int level, int size, MapRecord... buckets) throws IOException {
            checkElementIndex(size, MapRecord.MAX_SIZE);
            int bitmap = 0;
            List<RecordId> bucketIds = new ArrayList<>(buckets.length);
            for (int i = 0; i < buckets.length; i++) {
                if (buckets[i] != null) {
                    bitmap |= 1L << i;
                    bucketIds.add(buckets[i].getRecordId());
                }
            }
            return writeOperationHandler.execute(gcGeneration, newWriteOperation(
                RecordWriters.newMapBranchWriter(level, size, bitmap, bucketIds)));
        }

        private RecordId writeMapBucket(MapRecord base, Collection<MapEntry> entries, int level)
                throws IOException {
            // when no changed entries, return the base map (if any) as-is
            if (entries == null || entries.isEmpty()) {
                if (base != null) {
                    return base.getRecordId();
                } else if (level == 0) {
                    return writeOperationHandler.execute(gcGeneration, newWriteOperation(
                        RecordWriters.newMapLeafWriter()));
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
                Map<String, MapEntry> map = new HashMap<>();
                for (MapEntry entry : base.getEntries()) {
                    map.put(entry.getName(), entry);
                }
                for (MapEntry entry : entries) {
                    if (entry.isDeleted()) {
                        map.remove(entry.getName());
                    } else {
                        map.put(entry.getName(), entry);
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
                List<MapEntry> list = new ArrayList<>();
                for (MapRecord bucket : buckets) {
                    if (bucket != null) {
                        addAll(list, bucket.getEntries());
                    }
                }
                return writeMapLeaf(level, list);
            }
        }

        private MapRecord mapRecordOrNull(RecordId id) {
            return id == null ? null : new MapRecord(reader, id);
        }

        /**
         * Writes a list record containing the given list of record identifiers.
         *
         * @param list list of record identifiers
         * @return list record identifier
         */
        private RecordId writeList(@NotNull List<RecordId> list) throws IOException {
            requireNonNull(list);
            checkArgument(!list.isEmpty());
            List<RecordId> thisLevel = list;
            while (thisLevel.size() > 1) {
                List<RecordId> nextLevel = new ArrayList<>();
                for (List<RecordId> bucket : CollectionUtils.partitionList(thisLevel, ListRecord.LEVEL_SIZE)) {
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
            return writeOperationHandler.execute(gcGeneration, newWriteOperation(
                RecordWriters.newListBucketWriter(bucket)));
        }

        private List<List<MapEntry>> splitToBuckets(Collection<MapEntry> entries, int level) {
            int mask = (1 << MapRecord.BITS_PER_LEVEL) - 1;
            int shift = 32 - (level + 1) * MapRecord.BITS_PER_LEVEL;

            List<List<MapEntry>> buckets =
                    new ArrayList<>(nCopies(MapRecord.BUCKETS_PER_LEVEL, (List<MapEntry>) null));
            for (MapEntry entry : entries) {
                int index = (entry.getHash() >> shift) & mask;
                List<MapEntry> bucket = buckets.get(index);
                if (bucket == null) {
                    bucket = new ArrayList<>();
                    buckets.set(index, bucket);
                }
                bucket.add(entry);
            }
            return buckets;
        }

        private RecordId writeValueRecord(long length, RecordId blocks) throws IOException {
            long len = (length - Segment.MEDIUM_LIMIT) | (0x3L << 62);
            return writeOperationHandler.execute(gcGeneration, newWriteOperation(
                RecordWriters.newValueWriter(blocks, len)));
        }

        private RecordId writeValueRecord(int length, byte... data) throws IOException {
            checkArgument(length < Segment.MEDIUM_LIMIT);
            return writeOperationHandler.execute(gcGeneration, newWriteOperation(
                RecordWriters.newValueWriter(length, data)));
        }

        /**
         * Writes a string value record.
         *
         * @param string string to be written
         * @return value record identifier
         */
        private RecordId writeString(@NotNull String string) throws IOException {
            RecordId id = stringCache.get(string);
            if (id != null) {
                return id; // shortcut if the same string was recently stored
            }

            byte[] data = string.getBytes(StandardCharsets.UTF_8);

            if (data.length < Segment.MEDIUM_LIMIT) {
                // only cache short strings to avoid excessive memory use
                id = writeValueRecord(data.length, data);
                stringCache.put(string, id);
                return id;
            }

            int pos = 0;
            List<RecordId> blockIds = new ArrayList<>(
                    data.length / SegmentStream.BLOCK_SIZE + 1);

            // write as many full bulk segments as possible
            while (pos + Segment.MAX_SEGMENT_SIZE <= data.length) {
                SegmentId bulkId = idProvider.newBulkSegmentId();
                store.writeSegment(bulkId, data, pos, Segment.MAX_SEGMENT_SIZE);
                for (int i = 0; i < Segment.MAX_SEGMENT_SIZE; i += SegmentStream.BLOCK_SIZE) {
                    blockIds.add(new RecordId(bulkId, i));
                }
                pos += Segment.MAX_SEGMENT_SIZE;
            }

            // inline the remaining data as block records
            while (pos < data.length) {
                int len = min(SegmentStream.BLOCK_SIZE, data.length - pos);
                blockIds.add(writeBlock(data, pos, len));
                pos += len;
            }

            return writeValueRecord(data.length, writeList(blockIds));
        }

        private boolean sameStore(SegmentId id) {
            return id.sameStore(store);
        }

        /**
         * @param blob
         * @return {@code true} iff {@code blob} is a {@code SegmentBlob} and
         * originates from the same segment store.
         */
        private boolean sameStore(Blob blob) {
            return (blob instanceof SegmentBlob)
                    && sameStore(((Record) blob).getRecordId().getSegmentId());
        }

        private RecordId writeBlob(@NotNull Blob blob) throws IOException {
            if (sameStore(blob)) {
                SegmentBlob segmentBlob = (SegmentBlob) blob;
                if (!isOldGeneration(segmentBlob.getRecordId())) {
                    return segmentBlob.getRecordId();
                }
                if (segmentBlob.isExternal()) {
                    return writeBlobId(segmentBlob.getBlobId());
                }
            }

            if (blob instanceof BlobStoreBlob) {
                String blobId = ((BlobStoreBlob) blob).getBlobId();
                if (blobId != null) {
                    return writeBlobId(blobId);
                }
            }

            String reference = blob.getReference();
            if (reference != null && blobStore != null) {
                String blobId = blobStore.getBlobId(reference);
                if (blobId != null) {
                    return writeBlobId(blobId);
                } else {
                    LOG.debug("No blob found for reference {}, inlining...", reference);
                }
            }

            return writeStream(blob.getNewStream());
        }

        /**
         * Write a reference to an external blob. This method handles blob IDs
         * of every length, but behaves differently for small and large blob
         * IDs.
         *
         * @param blobId Blob ID.
         * @return Record ID pointing to the written blob ID.
         * @see Segment#BLOB_ID_SMALL_LIMIT
         */
        private RecordId writeBlobId(String blobId) throws IOException {
            byte[] data = blobId.getBytes(StandardCharsets.UTF_8);

            if (data.length < Segment.BLOB_ID_SMALL_LIMIT) {
                return writeOperationHandler.execute(gcGeneration, newWriteOperation(
                    RecordWriters.newBlobIdWriter(data)));
            } else {
                RecordId refId = writeString(blobId);
                return writeOperationHandler.execute(gcGeneration, newWriteOperation(
                    RecordWriters.newBlobIdWriter(refId)));
            }
        }

        private RecordId writeBlock(@NotNull byte[] bytes, int offset, int length)
                throws IOException {
            requireNonNull(bytes);
            checkPositionIndexes(offset, offset + length, bytes.length);
            return writeOperationHandler.execute(gcGeneration, newWriteOperation(
                RecordWriters.newBlockWriter(bytes, offset, length)));
        }

        private RecordId writeStream(@NotNull InputStream stream) throws IOException {
            boolean threw = true;
            try {
                RecordId id = SegmentStream.getRecordIdIfAvailable(stream, store);
                if (id == null) {
                    // This is either not a segment stream or a one from another store:
                    // fully serialise the stream.
                    id = internalWriteStream(stream);
                } else if (isOldGeneration(id)) {
                    // This is a segment stream from this store but from an old generation:
                    // try to link to the blocks if there are any.
                    SegmentStream segmentStream = (SegmentStream) stream;
                    List<RecordId> blockIds = segmentStream.getBlockIds();
                    if (blockIds == null) {
                        return internalWriteStream(stream);
                    } else {
                        return writeValueRecord(segmentStream.getLength(), writeList(blockIds));
                    }
                }
                threw = false;
                return id;
            } finally {
                Closeables.close(stream, threw);
            }
        }

        private RecordId internalWriteStream(@NotNull InputStream stream) throws IOException {
            // Special case for short binaries (up to about binariesInlineThreshold, 16kB by default):
            // store them directly as small- or medium-sized value records
                    	
            byte[] data = new byte[binariesInlineThreshold];
            int n = read(stream, data, 0, data.length);
            
            if (n < binariesInlineThreshold) {
                return writeValueRecord(n, data);
            }

            if (blobStore != null) {
                String blobId = blobStore.writeBlob(new SequenceInputStream(
                        new ByteArrayInputStream(data, 0, n), stream));
                return writeBlobId(blobId);
            }
            
            // handle case in which blob store is not configured and
            // binariesInlineThreshold < Segment.MEDIUM_LIMIT
            // store the binaries as small or medium sized value records 
            
            data = Arrays.copyOf(data, Segment.MEDIUM_LIMIT);
            n += read(stream, data, n, Segment.MEDIUM_LIMIT - n);
            
            if (n < Segment.MEDIUM_LIMIT) {
                return writeValueRecord(n, data);
            }

            data = Arrays.copyOf(data, Segment.MAX_SEGMENT_SIZE);
            n += read(stream, data, n, Segment.MAX_SEGMENT_SIZE - n);
            long length = n;
            List<RecordId> blockIds =
                    new ArrayList<>(2 * n / SegmentStream.BLOCK_SIZE);

            // Write the data to bulk segments and collect the list of block ids
            while (n != 0) {
                SegmentId bulkId = idProvider.newBulkSegmentId();
                LOG.debug("Writing bulk segment {} ({} bytes)", bulkId, n);
                store.writeSegment(bulkId, data, 0, n);

                for (int i = 0; i < n; i += SegmentStream.BLOCK_SIZE) {
                    blockIds.add(new RecordId(bulkId, data.length - n + i));
                }

                n = read(stream, data, 0, data.length);
                length += n;
            }

            return writeValueRecord(length, writeList(blockIds));
        }

        private RecordId writeProperty(@NotNull PropertyState state) throws IOException {
            Map<String, RecordId> previousValues = emptyMap();
            return writeProperty(state, previousValues);
        }

        private RecordId writeProperty(@NotNull PropertyState state,
                @NotNull Map<String, RecordId> previousValues
        )
                throws IOException {
            Type<?> type = state.getType();
            int count = state.count();

            List<RecordId> valueIds = new ArrayList<>();
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
                return writeOperationHandler.execute(gcGeneration, newWriteOperation(
                    RecordWriters.newListWriter()));
            } else {
                RecordId lid = writeList(valueIds);
                return writeOperationHandler.execute(gcGeneration, newWriteOperation(
                    RecordWriters.newListWriter(count, lid)));
            }
        }

        private RecordId writeTemplate(Template template) throws IOException {
            requireNonNull(template);

            RecordId id = templateCache.get(template);
            if (id != null) {
                return id; // shortcut if the same template was recently stored
            }

            Collection<RecordId> ids = new ArrayList<>();
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
                mixinIds = new ArrayList<>();
                for (String mixin : mixinTypes.getValue(NAMES)) {
                    mixinIds.add(writeString(mixin));
                }
                ids.addAll(mixinIds);
                Validate.checkState(mixinIds.size() < (1 << 10));
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
            if (propertyNames.length > 0) {
                propNamesId = writeList(asList(propertyNames));
                ids.add(propNamesId);
            }

            Validate.checkState(propertyNames.length < (1 << 18));
            head |= propertyNames.length;

            RecordId tid = writeOperationHandler.execute(gcGeneration, newWriteOperation(
                RecordWriters.newTemplateWriter(
                        ids, propertyNames, propertyTypes, head, primaryId, mixinIds,
                        childNameId, propNamesId)));
            templateCache.put(template, tid);
            return tid;
        }

        private RecordId writeNode(@NotNull NodeState state, @Nullable Buffer stableIdBytes)
        throws IOException {
            RecordId compactedId = deduplicateNode(state);

            if (compactedId != null) {
                return compactedId;
            }

            if (state instanceof SegmentNodeState && stableIdBytes == null) {
                stableIdBytes = ((SegmentNodeState) state).getStableIdBytes();
            }
            RecordId recordId = writeNodeUncached(state, stableIdBytes);

            if (stableIdBytes != null) {
                // This node state has been rewritten because it is from an older
                // generation (e.g. due to compaction). Put it into the cache for
                // deduplication of hard links to it (e.g. checkpoints).
                nodeCache.put(getStableId(stableIdBytes), recordId, cost(state));
            }
            return recordId;
        }

        private byte cost(NodeState node) {
            long childCount = node.getChildNodeCount(Long.MAX_VALUE);
            return (byte) (Byte.MIN_VALUE + 64 - numberOfLeadingZeros(childCount));
        }

        private RecordId writeNodeUncached(@NotNull NodeState state, @Nullable Buffer stableIdBytes)
        throws IOException {

            RecordId beforeId = null;
            if (state instanceof ModifiedNodeState) {
                // Pass null to indicate we don't want to update the node write statistics
                // when deduplicating the base state
                beforeId = deduplicateNode(((ModifiedNodeState) state).getBaseState());
            }

            SegmentNodeState before = null;
            Template beforeTemplate = null;

            if (beforeId != null) {
                before = reader.readNode(beforeId);
                beforeTemplate = before.getTemplate();
            }

            List<RecordId> ids = new ArrayList<>();
            Template template = new Template(reader, state);
            if (template.equals(beforeTemplate)) {
                ids.add(before.getTemplateId());
            } else {
                ids.add(writeTemplate(template));
            }

            String childName = template.getChildName();
            if (childName == Template.MANY_CHILD_NODES) {
                ids.add(writeChildNodes(before, state));
            } else if (childName != Template.ZERO_CHILD_NODES) {
                ids.add(writeNode(state.getChildNode(template.getChildName()), null));
            }

            List<RecordId> pIds = new ArrayList<>();
            for (PropertyTemplate pt : template.getPropertyTemplates()) {
                String name = pt.getName();
                PropertyState property = state.getProperty(name);
                assert property != null;

                if (before != null) {
                    // If this property is already present in before (the base state)
                    // and it hasn't been modified use that one. This will result
                    // in an already compacted property to be reused given before
                    // has been already compacted.
                    PropertyState beforeProperty = before.getProperty(name);
                    if (property.equals(beforeProperty)) {
                        property = beforeProperty;
                    }
                }

                if (sameStore(property)) {
                    RecordId pid = ((Record) property).getRecordId();
                    if (isOldGeneration(pid)) {
                        pIds.add(writeProperty(property));
                    } else {
                        pIds.add(pid);
                    }
                } else if (before == null || !sameStore(before)) {
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
                ids.add(writeList(pIds));
            }

            RecordId stableId;
            if (stableIdBytes != null) {
                Buffer buffer = stableIdBytes.duplicate();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                stableId = writeBlock(bytes, 0, bytes.length);
            } else {
                stableId = null;
            }

            return writeOperationHandler.execute(gcGeneration, newWriteOperation(
                newNodeStateWriter(stableId, ids)));
        }

        @NotNull
        private RecordId writeChildNodes(@Nullable SegmentNodeState before, @NotNull NodeState after)
        throws IOException {
            if (before != null
                    && before.getChildNodeCount(2) > 1
                    && after.getChildNodeCount(2) > 1) {
                return new ChildNodeCollectorDiff(before.getChildNodeMap())
                    .diff(before, after);
            } else {
                return new ChildNodeCollectorDiff()
                    .diff(after);
            }
        }

        /**
         * Try to deduplicate the passed {@code node}. This succeeds if
         * the passed node state has already been persisted to this store and
         * either it has the same generation or it has been already compacted
         * and is still in the de-duplication cache for nodes.
         *
         * @param node The node states to de-duplicate.
         * @return the id of the de-duplicated node or {@code null} if none.
         */
        @Nullable
        private RecordId deduplicateNode(@NotNull NodeState node) {
            if (!(node instanceof SegmentNodeState)) {
                // De-duplication only for persisted node states
                return null;
            }

            SegmentNodeState sns = (SegmentNodeState) node;

            if (!sameStore(sns)) {
                // De-duplication only within same store
                return null;
            }

            if (!isOldGeneration(sns.getRecordId())) {
                // This segment node state is already in this store, no need to
                // write it again
                return sns.getRecordId();
            }

            // This is a segment node state from an old generation. Check
            // whether an equivalent one of the current generation is in the
            // cache
            return nodeCache.get(sns.getStableId());
        }

        /**
         * @param node
         * @return {@code true} iff {@code node} originates from the same
         * segment store.
         */
        private boolean sameStore(SegmentNodeState node) {
            return sameStore(node.getRecordId().getSegmentId());
        }

        /**
         * @param property
         * @return {@code true} iff {@code property} is a {@code
         * SegmentPropertyState} and originates from the same segment store.
         */
        private boolean sameStore(PropertyState property) {
            return (property instanceof SegmentPropertyState)
                    && sameStore(((Record) property).getRecordId().getSegmentId());
        }

        private boolean isOldGeneration(RecordId id) {
            try {
                GCGeneration thatGen = id.getSegmentId().getGcGeneration();
                GCGeneration thisGen = gcGeneration;
                if (thatGen.isCompacted()) {
                    // If the segment containing the base state is compacted it is
                    // only considered old if it is from a earlier full generation.
                    // Otherwise it is from the same tail and it is safe to reference.
                    return thatGen.getFullGeneration() < thisGen.getFullGeneration();
                } else {
                    // If the segment containing the base state is from a regular writer
                    // it is considered old as soon as it is from an earlier generation.
                    return thatGen.compareWith(thisGen) < 0;
                }
            } catch (SegmentNotFoundException snfe) {
                // This SNFE means a defer compacted node state is too far
                // in the past. It has been gc'ed already and cannot be
                // compacted.
                // Consider increasing SegmentGCOptions.getRetainedGenerations()
                throw new SegmentNotFoundException(
                        "Cannot copy record from a generation that has been gc'ed already", snfe);
            }
        }

        private class ChildNodeCollectorDiff extends DefaultNodeStateDiff {
            private final Map<String, RecordId> childNodes = new HashMap<>();

            @Nullable
            private MapRecord base;

            private IOException exception;

            private ChildNodeCollectorDiff(@Nullable MapRecord base) {
                this.base = base;
            }

            private ChildNodeCollectorDiff() {
                this(null);
            }

            public RecordId diff(NodeState after) throws IOException {
                compareAgainstEmptyState(after, this);
                if (exception != null) {
                    throw new IOException(exception);
                }
                return flush();
            }

            public RecordId diff(NodeState before, NodeState after)
            throws IOException {
                after.compareAgainstBaseState(before, this);
                if (exception != null) {
                    throw new IOException(exception);
                }
                return flush();
            }

            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                try {
                    onChildNode(name, writeNode(after, null));
                } catch (IOException e) {
                    exception = e;
                    return false;
                }
                return true;
            }

            @Override
            public boolean childNodeChanged(
                    String name, NodeState before, NodeState after
            ) {
                try {
                    onChildNode(name, writeNode(after, null));
                } catch (IOException e) {
                    exception = e;
                    return false;
                }
                return true;
            }

            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                try {
                    onChildNode(name, null);
                } catch (IOException e) {
                    exception = e;
                    return false;
                }
                return true;
            }

            private void onChildNode(String nodeName, @Nullable RecordId recordId) throws IOException {
                childNodes.put(nodeName, recordId);
                if (childNodes.size() > CHILD_NODE_UPDATE_LIMIT) {
                    flush();
                }
            }

            private RecordId flush() throws IOException {
                RecordId mapId = writeMap(base, childNodes);
                base = reader.readMap(mapId);
                childNodes.clear();
                return mapId;
            }

        }
    }

}
