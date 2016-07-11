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
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.nCopies;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.segment.MapRecord.BUCKETS_PER_LEVEL;
import static org.apache.jackrabbit.oak.segment.RecordWriters.newNodeStateWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.io.Closeables;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.segment.WriteOperationHandler.WriteOperation;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code SegmentWriter} converts nodes, properties, values, etc. to records and
 * persists them with the help of a {@link WriteOperationHandler}.
 * All public methods of this class are thread safe if and only if the
 * {@link WriteOperationHandler} passed to the constructor is thread safe.
 */
public class SegmentWriter {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentWriter.class);

    static final int BLOCK_SIZE = 1 << 12; // 4kB

    @Nonnull
    private final WriterCacheManager cacheManager;

    @Nonnull
    private final SegmentStore store;

    @Nonnull
    private final SegmentReader reader;

    @CheckForNull
    private final BlobStore blobStore;

    @Nonnull
    private final WriteOperationHandler writeOperationHandler;

    @Nonnull
    private final BinaryReferenceConsumer binaryReferenceConsumer;

    /**
     * Create a new instance of a {@code SegmentWriter}. Note the thread safety properties
     * pointed out in the class comment.
     *
     * @param store      store to write to
     * @param reader     segment reader for the {@code store}
     * @param blobStore  the blog store or {@code null} for inlined blobs
     * @param cacheManager  cache manager instance for the de-duplication caches used by this writer
     * @param writeOperationHandler  handler for write operations.
     */
    public SegmentWriter(@Nonnull SegmentStore store,
                         @Nonnull SegmentReader reader,
                         @Nullable BlobStore blobStore,
                         @Nonnull WriterCacheManager cacheManager,
            @Nonnull WriteOperationHandler writeOperationHandler,
            @Nonnull BinaryReferenceConsumer binaryReferenceConsumer
    ) {
        this.store = checkNotNull(store);
        this.reader = checkNotNull(reader);
        this.blobStore = blobStore;
        this.cacheManager = checkNotNull(cacheManager);
        this.writeOperationHandler = checkNotNull(writeOperationHandler);
        this.binaryReferenceConsumer = checkNotNull(binaryReferenceConsumer);
    }

    public void flush() throws IOException {
        writeOperationHandler.flush();
    }

    /**
     * Write a map record.
     * @param base      base map relative to which the {@code changes} are applied ot
     *                  {@code null} for the empty map.
     * @param changes   the changed mapping to apply to the {@code base} map.
     * @return          the map record written
     * @throws IOException
     */
    @Nonnull
    public MapRecord writeMap(@Nullable final MapRecord base,
                              @Nonnull final Map<String, RecordId> changes)
    throws IOException {
        RecordId mapId = writeOperationHandler.execute(new SegmentWriteOperation() {
            @Override
            public RecordId execute(SegmentBufferWriter writer) throws IOException {
                return with(writer).writeMap(base, changes);
            }
        });
        return new MapRecord(reader, mapId);
    }

    /**
     * Write a list record.
     * @param list  the list to write.
     * @return      the record id of the list written
     * @throws IOException
     */
    @Nonnull
    public RecordId writeList(@Nonnull final List<RecordId> list) throws IOException {
        return writeOperationHandler.execute(new SegmentWriteOperation() {
            @Override
            public RecordId execute(SegmentBufferWriter writer) throws IOException {
                return with(writer).writeList(list);
            }
        });
    }

    /**
     * Write a string record.
     * @param string  the string to write.
     * @return         the record id of the string written.
     * @throws IOException
     */
    @Nonnull
    public RecordId writeString(@Nonnull final String string) throws IOException {
        return writeOperationHandler.execute(new SegmentWriteOperation() {
            @Override
            public RecordId execute(SegmentBufferWriter writer) throws IOException {
                return with(writer).writeString(string);
            }
        });
    }

    /**
     * Write a blob (as list of block records)
     * @param blob  blob to write
     * @return      The segment blob written
     * @throws IOException
     */
    @Nonnull
    public SegmentBlob writeBlob(@Nonnull final Blob blob) throws IOException {
        RecordId blobId = writeOperationHandler.execute(new SegmentWriteOperation() {
            @Override
            public RecordId execute(SegmentBufferWriter writer) throws IOException {
                return with(writer).writeBlob(blob);
            }
        });
        return new SegmentBlob(blobStore, blobId);
    }

    /**
     * Writes a block record containing the given block of bytes.
     *
     * @param bytes source buffer
     * @param offset offset within the source buffer
     * @param length number of bytes to write
     * @return block record identifier
     */
    @Nonnull
    public RecordId writeBlock(@Nonnull final byte[] bytes, final int offset, final int length)
    throws IOException {
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
    @Nonnull
    public SegmentBlob writeStream(@Nonnull final InputStream stream) throws IOException {
        RecordId blobId = writeOperationHandler.execute(new SegmentWriteOperation() {
            @Override
            public RecordId execute(SegmentBufferWriter writer) throws IOException {
                return with(writer).writeStream(stream);
            }
        });
        return new SegmentBlob(blobStore, blobId);
    }

    /**
     * Write a property.
     * @param state  the property to write
     * @return       the property state written
     * @throws IOException
     */
    @Nonnull
    public SegmentPropertyState writeProperty(@Nonnull final PropertyState state)
    throws IOException {
        RecordId id = writeOperationHandler.execute(new SegmentWriteOperation() {
            @Override
            public RecordId execute(SegmentBufferWriter writer) throws IOException {
                return with(writer).writeProperty(state);
            }
        });
        return new SegmentPropertyState(reader, id, state.getName(), state.getType());
    }

    /**
     * Write a node state
     * @param state node state to write
     * @return segment node state equal to {@code state}
     * @throws IOException
     */
    @Nonnull
    public SegmentNodeState writeNode(@Nonnull final NodeState state) throws IOException {
        RecordId nodeId = writeOperationHandler.execute(new SegmentWriteOperation() {
            @Override
            public RecordId execute(SegmentBufferWriter writer) throws IOException {
                return new CompactionStats(writeNodeStats, compactNodeStats, false)
                        .writeNode(this, writer, state);
            }
        });
        return new SegmentNodeState(reader, this, nodeId);
    }

    /**
     * Write a node state, unless cancelled using a dedicated write operation handler.
     * The write operation handler is automatically {@link WriteOperationHandler#flush() flushed}
     * once the node has been written successfully.
     * @param state   node state to write
     * @param writeOperationHandler  the write operation handler through which all write calls
     *                               induced by by this call are routed.
     * @param cancel  supplier to signal cancellation of this write operation
     * @return segment node state equal to {@code state} or {@code null} if cancelled.
     * @throws IOException
     */
    @CheckForNull
    public SegmentNodeState writeNode(@Nonnull final NodeState state,
                                      @Nonnull WriteOperationHandler writeOperationHandler,
                                      @Nonnull Supplier<Boolean> cancel)
    throws IOException {
        try {
            RecordId nodeId = writeOperationHandler.execute(new SegmentWriteOperation(cancel) {
                @Override
                public RecordId execute(SegmentBufferWriter writer) throws IOException {
                    return new CompactionStats(writeNodeStats, compactNodeStats, true)
                            .writeNode(this, writer, state);
                }
            });
            writeOperationHandler.flush();
            return new SegmentNodeState(reader, this, nodeId);
        } catch (SegmentWriteOperation.CancelledWriteException ignore) {
            return null;
        }
    }

    private final SynchronizedDescriptiveStatistics writeNodeStats = new SynchronizedDescriptiveStatistics();
    private final SynchronizedDescriptiveStatistics compactNodeStats = new SynchronizedDescriptiveStatistics();

    // FIXME OAK-4445: Collect write statistics: clean this up:
    // - It should be possible to switch the statistics on/off. There should be no
    // performance penalty when off.
    // - Expose via logging and/or MBean?
    // - What metrics should we collect? Use the Metrics API!?
    // - Decouple this from the SegmentWriter
    private static class CompactionStats {
        @Nonnull
        private final SynchronizedDescriptiveStatistics writeNodeStats;
        @Nonnull
        private final SynchronizedDescriptiveStatistics compactNodeStats;

        /*
         * {@code true} iff this is an explicit compaction (vs. an implicit
         * and deferred one triggered by a commit referring to an "old" base
         * state.
         */
        private final boolean isCompaction;

        /*
         * Total number of nodes in the subtree rooted at the node passed
         * to {@link #writeNode(SegmentWriteOperation, SegmentBufferWriter, NodeState)}
         */
        public int nodeCount;

        /*
         * Number of cache hits for a deferred compacted node
         */
        public int cacheHits;

        /*
         * Number of cache misses for a deferred compacted node
         */
        public int cacheMiss;

        /*
         * Number of nodes that where de-duplicated as the store already contained
         * them.
         */
        public int deDupNodes;

        /*
         * Number of nodes that actually had to be written as there was no de-duplication
         * and a cache miss (in case of a deferred compaction).
         */
        public int writesOps;

        public CompactionStats(
                @Nonnull SynchronizedDescriptiveStatistics writeNodeStats,
                @Nonnull SynchronizedDescriptiveStatistics compactNodeStats,
                boolean isCompaction) {
            this.writeNodeStats = writeNodeStats;
            this.compactNodeStats = compactNodeStats;
            this.isCompaction = isCompaction;
        }

        /*
         * The operation caused a deferred compaction iff it accessed the cache.
         */
        public boolean isDeferredCompactionOp() {
            return cacheHits + cacheMiss > 0;
        }

        @Nonnull
        public RecordId writeNode(
                @Nonnull SegmentWriteOperation op,
                @Nonnull SegmentBufferWriter writer,
                @Nonnull NodeState state)
        throws IOException {
            long t = System.nanoTime();
            try {
                return op.with(writer).with(this).writeNode(state, 0);
            } finally {
                if (isCompaction) {
                    LOG.info("Write node stats: {}", writeNodeStats);
                    LOG.info("Compact node stats: {}", compactNodeStats);
                    writeNodeStats.clear();
                    compactNodeStats.clear();
                } else {
                    if (isDeferredCompactionOp()) {
                        compactNodeStats.addValue(System.nanoTime() - t);
                        LOG.info(toString());
                    } else {
                        writeNodeStats.addValue(System.nanoTime() - t);
                    }
                }
            }
        }

        @Override
        public String toString() {
            return "NodeStats{" +
                "op=" + (isDeferredCompactionOp() ? "compact" : "write") +
                ", nodeCount=" + nodeCount +
                ", writeOps=" + writesOps +
                ", deDupNodes=" + deDupNodes +
                ", cacheHits=" + cacheHits +
                ", cacheMiss=" + cacheMiss +
                ", hitRate=" + (100*(double) cacheHits / ((double) cacheHits + (double) cacheMiss)) +
                '}';
        }
    }

    /**
     * This {@code WriteOperation} implementation is used internally to provide
     * context to a recursive chain of calls without having pass the context
     * as a separate argument (a poor mans monad). As such it is entirely
     * <em>not thread safe</em>.
     */
    private abstract class SegmentWriteOperation implements WriteOperation {

        /**
         * This exception is used internally to signal cancellation of a (recursive)
         * write node operation.
         */
        private class CancelledWriteException extends IOException {
            public CancelledWriteException() {
                super("Cancelled write operation");
            }
        }

        @Nonnull
        private final Supplier<Boolean> cancel;

        @CheckForNull
        private CompactionStats compactionStats;

        private SegmentBufferWriter writer;
        private RecordCache<String> stringCache;
        private RecordCache<Template> templateCache;
        private NodeCache nodeCache;

        protected SegmentWriteOperation(@Nonnull Supplier<Boolean> cancel) {
            this.cancel = cancel;
        }

        protected SegmentWriteOperation() {
            this(Suppliers.ofInstance(false));
        }

        @Override
        public abstract RecordId execute(SegmentBufferWriter writer) throws IOException;

        @Nonnull
        SegmentWriteOperation with(@Nonnull SegmentBufferWriter writer) {
            checkState(this.writer == null);
            this.writer = writer;
            int generation = writer.getGeneration();
            this.stringCache = cacheManager.getStringCache(generation);
            this.templateCache = cacheManager.getTemplateCache(generation);
            this.nodeCache = cacheManager.getNodeCache(generation);
            return this;
        }

        @Nonnull
        SegmentWriteOperation with(@Nonnull CompactionStats compactionStats) {
            this.compactionStats = compactionStats;
            return this;
        }

        private RecordId writeMap(@Nullable MapRecord base,
                                  @Nonnull Map<String, RecordId> changes)
        throws IOException {
            if (base != null && base.isDiff()) {
                Segment segment = base.getSegment();
                RecordId key = segment.readRecordId(base.getOffset(8));
                String name = reader.readString(key);
                if (!changes.containsKey(name)) {
                    changes.put(name, segment.readRecordId(base.getOffset(8, 1)));
                }
                base = new MapRecord(reader, segment.readRecordId(base.getOffset(8, 2)));
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
                            return RecordWriters.newMapBranchWriter(entry.getHash(), asList(entry.getKey(),
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
                    entries.add(new MapEntry(reader, key, keyId, entry.getValue()));
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
            return RecordWriters.newMapLeafWriter(level, entries).write(writer);
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
            return RecordWriters.newMapBranchWriter(level, size, bitmap, bucketIds).write(writer);
        }

        private RecordId writeMapBucket(MapRecord base, Collection<MapEntry> entries, int level)
                throws IOException {
            // when no changed entries, return the base map (if any) as-is
            if (entries == null || entries.isEmpty()) {
                if (base != null) {
                    return base.getRecordId();
                } else if (level == 0) {
                    return RecordWriters.newMapLeafWriter().write(writer);
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
            return id == null ? null : new MapRecord(reader, id);
        }

        /**
         * Writes a list record containing the given list of record identifiers.
         *
         * @param list list of record identifiers
         * @return list record identifier
         */
        private RecordId writeList(@Nonnull List<RecordId> list) throws IOException {
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
            return RecordWriters.newListBucketWriter(bucket).write(writer);
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
            return RecordWriters.newValueWriter(blocks, len).write(writer);
        }

        private RecordId writeValueRecord(int length, byte... data) throws IOException {
            checkArgument(length < Segment.MEDIUM_LIMIT);
            return RecordWriters.newValueWriter(length, data).write(writer);
        }

        /**
         * Writes a string value record.
         *
         * @param string string to be written
         * @return value record identifier
         */
        private RecordId writeString(@Nonnull String string) throws IOException {
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
            while (pos + Segment.MAX_SEGMENT_SIZE <= data.length) {
                SegmentId bulkId = store.newBulkSegmentId();
                store.writeSegment(bulkId, data, pos, Segment.MAX_SEGMENT_SIZE);
                for (int i = 0; i < Segment.MAX_SEGMENT_SIZE; i += BLOCK_SIZE) {
                    blockIds.add(new RecordId(bulkId, i));
                }
                pos += Segment.MAX_SEGMENT_SIZE;
            }

            // inline the remaining data as block records
            while (pos < data.length) {
                int len = Math.min(BLOCK_SIZE, data.length - pos);
                blockIds.add(writeBlock(data, pos, len));
                pos += len;
            }

            return writeValueRecord(data.length, writeList(blockIds));
        }

        private boolean sameStore(SegmentId id) {
            return id.sameStore(store);
        }

        /**
         * @param   blob
         * @return  {@code true} iff {@code blob} is a {@code SegmentBlob}
         *          and originates from the same segment store.
         */
        private boolean sameStore(Blob blob) {
            return (blob instanceof SegmentBlob)
                    && sameStore(((Record) blob).getRecordId().getSegmentId());
        }

        private RecordId writeBlob(@Nonnull Blob blob) throws IOException {
            if (sameStore(blob)) {
                SegmentBlob segmentBlob = (SegmentBlob) blob;
                if (!isOldGeneration(segmentBlob.getRecordId())) {
                    return segmentBlob.getRecordId();
                }
                if (segmentBlob.isExternal()) {
                    return writeBlobId(segmentBlob.getBlobId());
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
         * Write a reference to an external blob. This method handles blob IDs of
         * every length, but behaves differently for small and large blob IDs.
         *
         * @param blobId Blob ID.
         * @return Record ID pointing to the written blob ID.
         * @see Segment#BLOB_ID_SMALL_LIMIT
         */
        private RecordId writeBlobId(String blobId) throws IOException {
            byte[] data = blobId.getBytes(UTF_8);

            RecordId recordId;

            if (data.length < Segment.BLOB_ID_SMALL_LIMIT) {
                recordId = RecordWriters.newBlobIdWriter(data).write(writer);
            } else {
                recordId = RecordWriters.newBlobIdWriter(writeString(blobId)).write(writer);
            }

            binaryReferenceConsumer.consume(writer.getGeneration(), blobId);

            return recordId;
        }

        private RecordId writeBlock(@Nonnull byte[] bytes, int offset, int length)
        throws IOException {
            checkNotNull(bytes);
            checkPositionIndexes(offset, offset + length, bytes.length);
            return RecordWriters.newBlockWriter(bytes, offset, length).write(writer);
        }

        private RecordId writeStream(@Nonnull InputStream stream) throws IOException {
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

        private RecordId internalWriteStream(@Nonnull InputStream stream) throws IOException {
            // Special case for short binaries (up to about 16kB):
            // store them directly as small- or medium-sized value records
            byte[] data = new byte[Segment.MEDIUM_LIMIT];
            int n = read(stream, data, 0, data.length);
            if (n < Segment.MEDIUM_LIMIT) {
                return writeValueRecord(n, data);
            }

            if (blobStore != null) {
                String blobId = blobStore.writeBlob(new SequenceInputStream(
                    new ByteArrayInputStream(data, 0, n), stream));
                return writeBlobId(blobId);
            }

            data = Arrays.copyOf(data, Segment.MAX_SEGMENT_SIZE);
            n += read(stream, data, n, Segment.MAX_SEGMENT_SIZE - n);
            long length = n;
            List<RecordId> blockIds =
                newArrayListWithExpectedSize(2 * n / BLOCK_SIZE);

            // Write the data to bulk segments and collect the list of block ids
            while (n != 0) {
                SegmentId bulkId = store.newBulkSegmentId();
                int len = Segment.align(n, 1 << Segment.RECORD_ALIGN_BITS);
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

        private RecordId writeProperty(@Nonnull PropertyState state) throws IOException {
            Map<String, RecordId> previousValues = emptyMap();
            return writeProperty(state, previousValues);
        }

        private RecordId writeProperty(@Nonnull PropertyState state,
                                       @Nonnull Map<String, RecordId> previousValues)
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
                return RecordWriters.newListWriter().write(writer);
            } else {
                return RecordWriters.newListWriter(count, writeList(valueIds)).write(writer);
            }
        }

        private RecordId writeTemplate(Template template) throws IOException {
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
            if (propertyNames.length > 0) {
                propNamesId = writeList(asList(propertyNames));
                ids.add(propNamesId);
            }

            checkState(propertyNames.length < (1 << 18));
            head |= propertyNames.length;

            RecordId tid = RecordWriters.newTemplateWriter(ids, propertyNames,
                propertyTypes, head, primaryId, mixinIds, childNameId,
                propNamesId).write(writer);
            templateCache.put(template, tid);
            return tid;
        }

        private RecordId writeNode(@Nonnull NodeState state, int depth) throws IOException {
            if (cancel.get()) {
                // Poor man's Either Monad
                throw new CancelledWriteException();
            }
            assert compactionStats != null;
            compactionStats.nodeCount++;
            if (state instanceof SegmentNodeState) {
                SegmentNodeState sns = ((SegmentNodeState) state);
                if (sameStore(sns)) {
                    // This is a segment node state from an old generation. Check whether
                    // an equivalent one of the current generation is in the cache
                    if (isOldGeneration(sns.getRecordId())) {
                        RecordId cachedId = nodeCache.get(sns.getStableId());
                        if (cachedId != null) {
                            compactionStats.cacheHits++;
                            return cachedId;
                        } else {
                            compactionStats.cacheMiss++;
                        }
                    } else {
                        // This segment node state is already in this store,
                        // no need to write it again,
                        compactionStats.deDupNodes++;
                        return sns.getRecordId();
                    }
                }
            }

            compactionStats.writesOps++;
            RecordId recordId = writeNodeUncached(state, depth);
            if (state instanceof SegmentNodeState) {
                // This node state has been rewritten because it is from an older
                // generation (e.g. due to compaction). Put it into the cache for
                // deduplication of hard links to it (e.g. checkpoints).
                SegmentNodeState sns = (SegmentNodeState) state;
                nodeCache.put(sns.getStableId(), recordId, depth);
            }
            return recordId;
        }

        private RecordId writeNodeUncached(@Nonnull NodeState state, int depth) throws IOException {
            SegmentNodeState before = null;
            Template beforeTemplate = null;
            ModifiedNodeState after = null;
            if (state instanceof ModifiedNodeState) {
                after = (ModifiedNodeState) state;
                NodeState base = after.getBaseState();
                if (base instanceof SegmentNodeState) {
                    SegmentNodeState sns = ((SegmentNodeState) base);
                    if (sameStore(sns)) {
                        if (!isOldGeneration(sns.getRecordId())) {
                            before = sns;
                            beforeTemplate = before.getTemplate();
                        }
                    }
                }
            }

            List<RecordId> ids = newArrayList();
            Template template = new Template(reader, state);
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
                assert property != null;

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

            RecordId stableId = null;
            if (state instanceof SegmentNodeState) {
                byte[] id = ((Record) state).getRecordId().getBytes();
                stableId = writeBlock(id, 0, id.length);
            }
            return newNodeStateWriter(stableId, ids).write(writer);
        }

        /**
         * @param   node
         * @return  {@code true} iff {@code node} originates from the same segment store.
         */
        private boolean sameStore(SegmentNodeState node) {
            return sameStore(node.getRecordId().getSegmentId());
        }

        /**
         * @param property
         * @return  {@code true} iff {@code property} is a {@code SegmentPropertyState}
         *          and originates from the same segment store.
         */
        private boolean sameStore(PropertyState property) {
            return (property instanceof SegmentPropertyState)
                && sameStore(((Record) property).getRecordId().getSegmentId());
        }

        private boolean isOldGeneration(RecordId id) {
            try {
                int thatGen = id.getSegment().getGcGeneration();
                int thisGen = writer.getGeneration();
                return thatGen < thisGen;
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

}
