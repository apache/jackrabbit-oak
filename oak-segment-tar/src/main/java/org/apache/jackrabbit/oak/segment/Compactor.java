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
package org.apache.jackrabbit.oak.segment;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static java.lang.Long.numberOfLeadingZeros;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.file.PriorityCache.nextPowerOfTwo;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Supplier;
import com.google.common.hash.Hashing;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.PriorityCache;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool for compacting segments.
 */
public class Compactor {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(Compactor.class);

    private static boolean eagerFlush = Boolean.getBoolean("oak.compaction.eagerFlush");

    static {
        if (eagerFlush) {
            log.debug("Eager flush enabled.");
        }
    }

    private final SegmentReader reader;

    private final BlobStore blobStore;

    private final SegmentWriter writer;

    private final ProgressTracker progress = new ProgressTracker();

    /**
     * Enables content based de-duplication of binaries. Involves a fair amount
     * of I/O when reading/comparing potentially equal blobs.
     */
    private final boolean binaryDedup;

    /**
     * Set the upper bound for the content based de-duplication checks.
     */
    private final long binaryDedupMaxSize;

    /**
     * Map from {@link #getBlobKey(Blob) blob keys} to matching compacted blob
     * record identifiers. Used to de-duplicate copies of the same binary
     * values.
     */
    private final Map<String, List<RecordId>> binaries = newHashMap();

    /**
     * Flag to use content equality verification before actually compacting the
     * state, on the childNodeChanged diff branch (Used in Backup scenario)
     */
    private boolean contentEqualityCheck;

    /**
     * Allows the cancellation of the compaction process. If this
     * {@code Supplier} returns {@code true}, this compactor will cancel
     * compaction and return a partial {@code SegmentNodeState} containing the
     * changes compacted before the cancellation.
     */
    private final Supplier<Boolean> cancel;

    private static final int cacheSize;

    static {
        Integer ci = Integer.getInteger("compress-interval");
        Integer size = Integer.getInteger("oak.segment.compaction.cacheSize");
        if (size != null) {
            cacheSize = size;
        } else if (ci != null) {
            log.warn("Deprecated argument 'compress-interval', please use 'oak.segment.compaction.cacheSize' instead.");
            cacheSize = ci;
        } else {
            cacheSize = 100000;
        }
    }

    /**
     * Deduplication cache for blobs. 10% of the total cache size.
     */
    private final PriorityCache<RecordId, RecordId> blobCache =
            new PriorityCache<>((int) nextPowerOfTwo(cacheSize/10));

    /**
     * Deduplication cache for nodes. 90% of the total cache size.
     */
    private final PriorityCache<RecordId, RecordId> nodeCache =
            new PriorityCache<>((int) nextPowerOfTwo(cacheSize/10*9));

    public Compactor(SegmentReader reader, SegmentWriter writer,
            BlobStore blobStore, Supplier<Boolean> cancel, SegmentGCOptions gc) {
        this.reader = reader;
        this.writer = writer;
        this.blobStore = blobStore;
        this.cancel = cancel;
        this.binaryDedup = gc.isBinaryDeduplication();
        this.binaryDedupMaxSize = gc.getBinaryDeduplicationMaxSize();
    }

    private SegmentNodeBuilder process(NodeState before, NodeState after,
            NodeState onto) throws IOException {
        SegmentNodeBuilder builder = new SegmentNodeBuilder(
                writer.writeNode(onto), writer);
        new CompactDiff(builder).diff(before, after);
        return builder;
    }

    /**
     * Compact the differences between a {@code before} and a {@code after} on
     * top of an {@code onto} state.
     * 
     * @param before
     *            the before state
     * @param after
     *            the after state
     * @param onto
     *            the onto state
     * @return the compacted state
     */
    public SegmentNodeState compact(NodeState before, NodeState after,
            NodeState onto) throws IOException {
        progress.start();
        SegmentNodeState compacted = process(before, after, onto)
                .getNodeState();
        writer.flush();
        progress.stop();
        return compacted;
    }

    private class CompactDiff extends ApplyDiff {
        private IOException exception;

        /**
         * Current processed path, or null if the trace log is not enabled at
         * the beginning of the compaction call. The null check will also be
         * used to verify if a trace log will be needed or not
         */
        private final String path;

        CompactDiff(NodeBuilder builder) {
            super(builder);
            if (log.isTraceEnabled()) {
                this.path = "/";
            } else {
                this.path = null;
            }
        }

        private CompactDiff(NodeBuilder builder, String path, String childName) {
            super(builder);
            if (path != null) {
                this.path = concat(path, childName);
            } else {
                this.path = null;
            }
        }

        boolean diff(NodeState before, NodeState after) throws IOException {
            boolean success = after.compareAgainstBaseState(before,
                    new CancelableDiff(this, cancel));
            if (exception != null) {
                throw new IOException(exception);
            }
            return success;
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            if (path != null) {
                log.trace("propertyAdded {}/{}", path, after.getName());
            }
            progress.onProperty();
            return super.propertyAdded(compact(after));
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            if (path != null) {
                log.trace("propertyChanged {}/{}", path, after.getName());
            }
            progress.onProperty();
            return super.propertyChanged(before, compact(after));
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (path != null) {
                log.trace("childNodeAdded {}/{}", path, name);
            }

            RecordId id = null;
            if (after instanceof SegmentNodeState) {
                id = ((SegmentNodeState) after).getRecordId();
                RecordId compactedId = nodeCache.get(id, 0);
                if (compactedId != null) {
                    builder.setChildNode(name, new SegmentNodeState(reader,
                            writer, compactedId));
                    return true;
                }
            }

            progress.onNode();
            try {
                NodeBuilder child;
                if (eagerFlush) {
                    child = builder.setChildNode(name);
                } else {
                    child = EMPTY_NODE.builder();
                }
                boolean success = new CompactDiff(child, path, name).diff(
                        EMPTY_NODE, after);
                if (success) {
                    SegmentNodeState state = writer.writeNode(child.getNodeState());
                    builder.setChildNode(name, state);
                    if (id != null) {
                        nodeCache.put(id, state.getRecordId(), 0, cost(state));
                    }
                }
                return success;
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before,
                NodeState after) {
            if (path != null) {
                log.trace("childNodeChanged {}/{}", path, name);
            }

            RecordId id = null;
            if (after instanceof SegmentNodeState) {
                id = ((SegmentNodeState) after).getRecordId();
                RecordId compactedId = nodeCache.get(id, 0);
                if (compactedId != null) {
                    builder.setChildNode(name, new SegmentNodeState(reader,
                            writer, compactedId));
                    return true;
                }
            }

            if (contentEqualityCheck && before.equals(after)) {
                return true;
            }

            progress.onNode();
            try {
                NodeBuilder child = builder.getChildNode(name);
                boolean success = new CompactDiff(child, path, name).diff(
                        before, after);
                if (success) {
                    SegmentNodeState state = writer.writeNode(child.getNodeState());
                    if (id != null) {
                        nodeCache.put(id, state.getRecordId(), 0, cost(state));
                    }
                }
                return success;
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }
    }

    private static byte cost(SegmentNodeState node) {
        long childCount = node.getChildNodeCount(Long.MAX_VALUE);
        return cost(childCount);
    }

    private static byte cost(SegmentBlob blob) {
        long length = blob.length();
        return cost(length);
    }

    private static byte cost(long n) {
        return (byte) (Byte.MIN_VALUE + 64 - numberOfLeadingZeros(n));
    }

    private PropertyState compact(PropertyState property) {
        String name = property.getName();
        Type<?> type = property.getType();
        if (type == BINARY) {
            Blob blob = compact(property.getValue(Type.BINARY));
            return BinaryPropertyState.binaryProperty(name, blob);
        } else if (type == BINARIES) {
            List<Blob> blobs = new ArrayList<Blob>();
            for (Blob blob : property.getValue(BINARIES)) {
                blobs.add(compact(blob));
            }
            return MultiBinaryPropertyState.binaryPropertyFromBlob(name, blobs);
        } else {
            Object value = property.getValue(type);
            return PropertyStates.createProperty(name, value, type);
        }
    }

    /**
     * Compacts (and de-duplicates) the given blob.
     * 
     * @param blob
     *            blob to be compacted
     * @return compacted blob
     */
    private Blob compact(Blob blob) {
        if (blob instanceof SegmentBlob) {
            SegmentBlob sb = (SegmentBlob) blob;
            try {
                // Check if we've already cloned this specific record
                RecordId id = sb.getRecordId();

                // TODO verify binary impact on cache
                RecordId compactedId = blobCache.get(id, 0);
                if (compactedId != null) {
                    return new SegmentBlob(blobStore, compactedId);
                }

                progress.onBinary();

                // if the blob is external, just clone it
                if (sb.isExternal()) {
                    return writer.writeBlob(sb);
                }
                // if the blob is inlined, just clone it
                if (sb.length() < Segment.MEDIUM_LIMIT) {
                    SegmentBlob clone = writer.writeBlob(blob);
                    blobCache.put(id, clone.getRecordId(), 0, cost(clone));
                    return clone;
                }

                List<RecordId> ids = null;
                String key = null;
                boolean dedup = binaryDedup
                        && blob.length() <= binaryDedupMaxSize;
                if (dedup) {
                    // alternatively look if the exact same binary has been
                    // cloned
                    key = getBlobKey(blob);
                    ids = binaries.get(key);
                    if (ids != null) {
                        for (RecordId duplicateId : ids) {
                            if (new SegmentBlob(blobStore, duplicateId)
                                    .equals(sb)) {
                                return new SegmentBlob(blobStore, duplicateId);
                            }
                        }
                    }
                }

                // if not, clone the large blob and keep track of the result
                sb = writer.writeBlob(blob);
                blobCache.put(id, sb.getRecordId(), 0, cost(sb));

                if (dedup) {
                    if (ids == null) {
                        ids = newArrayList();
                        binaries.put(key, ids);
                    }
                    ids.add(sb.getRecordId());
                }

                return sb;
            } catch (IOException e) {
                log.warn("Failed to compact a blob", e);
                // fall through
            }
        }

        // no way to compact this blob, so we'll just keep it as-is
        return blob;
    }

    private static String getBlobKey(Blob blob) throws IOException {
        InputStream stream = blob.getNewStream();
        try {
            byte[] buffer = new byte[SegmentWriter.BLOCK_SIZE];
            int n = IOUtils.readFully(stream, buffer, 0, buffer.length);
            return blob.length() + ":" + Hashing.sha1().hashBytes(buffer, 0, n);
        } finally {
            stream.close();
        }
    }

    private static class ProgressTracker {
        private final long logAt = Long.getLong("compaction-progress-log",
                150000);

        private long start = 0;

        private long nodes = 0;
        private long properties = 0;
        private long binaries = 0;

        void start() {
            nodes = 0;
            properties = 0;
            binaries = 0;
            start = System.currentTimeMillis();
        }

        void onNode() {
            if (++nodes % logAt == 0) {
                logProgress(start, false);
                start = System.currentTimeMillis();
            }
        }

        void onProperty() {
            properties++;
        }

        void onBinary() {
            binaries++;
        }

        void stop() {
            logProgress(start, true);
        }

        private void logProgress(long start, boolean done) {
            log.debug(
                    "Compacted {} nodes, {} properties, {} binaries in {} ms.",
                    nodes, properties, binaries, System.currentTimeMillis()
                            - start);
            if (done) {
                log.info(
                        "Finished compaction: {} nodes, {} properties, {} binaries.",
                        nodes, properties, binaries);
            }
        }
    }

    public void setContentEqualityCheck(boolean contentEqualityCheck) {
        this.contentEqualityCheck = contentEqualityCheck;
    }

}
