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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.hash.Hashing;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool for compacting segments.
 */
public class Compactor {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(Compactor.class);

    /**
     * Locks down the RecordId persistence structure
     */
    static long[] recordAsKey(RecordId r) {
        return new long[] { r.getSegmentId().getMostSignificantBits(),
                r.getSegmentId().getLeastSignificantBits(), r.getOffset() };
    }

    private final SegmentWriter writer;

    private final PartialCompactionMap map;

    /**
     * Filters nodes that will be included in the compaction map, allowing for
     * optimization in case of an offline compaction
     */
    private Predicate<NodeState> includeInMap = Predicates.alwaysTrue();

    private final ProgressTracker progress = new ProgressTracker();

    /**
     * Map from {@link #getBlobKey(Blob) blob keys} to matching compacted
     * blob record identifiers. Used to de-duplicate copies of the same
     * binary values.
     */
    private final Map<String, List<RecordId>> binaries = newHashMap();

    /**
     * If the compactor should copy large binaries as streams or just copy the
     * refs
     */
    private final boolean cloneBinaries;

    /**
     * Allows the cancellation of the compaction process. If this {@code
     * Supplier} returns {@code true}, this compactor will cancel compaction and
     * return a partial {@code SegmentNodeState} containing the changes
     * compacted before the cancellation.
     */
    private final Supplier<Boolean> cancel;

    public Compactor(SegmentStore store) {
        this(store, Suppliers.ofInstance(false));
    }

    public Compactor(SegmentStore store, Supplier<Boolean> cancel) {
        this.writer = store.getTracker().getWriter();
        this.map = new InMemoryCompactionMap(store.getTracker());
        this.cloneBinaries = false;
        this.cancel = cancel;
    }

    public Compactor(FileStore store, CompactionStrategy compactionStrategy) {
        this(store, compactionStrategy, Suppliers.ofInstance(false));
    }

    public Compactor(FileStore store, CompactionStrategy compactionStrategy, Supplier<Boolean> cancel) {
        String wid = "c-" + store.getTracker().getCompactionMap().getGeneration() + 1;
        this.writer = store.createSegmentWriter(wid);
        if (compactionStrategy.getPersistCompactionMap()) {
            this.map = new PersistedCompactionMap(store);
        } else {
            this.map = new InMemoryCompactionMap(store.getTracker());
        }
        this.cloneBinaries = compactionStrategy.cloneBinaries();
        if (compactionStrategy.isOfflineCompaction()) {
            includeInMap = new OfflineCompactionPredicate();
        }
        this.cancel = cancel;
    }

    protected SegmentNodeBuilder process(NodeState before, NodeState after, NodeState onto) {
        SegmentNodeBuilder builder = new SegmentNodeBuilder(writer.writeNode(onto), writer);
        after.compareAgainstBaseState(before, newCompactionDiff(builder));
        return builder;
    }

    /**
     * Compact the differences between a {@code before} and a {@code after}
     * on top of the {@code before} state.
     * <p>
     * Equivalent to {@code compact(before, after, before)}
     *
     * @param before  the before state
     * @param after   the after state
     * @return  the compacted state
     */
    public SegmentNodeState compact(NodeState before, NodeState after) {
        progress.start();
        SegmentNodeState compacted = process(before, after, before).getNodeState();
        writer.flush();
        progress.stop();
        return compacted;
    }

    /**
     * Compact the differences between a {@code before} and a {@code after}
     * on top of an {@code onto} state.
     * @param before  the before state
     * @param after   the after state
     * @param onto    the onto state
     * @return  the compacted state
     */
    public SegmentNodeState compact(NodeState before, NodeState after, NodeState onto) {
        progress.start();
        SegmentNodeState compacted = process(before, after, onto).getNodeState();
        writer.flush();
        progress.stop();
        return compacted;
    }

    public PartialCompactionMap getCompactionMap() {
        map.compress();
        return map;
    }

    private class CompactDiff extends ApplyDiff {

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
                RecordId compactedId = map.get(id);
                if (compactedId != null) {
                    builder.setChildNode(name, new SegmentNodeState(compactedId));
                    return true;
                }
            }

            progress.onNode();
            NodeBuilder child = EmptyNodeState.EMPTY_NODE.builder();
            boolean success = EmptyNodeState.compareAgainstEmptyState(after,
                    newCompactionDiff(child, path, name));

            if (success) {
                SegmentNodeState state = writer.writeNode(child.getNodeState());
                builder.setChildNode(name, state);
                if (id != null && includeInMap.apply(state)) {
                    map.put(id, state.getRecordId());
                }
            }

            return success;
        }

        @Override
        public boolean childNodeChanged(
                String name, NodeState before, NodeState after) {
            if (path != null) {
                log.trace("childNodeChanged {}/{}", path, name);
            }

            RecordId id = null;
            if (after instanceof SegmentNodeState) {
                id = ((SegmentNodeState) after).getRecordId();
                RecordId compactedId = map.get(id);
                if (compactedId != null) {
                    builder.setChildNode(name, new SegmentNodeState(compactedId));
                    return true;
                }
            }

            progress.onNode();
            NodeBuilder child = builder.getChildNode(name);
            boolean success = after.compareAgainstBaseState(before,
                    newCompactionDiff(child, path, name));

            if (success) {
                RecordId compactedId = writer.writeNode(child.getNodeState())
                        .getRecordId();
                if (id != null) {
                    map.put(id, compactedId);
                }
            }

            return success;
        }

    }

    private NodeStateDiff newCompactionDiff(NodeBuilder builder) {
        return new CancelableDiff(new CompactDiff(builder), cancel);
    }

    private NodeStateDiff newCompactionDiff(NodeBuilder child, String path, String name) {
        return new CancelableDiff(new CompactDiff(child, path, name), cancel);
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
     * @param blob blob to be compacted
     * @return compacted blob
     */
    private Blob compact(Blob blob) {
        if (blob instanceof SegmentBlob) {
            SegmentBlob sb = (SegmentBlob) blob;
            try {
                // Check if we've already cloned this specific record
                RecordId id = sb.getRecordId();
                RecordId compactedId = map.get(id);
                if (compactedId != null) {
                    return new SegmentBlob(compactedId);
                }

                progress.onBinary();

                // if the blob is inlined or external, just clone it
                if (sb.isExternal() || sb.length() < Segment.MEDIUM_LIMIT) {
                    SegmentBlob clone = sb.clone(writer, cloneBinaries);
                    map.put(id, clone.getRecordId());
                    return clone;
                }

                // alternatively look if the exact same binary has been cloned
                String key = getBlobKey(blob);
                List<RecordId> ids = binaries.get(key);
                if (ids != null) {
                    for (RecordId duplicateId : ids) {
                        if (new SegmentBlob(duplicateId).equals(sb)) {
                            map.put(id, duplicateId);
                            return new SegmentBlob(duplicateId);
                        }
                    }
                }

                // if not, clone the blob and keep track of the result
                sb = sb.clone(writer, cloneBinaries);
                map.put(id, sb.getRecordId());
                if (ids == null) {
                    ids = newArrayList();
                    binaries.put(key, ids);
                }
                ids.add(sb.getRecordId());

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

    private class ProgressTracker {

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

    private static class OfflineCompactionPredicate implements
            Predicate<NodeState> {

        /**
         * over 64K in size, node will be included in the compaction map
         */
        private static final long offlineThreshold = 65536;

        @Override
        public boolean apply(NodeState state) {
            if (state.getChildNodeCount(2) > 1) {
                return true;
            }
            long count = 0;
            for (PropertyState ps : state.getProperties()) {
                for (int i = 0; i < ps.count(); i++) {
                    long size = ps.size(i);
                    count += size;
                    if (size >= offlineThreshold || count >= offlineThreshold) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

}
