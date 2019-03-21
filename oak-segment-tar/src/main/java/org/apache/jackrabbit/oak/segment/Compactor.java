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
 *
 */
package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState.binaryProperty;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState.binaryPropertyFromBlob;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import java.io.IOException;
import java.util.List;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Instances of this class can be used to compact a node state. I.e. to create a clone
 * of a given node state without value sharing except for binaries. Binaries that are
 * stored in a list of bulk segments will still value share the bulk segments (but not
 * the list records).
 * A node can either be compacted on its own or alternatively the difference between
 * two nodes can be compacted on top of an already compacted node.
 */
public class Compactor {

    /**
     * Number of content updates that need to happen before the updates
     * are automatically purged to the underlying segments.
     */
    static final int UPDATE_LIMIT =
            Integer.getInteger("compaction.update.limit", 10000);

    @NotNull
    private final SegmentWriter writer;

    @NotNull
    private final SegmentReader reader;

    @Nullable
    private final BlobStore blobStore;

    @NotNull
    private final GCNodeWriteMonitor compactionMonitor;

    /**
     * Create a new instance based on the passed arguments.
     * @param reader     segment reader used to read from the segments
     * @param writer     segment writer used to serialise to segments
     * @param blobStore  the blob store or {@code null} if none
     * @param compactionMonitor   notification call back for each compacted nodes,
     *                            properties, and binaries
     */
    public Compactor(
            @NotNull SegmentReader reader,
            @NotNull SegmentWriter writer,
            @Nullable BlobStore blobStore,
            @NotNull GCNodeWriteMonitor compactionMonitor) {
        this.writer = checkNotNull(writer);
        this.reader = checkNotNull(reader);
        this.blobStore = blobStore;
        this.compactionMonitor = checkNotNull(compactionMonitor);
    }

    /**
     * Compact a given {@code state}
     * @param state  the node state to compact
     * @return       the compacted node state or {@code null} if cancelled.
     * @throws IOException
     */
    @Nullable
    public SegmentNodeState compact(@NotNull NodeState state, Canceller canceller) throws IOException {
        return compact(EMPTY_NODE, state, EMPTY_NODE, canceller);
    }

    /**
     * compact the differences between {@code after} and {@code before} on top of {@code ont}.
     * @param before   the node state to diff against from {@code after}
     * @param after    the node state diffed against {@code before}
     * @param onto     the node state compacted onto
     * @return         the compacted node state or {@code null} if cancelled.
     * @throws IOException
     */
    @Nullable
    public SegmentNodeState compact(
        @NotNull NodeState before,
        @NotNull NodeState after,
        @NotNull NodeState onto,
        Canceller canceller
    ) throws IOException {
        checkNotNull(before);
        checkNotNull(after);
        checkNotNull(onto);
        return new CompactDiff(onto, canceller).diff(before, after);
    }

    @Nullable
    private static Buffer getStableIdBytes(NodeState state) {
        if (state instanceof SegmentNodeState) {
            return ((SegmentNodeState) state).getStableIdBytes();
        } else {
            return null;
        }
    }

    private class CompactDiff implements NodeStateDiff {
        @NotNull
        private MemoryNodeBuilder builder;

        @NotNull
        private final NodeState base;

        private final Canceller canceller;

        @Nullable
        private IOException exception;

        private long modCount;

        private void updated() throws IOException {
            if (++modCount % UPDATE_LIMIT == 0) {
                RecordId newBaseId = writer.writeNode(builder.getNodeState(), null);
                SegmentNodeState newBase = new SegmentNodeState(reader, writer, blobStore, newBaseId);
                builder = new MemoryNodeBuilder(newBase);
            }
        }

        CompactDiff(@NotNull NodeState base, Canceller canceller) {
            this.builder = new MemoryNodeBuilder(checkNotNull(base));
            this.canceller = canceller;
            this.base = base;
        }

        @Nullable
        SegmentNodeState diff(@NotNull NodeState before, @NotNull NodeState after) throws IOException {
            boolean success = after.compareAgainstBaseState(before, new CancelableDiff(this, () -> canceller.check().isCancelled()));
            if (exception != null) {
                throw new IOException(exception);
            } else if (success) {
                NodeState nodeState = builder.getNodeState();
                checkState(modCount == 0 || !(nodeState instanceof SegmentNodeState));
                RecordId nodeId = writer.writeNode(nodeState, getStableIdBytes(after));
                compactionMonitor.onNode();
                return new SegmentNodeState(reader, writer, blobStore, nodeId);
            } else {
                return null;
            }
        }

        @Override
        public boolean propertyAdded(@NotNull PropertyState after) {
            builder.setProperty(compact(after));
            return true;
        }

        @Override
        public boolean propertyChanged(@NotNull PropertyState before, @NotNull PropertyState after) {
            builder.setProperty(compact(after));
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            builder.removeProperty(before.getName());
            return true;
        }

        @Override
        public boolean childNodeAdded(@NotNull String name, @NotNull NodeState after) {
            try {
                SegmentNodeState compacted = compact(after, canceller);
                if (compacted != null) {
                    updated();
                    builder.setChildNode(name, compacted);
                    return true;
                } else {
                    return false;
                }
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        @Override
        public boolean childNodeChanged(@NotNull String name, @NotNull NodeState before, @NotNull NodeState after) {
            try {
                SegmentNodeState compacted = compact(before, after, base.getChildNode(name), canceller);
                if (compacted != null) {
                    updated();
                    builder.setChildNode(name, compacted);
                    return true;
                } else {
                    return false;
                }
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            try {
                updated();
                builder.getChildNode(name).remove();
                return true;
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }
    }

    @NotNull
    private  PropertyState compact(@NotNull PropertyState property) {
        compactionMonitor.onProperty();
        String name = property.getName();
        Type<?> type = property.getType();
        if (type == BINARY) {
            compactionMonitor.onBinary();
            return binaryProperty(name, property.getValue(Type.BINARY));
        } else if (type == BINARIES) {
            List<Blob> blobs = newArrayList();
            for (Blob blob : property.getValue(BINARIES)) {
                compactionMonitor.onBinary();
                blobs.add(blob);
            }
            return binaryPropertyFromBlob(name, blobs);
        } else {
            return createProperty(name, property.getValue(type), type);
        }
    }

}
