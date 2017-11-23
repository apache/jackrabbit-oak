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
import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

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

    @Nonnull
    private final SegmentWriter writer;

    @Nonnull
    private final SegmentReader reader;

    @Nullable
    private final BlobStore blobStore;

    @Nonnull
    private final Supplier<Boolean> cancel;

    @Nonnull
    private final GCNodeWriteMonitor compactionMonitor;

    /**
     * Create a new instance based on the passed arguments.
     * @param reader     segment reader used to read from the segments
     * @param writer     segment writer used to serialise to segments
     * @param blobStore  the blob store or {@code null} if none
     * @param cancel     a flag that can be used to cancel the compaction process
     * @param compactionMonitor   notification call back for each compacted nodes,
     *                            properties, and binaries
     */
    public Compactor(
            @Nonnull SegmentReader reader,
            @Nonnull SegmentWriter writer,
            @Nullable BlobStore blobStore,
            @Nonnull Supplier<Boolean> cancel,
            @Nonnull GCNodeWriteMonitor compactionMonitor) {
        this.writer = checkNotNull(writer);
        this.reader = checkNotNull(reader);
        this.blobStore = blobStore;
        this.cancel = checkNotNull(cancel);
        this.compactionMonitor = checkNotNull(compactionMonitor);
    }

    /**
     * Compact a given {@code state}
     * @param state  the node state to compact
     * @return       the compacted node state or {@code null} if cancelled.
     * @throws IOException
     */
    @CheckForNull
    public SegmentNodeState compact(@Nonnull NodeState state) throws IOException {
        return compact(EMPTY_NODE, state, EMPTY_NODE);
    }

    /**
     * compact the differences between {@code after} and {@code before} on top of {@code ont}.
     * @param before   the node state to diff against from {@code after}
     * @param after    the node state diffed against {@code before}
     * @param onto     the node state compacted onto
     * @return         the compacted node state or {@code null} if cancelled.
     * @throws IOException
     */
    @CheckForNull
    public SegmentNodeState compact(
            @Nonnull NodeState before,
            @Nonnull NodeState after,
            @Nonnull NodeState onto)
    throws IOException {
        checkNotNull(before);
        checkNotNull(after);
        checkNotNull(onto);
        return new CompactDiff(onto).diff(before, after);
    }

    @CheckForNull
    private static ByteBuffer getStableIdBytes(NodeState state) {
        if (state instanceof SegmentNodeState) {
            return ((SegmentNodeState) state).getStableIdBytes();
        } else {
            return null;
        }
    }

    private class CompactDiff implements NodeStateDiff {
        @Nonnull
        private MemoryNodeBuilder builder;

        @Nonnull
        private final NodeState base;

        @CheckForNull
        private IOException exception;

        private long modCount;

        private void updated() throws IOException {
            if (++modCount % UPDATE_LIMIT == 0) {
                RecordId newBaseId = writer.writeNode(builder.getNodeState(), null);
                SegmentNodeState newBase = new SegmentNodeState(reader, writer, blobStore, newBaseId);
                builder = new MemoryNodeBuilder(newBase);
            }
        }

        CompactDiff(@Nonnull NodeState base) {
            this.builder = new MemoryNodeBuilder(checkNotNull(base));
            this.base = base;
        }

        @CheckForNull
        SegmentNodeState diff(@Nonnull NodeState before, @Nonnull NodeState after) throws IOException {
            boolean success = after.compareAgainstBaseState(before, new CancelableDiff(this, cancel));
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
        public boolean propertyAdded(@Nonnull PropertyState after) {
            builder.setProperty(compact(after));
            return true;
        }

        @Override
        public boolean propertyChanged(@Nonnull PropertyState before, @Nonnull PropertyState after) {
            builder.setProperty(compact(after));
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            builder.removeProperty(before.getName());
            return true;
        }

        @Override
        public boolean childNodeAdded(@Nonnull String name, @Nonnull NodeState after) {
            try {
                SegmentNodeState compacted = compact(after);
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
        public boolean childNodeChanged(@Nonnull String name, @Nonnull NodeState before, @Nonnull NodeState after) {
            try {
                SegmentNodeState compacted = compact(before, after, base.getChildNode(name));
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

    @Nonnull
    private  PropertyState compact(@Nonnull PropertyState property) {
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
