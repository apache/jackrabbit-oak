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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState.binaryProperty;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState.binaryPropertyFromBlob;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.segment.file.CompactedNodeState;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.CompactionWriter;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
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
public class ClassicCompactor extends Compactor {

    /**
     * Number of content updates that need to happen before the updates
     * are automatically purged to the underlying segments.
     */
    static final int UPDATE_LIMIT =
            Integer.getInteger("compaction.update.limit", 10000);

    private final @NotNull CompactionWriter writer;

    private final @NotNull GCNodeWriteMonitor compactionMonitor;

    /**
     * Create a new instance based on the passed arguments.
     * @param writer     segment writer used to serialise to segments
     * @param compactionMonitor   notification call back for each compacted nodes,
     *                            properties, and binaries
     */
    public ClassicCompactor(
            @NotNull CompactionWriter writer,
            @NotNull GCNodeWriteMonitor compactionMonitor) {
        this.writer = requireNonNull(writer);
        this.compactionMonitor = requireNonNull(compactionMonitor);
    }

    @Override
    public @Nullable CompactedNodeState compactDown(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull Canceller hardCanceller,
            @NotNull Canceller softCanceller
    ) throws IOException {
        return compact(before, after, after, hardCanceller, softCanceller);
    }

    @Override
    public @Nullable CompactedNodeState compact(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull NodeState onto,
            @NotNull Canceller canceller
    ) throws IOException {
        return compact(before, after, onto, canceller, Canceller.newCanceller());
    }

    private @Nullable CompactedNodeState compact(
        @NotNull NodeState before,
        @NotNull NodeState after,
        @NotNull NodeState onto,
        @NotNull Canceller hardCanceller,
        @NotNull Canceller softCanceller
    ) throws IOException {
        CompactedNodeState compactedState = getPreviouslyCompactedState(after);
        if (compactedState == null) {
            compactedState = new CompactDiff(onto, hardCanceller, softCanceller).diff(before, after);
        }
        return compactedState;
    }

    protected @Nullable CompactedNodeState writeNodeState(
            @NotNull NodeState nodeState,
            @Nullable Buffer stableIdBytes,
            boolean complete
    ) throws IOException {
        if (complete) {
            CompactedNodeState compacted = writer.writeFullyCompactedNode(nodeState, stableIdBytes);
            compactionMonitor.onNode();
            return compacted;
        } else {
            return writer.writePartiallyCompactedNode(nodeState, stableIdBytes);
        }
    }

    protected @Nullable CompactedNodeState getPreviouslyCompactedState(NodeState nodeState) {
        return writer.getPreviouslyCompactedState(nodeState);
    }

    private class CompactDiff implements NodeStateDiff {
        private final @NotNull NodeState base;
        private final @NotNull Canceller hardCanceller;
        private final @NotNull Canceller softCanceller;
        private final @NotNull List<PropertyState> modifiedProperties = new ArrayList<>();
        private @NotNull NodeBuilder builder;
        private @Nullable IOException exception;
        private long modCount;

        private void updated() throws IOException {
            if (++modCount % UPDATE_LIMIT == 0) {
                SegmentNodeState newBase = writeNodeState(builder.getNodeState(), null, false);
                requireNonNull(newBase);
                builder = new MemoryNodeBuilder(newBase);
            }
        }

        CompactDiff(@NotNull NodeState base, @NotNull Canceller hardCanceller, @NotNull Canceller softCanceller) {
            this.base = requireNonNull(base);
            this.builder = new MemoryNodeBuilder(base);
            this.hardCanceller = requireNonNull(hardCanceller);
            this.softCanceller = requireNonNull(softCanceller);
        }

        private @NotNull CancelableDiff newCancelableDiff() {
            return new CancelableDiff(this, () ->
                    softCanceller.check().isCancelled() || hardCanceller.check().isCancelled());
        }

        @Nullable CompactedNodeState diff(@NotNull NodeState before, @NotNull NodeState after) throws IOException {
            boolean success = after.compareAgainstBaseState(before, newCancelableDiff());
            if (exception != null) {
                throw new IOException(exception);
            } else if (success) {
                // delay property compaction until the end in case compaction is cancelled
                modifiedProperties.forEach(property -> builder.setProperty(compact(property)));
                NodeState nodeState = builder.getNodeState();
                Validate.checkState(modCount == 0 || nodeState instanceof ModifiedNodeState);
                return writeNodeState(nodeState, CompactorUtils.getStableIdBytes(after), true);
            } else if (hardCanceller.check().isCancelled()) {
                return null;
            } else {
                return writeNodeState(builder.getNodeState(), CompactorUtils.getStableIdBytes(after), false);
            }
        }

        @Override
        public boolean propertyAdded(@NotNull PropertyState after) {
            modifiedProperties.add(after);
            return true;
        }

        @Override
        public boolean propertyChanged(@NotNull PropertyState before, @NotNull PropertyState after) {
            modifiedProperties.add(after);
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            builder.removeProperty(before.getName());
            return true;
        }

        private boolean childNodeUpdated(@NotNull String name, @NotNull NodeState before, @NotNull NodeState after) {
            try {
                NodeState child = base.getChildNode(name);
                NodeState onto = child.exists() ? child : EMPTY_NODE;
                CompactedNodeState compacted = compact(before, after, onto, hardCanceller, softCanceller);
                if (compacted == null) {
                    return false;
                }
                updated();
                builder.setChildNode(name, compacted);
                return compacted.isComplete();
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        @Override
        public boolean childNodeAdded(@NotNull String name, @NotNull NodeState after) {
            return childNodeUpdated(name, EMPTY_NODE, after);
        }

        @Override
        public boolean childNodeChanged(@NotNull String name, @NotNull NodeState before, @NotNull NodeState after) {
            return childNodeUpdated(name, before, after);
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

    protected @NotNull PropertyState compact(@NotNull PropertyState property) {
        compactionMonitor.onProperty();
        String name = property.getName();
        Type<?> type = property.getType();
        if (type == BINARY) {
            compactionMonitor.onBinary();
            return binaryProperty(name, property.getValue(Type.BINARY));
        } else if (type == BINARIES) {
            List<Blob> blobs = new ArrayList<>();
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
