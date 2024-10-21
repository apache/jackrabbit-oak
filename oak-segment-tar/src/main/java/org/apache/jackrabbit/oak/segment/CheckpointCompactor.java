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

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.segment.file.CompactedNodeState;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.CompactionWriter;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This compactor implementation is aware of the checkpoints in the repository.
 * It uses this information to further optimise the compaction result by
 * <ul>
 *     <li>Rebasing the checkpoints and subsequently the root on top of each other
 *     in chronological order. This results minimises the deltas that need to be
 *     processed and stored.</li>
 *     <li>Caching the compacted checkpoints and root states for deduplication should
 *     the same checkpoint or root state occur again in a later compaction retry cycle.</li>
 * </ul>
 */
public class CheckpointCompactor extends Compactor {
    protected final @NotNull GCMonitor gcListener;

    private final @NotNull Map<NodeState, CompactedNodeState> cpCache = new HashMap<>();

    protected final @NotNull ClassicCompactor compactor;

    /**
     * Create a new instance based on the passed arguments.
     *
     * @param gcListener        listener receiving notifications about the garbage collection process
     * @param writer           segment writer used to serialise to segments
     * @param compactionMonitor notification call back for each compacted nodes,
     *                          properties, and binaries
     */
    public CheckpointCompactor(
            @NotNull GCMonitor gcListener,
            @NotNull CompactionWriter writer,
            @NotNull GCNodeWriteMonitor compactionMonitor) {
        this.gcListener = gcListener;
        this.compactor = new ClassicCompactor(writer, compactionMonitor);
    }

    @Override
    public @Nullable CompactedNodeState compactDown(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull Canceller hardCanceller,
            @NotNull Canceller softCanceller
    ) throws IOException {
        Iterator<Entry<String, NodeState>> iterator = collectRoots(before, after).entrySet().iterator();
        Entry<String, NodeState> entry = iterator.next();
        String path = entry.getKey();

        // could already be in cache if compactor is reused
        CompactedNodeState compacted = cpCache.get(entry.getValue());
        gcListener.info("compacting {}.", path);
        if (compacted == null) {
            compacted = compactDownWithDelegate(getRoot(before), entry.getValue(), hardCanceller, softCanceller);
            if (compacted == null) {
                return null;
            }
        }

        NodeBuilder builder = after.builder();
        Buffer stableIdBytes = requireNonNull(CompactorUtils.getStableIdBytes(after));

        getChild(builder, getParentPath(path)).setChildNode(getName(path), compacted);

        if (compacted.isComplete()) {
            cpCache.put(entry.getValue(), compacted);
        } else {
            return compactor.writeNodeState(builder.getNodeState(), stableIdBytes, false);
        }

        before = entry.getValue();

        while (iterator.hasNext()) {
            entry = iterator.next();
            path = entry.getKey();
            gcListener.info("compacting {}.", path);

            compacted = compactWithCache(before, entry.getValue(), compacted, hardCanceller);
            if (compacted == null) {
                return null;
            }

            before = entry.getValue();
            Validate.checkState(compacted.isComplete());
            getChild(builder, getParentPath(path)).setChildNode(getName(path), compacted);

            if (softCanceller.check().isCancelled()) {
                return compactor.writeNodeState(builder.getNodeState(), stableIdBytes, false);
            }
        }

        return compactor.writeNodeState(builder.getNodeState(), stableIdBytes, true);
    }

    @Override
    public @Nullable CompactedNodeState compact(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull NodeState onto,
            @NotNull Canceller canceller
    ) throws IOException {
        LinkedHashMap<String, NodeState> roots = collectRoots(before, after);

        NodeBuilder builder = after.builder();
        Buffer stableIdBytes = requireNonNull(CompactorUtils.getStableIdBytes(after));

        before = getRoot(before);
        onto = getRoot(onto);

        for (Entry<String, NodeState> entry : roots.entrySet()) {
            String path = entry.getKey();
            after = entry.getValue();
            CompactedNodeState compacted = compactWithCache(before, after, onto, canceller);
            if (compacted == null) {
                return null;
            }
            Validate.checkState(compacted.isComplete());
            getChild(builder, getParentPath(path)).setChildNode(getName(path), compacted);
            before = after;
            onto = compacted;
        }

        return compactor.writeNodeState(builder.getNodeState(), stableIdBytes, true);
    }

    private @Nullable CompactedNodeState compactWithCache(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull NodeState onto,
            @NotNull Canceller canceller
    ) throws IOException {
        CompactedNodeState compacted = cpCache.get(after);
        if (compacted == null) {
            compacted = compactWithDelegate(before, after, onto, canceller);
            if (compacted != null) {
                cpCache.put(after, compacted);
            }
        } else {
            gcListener.info("found checkpoint in cache.");
        }
        return compacted;
    }

    /**
     * Collect a chronologically ordered list of roots for the base and the uncompacted
     * state from a {@code superRoot}. This list consists of all checkpoints followed by
     * the root.
     */
    private @NotNull LinkedHashMap<String, NodeState> collectRoots(
            @NotNull NodeState superRootBefore,
            @NotNull NodeState superRootAfter) {
        List<ChildNodeEntry> checkpoints = new ArrayList<>();
        superRootAfter.getChildNode("checkpoints").compareAgainstBaseState(
                superRootBefore.getChildNode("checkpoints"), new DefaultNodeStateDiff() {
                    @Override
                    public boolean childNodeAdded(String name, NodeState after) {
                        checkpoints.add(new MemoryChildNodeEntry(name, after));
                        return true;
                    }
                }
        );

        checkpoints.sort((cne1, cne2) -> {
            long c1 = cne1.getNodeState().getLong("created");
            long c2 = cne2.getNodeState().getLong("created");
            return Long.compare(c1, c2);
        });

        LinkedHashMap<String, NodeState> roots = new LinkedHashMap<>();
        for (ChildNodeEntry checkpoint : checkpoints) {
            String name = checkpoint.getName();
            NodeState node = checkpoint.getNodeState();
            gcListener.info("found checkpoint {} created on {}.",
                    name, new Date(node.getLong("created")));
            roots.put("checkpoints/" + name + "/root", node.getChildNode("root"));
        }
        roots.put("root", superRootAfter.getChildNode("root"));

        return roots;
    }

    private static @NotNull NodeState getRoot(@NotNull NodeState node) {
        return node.hasChildNode("root") ? node.getChildNode("root") : EMPTY_NODE;
    }

    private static @NotNull NodeBuilder getChild(NodeBuilder builder, String path) {
        for (String name : elements(path)) {
            builder = builder.getChildNode(name);
        }
        return builder;
    }

    /**
     * Delegate compaction to another, usually simpler, implementation.
     */
    protected @Nullable CompactedNodeState compactDownWithDelegate(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull Canceller hardCanceller,
            @NotNull Canceller softCanceller
    ) throws IOException {
        return compactor.compactDown(before, after, hardCanceller, softCanceller);
    }

    protected @Nullable CompactedNodeState compactWithDelegate(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull NodeState onto,
            @NotNull Canceller canceller
    ) throws IOException {
        return compactor.compact(before, after, onto, canceller);
    }
}
