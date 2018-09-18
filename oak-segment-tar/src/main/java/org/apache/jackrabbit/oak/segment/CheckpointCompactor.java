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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
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
public class CheckpointCompactor {
    @NotNull
    private final GCMonitor gcListener;

    @NotNull
    private final Map<NodeState, NodeState> cpCache = newHashMap();

    @NotNull
    private final Compactor compactor;

    @NotNull
    private final NodeWriter nodeWriter;

    private interface NodeWriter {
        @NotNull
        SegmentNodeState writeNode(@NotNull NodeState node, @Nullable ByteBuffer stableId) throws IOException;
    }

    /**
     * Create a new instance based on the passed arguments.
     * @param reader     segment reader used to read from the segments
     * @param writer     segment writer used to serialise to segments
     * @param blobStore  the blob store or {@code null} if none
     * @param compactionMonitor   notification call back for each compacted nodes,
     *                            properties, and binaries
     */
    public CheckpointCompactor(
            @NotNull GCMonitor gcListener,
            @NotNull SegmentReader reader,
            @NotNull SegmentWriter writer,
            @Nullable BlobStore blobStore,
            @NotNull GCNodeWriteMonitor compactionMonitor) {
        this.gcListener = gcListener;
        this.compactor = new Compactor(reader, writer, blobStore, compactionMonitor);
        this.nodeWriter = (node, stableId) -> {
            RecordId nodeId = writer.writeNode(node, stableId);
            return new SegmentNodeState(reader, writer, blobStore, nodeId);
        };
    }

    /**
     * Compact {@code uncompacted} on top of an optional {@code base}.
     * @param base         the base state to compact against
     * @param uncompacted  the uncompacted state to compact
     * @param onto         the state onto which to compact the change between {@code base} and
     *                     {@code uncompacted}
     * @return  compacted clone of {@code uncompacted} or {@code null} if cancelled.
     * @throws IOException
     */
    @Nullable
    public SegmentNodeState compact(
        @NotNull NodeState base,
        @NotNull NodeState uncompacted,
        @NotNull NodeState onto,
        Canceller canceller
    ) throws IOException {
        // Collect a chronologically ordered list of roots for the uncompacted
        // state. This list consists of all checkpoints followed by the root.
        LinkedHashMap<String, NodeState> uncompactedRoots = collectRoots(uncompacted);

        // Compact the list of uncompacted roots to a list of compacted roots.
        LinkedHashMap<String, NodeState> compactedRoots = compact(
            getRoot(base),
            uncompactedRoots,
            getRoot(onto),
            canceller
        );

        if (compactedRoots == null) {
            return null;
        }

        // Build a compacted super root by replacing the uncompacted roots with
        // the compacted ones in the original node.
        NodeBuilder builder = uncompacted.builder();
        for (Entry<String, NodeState> compactedRoot : compactedRoots.entrySet()) {
            String path = compactedRoot.getKey();
            NodeState state = compactedRoot.getValue();
            NodeBuilder childBuilder = getChild(builder, getParentPath(path));
            childBuilder.setChildNode(getName(path), state);
        }

        return nodeWriter.writeNode(builder.getNodeState(), getStableIdBytes(uncompacted));
    }

    @Nullable
    private static ByteBuffer getStableIdBytes(@NotNull NodeState node) {
        return node instanceof SegmentNodeState
            ? ((SegmentNodeState) node).getStableIdBytes()
            : null;
    }

    @NotNull
    private static NodeState getRoot(@NotNull NodeState node) {
        return node.hasChildNode("root")
            ? node.getChildNode("root")
            : EMPTY_NODE;
    }

    /**
     * Compact a list of uncompacted roots on top of base roots of the same key or
     * an empty node if none.
     */
    @Nullable
    private LinkedHashMap<String, NodeState> compact(
        @NotNull NodeState base,
        @NotNull LinkedHashMap<String, NodeState> uncompactedRoots,
        @NotNull NodeState onto,
        Canceller canceller
    ) throws IOException {
        LinkedHashMap<String, NodeState> compactedRoots = newLinkedHashMap();
        for (Entry<String, NodeState> uncompactedRoot : uncompactedRoots.entrySet()) {
            String path = uncompactedRoot.getKey();
            NodeState uncompacted = uncompactedRoot.getValue();
            Result result = compactWithCache(base, uncompacted, onto, path, canceller);
            if (result == null) {
                return null;
            }
            base = result.nextBefore;
            onto = result.nextOnto;
            compactedRoots.put(path, result.compacted);
        }
        return compactedRoots;
    }

    /**
     * Collect a chronologically ordered list of roots for the base and the uncompacted
     * state from a {@code superRoot}. This list consists of all checkpoints followed by
     * the root.
     */
    @NotNull
    private LinkedHashMap<String, NodeState> collectRoots(@Nullable NodeState superRoot) {
        LinkedHashMap<String, NodeState> roots = newLinkedHashMap();
        if (superRoot != null) {
            List<ChildNodeEntry> checkpoints = newArrayList(
                    superRoot.getChildNode("checkpoints").getChildNodeEntries());

            checkpoints.sort((cne1, cne2) -> {
                long c1 = cne1.getNodeState().getLong("created");
                long c2 = cne2.getNodeState().getLong("created");
                return Long.compare(c1, c2);
            });

            for (ChildNodeEntry checkpoint : checkpoints) {
                String name = checkpoint.getName();
                NodeState node = checkpoint.getNodeState();
                gcListener.info("found checkpoint {} created at {}.",
                    name, new Date(node.getLong("created")));
                roots.put("checkpoints/" + name + "/root", node.getChildNode("root"));
            }
            roots.put("root", superRoot.getChildNode("root"));
        }
        return roots;
    }

    @NotNull
    private static NodeBuilder getChild(NodeBuilder builder, String path) {
        for (String name : elements(path)) {
            builder = builder.getChildNode(name);
        }
        return builder;
    }

    private static class Result {
            final NodeState compacted;
            final NodeState nextBefore;
            final NodeState nextOnto;

            Result(@NotNull NodeState compacted, @NotNull NodeState nextBefore, @NotNull NodeState nextOnto) {
                this.compacted = compacted;
                this.nextBefore = nextBefore;
                this.nextOnto = nextOnto;
            }
        }

    /**
     * Compact {@code after} against {@code before} on top of {@code onto} unless
     * {@code after} has been compacted before and is found in the cache. In this
     * case the cached version of the previously compacted {@code before} is returned.
     */
    @Nullable
    private Result compactWithCache(
        @NotNull NodeState before,
        @NotNull NodeState after,
        @NotNull NodeState onto,
        @NotNull String path,
        Canceller canceller
    ) throws IOException {
        gcListener.info("compacting {}.", path);
        NodeState compacted = cpCache.get(after);
        if (compacted == null) {
            compacted = compactor.compact(before, after, onto, canceller);
            if (compacted == null) {
                return null;
            } else {
                cpCache.put(after, compacted);
                return new Result(compacted, after, compacted);
            }
        } else {
            gcListener.info("found {} in cache.", path);
            return new Result(compacted, before, onto);
        }
    }

}
