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

import static java.util.Objects.requireNonNullElseGet;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStore.CHECKPOINTS;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStore.ROOT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.segment.file.CompactedNodeState;
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
 *     processed and stored.
 *     <li>Caching the compacted checkpoints and root states for deduplication should
 *     the same checkpoint or root state occur again in a later compaction retry cycle.
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
     * @param compactor         the delegate compactor to use for the actual compaction work
     */
    public CheckpointCompactor(
            @NotNull GCMonitor gcListener,
            @NotNull ClassicCompactor compactor) {
        this.gcListener = gcListener;
        this.compactor = compactor;
    }

    @Override
    public @Nullable CompactedNodeState compactDown(@NotNull NodeState before, @NotNull NodeState after, @NotNull Canceller hardCanceller, @NotNull Canceller softCanceller) throws IOException {
        return doCompact(before, after, after, hardCanceller, softCanceller);
    }

    @Override
    public @Nullable CompactedNodeState compact(@NotNull NodeState before, @NotNull NodeState after, @NotNull NodeState onto, @NotNull Canceller canceller) throws IOException {
        return doCompact(before, after, onto, canceller, null);
    }

    /**
     * Implementation that compacts checkpoints chronologically on top of each other. The implementation
     * supports both {@link #compactDown(NodeState, Canceller, Canceller)} type and
     * {@link #compactUp(NodeState, Canceller)} type operations.
     * <p>
     * Soft cancellation is only supported for {@link #compactDown(NodeState, Canceller, Canceller)} type
     * scenarios, i.e. when {@code after.equals(onto)}, and will return a partially compacted state if cancelled.
     * <p>
     * Hard cancellation will abandon the compaction and return {@code null}.
     * <p>
     * If compaction completes successfully, a fully compacted state is returned.
     *
     * @param before        the node state to diff against from {@code after}
     * @param after         the node state diffed against {@code before}
     * @param onto          the node state to compact to apply the diff to
     * @param hardCanceller the trigger for hard cancellation, will abandon compaction if cancelled
     * @param softCanceller the trigger for soft cancellation, will return partially compacted state if cancelled; may only be set if {@code after.equals(onto)}, implementations should validate the arguments
     * @return              a fully-compacted or partially-compacted node state, or {@code null} if hard-cancelled
     * @throws IOException will throw exception if any errors occur during compaction
     */
    private @Nullable CompactedNodeState doCompact(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull NodeState onto,
            @NotNull Canceller hardCanceller,
            @Nullable Canceller softCanceller
    ) throws IOException {
        Validate.checkArgument(softCanceller == null || Objects.equals(after, onto),
                "softCanceller is only supported for compactDown, i.e. when Objects.equals(after, onto)");

        Set<String> superRoots = collectSuperRootPaths(before, after);
        Buffer stableIdBytes = requireNonNull(CompactorUtils.getStableIdBytes(after));

        NodeBuilder rootBuilder = onto.builder();

        Set<String> deletedCheckpoints = collectDeletedCheckpointPaths(before, after);
        for (String deletedCheckpoint : deletedCheckpoints) {
            rootBuilder.getChildNode(CHECKPOINTS).getChildNode(deletedCheckpoint).remove();
        }

        CompactedNodeState compacted = null;
        for (String path : superRoots) {
            NodeState afterSuperRoot = getDescendant(after, path, NodeState::getChildNode);

            NodeState baseRoot = requireNonNullElseGet(compacted, () -> getRoot(before));
            NodeState ontoRoot = requireNonNullElseGet(compacted, () -> getRoot(onto));

            compacted = compactRootState(baseRoot, getRoot(afterSuperRoot), ontoRoot, hardCanceller, softCanceller);
            if (compacted == null) {
                // only happens for hard cancellation
                return null;
            }

            Validate.checkState(compacted.isComplete() || isCancelled(softCanceller),
                    "compaction must be complete unless cancelled");

            NodeBuilder builder = getDescendant(rootBuilder, path, NodeBuilder::child);
            builder.setChildNode(ROOT, compacted);
            if (path.startsWith(CHECKPOINTS + '/')) {
                compactCheckpointMetadata(builder, afterSuperRoot);
            }

            if (isCancelled(softCanceller)) {
                break;
            }
        }

        return compactor.writeNodeState(rootBuilder.getNodeState(), stableIdBytes, !isCancelled(softCanceller));
    }

    private @Nullable CompactedNodeState compactRootState(@NotNull NodeState baseRoot, @NotNull NodeState afterRoot, @NotNull NodeState ontoRoot, @NotNull Canceller hardCanceller, @Nullable Canceller softCanceller) throws IOException {
        if (Objects.equals(ontoRoot, afterRoot)) {
            // down compaction only affects the first iteration, when compacted == null and ontoRoot.equals(afterRoot).
            return compactWithCache(baseRoot, afterRoot, ontoRoot, hardCanceller, softCanceller);
        } else {
            // for subsequent iterations, ontoRoot == compacted and thus ontoRoot.equals(afterRoot) no longer holds true.
            return compactWithCache(baseRoot, afterRoot, ontoRoot, hardCanceller, null);
        }
    }

    private void compactCheckpointMetadata(NodeBuilder builder, NodeState afterSuperRoot) {
        // copy checkpoint "properties" child node
        NodeBuilder props = builder.setChildNode("properties");
        for (PropertyState property : afterSuperRoot.getChildNode("properties").getProperties()) {
            props.setProperty(compactor.compact(property));
        }
        // copy checkpoint properties (on the parent of the root node)
        for (PropertyState property : afterSuperRoot.getProperties()) {
            builder.setProperty(compactor.compact(property));
        }
    }

    private static boolean isCancelled(@Nullable Canceller softCanceller) {
        return softCanceller != null && softCanceller.check().isCancelled();
    }

    private @Nullable CompactedNodeState compactWithCache(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull NodeState onto,
            @NotNull Canceller hardCanceller,
            @Nullable Canceller softCanceller
    ) throws IOException {
        CompactedNodeState compacted = cpCache.get(after);
        if (compacted == null) {
            compacted = compactor.compact(before, after, onto, hardCanceller, softCanceller);
            if (compacted != null && compacted.isComplete()) {
                cpCache.put(after, compacted);
            }
        } else {
            gcListener.info("found checkpoint in cache.");
        }
        return compacted;
    }

    private @NotNull Set<String> collectDeletedCheckpointPaths(
            @NotNull NodeState superRootBefore,
            @NotNull NodeState superRootAfter) {
        Stream.Builder<String> deletedCheckpoints = Stream.builder();
        superRootAfter.getChildNode("checkpoints").compareAgainstBaseState(
                superRootBefore.getChildNode("checkpoints"), new DefaultNodeStateDiff() {
                    @Override
                    public boolean childNodeDeleted(String name, NodeState before) {
                        deletedCheckpoints.add(name);
                        return true;
                    }
                }
        );
        return deletedCheckpoints.build().collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Collect a chronologically ordered list of roots for the base and the uncompacted
     * state from a {@code superRoot}. This list consists of all checkpoints followed by
     * the root.
     */
    private @NotNull LinkedHashSet<String> collectSuperRootPaths(
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

        LinkedHashSet<String> roots = new LinkedHashSet<>();
        for (ChildNodeEntry checkpoint : checkpoints) {
            String name = checkpoint.getName();
            NodeState node = checkpoint.getNodeState();
            gcListener.info("found checkpoint {} created on {}.",
                    name, new Date(node.getLong("created")));
            roots.add("checkpoints/" + name);
        }
        roots.add("");

        return roots;
    }

    private static @NotNull NodeState getRoot(@NotNull NodeState node) {
        return node.hasChildNode(ROOT) ? node.getChildNode(ROOT) : EMPTY_NODE;
    }

    private static <T> T getDescendant(T t, String path, BiFunction<T, String, T> getChild) {
        for (String name : elements(path)) {
            t = getChild.apply(t, name);
        }
        return t;
    }
}
