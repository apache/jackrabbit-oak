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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.segment.file.CompactedNodeState;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.CompactionWriter;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

/**
 * This compactor implementation leverages the tree structure of the repository for concurrent compaction.
 * It explores the tree breadth-first until the target node count ({@value EXPLORATION_LOWER_LIMIT}) is reached.
 * Every node at this depth will be an entry point for asynchronous compaction. After the exploration phase,
 * the main thread will collect these compaction results and write their parents' node state to disk.
 */
public class ParallelCompactor extends CheckpointCompactor {
    /**
     * Expand repository tree until there are this many nodes for each worker to compact. Tradeoff
     * between inefficiency of many small tasks and high risk of at least one of the subtrees being
     * significantly larger than totalSize / {@code numWorkers} (unequal work distribution).
     */
    private static final int EXPLORATION_LOWER_LIMIT = 10_000;

    /**
     * Stop expansion if tree size grows beyond this many nodes.
     */
    private static final int EXPLORATION_UPPER_LIMIT = 100_000;

    private final int numWorkers;

    private final long totalSizeEstimate;

    /**
     * Manages workers for asynchronous compaction.
     */
    private @Nullable ExecutorService executorService;

    /**
     * Create a new instance based on the passed arguments.
     *
     * @param gcListener        listener receiving notifications about the garbage collection process
     * @param writer            segment writer used to serialise to segments
     * @param compactionMonitor notification call back for each compacted nodes, properties, and binaries
     * @param nThreads          number of threads to use for parallel compaction,
     *                          negative numbers are interpreted relative to the number of available processors
     */
    public ParallelCompactor(
            @NotNull GCMonitor gcListener,
            @NotNull CompactionWriter writer,
            @NotNull GCNodeWriteMonitor compactionMonitor,
            int nThreads) {
        super(gcListener, writer, compactionMonitor);
        if (nThreads < 0) {
            nThreads += Runtime.getRuntime().availableProcessors() + 1;
        }
        numWorkers = Math.max(0, nThreads - 1);
        totalSizeEstimate = compactionMonitor.getEstimatedTotal();
    }

    /**
     * Implementation of {@link NodeStateDiff} to represent structure of repository changes.
     * Tree is built by exploration process and subsequently used to collect and merge
     * asynchronous compaction results.
     */
    private class CompactionTree implements NodeStateDiff {
        private final @NotNull NodeState before;
        private final @NotNull NodeState after;
        private final @NotNull NodeState onto;
        private final @NotNull List<Entry<String, CompactionTree>> modifiedChildren = new ArrayList<>();
        private final @NotNull List<Property> modifiedProperties = new ArrayList<>();
        private final @NotNull List<String> removedChildNames = new ArrayList<>();
        private final @NotNull List<String> removedPropertyNames = new ArrayList<>();
        /**
         * Stores result of asynchronous compaction.
         */
        private @Nullable Future<CompactedNodeState> compactionFuture;

        CompactionTree(@NotNull NodeState before, @NotNull NodeState after, @NotNull NodeState onto) {
            this.before = requireNonNull(before);
            this.after = requireNonNull(after);
            this.onto = requireNonNull(onto);
        }

        private class Property {
            private final @NotNull PropertyState state;

            Property(@NotNull PropertyState state) {
                this.state = state;
            }

            @NotNull PropertyState compact() {
                return compactor.compact(state);
            }
        }

        private boolean compareState(@NotNull Canceller canceller) {
            return after.compareAgainstBaseState(before,
                    new CancelableDiff(this, () -> canceller.check().isCancelled()));
        }

        @Nullable List<Entry<String, CompactionTree>> expand(@NotNull Canceller hardCanceller) {
            Validate.checkState(compactionFuture == null);
            CompactedNodeState compactedState = compactor.getPreviouslyCompactedState(after);
            if (compactedState != null) {
                compactionFuture = CompletableFuture.completedFuture(compactedState);
                return Collections.emptyList();
            } else if (compareState(hardCanceller)) {
                return modifiedChildren;
            } else {
                return null;
            }
        }

        long getEstimatedSize() {
            return ApproximateCounter.getCountSync(after);
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            modifiedProperties.add(new Property(after));
            return true;
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            modifiedProperties.add(new Property(after));
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            removedPropertyNames.add(before.getName());
            return true;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            NodeState childOnto = onto.getChildNode(name);
            CompactionTree child = new CompactionTree(EMPTY_NODE, after,
                    childOnto.exists() ? childOnto : EMPTY_NODE);
            modifiedChildren.add(new SimpleImmutableEntry<>(name, child));
            return true;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            CompactionTree child = new CompactionTree(before, after, onto.getChildNode(name));
            modifiedChildren.add(new SimpleImmutableEntry<>(name, child));
            return true;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            removedChildNames.add(name);
            return true;
        }

        /**
         * Start asynchronous compaction.
         */
        void compactAsync(@NotNull Canceller hardCanceller, @Nullable Canceller softCanceller) {
            if (compactionFuture == null) {
                requireNonNull(executorService);
                if (softCanceller == null) {
                    compactionFuture = executorService.submit(() ->
                            compactor.compact(before, after, onto, hardCanceller));
                } else {
                    Validate.checkState(onto.equals(after));
                    compactionFuture = executorService.submit(() ->
                            compactor.compactDown(before, after, hardCanceller, softCanceller));
                }
            }
        }

        /**
         * Will attempt to cancel pending asynchronous compaction. Already running tasks will not be affected.
         * Waiting for the compactor to return internally after checking Canceller for all scheduled tasks
         * causes a lot of overhead which can hereby be avoided.
         */
        private boolean tryCancelCompaction() {
            if (compactionFuture != null && compactionFuture.cancel(false)) {
                compactionFuture = null;
                return true;
            } else {
                return false;
            }
        }

        /**
         * Start synchronous compaction on tree or collect result of asynchronous compaction if it has been started.
         */
        @Nullable CompactedNodeState compact() throws IOException {
            if (compactionFuture != null) {
                try {
                    return compactionFuture.get();
                } catch (InterruptedException e) {
                    return null;
                } catch (ExecutionException e) {
                    throw new IOException(e);
                }
            }

            NodeBuilder builder = new MemoryNodeBuilder(onto);
            Buffer stableIdBytes = CompactorUtils.getStableIdBytes(after);

            for (int i = 0; i < modifiedChildren.size(); i++) {
                Entry<String, CompactionTree> entry = modifiedChildren.get(i);
                CompactionTree child = entry.getValue();
                CompactedNodeState compactedState = child.compact();
                if (compactedState == null) {
                    return null;
                }
                builder.setChildNode(entry.getKey(), compactedState);

                // collect results and cancel unfinished tasks in reverse order
                // increases cancellation success rate since tasks are executed in order
                if (!compactedState.isComplete()) {
                    for (int j = modifiedChildren.size()-1; j > i; j--) {
                        entry = modifiedChildren.get(j);
                        if (!entry.getValue().tryCancelCompaction()) {
                            compactedState = entry.getValue().compact();
                            if (compactedState == null) {
                                return null;
                            }
                            builder.setChildNode(entry.getKey(), compactedState);
                        }
                    }
                    return compactor.writeNodeState(builder.getNodeState(), stableIdBytes, false);
                }
            }

            for (String name : removedChildNames) {
                builder.getChildNode(name).remove();
            }

            for (Property property : modifiedProperties) {
                builder.setProperty(property.compact());
            }

            for (String name : removedPropertyNames) {
                builder.removeProperty(name);
            }

            return compactor.writeNodeState(builder.getNodeState(), stableIdBytes, true);
        }
    }

    /**
     * Handler class to build {@link CompactionTree} and start asynchronous compaction at
     * suitable entry points. Performs what is referred to as the exploration phase in other comments.
     */
    private class CompactionHandler {
        private final @NotNull NodeState base;
        private final @NotNull Canceller hardCanceller;
        private final @Nullable Canceller softCanceller;

        CompactionHandler(@NotNull NodeState base, @NotNull Canceller hardCanceller) {
            this.base = base;
            this.hardCanceller = hardCanceller;
            this.softCanceller = null;
        }

        CompactionHandler(@NotNull NodeState base, @NotNull Canceller hardCanceller, @NotNull Canceller softCanceller) {
            this.base = base;
            this.hardCanceller = hardCanceller;
            this.softCanceller = softCanceller;
        }

        @Nullable CompactedNodeState diff(@NotNull NodeState before, @NotNull NodeState after) throws IOException {
            requireNonNull(executorService);
            Validate.checkState(!executorService.isShutdown());

            gcListener.info("compacting with {} threads.", numWorkers + 1);
            gcListener.info("exploring content tree to find subtrees for parallel compaction.");
            gcListener.info("target node count for expansion is {}.", EXPLORATION_LOWER_LIMIT);

            CompactionTree root = new CompactionTree(before, after, base);

            if (diff(0, Collections.singletonList(root))) {
                CompactedNodeState compacted = root.compact();
                if (compacted != null) {
                    return compacted;
                }
            }

            try {
                // compaction failed, terminate remaining tasks
                executorService.shutdown();
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }

            return null;
        }

        private boolean diff(int depth, List<CompactionTree> nodes) {
            gcListener.info("found {} nodes at depth {}.", nodes.size(), depth);

            if (nodes.size() >= EXPLORATION_LOWER_LIMIT) {
                nodes.forEach(node -> node.compactAsync(hardCanceller, softCanceller));
                return true;
            } else if (nodes.isEmpty()) {
                return true;
            }

            List<CompactionTree> nextDepth = new ArrayList<>();
            for (CompactionTree node : nodes) {
                long estimatedSize = node.getEstimatedSize();
                if (estimatedSize != -1 && estimatedSize <= (totalSizeEstimate / numWorkers)) {
                    node.compactAsync(hardCanceller, softCanceller);
                } else if (nextDepth.size() < EXPLORATION_UPPER_LIMIT) {
                    List<Entry<String, CompactionTree>> children = node.expand(hardCanceller);
                    if (children == null) {
                        return false;
                    }
                    children.forEach(entry -> nextDepth.add(entry.getValue()));
                } else {
                    nextDepth.add(node);
                }
            }

            if (nextDepth.size() < nodes.size()) {
                nodes.forEach(node -> node.compactAsync(hardCanceller, softCanceller));
                return true;
            }

            return diff(depth + 1, nextDepth);
        }
    }

    private boolean initializeExecutor() {
        if (numWorkers <= 0) {
            gcListener.info("using sequential compaction.");
            return false;
        }
        if (executorService == null || executorService.isShutdown()) {
            executorService = Executors.newFixedThreadPool(numWorkers);
        }
        return true;
    }

    @Override
    protected @Nullable CompactedNodeState compactDownWithDelegate(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull Canceller hardCanceller,
            @NotNull Canceller softCanceller
    ) throws IOException {
        if (initializeExecutor()) {
            return new CompactionHandler(after, hardCanceller, softCanceller).diff(before, after);
        } else {
            return super.compactDownWithDelegate(before, after, hardCanceller, softCanceller);
        }
    }

    @Override
    protected @Nullable CompactedNodeState compactWithDelegate(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull NodeState onto,
            @NotNull Canceller canceller
    ) throws IOException {
        if (initializeExecutor()) {
            return new CompactionHandler(onto, canceller).diff(before, after);
        } else {
            return super.compactWithDelegate(before, after, onto, canceller);
        }
    }
}
