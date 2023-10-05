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
import org.apache.jackrabbit.oak.plugins.index.counter.ApproximateCounter;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.guava.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.CompactorUtils.getStableIdBytes;

/**
 * This compactor implementation leverages the tree structure of the repository for concurrent compaction.
 * It explores the tree breadth-first until the target node count is reached. Every node at this depth will be
 * an entry point for asynchronous compaction. After the exploration phase, the main thread will collect
 * these compaction results and write their parents' node state to disk.
 */
public class ParallelCompactor extends CheckpointCompactor {
    /**
     * Expand repository tree until there are this many nodes for each worker to compact. Tradeoff
     * between low efficiency of many small tasks and high risk of at least one of the subtrees being
     * significantly larger than totalSize / numWorkers (unequal work distribution).
     */
    private static final int MIN_NODES_PER_WORKER = 1000;

    /**
     * Stop expansion if tree size grows beyond this many nodes per worker at the latest.
     */
    private static final int MAX_NODES_PER_WORKER = 10_000;

    private final int numWorkers;

    private final long totalSizeEstimate;

    /**
     * Manages workers for asynchronous compaction.
     */
    @Nullable
    private ExecutorService executorService;

    /**
     * Create a new instance based on the passed arguments.
     * @param gcListener listener receiving notifications about the garbage collection process
     * @param reader     segment reader used to read from the segments
     * @param writer     segment writer used to serialise to segments
     * @param blobStore  the blob store or {@code null} if none
     * @param compactionMonitor   notification call back for each compacted nodes, properties, and binaries
     * @param nThreads   number of threads to use for parallel compaction,
     *                   negative numbers are interpreted relative to the number of available processors
     */
    public ParallelCompactor(
            @NotNull GCMonitor gcListener,
            @NotNull SegmentReader reader,
            @NotNull SegmentWriter writer,
            @Nullable BlobStore blobStore,
            @NotNull GCNodeWriteMonitor compactionMonitor,
            int nThreads) {
        super(gcListener, reader, writer, blobStore, compactionMonitor);

        int availableProcessors = Runtime.getRuntime().availableProcessors();
        if (nThreads < 0) {
            nThreads += availableProcessors + 1;
        }
        numWorkers = Math.max(0, nThreads - 1);
        totalSizeEstimate = compactionMonitor.getEstimatedTotal();
    }

    /**
     * Calculates the minimum number of entry points for asynchronous compaction.
     */
    private int getMinNodeCount() {
        return numWorkers * MIN_NODES_PER_WORKER;
    }

    private int getMaxNodeCount() {
        return numWorkers * MAX_NODES_PER_WORKER;
    }

    /**
     * Represents structure of repository changes. Tree is built by exploration process and subsequently
     * used to collect and merge asynchronous compaction results.
     */
    private class CompactionTree implements NodeStateDiff {
        @NotNull
        private final NodeState before;
        @NotNull
        private final NodeState after;
        @NotNull
        private final NodeState onto;
        @NotNull
        private final HashMap<String, CompactionTree> modifiedChildren = new HashMap<>();
        @NotNull
        private final List<Property> modifiedProperties = new ArrayList<>();
        @NotNull
        private final List<String> removedChildNames = new ArrayList<>();
        @NotNull
        private final List<String> removedPropertyNames = new ArrayList<>();
        /**
         * Stores result of asynchronous compaction.
         */
        @Nullable
        private Future<SegmentNodeState> compactionFuture;

        CompactionTree(@NotNull NodeState before, @NotNull NodeState after, @NotNull NodeState onto) {
            this.before = checkNotNull(before);
            this.after = checkNotNull(after);
            this.onto = checkNotNull(onto);
        }

        private class Property {
            @NotNull
            private final PropertyState state;

            Property(@NotNull PropertyState state) {
                this.state = state;
            }

            @NotNull
            PropertyState compact() {
                return compactor.compact(state);
            }
        }

        boolean compareStates(Canceller canceller) {
            return after.compareAgainstBaseState(before,
                    new CancelableDiff(this, () -> canceller.check().isCancelled()));
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
            CompactionTree child = new CompactionTree(EMPTY_NODE, after, EMPTY_NODE);
            modifiedChildren.put(name, child);
            return true;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            CompactionTree child = new CompactionTree(before, after, onto.getChildNode(name));
            modifiedChildren.put(name, child);
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
        boolean compactAsync(Canceller canceller) {
            if (compactionFuture != null) {
                return false;
            }
            checkNotNull(executorService);
            compactionFuture = executorService.submit(() -> compactor.compact(before, after, onto, canceller));
            return true;
        }

        /**
         * Start synchronous compaction on tree or collect result of asynchronous compaction if it has been started.
         */
        @Nullable
        SegmentNodeState compact() throws IOException {
            if (compactionFuture != null) {
                try {
                    return compactionFuture.get();
                } catch (InterruptedException e) {
                    return null;
                } catch (ExecutionException e) {
                    throw new IOException(e);
                }
            }

            MemoryNodeBuilder builder = new MemoryNodeBuilder(onto);

            for (Map.Entry<String, CompactionTree> entry : modifiedChildren.entrySet()) {
                SegmentNodeState compactedState = entry.getValue().compact();
                if (compactedState == null) {
                    return null;
                }
                builder.setChildNode(entry.getKey(), compactedState);
            }
            for (String childName : removedChildNames) {
                builder.getChildNode(childName).remove();
            }
            for (Property property : modifiedProperties) {
                builder.setProperty(property.compact());
            }
            for (String propertyName : removedPropertyNames) {
                builder.removeProperty(propertyName);
            }
            return compactor.writeNodeState(builder.getNodeState(), getStableIdBytes(after));
        }
    }

    /**
     * Implementation of {@link NodeStateDiff} to build {@link CompactionTree} and start asynchronous compaction on
     * suitable entry points. Performs what is referred to as the exploration phase in other comments.
     */
    private class CompactionHandler {
        @NotNull
        private final NodeState base;

        @NotNull
        private final Canceller canceller;

        CompactionHandler(@NotNull NodeState base, @NotNull Canceller canceller) {
            this.base = base;
            this.canceller = canceller;
        }

        @Nullable
        SegmentNodeState diff(@NotNull NodeState before, @NotNull NodeState after) throws IOException {
            checkNotNull(executorService);
            checkState(!executorService.isShutdown());

            gcListener.info("compacting with {} threads.", numWorkers + 1);
            gcListener.info("exploring content tree to find subtrees for parallel compaction.");
            gcListener.info("target node count for expansion is {}, based on {} available workers.",
                    getMinNodeCount(), numWorkers);

            CompactionTree compactionTree = new CompactionTree(before, after, base);
            if (!compactionTree.compareStates(canceller)) {
                return null;
            }

            List<CompactionTree> topLevel = new ArrayList<>();
            for (Map.Entry<String, CompactionTree> childEntry : compactionTree.modifiedChildren.entrySet()) {
                switch (childEntry.getKey()) {
                    // these tend to be the largest directories, others will not be split up
                    case "content":
                    case "oak:index":
                    case "jcr:system":
                        topLevel.add(childEntry.getValue());
                        break;
                    default:
                        checkState(childEntry.getValue().compactAsync(canceller));
                        break;
                }
            }

            if (diff(1, topLevel)) {
                SegmentNodeState compacted = compactionTree.compact();
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
            int targetCount = getMinNodeCount();
            gcListener.info("Found {} nodes at depth {}, target is {}.", nodes.size(), depth, targetCount);

            if (nodes.size() >= targetCount) {
                nodes.forEach(node -> node.compactAsync(canceller));
                return true;
            } else if (nodes.isEmpty()) {
                gcListener.info("Amount of changes too small, tree will not be split.");
                return true;
            }

            List<CompactionTree> nextDepth = new ArrayList<>();
            for (CompactionTree node : nodes) {
                long estimatedSize = node.getEstimatedSize();
                if (estimatedSize != -1 && estimatedSize <= (totalSizeEstimate / numWorkers)) {
                    checkState(node.compactAsync(canceller));
                } else if (nextDepth.size() < getMaxNodeCount()) {
                    if (!node.compareStates(canceller)) {
                        return false;
                    }
                    nextDepth.addAll(node.modifiedChildren.values());
                } else {
                    nextDepth.add(node);
                }
            }

            return diff(depth + 1, nextDepth);
        }
    }

    @Nullable
    @Override
    protected SegmentNodeState compactWithDelegate(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull NodeState onto,
            Canceller canceller
    ) throws IOException {
        if (numWorkers <= 0) {
            gcListener.info("using sequential compaction.");
            return super.compactWithDelegate(before, after, onto, canceller);
        } else if (executorService == null || executorService.isShutdown()) {
            executorService = Executors.newFixedThreadPool(numWorkers);
        }
        return new CompactionHandler(onto, canceller).diff(before, after);
    }
}
