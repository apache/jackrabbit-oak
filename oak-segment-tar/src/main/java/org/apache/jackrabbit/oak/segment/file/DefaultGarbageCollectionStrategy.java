/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.collect.Sets.newHashSet;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType.FULL;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType.TAIL;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.CLEANUP;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.COMPACTION;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.COMPACTION_FORCE_COMPACT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.COMPACTION_RETRY;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.ESTIMATION;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.IDLE;
import static org.apache.jackrabbit.oak.segment.file.PrintableBytes.newPrintableBytes;
import static org.apache.jackrabbit.oak.segment.file.TarRevisions.EXPEDITE_OPTION;
import static org.apache.jackrabbit.oak.segment.file.TarRevisions.timeout;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.segment.CheckpointCompactor;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.tar.CleanupContext;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class DefaultGarbageCollectionStrategy implements GarbageCollectionStrategy {

    private GCGeneration getGcGeneration(Context context) {
        return context.getRevisions().getHead().getSegmentId().getGcGeneration();
    }

    private SegmentNodeState getBase(Context context) {
        String root = context.getGCJournal().read().getRoot();
        RecordId rootId = RecordId.fromString(context.getSegmentTracker(), root);
        if (RecordId.NULL.equals(rootId)) {
            return null;
        }
        try {
            SegmentNodeState node = context.getSegmentReader().readNode(rootId);
            node.getPropertyCount();  // Resilience: fail early with a SNFE if the segment is not there
            return node;
        } catch (SegmentNotFoundException snfe) {
            context.getGCListener().error("base state " + rootId + " is not accessible", snfe);
            return null;
        }
    }

    private SegmentNodeState getHead(Context context) {
        return context.getSegmentReader().readHeadState(context.getRevisions());
    }

    @Override
    public synchronized void collectGarbage(Context context) throws IOException {
        switch (context.getGCOptions().getGCType()) {
            case FULL:
                collectFullGarbage(context);
                break;
            case TAIL:
                collectTailGarbage(context);
                break;
            default:
                throw new IllegalStateException("Invalid GC type");
        }
    }

    @Override
    public synchronized void collectFullGarbage(Context context) throws IOException {
        run(context, true, this::compactFull);
    }

    @Override
    public synchronized void collectTailGarbage(Context context) throws IOException {
        run(context, false, this::compactTail);
    }

    private interface Compactor {

        CompactionResult compact(Context contex) throws IOException;

    }

    private void run(Context context, boolean full, Compactor compact) throws IOException {
        try {
            context.getGCListener().info("started");

            long dt = System.currentTimeMillis() - context.getLastSuccessfulGC();

            if (dt < context.getGCBackOff()) {
                context.getGCListener().skipped("skipping garbage collection as it already ran less than {} hours ago ({} s).", context.getGCBackOff() / 3600000, dt / 1000);
                return;
            }

            boolean sufficientEstimatedGain = true;
            if (context.getGCOptions().isEstimationDisabled()) {
                context.getGCListener().info("estimation skipped because it was explicitly disabled");
            } else if (context.getGCOptions().isPaused()) {
                context.getGCListener().info("estimation skipped because compaction is paused");
            } else {
                context.getGCListener().info("estimation started");
                context.getGCListener().updateStatus(ESTIMATION.message());

                PrintableStopwatch watch = PrintableStopwatch.createStarted();
                GCEstimationResult estimation = estimateCompactionGain(context, full);
                sufficientEstimatedGain = estimation.isGcNeeded();
                String gcLog = estimation.getGcLog();
                if (sufficientEstimatedGain) {
                    context.getGCListener().info("estimation completed in {}. {}", watch, gcLog);
                } else {
                    context.getGCListener().skipped("estimation completed in {}. {}", watch, gcLog);
                }
            }

            if (sufficientEstimatedGain) {
                try (GCMemoryBarrier ignored = new GCMemoryBarrier(context.getSufficientMemory(), context.getGCListener(), context.getGCOptions())) {
                    if (context.getGCOptions().isPaused()) {
                        context.getGCListener().skipped("compaction paused");
                    } else if (!context.getSufficientMemory().get()) {
                        context.getGCListener().skipped("compaction skipped. Not enough memory");
                    } else {
                        CompactionResult compactionResult = compact.compact(context);
                        if (compactionResult.isSuccess()) {
                            context.getSuccessfulGarbageCollectionListener().onSuccessfulGarbageCollection();
                        } else {
                            context.getGCListener().info("cleaning up after failed compaction");
                        }
                        context.getFileReaper().add(cleanup(context, compactionResult));
                    }
                }
            }
        } finally {
            context.getCompactionMonitor().finished();
            context.getGCListener().updateStatus(IDLE.message());
        }
    }

    @Override
    public synchronized CompactionResult compactFull(Context context) {
        context.getGCListener().info("running full compaction");
        return compact(context, FULL, EMPTY_NODE, getGcGeneration(context).nextFull());
    }

    @Override
    public synchronized CompactionResult compactTail(Context context) {
        context.getGCListener().info("running tail compaction");
        SegmentNodeState base = getBase(context);
        if (base != null) {
            return compact(context, TAIL, base, getGcGeneration(context).nextTail());
        }
        context.getGCListener().info("no base state available, running full compaction instead");
        return compact(context, FULL, EMPTY_NODE, getGcGeneration(context).nextFull());
    }

    private CompactionResult compact(Context context, SegmentGCOptions.GCType gcType, NodeState base, GCGeneration newGeneration) {
        try {
            PrintableStopwatch watch = PrintableStopwatch.createStarted();
            context.getGCListener().info(
                "compaction started, gc options={}, current generation={}, new generation={}",
                context.getGCOptions(),
                getHead(context).getRecordId().getSegment().getGcGeneration(),
                newGeneration
            );
            context.getGCListener().updateStatus(COMPACTION.message());

            GCJournal.GCJournalEntry gcEntry = context.getGCJournal().read();
            long initialSize = size(context);

            SegmentWriter writer = context.getSegmentWriterFactory().newSegmentWriter(newGeneration);

            context.getCompactionMonitor().init(gcEntry.getRepoSize(), gcEntry.getNodes(), initialSize);

            CheckpointCompactor compactor = new CheckpointCompactor(
                context.getGCListener(),
                context.getSegmentReader(),
                writer,
                context.getBlobStore(),
                context.getCanceller(),
                context.getCompactionMonitor()
            );

            SegmentNodeState head = getHead(context);
            SegmentNodeState compacted = compactor.compact(base, head, base);
            if (compacted == null) {
                context.getGCListener().warn("compaction cancelled: {}.", context.getCanceller());
                return compactionAborted(context, newGeneration);
            }

            context.getGCListener().info("compaction cycle 0 completed in {}. Compacted {} to {}",
                watch, head.getRecordId(), compacted.getRecordId());

            int cycles = 0;
            boolean success = false;
            SegmentNodeState previousHead = head;
            while (cycles < context.getGCOptions().getRetryCount() &&
                !(success = context.getRevisions().setHead(previousHead.getRecordId(), compacted.getRecordId(), EXPEDITE_OPTION))) {
                // Some other concurrent changes have been made.
                // Rebase (and compact) those changes on top of the
                // compacted state before retrying to set the head.
                cycles++;
                context.getGCListener().info("compaction detected concurrent commits while compacting. " +
                        "Compacting these commits. Cycle {} of {}",
                    cycles, context.getGCOptions().getRetryCount());
                context.getGCListener().updateStatus(COMPACTION_RETRY.message() + cycles);
                PrintableStopwatch cycleWatch = PrintableStopwatch.createStarted();

                head = getHead(context);
                compacted = compactor.compact(previousHead, head, compacted);
                if (compacted == null) {
                    context.getGCListener().warn("compaction cancelled: {}.", context.getCanceller());
                    return compactionAborted(context, newGeneration);
                }

                context.getGCListener().info("compaction cycle {} completed in {}. Compacted {} against {} to {}",
                    cycles, cycleWatch, head.getRecordId(), previousHead.getRecordId(), compacted.getRecordId());
                previousHead = head;
            }

            if (!success) {
                context.getGCListener().info("compaction gave up compacting concurrent commits after {} cycles.", cycles);
                int forceTimeout = context.getGCOptions().getForceTimeout();
                if (forceTimeout > 0) {
                    context.getGCListener().info("trying to force compact remaining commits for {} seconds. " +
                            "Concurrent commits to the store will be blocked.",
                        forceTimeout);
                    context.getGCListener().updateStatus(COMPACTION_FORCE_COMPACT.message());
                    PrintableStopwatch forceWatch = PrintableStopwatch.createStarted();

                    cycles++;
                    context.getCanceller().timeOutAfter(forceTimeout, SECONDS);
                    compacted = forceCompact(context, previousHead, compacted, compactor);
                    success = compacted != null;
                    if (success) {
                        context.getGCListener().info("compaction succeeded to force compact remaining commits after {}.", forceWatch);
                    } else {
                        if (context.getCanceller().get()) {
                            context.getGCListener().warn("compaction failed to force compact remaining commits " +
                                    "after {}. Compaction was cancelled: {}.",
                                forceWatch, context.getCanceller());
                        } else {
                            context.getGCListener().warn("compaction failed to force compact remaining commits. " +
                                    "after {}. Could not acquire exclusive access to the node store.",
                                forceWatch);
                        }
                    }
                }
            }

            if (success) {
                // Update type of the last compaction before calling methods that could throw an exception.
                context.getSuccessfulCompactionListener().onSuccessfulCompaction(gcType);
                writer.flush();
                context.getFlusher().flush();
                context.getGCListener().info("compaction succeeded in {}, after {} cycles", watch, cycles);
                return compactionSucceeded(context, gcType, newGeneration, compacted.getRecordId());
            } else {
                context.getGCListener().info("compaction failed after {}, and {} cycles", watch, cycles);
                return compactionAborted(context, newGeneration);
            }
        } catch (InterruptedException e) {
            context.getGCListener().error("compaction interrupted", e);
            currentThread().interrupt();
            return compactionAborted(context, newGeneration);
        } catch (IOException e) {
            context.getGCListener().error("compaction encountered an error", e);
            return compactionAborted(context, newGeneration);
        }
    }

    private CompactionResult compactionAborted(Context context, GCGeneration generation) {
        context.getGCListener().compactionFailed(generation);
        return CompactionResult.aborted(getGcGeneration(context), generation, context.getGCCount());
    }

    private CompactionResult compactionSucceeded(
        Context context,
        SegmentGCOptions.GCType gcType,
        GCGeneration generation,
        RecordId compactedRootId
    ) {
        context.getGCListener().compactionSucceeded(generation);
        return CompactionResult.succeeded(gcType, generation, context.getGCOptions(), compactedRootId, context.getGCCount());
    }

    private SegmentNodeState forceCompact(
        Context context,
        final NodeState base,
        final NodeState onto,
        final CheckpointCompactor compactor
    ) throws InterruptedException {
        RecordId compactedId = setHead(context, headId -> {
            try {
                PrintableStopwatch t = PrintableStopwatch.createStarted();
                SegmentNodeState after = compactor.compact(base, context.getSegmentReader().readNode(headId), onto);
                if (after != null) {
                    return after.getRecordId();
                }
                context.getGCListener().info("compaction cancelled after {}", t);
                return null;
            } catch (IOException e) {
                context.getGCListener().error("error during forced compaction.", e);
                return null;
            }
        });
        if (compactedId == null) {
            return null;
        }
        return context.getSegmentReader().readNode(compactedId);
    }

    private RecordId setHead(Context context, Function<RecordId, RecordId> f) throws InterruptedException {
        return context.getRevisions().setHead(f, timeout(context.getGCOptions().getForceTimeout(), SECONDS));
    }

    private GCEstimationResult estimateCompactionGain(Context context, boolean full) {
        return new SizeDeltaGcEstimation(
            context.getGCOptions().getGcSizeDeltaEstimation(),
            context.getGCJournal(),
            context.getTarFiles().size(),
            full
        ).estimate();
    }

    private long size(Context context) {
        return context.getTarFiles().size();
    }

    @Override
    public synchronized List<String> cleanup(Context context) throws IOException {
        return cleanup(context, CompactionResult.skipped(
            context.getLastCompactionType(),
            getGcGeneration(context),
            context.getGCOptions(),
            context.getRevisions().getHead(),
            context.getGCCount()
        ));
    }

    private List<String> cleanup(Context context, CompactionResult compactionResult)
        throws IOException {
        PrintableStopwatch watch = PrintableStopwatch.createStarted();

        context.getGCListener().info("cleanup started using reclaimer {}", compactionResult.reclaimer());
        context.getGCListener().updateStatus(CLEANUP.message());
        context.getSegmentCache().clear();

        // Suggest to the JVM that now would be a good time
        // to clear stale weak references in the SegmentTracker
        System.gc();

        TarFiles.CleanupResult cleanupResult = context.getTarFiles().cleanup(newCleanupContext(context, compactionResult.reclaimer()));
        if (cleanupResult.isInterrupted()) {
            context.getGCListener().info("cleanup interrupted");
        }
        context.getSegmentTracker().clearSegmentIdTables(cleanupResult.getReclaimedSegmentIds(), compactionResult.gcInfo());
        context.getGCListener().info("cleanup marking files for deletion: {}", toFileNames(cleanupResult.getRemovableFiles()));

        long finalSize = size(context);
        long reclaimedSize = cleanupResult.getReclaimedSize();
        context.getFileStoreStats().reclaimed(reclaimedSize);
        context.getGCJournal().persist(
            reclaimedSize,
            finalSize,
            getGcGeneration(context),
            context.getCompactionMonitor().getCompactedNodes(),
            compactionResult.getCompactedRootId().toString10()
        );
        context.getGCListener().cleaned(reclaimedSize, finalSize);
        context.getGCListener().info(
            "cleanup completed in {}. Post cleanup size is {} and space reclaimed {}.",
            watch,
            newPrintableBytes(finalSize),
            newPrintableBytes(reclaimedSize)
        );
        return cleanupResult.getRemovableFiles();
    }

    private CleanupContext newCleanupContext(Context context, Predicate<GCGeneration> old) {
        return new CleanupContext() {

            private boolean isUnreferencedBulkSegment(UUID id, boolean referenced) {
                return !isDataSegmentId(id.getLeastSignificantBits()) && !referenced;
            }

            private boolean isOldDataSegment(UUID id, GCGeneration generation) {
                return isDataSegmentId(id.getLeastSignificantBits()) && old.apply(generation);
            }

            @Override
            public Collection<UUID> initialReferences() {
                Set<UUID> references = newHashSet();
                for (SegmentId id : context.getSegmentTracker().getReferencedSegmentIds()) {
                    if (id.isBulkSegmentId()) {
                        references.add(id.asUUID());
                    }
                }
                return references;
            }

            @Override
            public boolean shouldReclaim(UUID id, GCGeneration generation, boolean referenced) {
                return isUnreferencedBulkSegment(id, referenced) || isOldDataSegment(id, generation);
            }

            @Override
            public boolean shouldFollow(UUID from, UUID to) {
                return !isDataSegmentId(to.getLeastSignificantBits());
            }

        };
    }

    private String toFileNames(@Nonnull List<String> files) {
        if (files.isEmpty()) {
            return "none";
        } else {
            return Joiner.on(",").join(files);
        }
    }

}
