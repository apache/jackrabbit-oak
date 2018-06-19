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

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.COMPACTION;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.COMPACTION_FORCE_COMPACT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.COMPACTION_RETRY;
import static org.apache.jackrabbit.oak.segment.file.TarRevisions.EXPEDITE_OPTION;
import static org.apache.jackrabbit.oak.segment.file.TarRevisions.timeout;

import java.io.IOException;

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.segment.CheckpointCompactor;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType;
import org.apache.jackrabbit.oak.segment.file.cancel.Cancellation;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.spi.state.NodeState;

abstract class AbstractCompactionStrategy implements CompactionStrategy {

    abstract GCType getCompactionType();

    abstract GCGeneration nextGeneration(GCGeneration current);

    private CompactionResult compactionSucceeded(
        Context context,
        GCGeneration generation,
        RecordId compactedRootId
    ) {
        context.getGCListener().compactionSucceeded(generation);
        return CompactionResult.succeeded(getCompactionType(), generation, context.getGCOptions(), compactedRootId, context.getGCCount());
    }

    private static GCGeneration getGcGeneration(Context context) {
        return context.getRevisions().getHead().getSegmentId().getGcGeneration();
    }

    private static SegmentNodeState getHead(Context context) {
        return context.getSegmentReader().readHeadState(context.getRevisions());
    }

    private static long size(Context context) {
        return context.getTarFiles().size();
    }

    private static CompactionResult compactionAborted(Context context, GCGeneration generation) {
        context.getGCListener().compactionFailed(generation);
        return CompactionResult.aborted(getGcGeneration(context), generation, context.getGCCount());
    }

    private static SegmentNodeState forceCompact(
        Context context,
        NodeState base,
        NodeState onto,
        CheckpointCompactor compactor,
        Canceller canceller
    ) throws InterruptedException {
        RecordId compactedId = setHead(context, headId -> {
            try {
                PrintableStopwatch t = PrintableStopwatch.createStarted();
                SegmentNodeState after = compactor.compact(base, context.getSegmentReader().readNode(headId), onto, canceller);
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

    private static RecordId setHead(Context context, Function<RecordId, RecordId> f) throws InterruptedException {
        return context.getRevisions().setHead(f, timeout(context.getGCOptions().getForceTimeout(), SECONDS));
    }

    private static String formatCompactionType(GCType compactionType) {
        switch (compactionType) {
            case FULL:
                return "full";
            case TAIL:
                return "tail";
            default:
                throw new IllegalStateException("unsupported compaction type: " + compactionType);
        }
    }

    final CompactionResult compact(Context context, NodeState base) {
        context.getGCListener().info("running {} compaction", formatCompactionType(getCompactionType()));

        GCGeneration nextGeneration = nextGeneration(getGcGeneration(context));

        try {
            PrintableStopwatch watch = PrintableStopwatch.createStarted();
            context.getGCListener().info(
                "compaction started, gc options={}, current generation={}, new generation={}",
                context.getGCOptions(),
                getHead(context).getRecordId().getSegment().getGcGeneration(),
                nextGeneration
            );
            context.getGCListener().updateStatus(COMPACTION.message());

            GCJournal.GCJournalEntry gcEntry = context.getGCJournal().read();
            long initialSize = size(context);

            SegmentWriter writer = context.getSegmentWriterFactory().newSegmentWriter(nextGeneration);

            context.getCompactionMonitor().init(gcEntry.getRepoSize(), gcEntry.getNodes(), initialSize);

            Canceller compactionCanceller = context.getCanceller().withShortCircuit();

            CheckpointCompactor compactor = new CheckpointCompactor(
                context.getGCListener(),
                context.getSegmentReader(),
                writer,
                context.getBlobStore(),
                context.getCompactionMonitor()
            );

            SegmentNodeState head = getHead(context);
            SegmentNodeState compacted = compactor.compact(base, head, base, compactionCanceller);
            if (compacted == null) {
                context.getGCListener().warn("compaction cancelled: {}.", compactionCanceller.check().getReason().orElse("unknown reason"));
                return compactionAborted(context, nextGeneration);
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
                compacted = compactor.compact(previousHead, head, compacted, compactionCanceller);
                if (compacted == null) {
                    context.getGCListener().warn("compaction cancelled: {}.", compactionCanceller.check().getReason().orElse("unknown reason"));
                    return compactionAborted(context, nextGeneration);
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

                    Canceller forcedCompactionCanceller = compactionCanceller
                        .withTimeout("forced compaction timeout exceeded", forceTimeout, SECONDS)
                        .withShortCircuit();
                    compacted = forceCompact(context, previousHead, compacted, compactor, forcedCompactionCanceller);
                    success = compacted != null;
                    if (success) {
                        context.getGCListener().info("compaction succeeded to force compact remaining commits after {}.", forceWatch);
                    } else {
                        Cancellation cancellation = forcedCompactionCanceller.check();
                        if (cancellation.isCancelled()) {
                            context.getGCListener().warn("compaction failed to force compact remaining commits " +
                                    "after {}. Compaction was cancelled: {}.",
                                forceWatch, cancellation.getReason().orElse("unknown reason"));
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
                context.getSuccessfulCompactionListener().onSuccessfulCompaction(getCompactionType());
                writer.flush();
                context.getFlusher().flush();
                context.getGCListener().info("compaction succeeded in {}, after {} cycles", watch, cycles);
                return compactionSucceeded(context, nextGeneration, compacted.getRecordId());
            } else {
                context.getGCListener().info("compaction failed after {}, and {} cycles", watch, cycles);
                return compactionAborted(context, nextGeneration);
            }
        } catch (InterruptedException e) {
            context.getGCListener().error("compaction interrupted", e);
            currentThread().interrupt();
            return compactionAborted(context, nextGeneration);
        } catch (IOException e) {
            context.getGCListener().error("compaction encountered an error", e);
            return compactionAborted(context, nextGeneration);
        }
    }

}
