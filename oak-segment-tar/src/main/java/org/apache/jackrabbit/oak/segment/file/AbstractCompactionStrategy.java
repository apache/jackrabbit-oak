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

import org.apache.jackrabbit.oak.segment.CheckpointCompactor;
import org.apache.jackrabbit.oak.segment.ClassicCompactor;
import org.apache.jackrabbit.oak.segment.Compactor;
import org.apache.jackrabbit.oak.segment.ParallelCompactor;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.CompactorType;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType;
import org.apache.jackrabbit.oak.segment.file.cancel.Cancellation;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

abstract class AbstractCompactionStrategy implements CompactionStrategy {

    abstract GCType getCompactionType();

    abstract GCGeneration partialGeneration(GCGeneration current);

    abstract GCGeneration targetGeneration(GCGeneration current);

    private CompactionResult compactionSucceeded(
        Context context,
        GCGeneration generation,
        RecordId compactedRootId
    ) {
        context.getGCListener().compactionSucceeded(generation);
        return CompactionResult.succeeded(getCompactionType(), generation, context.getGCOptions(), compactedRootId, context.getGCCount());
    }

    private CompactionResult compactionPartiallySucceeded(
            Context context,
            GCGeneration generation,
            RecordId compactedRootId
    ) {
        context.getGCListener().compactionSucceeded(generation);
        return CompactionResult.partiallySucceeded(generation, compactedRootId, context.getGCCount());
    }

    private static CompactionResult compactionAborted(Context context, GCGeneration generation) {
        context.getGCListener().compactionFailed(generation);
        return CompactionResult.aborted(getGcGeneration(context), context.getGCCount());
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

    private static CompactedNodeState forceCompact(
        Context context,
        NodeState base,
        NodeState onto,
        Compactor compactor,
        Canceller canceller
    ) throws InterruptedException {
        AtomicReference<CompactedNodeState> compacted = new AtomicReference<>();
        context.getRevisions().setHead(headId -> {
            try {
                PrintableStopwatch t = PrintableStopwatch.createStarted();
                NodeState currentHead = context.getSegmentReader().readNode(headId);
                CompactedNodeState after = compactor.compact(base, currentHead, onto, canceller);
                if (after != null) {
                    compacted.set(after);
                    return after.getRecordId();
                }
                context.getGCListener().info("compaction cancelled after {}", t);
                return null;
            } catch (IOException e) {
                context.getGCListener().error("error during forced compaction.", e);
                return null;
            }
        }, timeout(context.getGCOptions().getForceTimeout(), SECONDS));
        return compacted.get();
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

    private boolean setHead(Context context, SegmentNodeState previous, SegmentNodeState head) {
        return context.getRevisions().setHead(previous.getRecordId(), head.getRecordId(), EXPEDITE_OPTION);
    }

    final CompactionResult compact(Context context, NodeState base) {
        context.getGCListener().info("running {} compaction", formatCompactionType(getCompactionType()));

        GCGeneration baseGeneration = getGcGeneration(context);
        GCGeneration partialGeneration = partialGeneration(baseGeneration);
        GCGeneration targetGeneration = targetGeneration(baseGeneration);
        GCIncrement gcIncrement = new GCIncrement(baseGeneration, partialGeneration, targetGeneration);

        try {
            PrintableStopwatch watch = PrintableStopwatch.createStarted();
            context.getGCListener().info(
                "compaction started, gc options={},\n{}",
                context.getGCOptions(),
                gcIncrement
            );
            context.getGCListener().updateStatus(COMPACTION.message());

            GCJournal.GCJournalEntry gcEntry = context.getGCJournal().read();
            long initialSize = size(context);

            CompactionWriter writer = new CompactionWriter(
                    context.getSegmentReader(),
                    context.getBlobStore(),
                    gcIncrement,
                    context.getSegmentWriterFactory());

            context.getCompactionMonitor().init(gcEntry.getRepoSize(), gcEntry.getNodes(), initialSize);

            Canceller hardCanceller = context.getHardCanceller().withShortCircuit();
            Canceller softCanceller = context.getSoftCanceller().withShortCircuit();

            Compactor compactor = newCompactor(context, writer);
            CompactedNodeState compacted = null;

            int cycles;
            boolean success;
            final int retryCount = Math.max(0, context.getGCOptions().getRetryCount());

            SegmentNodeState head;
            Flusher flusher = () -> {
                writer.flush();
                context.getFlusher().flush();
            };

            do {
                head = getHead(context);
                SegmentNodeState after = (compacted == null) ? head : compacted;
                Canceller stateSaveTrigger = context.getStateSaveTriggerSupplier().get().withShortCircuit();

                if (stateSaveTrigger.isCancelable()) {
                    context.getGCListener().info("intermediate state save enabled.");
                    Canceller saveStateCanceller = softCanceller.withCondition(
                            "save intermediate compaction state", () -> stateSaveTrigger.check().isCancelled());
                    compacted = compactor.compactDown(base, after, hardCanceller, saveStateCanceller);
                } else if (softCanceller.isCancelable()) {
                    context.getGCListener().info("soft cancellation enabled.");
                    compacted = compactor.compactDown(base, after, hardCanceller, softCanceller);
                } else {
                    compacted = compactor.compactUp(base, after, hardCanceller);
                }

                if (compacted == null) {
                    context.getGCListener().warn("compaction cancelled: {}.",
                            hardCanceller.check().getReason().orElse("unknown reason"));
                    return compactionAborted(context, targetGeneration);
                }

                context.getGCListener().info("compaction cycle 0 completed in {}. Compacted {} to {}",
                        watch, head.getRecordId(), compacted.getRecordId());

                cycles = 0;

                while (!(success = setHead(context, head, compacted)) && cycles < retryCount) {
                    // Some other concurrent changes have been made.
                    // Rebase (and compact) those changes on top of the
                    // compacted state before retrying to set the head.
                    cycles++;
                    context.getGCListener().info("compaction detected concurrent commits while compacting. " +
                                    "Compacting these commits. Cycle {} of {}", cycles, retryCount);
                    context.getGCListener().updateStatus(COMPACTION_RETRY.message() + cycles);
                    PrintableStopwatch cycleWatch = PrintableStopwatch.createStarted();

                    SegmentNodeState newHead = getHead(context);
                    compacted = compactor.compact(head, newHead,compacted, hardCanceller);
                    if (compacted == null) {
                        context.getGCListener().warn("compaction cancelled: {}.",
                                hardCanceller.check().getReason().orElse("unknown reason"));
                        return compactionAborted(context, targetGeneration);
                    }

                    context.getGCListener().info("compaction cycle {} completed in {}. Compacted {} against {} to {}",
                            cycles, cycleWatch, head.getRecordId(), newHead.getRecordId(), compacted.getRecordId());
                    head = newHead;
                }

                if (success) {
                    flusher.flush();
                }
            } while (success && !compacted.isComplete() && !softCanceller.check().isCancelled());

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

                    Canceller forcedCompactionCanceller = hardCanceller
                        .withTimeout("forced compaction timeout exceeded", forceTimeout, SECONDS)
                        .withShortCircuit();
                    compacted = forceCompact(context, head, compacted, compactor, forcedCompactionCanceller);
                    if (compacted != null) {
                        success = true;
                        flusher.flush();
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
                context.getCompactionMonitor().finished();

                if (compacted.isComplete()) {
                    context.getGCListener().info("compaction succeeded in {}, after {} cycles", watch, cycles);
                    return compactionSucceeded(context, targetGeneration, compacted.getRecordId());
                } else {
                    context.getGCListener().info("compaction partially succeeded in {}: {}.",
                            watch, softCanceller.check().getReason().orElse("unknown reason"));
                    return compactionPartiallySucceeded(context, partialGeneration, compacted.getRecordId());
                }
            } else {
                context.getGCListener().info("compaction failed after {}, and {} cycles", watch, cycles);
                return compactionAborted(context, targetGeneration);
            }
        } catch (InterruptedException e) {
            context.getGCListener().error("compaction interrupted", e);
            currentThread().interrupt();
            return compactionAborted(context, targetGeneration);
        } catch (Throwable e) {
            context.getGCListener().error("compaction encountered an error", e instanceof Exception ? (Exception) e : new Exception(e));
            return compactionAborted(context, targetGeneration);
        }
    }

    private Compactor newCompactor(Context context, CompactionWriter writer) {
        CompactorType compactorType = context.getGCOptions().getCompactorType();
        switch (compactorType) {
            case PARALLEL_COMPACTOR:
                return new ParallelCompactor(context.getGCListener(), writer, context.getCompactionMonitor(),
                        context.getGCOptions().getConcurrency());
            case CHECKPOINT_COMPACTOR:
                return new CheckpointCompactor(context.getGCListener(), writer, context.getCompactionMonitor());
            case CLASSIC_COMPACTOR:
                return new ClassicCompactor(writer, context.getCompactionMonitor());
            default:
                throw new IllegalArgumentException("Unknown compactor type: " + compactorType);
            }
        }
    }
