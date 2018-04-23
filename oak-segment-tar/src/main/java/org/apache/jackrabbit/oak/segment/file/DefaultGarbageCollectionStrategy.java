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
import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.CLEANUP;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.ESTIMATION;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.IDLE;
import static org.apache.jackrabbit.oak.segment.file.PrintableBytes.newPrintableBytes;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.tar.CleanupContext;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

class DefaultGarbageCollectionStrategy implements GarbageCollectionStrategy {

    private final CompactionStrategy fullCompactionStrategy = new FullCompactionStrategy();

    private final CompactionStrategy tailCompactionStrategy = new FallbackCompactionStrategy(new TailCompactionStrategy(), fullCompactionStrategy);

    private GCGeneration getGcGeneration(Context context) {
        return context.getRevisions().getHead().getSegmentId().getGcGeneration();
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

    private static CompactionStrategy.Context newCompactionStrategyContext(Context context) {
        return new CompactionStrategy.Context() {

            @Override
            public SegmentTracker getSegmentTracker() {
                return context.getSegmentTracker();
            }

            @Override
            public GCListener getGCListener() {
                return context.getGCListener();
            }

            @Override
            public GCJournal getGCJournal() {
                return context.getGCJournal();
            }

            @Override
            public SegmentGCOptions getGCOptions() {
                return context.getGCOptions();
            }

            @Override
            public GCNodeWriteMonitor getCompactionMonitor() {
                return context.getCompactionMonitor();
            }

            @Override
            public SegmentReader getSegmentReader() {
                return context.getSegmentReader();
            }

            @Override
            public SegmentWriterFactory getSegmentWriterFactory() {
                return context.getSegmentWriterFactory();
            }

            @Override
            public Revisions getRevisions() {
                return context.getRevisions();
            }

            @Override
            public TarFiles getTarFiles() {
                return context.getTarFiles();
            }

            @Override
            public BlobStore getBlobStore() {
                return context.getBlobStore();
            }

            @Override
            public CancelCompactionSupplier getCanceller() {
                return context.getCanceller();
            }

            @Override
            public int getGCCount() {
                return context.getGCCount();
            }

            @Override
            public SuccessfulCompactionListener getSuccessfulCompactionListener() {
                return context.getSuccessfulCompactionListener();
            }

            @Override
            public Flusher getFlusher() {
                return context.getFlusher();
            }

        };
    }

    @Override
    public synchronized CompactionResult compactFull(Context context) {
        return fullCompactionStrategy.compact(newCompactionStrategyContext(context));
    }

    @Override
    public synchronized CompactionResult compactTail(Context context) {
        return tailCompactionStrategy.compact(newCompactionStrategyContext(context));
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
