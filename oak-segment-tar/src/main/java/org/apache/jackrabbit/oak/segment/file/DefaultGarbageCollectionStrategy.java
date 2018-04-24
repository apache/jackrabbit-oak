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

import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.ESTIMATION;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.IDLE;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

class DefaultGarbageCollectionStrategy implements GarbageCollectionStrategy {

    private final EstimationStrategy fullEstimationStrategy = new FullSizeDeltaEstimationStrategy();

    private final EstimationStrategy tailEstimationStrategy = new TailSizeDeltaEstimationStrategy();

    private final CompactionStrategy fullCompactionStrategy = new FullCompactionStrategy();

    private final CompactionStrategy tailCompactionStrategy = new FallbackCompactionStrategy(new TailCompactionStrategy(), fullCompactionStrategy);

    private final CleanupStrategy cleanupStrategy = new DefaultCleanupStrategy();

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
                EstimationResult estimation = estimateCompactionGain(context, full);
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

    private EstimationResult estimateCompactionGain(Context context, boolean full) {
        EstimationStrategy strategy;

        if (full) {
            strategy = fullEstimationStrategy;
        } else {
            strategy = tailEstimationStrategy;
        }

        return estimateCompactionGain(context, strategy);
    }

    private EstimationResult estimateCompactionGain(Context context, EstimationStrategy strategy) {
        return strategy.estimate(new EstimationStrategy.Context() {

            @Override
            public long getSizeDelta() {
                return context.getGCOptions().getGcSizeDeltaEstimation();
            }

            @Override
            public long getCurrentSize() {
                return context.getTarFiles().size();
            }

            @Override
            public GCJournal getGCJournal() {
                return context.getGCJournal();
            }

        });
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

    private List<String> cleanup(Context context, CompactionResult compactionResult) throws IOException {
        return cleanupStrategy.cleanup(new CleanupStrategy.Context() {

            @Override
            public GCListener getGCListener() {
                return context.getGCListener();
            }

            @Override
            public SegmentCache getSegmentCache() {
                return context.getSegmentCache();
            }

            @Override
            public SegmentTracker getSegmentTracker() {
                return context.getSegmentTracker();
            }

            @Override
            public FileStoreStats getFileStoreStats() {
                return context.getFileStoreStats();
            }

            @Override
            public GCNodeWriteMonitor getCompactionMonitor() {
                return context.getCompactionMonitor();
            }

            @Override
            public GCJournal getGCJournal() {
                return context.getGCJournal();
            }

            @Override
            public Predicate<GCGeneration> getReclaimer() {
                return compactionResult.reclaimer();
            }

            @Override
            public TarFiles getTarFiles() {
                return context.getTarFiles();
            }

            @Override
            public Revisions getRevisions() {
                return context.getRevisions();
            }

            @Override
            public String getCompactedRootId() {
                return compactionResult.getCompactedRootId().toString10();
            }

            @Override
            public String getSegmentEvictionReason() {
                return compactionResult.gcInfo();
            }

        });
    }

}
