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

import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCStatus.CLEANUP;
import static org.apache.jackrabbit.oak.segment.file.PrintableBytes.newPrintableBytes;

import java.io.IOException;
import java.util.List;

import org.apache.jackrabbit.oak.segment.file.tar.CleanupContext;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.jetbrains.annotations.NotNull;

class CleanupFirstCompactionStrategy implements CompactionStrategy {

    private final GarbageCollectionStrategy.Context parentContext;

    private final CompactionStrategy strategy;

    CleanupFirstCompactionStrategy(GarbageCollectionStrategy.Context parentContext, CompactionStrategy strategy) {
        this.parentContext = parentContext;
        this.strategy = strategy;
    }

    @Override
    public CompactionResult compact(Context context) throws IOException {

        // This is a slightly modified version of the default cleanup phase when
        // invoked with a successful compaction result. There are some important
        // differences deriving from the fact that we are assuming that the compaction
        // for `newGeneration` is going to succeed.

        // First, we don't have a RecordId for the compacted state, because it didn't
        // happen yet. This shouldn't matter, because we are not going to advance the
        // `gcJournal` to the `RecordId` of the compacted state.

        // Second, we are using a custom reclaimer that is similar to the one returned
        // by `newOldReclaimer`, but that also takes in consideration that
        // `newGeneration` has not been committed yet, and the most recent transient
        // state shouldn't be removed.

        // Third, we don't clear the segment cache. There might be transient segments
        // in there, and we don't want those segments to be removed.

        // Fourth, the following code assumes the number of retained generations fixed
        // to two, which is also the default value for the Segment Store. A complete
        // solution should be flexible enough to accommodate other values for the
        // number of retained generations.

        PrintableStopwatch watch = PrintableStopwatch.createStarted();

        context.getGCListener().info("pre-compaction cleanup started");
        context.getGCListener().updateStatus(CLEANUP.message());

        // Suggest to the JVM that now would be a good time to clear stale weak
        // references in the SegmentTracker

        System.gc();

        TarFiles.CleanupResult cleanupResult = context.getTarFiles().cleanup(newCleanupContext(context));

        if (cleanupResult.isInterrupted()) {
            context.getGCListener().info("cleanup interrupted");
        }

        context.getSegmentTracker().clearSegmentIdTables(cleanupResult.getReclaimedSegmentIds(), "[pre-compaction cleanup]");
        context.getGCListener().info("cleanup marking files for deletion: {}", toFileNames(cleanupResult.getRemovableFiles()));

        long finalSize = context.getTarFiles().size();
        long reclaimedSize = cleanupResult.getReclaimedSize();
        parentContext.getFileStoreStats().reclaimed(reclaimedSize);
        context.getGCListener().cleaned(reclaimedSize, finalSize);
        context.getGCListener().info(
            "pre-compaction cleanup completed in {}. Post cleanup size is {} and space reclaimed {}.",
            watch,
            newPrintableBytes(finalSize),
            newPrintableBytes(reclaimedSize));
        parentContext.getFileReaper().add(cleanupResult.getRemovableFiles());

        // Invoke the wrapped compaction phase

        return strategy.compact(context);
    }

    private static CleanupContext newCleanupContext(Context context) {
        GCGeneration currentGeneration = context.getRevisions().getHead().getSegmentId().getGcGeneration();
        String compactedRoot = context.getGCJournal().read().getRoot();

        switch (context.getGCOptions().getGCType()) {
            case FULL:
                return new DefaultCleanupContext(context.getSegmentTracker(), generation -> {
                    if (generation == null) {
                        return false;
                    }
                    if (generation.getFullGeneration() < currentGeneration.getFullGeneration()) {
                        return true;
                    }
                    return generation.getGeneration() < currentGeneration.getGeneration() && !generation.isCompacted();
                }, compactedRoot);
            case TAIL:
                return new DefaultCleanupContext(context.getSegmentTracker(), generation -> {
                    if (generation == null) {
                        return false;
                    }
                    if (generation.getFullGeneration() < currentGeneration.getFullGeneration() - 1) {
                        return true;
                    }
                    if (generation.getFullGeneration() == currentGeneration.getFullGeneration() - 1) {
                        return !generation.isCompacted();
                    }
                    return generation.getGeneration() < currentGeneration.getGeneration() && !generation.isCompacted();
                }, compactedRoot);
            default:
                throw new IllegalArgumentException("invalid garbage collection type");
        }
    }

    private static String toFileNames(@NotNull List<String> files) {
        if (files.isEmpty()) {
            return "none";
        } else {
            return String.join(",", files);
        }
    }
}
