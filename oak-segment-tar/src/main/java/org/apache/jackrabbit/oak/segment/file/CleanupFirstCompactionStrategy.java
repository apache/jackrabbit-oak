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
import static org.apache.jackrabbit.oak.segment.file.PrintableBytes.newPrintableBytes;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.file.tar.CleanupContext;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;

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

        Predicate<GCGeneration> reclaimer;

        GCGeneration currentGeneration = context.getRevisions().getHead().getSegmentId().getGcGeneration();

        switch (context.getGCOptions().getGCType()) {
            case FULL:
                reclaimer = generation -> {
                    if (generation == null) {
                        return false;
                    }
                    if (generation.getFullGeneration() < currentGeneration.getFullGeneration()) {
                        return true;
                    }
                    if (generation.getFullGeneration() > currentGeneration.getFullGeneration()) {
                        return true;
                    }
                    return generation.getGeneration() < currentGeneration.getGeneration() && !generation.isCompacted();
                };
                break;
            case TAIL:
                reclaimer = generation -> {
                    if (generation == null) {
                        return false;
                    }
                    if (generation.getFullGeneration() < currentGeneration.getFullGeneration() - 1) {
                        return true;
                    }
                    if (generation.getFullGeneration() == currentGeneration.getFullGeneration() - 1) {
                        return !generation.isCompacted();
                    }
                    if (generation.getFullGeneration() > currentGeneration.getFullGeneration()) {
                        return true;
                    }
                    return generation.getGeneration() < currentGeneration.getGeneration() && !generation.isCompacted();
                };
                break;
            default:
                throw new IllegalArgumentException("invalid garbage collection type");
        }

        context.getGCListener().info("pre-compaction cleanup started");
        context.getGCListener().updateStatus(CLEANUP.message());

        // Suggest to the JVM that now would be a good time to clear stale weak
        // references in the SegmentTracker

        System.gc();

        TarFiles.CleanupResult cleanupResult = context.getTarFiles().cleanup(newCleanupContext(context, reclaimer));

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

    private static CleanupContext newCleanupContext(Context context, Predicate<GCGeneration> old) {
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

    private static String toFileNames(@Nonnull List<String> files) {
        if (files.isEmpty()) {
            return "none";
        } else {
            return Joiner.on(",").join(files);
        }
    }

    private static GCGeneration getGcGeneration(Context context) {
        return context.getRevisions().getHead().getSegmentId().getGcGeneration();
    }

    private static long size(Context context) {
        return context.getTarFiles().size();
    }

}
