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

class DefaultCleanupStrategy implements CleanupStrategy {

    @Override
    public List<String> cleanup(Context context) throws IOException {
        PrintableStopwatch watch = PrintableStopwatch.createStarted();

        context.getGCListener().info("cleanup started using reclaimer {}", context.getReclaimer());
        context.getGCListener().updateStatus(CLEANUP.message());
        context.getSegmentCache().clear();

        // Suggest to the JVM that now would be a good time
        // to clear stale weak references in the SegmentTracker
        System.gc();

        TarFiles.CleanupResult cleanupResult = context.getTarFiles().cleanup(newCleanupContext(context));
        if (cleanupResult.isInterrupted()) {
            context.getGCListener().info("cleanup interrupted");
        }
        context.getSegmentTracker().clearSegmentIdTables(cleanupResult.getReclaimedSegmentIds(), context.getSegmentEvictionReason());
        context.getGCListener().info("cleanup marking files for deletion: {}", toFileNames(cleanupResult.getRemovableFiles()));

        long finalSize = size(context);
        long reclaimedSize = cleanupResult.getReclaimedSize();
        context.getFileStoreStats().reclaimed(reclaimedSize);

        GCJournal gcJournal = context.getGCJournal();
        if (gcJournal != null) {
            gcJournal.persist(
                    reclaimedSize,
                    finalSize,
                    getGcGeneration(context),
                    context.getCompactionMonitor().getCompactedNodes(),
                    context.getCompactedRootId()
            );
        }
        context.getGCListener().cleaned(reclaimedSize, finalSize);
        context.getGCListener().info(
            "cleanup completed in {}. Post cleanup size is {} and space reclaimed {}.",
            watch,
            newPrintableBytes(finalSize),
            newPrintableBytes(reclaimedSize)
        );
        return cleanupResult.getRemovableFiles();
    }

    private static CleanupContext newCleanupContext(Context context) {
        return new DefaultCleanupContext(context.getSegmentTracker(), context.getReclaimer(),
                context.getCompactedRootId());
    }

    private static String toFileNames(@NotNull List<String> files) {
        if (files.isEmpty()) {
            return "none";
        } else {
            return String.join(",", files);
        }
    }

    private static GCGeneration getGcGeneration(Context context) {
        return context.getRevisions().getHead().getSegmentId().getGcGeneration();
    }

    private static long size(Context context) {
        return context.getTarFiles().size();
    }

}
