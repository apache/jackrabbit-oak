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
package org.apache.jackrabbit.oak.index.indexer.document;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.slf4j.Logger;

public final class IndexerStatisticsTracker {
    private static final int SLOW_DOCUMENT_LOG_THRESHOLD = Integer.getInteger("oak.indexer.slowDocumentLogThreshold", 1000);

    private final Logger logger;

    // Timestamp of when indexing started.
    private long startIndexingNanos = 0;
    // Time spent indexing entries. Should be almost the same as totalMakeDocumentTimeNanos+totalWriteTimeNanos
    private final AtomicLong totalIndexingTimeNanos = new AtomicLong();
    // Time generating the Lucene document.
    private final AtomicLong totalMakeDocumentTimeNanos = new AtomicLong();
    // Time writing the Lucene document to disk.
    private final AtomicLong totalWriteTimeNanos = new AtomicLong();
    private final AtomicLong nodesIndexed = new AtomicLong();

    public IndexerStatisticsTracker(Logger logger) {
        this.logger = logger;
    }

    public void onIndexingStarting() {
        this.startIndexingNanos = System.nanoTime();
    }

    /**
     * Mark that an entry completed indexing. If indexing the entry was slow, then a
     * message is logged.
     *
     * @param entryPath the path
     * @param startEntryNanos timestamp of when the current entry started being
     *                        indexed
     * @param endEntryMakeDocumentNanos timestamp of when the current entry finished
     *        the makeDocument phase.
     */
    public void onEntryEnd(String entryPath, long startEntryNanos, long endEntryMakeDocumentNanos) {
        long endEntryWriteNanos = System.nanoTime();
        nodesIndexed.incrementAndGet();
        long entryIndexingTimeNanos = endEntryWriteNanos - startEntryNanos;
        long entryMakeDocumentTimeNanos = endEntryMakeDocumentNanos - startEntryNanos;
        long entryWriteTimeNanos = endEntryWriteNanos - endEntryMakeDocumentNanos;
        totalIndexingTimeNanos.addAndGet(entryIndexingTimeNanos);
        totalMakeDocumentTimeNanos.addAndGet(entryMakeDocumentTimeNanos);
        totalWriteTimeNanos.addAndGet(entryWriteTimeNanos);
        if (entryIndexingTimeNanos >= (long) SLOW_DOCUMENT_LOG_THRESHOLD * 1_000_000) {
            logger.info("Slow document: {}. Times: total={}ms, makeDocument={}ms, writeToIndex={}ms",
                    entryPath, entryIndexingTimeNanos / 1_000_000, entryMakeDocumentTimeNanos / 1_000_000, entryWriteTimeNanos / 1_000_000);
        }
    }

    public String formatStats() {
        long endTimeNanos = System.nanoTime();
        long totalTimeNanos = endTimeNanos - startIndexingNanos;
        long avgTimePerDocumentMicros = Math.round(FormattingUtils.safeComputeAverage(totalIndexingTimeNanos.get() / 1000, nodesIndexed.get()));
        double percentageIndexing = FormattingUtils.safeComputePercentage(totalIndexingTimeNanos.get(), totalTimeNanos);
        double percentageMakingDocument = FormattingUtils.safeComputePercentage(totalMakeDocumentTimeNanos.get(), totalIndexingTimeNanos.get());
        double percentageWritingToIndex = FormattingUtils.safeComputePercentage(totalWriteTimeNanos.get(), totalIndexingTimeNanos.get());
        return String.format("Indexed %d nodes in %s. Avg per node: %d microseconds. indexingTime: %s (%2.1f%% of total time). Breakup of indexing time: makeDocument: %s (%2.1f%%), writeIndex: %s (%2.1f%%)",
                nodesIndexed.get(), FormattingUtils.formatNanosToSeconds(totalTimeNanos), avgTimePerDocumentMicros,
                FormattingUtils.formatNanosToSeconds(totalIndexingTimeNanos.get()), percentageIndexing,
                FormattingUtils.formatNanosToSeconds(totalMakeDocumentTimeNanos.get()), percentageMakingDocument,
                FormattingUtils.formatNanosToSeconds(totalWriteTimeNanos.get()), percentageWritingToIndex);
    }
}