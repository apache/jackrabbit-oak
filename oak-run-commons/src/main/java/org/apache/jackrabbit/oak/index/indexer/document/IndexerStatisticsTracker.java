package org.apache.jackrabbit.oak.index.indexer.document;

import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.slf4j.Logger;

public final class IndexerStatisticsTracker {
    private static final int SLOW_DOCUMENT_LOG_THRESHOLD = Integer.getInteger("oak.indexer.slowDocumentLogThreshold", 1000);

    private final Logger logger;

    // Timestamp of when indexing started.
    private long startIndexingNanos = 0;
    // Time spent indexing entries. Should be almost the same as totalMakeDocumentTimeNanos+totalWriteTimeNanos
    private long totalIndexingTimeNanos = 0;
    // Time making generating the Lucene document.
    private long totalMakeDocumentTimeNanos = 0;
    // Time writing the Lucene document to disk.
    private long totalWriteTimeNanos = 0;
    private long nodesIndexed = 0;

    // Timestamp of when the current entry started being indexed
    private long startEntryNanos = 0;
    // Timestamp of when the current entry finished the makeDocument phase.
    private long endEntryMakeDocumentNanos = 0;

    public IndexerStatisticsTracker(Logger logger) {
        this.logger = logger;
    }

    public void onIndexingStarting() {
        this.startIndexingNanos = System.nanoTime();
    }

    public void onEntryStart() {
        startEntryNanos = System.nanoTime();
    }

    public void onEntryEndMakeDocument() {
        endEntryMakeDocumentNanos = System.nanoTime();
    }

    public void onEntryEnd(String entryPath) {
        long endEntryWriteNanos = System.nanoTime();
        nodesIndexed++;
        long entryIndexingTimeNanos = endEntryWriteNanos - startEntryNanos;
        long entryMakeDocumentTimeNanos = endEntryMakeDocumentNanos - startEntryNanos;
        long entryWriteTimeNanos = endEntryWriteNanos - endEntryMakeDocumentNanos;
        totalIndexingTimeNanos += entryIndexingTimeNanos;
        totalMakeDocumentTimeNanos += entryMakeDocumentTimeNanos;
        totalWriteTimeNanos += entryWriteTimeNanos;
        if (entryIndexingTimeNanos >= (long) SLOW_DOCUMENT_LOG_THRESHOLD * 1_000_000) {
            logger.info("Slow document: {}. Times: total={}ms, makeDocument={}ms, writeToIndex={}ms",
                    entryPath, entryIndexingTimeNanos / 1_000_000, entryMakeDocumentTimeNanos / 1_000_000, entryWriteTimeNanos / 1_000_000);
        }
        startEntryNanos = 0;
        endEntryMakeDocumentNanos = 0;
    }

    public String formatStats() {
        long endTimeNanos = System.nanoTime();
        long totalTimeNanos = endTimeNanos - startIndexingNanos;
        long avgTimePerDocumentMicros = nodesIndexed == 0 ? -1 : (totalIndexingTimeNanos / nodesIndexed) / 1000;
        double percentageIndexing = FormattingUtils.safeComputePercentage(totalIndexingTimeNanos, totalTimeNanos);
        double percentageMakingDocument = FormattingUtils.safeComputePercentage(totalMakeDocumentTimeNanos, totalIndexingTimeNanos);
        double percentageWritingToIndex = FormattingUtils.safeComputePercentage(totalWriteTimeNanos, totalIndexingTimeNanos);
        return String.format("Indexed %d nodes in %s. Avg per node: %d microseconds. indexingTime: %s (%2.1f%% of total time). Breakup of indexing time: makeDocument: %s (%2.1f%%), writeIndex: %s (%2.1f%%)",
                nodesIndexed, FormattingUtils.formatNanosToSeconds(totalTimeNanos), avgTimePerDocumentMicros,
                FormattingUtils.formatNanosToSeconds(totalIndexingTimeNanos), percentageIndexing,
                FormattingUtils.formatNanosToSeconds(totalMakeDocumentTimeNanos), percentageMakingDocument,
                FormattingUtils.formatNanosToSeconds(totalWriteTimeNanos), percentageWritingToIndex);
    }
}