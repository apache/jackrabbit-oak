package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsUtils {
    private final static Logger LOG = LoggerFactory.getLogger(MetricsUtils.class);
    public static final String OAK_INDEXER_PIPELINED_DOCUMENTS_DOWNLOADED = "oak_indexer_pipelined_documentsDownloaded";
    public static final String OAK_INDEXER_PIPELINED_DOCUMENTS_TRAVERSED = "oak_indexer_pipelined_documentsTraversed";
    public static final String OAK_INDEXER_PIPELINED_DOCUMENTS_REJECTED_SPLIT = "oak_indexer_pipelined_documentsRejectedSplit";
    public static final String OAK_INDEXER_PIPELINED_DOCUMENTS_REJECTED_EMPTY_NODE_STATE = "oak_indexer_pipelined_documentsRejectedEmptyNodeState";
    public static final String OAK_INDEXER_PIPELINED_ENTRIES_ACCEPTED = "oak_indexer_pipelined_entriesAccepted";
    public static final String OAK_INDEXER_PIPELINED_ENTRIES_REJECTED = "oak_indexer_pipelined_entriesRejected";
    public static final String OAK_INDEXER_PIPELINED_ENTRIES_REJECTED_HIDDEN_PATHS = "oak_indexer_pipelined_entriesRejectedHiddenPaths";
    public static final String OAK_INDEXER_PIPELINED_ENTRIES_REJECTED_PATH_FILTERED = "oak_indexer_pipelined_entriesRejectedPathFiltered";
    public static final String OAK_INDEXER_PIPELINED_EXTRACTED_ENTRIES_TOTAL_SIZE = "oak_indexer_pipelined_extractedEntriesTotalSize";

    public static void setCounter(StatisticsProvider statisticsProvider, String name, long value) {
        CounterStats metric = statisticsProvider.getCounterStats(name, StatsOptions.METRICS_ONLY);
        LOG.debug("Adding metric: {} {}", name, value);
        if (metric.getCount() != 0) {
            LOG.warn("Counter was not 0: {} {}", name, metric.getCount());
            // Reset to 0
            metric.dec(metric.getCount());
        }
        metric.inc(value);
    }

}
