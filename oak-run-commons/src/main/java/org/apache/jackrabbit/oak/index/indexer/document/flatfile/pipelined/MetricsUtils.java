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
    public static final String OAK_INDEXER_PIPELINED_DOCUMENTS_ACCEPTED = "oak_indexer_pipelined_documentsAccepted";
    public static final String OAK_INDEXER_PIPELINED_DOCUMENTS_REJECTED = "oak_indexer_pipelined_documentsRejected";
    public static final String OAK_INDEXER_PIPELINED_DOCUMENTS_REJECTED_EMPTY_NODE_STATE = "oak_indexer_pipelined_documentsRejectedEmptyNodeState";
    public static final String OAK_INDEXER_PIPELINED_ENTRIES_TRAVERSED = "oak_indexer_pipelined_entriesTraversed";
    public static final String OAK_INDEXER_PIPELINED_ENTRIES_ACCEPTED = "oak_indexer_pipelined_entriesAccepted";
    public static final String OAK_INDEXER_PIPELINED_ENTRIES_REJECTED = "oak_indexer_pipelined_entriesRejected";
    public static final String OAK_INDEXER_PIPELINED_ENTRIES_REJECTED_HIDDEN_PATHS = "oak_indexer_pipelined_entriesRejectedHiddenPaths";
    public static final String OAK_INDEXER_PIPELINED_ENTRIES_REJECTED_PATH_FILTERED = "oak_indexer_pipelined_entriesRejectedPathFiltered";
    public static final String OAK_INDEXER_PIPELINED_EXTRACTED_ENTRIES_TOTAL_SIZE = "oak_indexer_pipelined_extractedEntriesTotalSize";
    public static final String OAK_INDEXER_PIPELINED_MONGO_DOWNLOAD_ENQUEUE_DELAY_PERCENTAGE= "oak_indexer_pipelined_mongoDownloadEnqueueDelayPercentage";
    public static final String OAK_INDEXER_PIPELINED_MERGE_SORT_INTERMEDIATE_FILES_COUNT = "oak_indexer_pipelined_mergeSortIntermediateFilesCount";
    public static final String OAK_INDEXER_PIPELINED_MERGE_SORT_EAGER_MERGES_RUNS = "oak_indexer_pipelined_mergeSortEagerMergesRuns";
    public static final String OAK_INDEXER_PIPELINED_MERGE_SORT_FINAL_MERGE_FILES_COUNT = "oak_indexer_pipelined_mergeSortFinalMergeFilesCount";
    public static final String OAK_INDEXER_PIPELINED_MERGE_SORT_FINAL_MERGE_TIME = "oak_indexer_pipelined_mergeSortFinalMergeTime";

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
