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

interface PipelinedMetrics {
    String METRIC_NAME_PREFIX = "oak_indexer_pipelined_";
    String OAK_INDEXER_PIPELINED_DOCUMENTS_DOWNLOADED = METRIC_NAME_PREFIX + "documentsDownloaded";
    String OAK_INDEXER_PIPELINED_DOCUMENTS_TRAVERSED = METRIC_NAME_PREFIX + "documentsTraversed";
    String OAK_INDEXER_PIPELINED_DOCUMENTS_REJECTED_SPLIT = METRIC_NAME_PREFIX + "documentsRejectedSplit";
    String OAK_INDEXER_PIPELINED_DOCUMENTS_ACCEPTED = METRIC_NAME_PREFIX + "documentsAccepted";
    String OAK_INDEXER_PIPELINED_DOCUMENTS_REJECTED = METRIC_NAME_PREFIX + "documentsRejected";
    String OAK_INDEXER_PIPELINED_DOCUMENTS_REJECTED_EMPTY_NODE_STATE = METRIC_NAME_PREFIX + "documentsRejectedEmptyNodeState";
    String OAK_INDEXER_PIPELINED_ENTRIES_TRAVERSED = METRIC_NAME_PREFIX + "entriesTraversed";
    String OAK_INDEXER_PIPELINED_ENTRIES_ACCEPTED = METRIC_NAME_PREFIX + "entriesAccepted";
    String OAK_INDEXER_PIPELINED_ENTRIES_REJECTED = METRIC_NAME_PREFIX + "entriesRejected";
    String OAK_INDEXER_PIPELINED_ENTRIES_REJECTED_HIDDEN_PATHS = METRIC_NAME_PREFIX + "entriesRejectedHiddenPaths";
    String OAK_INDEXER_PIPELINED_ENTRIES_REJECTED_PATH_FILTERED = METRIC_NAME_PREFIX + "entriesRejectedPathFiltered";
    String OAK_INDEXER_PIPELINED_EXTRACTED_ENTRIES_TOTAL_SIZE = METRIC_NAME_PREFIX + "extractedEntriesTotalSize";
    String OAK_INDEXER_PIPELINED_MONGO_DOWNLOAD_ENQUEUE_DELAY_PERCENTAGE= METRIC_NAME_PREFIX + "mongoDownloadEnqueueDelayPercentage";
    String OAK_INDEXER_PIPELINED_MERGE_SORT_INTERMEDIATE_FILES_COUNT = METRIC_NAME_PREFIX + "mergeSortIntermediateFilesCount";
    String OAK_INDEXER_PIPELINED_MERGE_SORT_EAGER_MERGES_RUNS = METRIC_NAME_PREFIX + "mergeSortEagerMergesRuns";
    String OAK_INDEXER_PIPELINED_MERGE_SORT_FINAL_MERGE_FILES_COUNT = METRIC_NAME_PREFIX + "mergeSortFinalMergeFilesCount";
    String OAK_INDEXER_PIPELINED_MERGE_SORT_FINAL_MERGE_TIME = METRIC_NAME_PREFIX + "mergeSortFinalMergeTime";

}
