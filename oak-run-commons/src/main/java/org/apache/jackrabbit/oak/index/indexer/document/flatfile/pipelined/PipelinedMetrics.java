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

public final class PipelinedMetrics {
    // Conventions for naming metrics: https://prometheus.io/docs/practices/naming/
    public static final String METRIC_NAME_PREFIX = "oak_indexer_pipelined_";
    public static final String OAK_INDEXER_PIPELINED_MONGO_DOWNLOAD_DURATION_SECONDS = METRIC_NAME_PREFIX + "mongo_download_duration_seconds";
    public static final String OAK_INDEXER_PIPELINED_MONGO_DOWNLOAD_ENQUEUE_DELAY_PERCENTAGE = METRIC_NAME_PREFIX + "mongo_download_enqueue_delay_percentage";
    public static final String OAK_INDEXER_PIPELINED_DOCUMENTS_DOWNLOADED_TOTAL = METRIC_NAME_PREFIX + "documents_downloaded_total";
    public static final String OAK_INDEXER_PIPELINED_DOCUMENTS_TRAVERSED_TOTAL = METRIC_NAME_PREFIX + "documents_traversed_total";
    public static final String OAK_INDEXER_PIPELINED_DOCUMENTS_REJECTED_SPLIT_TOTAL = METRIC_NAME_PREFIX + "documents_rejected_split_total";
    public static final String OAK_INDEXER_PIPELINED_DOCUMENTS_ACCEPTED_TOTAL = METRIC_NAME_PREFIX + "documents_accepted_total";
    public static final String OAK_INDEXER_PIPELINED_DOCUMENTS_REJECTED_TOTAL = METRIC_NAME_PREFIX + "documents_rejected_total";
    public static final String OAK_INDEXER_PIPELINED_DOCUMENTS_ACCEPTED_PERCENTAGE = METRIC_NAME_PREFIX + "documents_accepted_percentage";
    public static final String OAK_INDEXER_PIPELINED_DOCUMENTS_REJECTED_EMPTY_NODE_STATE_TOTAL = METRIC_NAME_PREFIX + "documents_rejected_empty_node_state_total";
    public static final String OAK_INDEXER_PIPELINED_ENTRIES_TRAVERSED_TOTAL = METRIC_NAME_PREFIX + "entries_traversed_total";
    public static final String OAK_INDEXER_PIPELINED_ENTRIES_ACCEPTED_TOTAL = METRIC_NAME_PREFIX + "entries_accepted_total";
    public static final String OAK_INDEXER_PIPELINED_ENTRIES_ACCEPTED_PERCENTAGE = METRIC_NAME_PREFIX + "entries_accepted_percentage";
    public static final String OAK_INDEXER_PIPELINED_ENTRIES_REJECTED_TOTAL = METRIC_NAME_PREFIX + "entries_rejected_total";
    public static final String OAK_INDEXER_PIPELINED_ENTRIES_REJECTED_HIDDEN_PATHS_TOTAL = METRIC_NAME_PREFIX + "entries_rejected_hidden_paths_total";
    public static final String OAK_INDEXER_PIPELINED_ENTRIES_REJECTED_PATH_FILTERED_TOTAL = METRIC_NAME_PREFIX + "entries_rejected_path_filtered_total";
    public static final String OAK_INDEXER_PIPELINED_EXTRACTED_ENTRIES_TOTAL_BYTES = METRIC_NAME_PREFIX + "extracted_entries_total_bytes";
    public static final String OAK_INDEXER_PIPELINED_SORT_BATCH_PHASE_CREATE_SORT_ARRAY_PERCENTAGE = METRIC_NAME_PREFIX + "sort_batch_phase_create_sort_array_percentage";
    public static final String OAK_INDEXER_PIPELINED_SORT_BATCH_PHASE_SORT_ARRAY_PERCENTAGE = METRIC_NAME_PREFIX + "sort_batch_phase_sort_array_percentage";
    public static final String OAK_INDEXER_PIPELINED_SORT_BATCH_PHASE_WRITE_TO_DISK_PERCENTAGE = METRIC_NAME_PREFIX + "sort_batch_phase_write_to_disk_percentage";
    public static final String OAK_INDEXER_PIPELINED_MERGE_SORT_INTERMEDIATE_FILES_TOTAL = METRIC_NAME_PREFIX + "merge_sort_intermediate_files_total";
    public static final String OAK_INDEXER_PIPELINED_MERGE_SORT_EAGER_MERGES_RUNS_TOTAL = METRIC_NAME_PREFIX + "merge_sort_eager_merges_runs_total";
    public static final String OAK_INDEXER_PIPELINED_MERGE_SORT_FINAL_MERGE_FILES_COUNT_TOTAL = METRIC_NAME_PREFIX + "merge_sort_final_merge_files_total";
    public static final String OAK_INDEXER_PIPELINED_MERGE_SORT_FLAT_FILE_STORE_SIZE_BYTES = METRIC_NAME_PREFIX + "merge_sort_flat_file_store_size_bytes";
    public static final String OAK_INDEXER_PIPELINED_MERGE_SORT_FINAL_MERGE_DURATION_SECONDS = METRIC_NAME_PREFIX + "merge_sort_final_merge_duration_seconds";

    private PipelinedMetrics() {
    }

}
