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
package org.apache.jackrabbit.oak.index;

public interface IndexerMetrics {
    String INDEXER_METRICS_PREFIX = "oak_indexer_";
    String METRIC_INDEXING_INDEX_DATA_SIZE = INDEXER_METRICS_PREFIX + "index_data_size";

    String INDEXER_PUBLISH_METRICS_PREFIX = "oak_indexer_publish_";
    String METRIC_INDEXING_PUBLISH_DURATION_SECONDS = INDEXER_PUBLISH_METRICS_PREFIX + "indexing_duration_seconds";
    String METRIC_INDEXING_PUBLISH_NODES_TRAVERSED = INDEXER_PUBLISH_METRICS_PREFIX + "nodes_traversed";
    String METRIC_INDEXING_PUBLISH_NODES_INDEXED = INDEXER_PUBLISH_METRICS_PREFIX + "nodes_indexed";
}
