/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

public interface ElasticsearchIndexConstants {
    String TYPE_ELASTICSEARCH = "elasticsearch";

    String BULK_ACTIONS = "bulkActions";
    int BULK_ACTIONS_DEFAULT = 250;

    String BULK_SIZE_BYTES = "bulkSizeBytes";
    long BULK_SIZE_BYTES_DEFAULT = 2_097_152; // 2MB

    String BULK_FLUSH_INTERVAL_MS = "bulkFlushIntervalMs";
    long BULK_FLUSH_INTERVAL_MS_DEFAULT = 3000;

    String BULK_RETRIES = "bulkRetries";
    int BULK_RETRIES_DEFAULT = 3;

    String BULK_RETRIES_BACKOFF = "bulkRetriesBackoff";
    long BULK_RETRIES_BACKOFF_DEFAULT = 200;
}
