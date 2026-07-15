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

import org.apache.commons.io.FileUtils;

/**
 * Indexer configuration for parallel indexing
 */
public class IndexerConfiguration {
    public static final String PROP_OAK_INDEXER_THREAD_POOL_SIZE = "oak.indexer.threadPoolSize";
    static final int DEFAULT_OAK_INDEXER_THREAD_POOL_SIZE = 1;
    
    public static int indexThreadPoolSize() {
        return Integer.getInteger(PROP_OAK_INDEXER_THREAD_POOL_SIZE, DEFAULT_OAK_INDEXER_THREAD_POOL_SIZE);
    }
    
    public static final String PROP_OAK_INDEXER_MIN_SPLIT_THRESHOLD = "oak.indexer.minSplitThreshold";
    static final long DEFAULT_OAK_INDEXER_MINIMUM_SPLIT_THRESHOLD = 10 * FileUtils.ONE_MB;
    
    public static long minSplitThreshold() {
        return
            Long.getLong(PROP_OAK_INDEXER_MIN_SPLIT_THRESHOLD, DEFAULT_OAK_INDEXER_MINIMUM_SPLIT_THRESHOLD);
    }

    /**
     * System property for specifying number of FlatFileStore to be split into.
     */
    public static final String PROP_SPLIT_STORE_SIZE = "oak.indexer.splitStoreSize";
    static final int DEFAULT_SPLIT_STORE_SIZE = 8;

    public static long splitSize() {
        return Integer.getInteger(PROP_SPLIT_STORE_SIZE, DEFAULT_SPLIT_STORE_SIZE);
    }

    public static final String PROP_OAK_INDEXER_PARALLEL_INDEX = "oak.indexer.parallelIndex";
    
    public static boolean parallelIndexEnabled() {
        return Boolean.getBoolean(PROP_OAK_INDEXER_PARALLEL_INDEX);
    }
}
