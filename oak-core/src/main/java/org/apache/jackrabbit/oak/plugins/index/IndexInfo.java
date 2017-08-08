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

package org.apache.jackrabbit.oak.plugins.index;

import javax.annotation.CheckForNull;

import org.osgi.annotation.versioning.ProviderType;

/**
 * Captures information related to index
 */
@ProviderType
public interface IndexInfo {
    /**
     * Returns paths of index definition in the repository
     */
    String getIndexPath();

    /**
     * Returns type of index definition like 'property' or 'lucene'
     */
    String getType();

    /**
     * Returns name of the async index lane to which this index is bound to
     * or null if its not an async index
     */
    @CheckForNull
    String getAsyncLaneName();

    /**
     * Time in millis at which index was last updated
     *
     * @return time in millis or -1 if unknown, -2 if synchronous
     */
    long getLastUpdatedTime();

    /**
     * Returns time in millis of the repository state up to which index is up-to-date.
     * This may or may not be same as {@code #getLastUpdatedTime}. For e.g.
     * consider an index at /oak:index/fooIndex bound to async lane "async".
     * The index might have got updated 2 cycle ago when async indexer traversed content
     * node which were indexed by this index and it was not updated in last index cycle.
     * Then {@code indexedUptoTime} is the time of last complete cycle while
     * {@code lastUpdatedTime} is the time of 2nd last cycle
     *
     * @return time in millis or -1 if unknown
     */
    long getIndexedUpToTime();

    /**
     * An estimate of entry count in the index
     */
    long getEstimatedEntryCount();

    /**
     * Index data storage size

     * @return storage size or -1 if unknown
     */
    long getSizeInBytes();

    /**
     * Determines if index definition has changed but no reindexing
     * was done for that change.
     */
    boolean hasIndexDefinitionChangedWithoutReindexing();

    /**
     * If the index definition has changed without doing any reindexing
     * then this method can be used to determine the diff in the index
     * definition
     * @return diff if the definition change otherwise null
     */
    @CheckForNull
    String getIndexDefinitionDiff();
}
