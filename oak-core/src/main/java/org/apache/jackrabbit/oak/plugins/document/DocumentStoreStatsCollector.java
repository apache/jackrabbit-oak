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

package org.apache.jackrabbit.oak.plugins.document;

import java.util.List;

import javax.annotation.Nullable;

public interface DocumentStoreStatsCollector {
    /**
     * Called when a document with given key is found from the cache
     *
     * @param collection the collection
     * @param key collection key which is found
     */
    void doneFindCached(Collection collection, String key);

    /**
     * Called when a document with given key is looked from remote store
     *  @param timeTakenNanos watch for determining time taken
     * @param collection the collection
     * @param key collection key
     * @param docFound true if document is found
     * @param isSlaveOk true if find was performed against a secondary instance
     */
    void doneFindUncached(long timeTakenNanos, Collection collection, String key, boolean docFound, boolean isSlaveOk);

    /**
     * Called when query with given parameters is performed
     *  @param timeTakenNanos watch for determining time taken
     * @param collection the collection
     * @param fromKey the start value (excluding)
     * @param toKey the end value (excluding)
     * @param indexedProperty the name of the indexed property (optional)
     * @param resultSize number of documents found for given query
     * @param lockTime time in millis to acquire any lock. If no lock was required then its -1
     * @param isSlaveOk true if find was performed against a secondary instance
     */
    void doneQuery(long timeTakenNanos, Collection collection, String fromKey, String toKey,
                   @Nullable String indexedProperty, int resultSize, long lockTime, boolean isSlaveOk);

    /**
     * Called when a document is created in the given collection
     * @param timeTakenNanos watch for determining time taken
     * @param collection the collection
     * @param ids list of ids which were sent for creation
     * @param insertSuccess true if the insert was successful
     */
    void doneCreate(long timeTakenNanos, Collection collection, List<String> ids, boolean insertSuccess);

    /**
     * Called when a given updated has modified multiple documents
     *  @param timeTakenNanos watch for determining time taken
     * @param collection the collection
     * @param updateCount number of updates performed
     */
    void doneUpdate(long timeTakenNanos, Collection collection, int updateCount);

    /**
     * Called when a update operation was completed which affected single
     * document.
     *  @param timeTakenNanos watch for determining time taken
     * @param collection the collection
     * @param key collection which got updated or inserted
     * @param newEntry true if the document was newly created due to given operation
     */
    void doneFindAndModify(long timeTakenNanos, Collection collection, String key, boolean newEntry);
}
