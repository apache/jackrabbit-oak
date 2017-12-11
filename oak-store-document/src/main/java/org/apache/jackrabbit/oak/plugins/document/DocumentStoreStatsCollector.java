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

public interface DocumentStoreStatsCollector {
    /**
     * Called when a document with given key is found from the cache
     *
     * @param collection the collection
     * @param key collection key which is found
     */
    void doneFindCached(Collection<? extends Document> collection, String key);

    /**
     * Called when a document with given key is read from remote store
     * @param timeTakenNanos time taken
     * @param collection the collection
     * @param key collection key
     * @param docFound true if document is found
     * @param isSlaveOk true if find was performed against a secondary instance
     */
    void doneFindUncached(long timeTakenNanos, Collection<? extends Document> collection, String key, boolean docFound, boolean isSlaveOk);

    /**
     * Called when query with given parameters is performed
     * @param timeTakenNanos time taken
     * @param collection the collection
     * @param fromKey the start value (excluding)
     * @param toKey the end value (excluding)
     * @param indexedProperty true if indexedProperty was specified
     * @param resultSize number of documents found for given query
     * @param lockTime time in millis to acquire any lock ({@code -1} if no lock was required)
     * @param isSlaveOk true if find was performed against a secondary instance
     */
    void doneQuery(long timeTakenNanos, Collection<? extends Document> collection, String fromKey, String toKey,
                   boolean indexedProperty, int resultSize, long lockTime, boolean isSlaveOk);

    /**
     * Called when a document is created in the given collection
     * @param timeTakenNanos time taken
     * @param collection the collection
     * @param ids list of ids request to be created
     * @param insertSuccess true if the insert was successful
     */
    void doneCreate(long timeTakenNanos, Collection<? extends Document> collection, List<String> ids, boolean insertSuccess);

    /**
     * Called when multiple document are either created or updated.
     *
     * @param timeTakenNanos time taken
     * @param collection the collection
     * @param ids list of ids request to be created or updated
     */
    void doneCreateOrUpdate(long timeTakenNanos, Collection<? extends Document> collection, List<String> ids);

    /**
     * Called when a update operation was completed which affected single
     * document.
     * @param timeTakenNanos time taken
     * @param collection the collection
     * @param key collection which got updated or inserted
     * @param newEntry true if the document was newly created due to given operation
     * @param success true if the update was success
     * @param retryCount number of retries done to get the update
     */
    void doneFindAndModify(long timeTakenNanos, Collection<? extends Document> collection, String key,
                           boolean newEntry, boolean success, int retryCount);

    /**
     * Called when a remove operation for documents was completed.
     * @param timeTakenNanos time taken
     * @param collection the collection
     * @param removeCount the number of removed documents
     */
    void doneRemove(long timeTakenNanos,
                    Collection<? extends Document> collection,
                    int removeCount);
}
