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

/**
 * Stats Collector for throttling operation.
 * <p/>It's implementation are required to provide the time taken for implementing each operation while throttling is ON.
 */
public interface ThrottlingStatsCollector {

    /**
     * Called when a document is created in the given collection
     * @param throttlingTimeNanos time taken
     * @param collection the collection
     * @param ids list of ids request to be created
     * @param insertSuccess true if the insert was successful
     */
    void doneCreate(long throttlingTimeNanos, Collection<? extends Document> collection, List<String> ids, boolean insertSuccess);

    /**
     * Called when multiple document are either created or updated.
     *
     * @param throttlingTimeNanos time taken
     * @param collection the collection
     * @param ids list of ids request to be created or updated
     */
    void doneCreateOrUpdate(long throttlingTimeNanos, Collection<? extends Document> collection, List<String> ids);

    /**
     * Called when a update operation was completed which affected single
     * document.
     * @param throttlingTimeNanos time taken
     * @param collection the collection
     * @param key collection which got updated or inserted
     * @param newEntry true if the document was newly created due to given operation
     * @param success true if the update was success
     * @param retryCount number of retries done to get the update
     */
    void doneFindAndModify(long throttlingTimeNanos, Collection<? extends Document> collection, String key, boolean newEntry,
                           boolean success, int retryCount);

    /**
     * Called when a remove operation for documents was completed.
     *
     * @param throttlingTimeNanos time taken
     * @param collection          the collection
     */
    void doneRemove(long throttlingTimeNanos, Collection<? extends Document> collection);
}
