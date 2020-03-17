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

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.util.Collection;
import java.util.Map;

public interface IndexingQueue {

    /**
     * Adds the given doc to a queue without any wait
     *
     * @param doc to be added
     * @return true if the doc was added to the queue
     */
    boolean addIfNotFullWithoutWait(LuceneDoc doc);

    /**
     * Adds the given doc to a queue with possible wait if queue is full.
     * The wait would be having an upper limit
     *
     * @param doc LuceneDoc to be added
     * @return true if the doc was added to the queue
     */
    boolean add(LuceneDoc doc);

    /**
     * The docs are added directly to the index without any queuing
     *
     * @param docsPerIndex map of LuceneDoc per index path
     */
    void addAllSynchronously(Map<String, Collection<LuceneDoc>> docsPerIndex);

    /**
     * Schedules the async processing of queued entries
     */
    void scheduleQueuedDocsProcessing();

}
