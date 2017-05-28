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

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;

public interface IndexingContext {

    /**
     * Path of the index definition in the repository
     * @return index path in the repository
     */
    String getIndexPath();

    /**
     * Commit info associated with commit as part of which
     * IndexEditor is being invoked
     */
    CommitInfo getCommitInfo();

    /**
     * Flag indicating that index is being reindex
     */
    boolean isReindexing();

    /**
     * Flag indicating that indexed is being done
     * asynchronously
     */
    boolean isAsync();

    /**
     * Invoked by IndexEditor to indicate that update of index has failed
     * @param e exception stack for failed updated
     */
    void indexUpdateFailed(Exception e);

    /**
     * registers {@code IndexCommitCallback} instance which can then be
     * notified of how indexing commit progresses.
     * @param callback
     */
    void registerIndexCommitCallback(IndexCommitCallback callback);
}
