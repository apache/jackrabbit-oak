/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Map;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Queues updates on a commit root document and batches them into a single
 * call to the {@link DocumentStore}.
 */
final class BatchCommitQueue {

    private static final Logger LOG = LoggerFactory.getLogger(BatchCommitQueue.class);

    /**
     * The pending batch commits.
     */
    private final Map<String, BatchCommit> pending = Maps.newHashMap();

    /**
     * The batch commits in progress.
     */
    private final Map<String, BatchCommit> inProgress = Maps.newHashMap();

    private final DocumentStore store;

    BatchCommitQueue(@Nonnull DocumentStore store) {
        this.store = checkNotNull(store);
    }

    Callable<NodeDocument> updateDocument(UpdateOp op) {
        String id = op.getId();
        // check if there is already a batch commit in progress for
        // the document
        synchronized (this) {
            BatchCommit commit = inProgress.get(id);
            if (commit != null) {
                LOG.debug("Commit with id {} in progress", id);
                // get or create a pending batch commit
                commit = pending.get(id);
                if (commit == null) {
                    LOG.debug("Creating pending BatchCommit for id {}", id);
                    commit = new BatchCommit(id, this, true);
                    pending.put(id, commit);
                }
            } else {
                commit = new BatchCommit(id, this, false);
                LOG.debug("Adding inProgress BatchCommit for id {}", id);
                inProgress.put(id, commit);
            }
            LOG.debug("Enqueueing operation with id {}", id);
            return commit.enqueue(op);
        }
    }

    void finished(BatchCommit commit) {
        String id = commit.getId();
        synchronized (this) {
            LOG.debug("BatchCommit finished with id {}", id);
            if (inProgress.remove(id) == null) {
                throw new IllegalStateException("BatchCommit for " +
                        id + " is not in progress");
            }
            commit = pending.remove(id);
            if (commit != null) {
                LOG.debug("Moving pending BatchCommit to inProgress with id {}", id);
                inProgress.put(id, commit);
                commit.release();
                LOG.debug("BatchCommit released with id {}", id);
            }
        }
    }

    DocumentStore getStore() {
        return store;
    }
}
