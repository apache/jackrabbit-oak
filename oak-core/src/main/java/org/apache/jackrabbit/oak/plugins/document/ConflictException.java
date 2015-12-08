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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.CommitFailedException.MERGE;

import java.util.Collections;
import java.util.Set;

/**
 * A document store exception with an optional conflict revision. The
 * DocumentNodeStore implementation will throw this exception when a commit
 * or merge fails with a conflict.
 */
class ConflictException extends DocumentStoreException {

    private static final long serialVersionUID = 1418838561903727231L;

    /**
     * Optional conflict revisions list.
     */
    private final Set<Revision> conflictRevisions;

    /**
     * @param message the exception / conflict message.
     * @param conflictRevision the conflict revision
     */
    ConflictException(@Nonnull String message,
                      @Nonnull Revision conflictRevision) {
        super(checkNotNull(message));
        this.conflictRevisions = Collections.singleton(checkNotNull(conflictRevision));
    }

    /**
     * @param message the exception / conflict message.
     * @param conflictRevisions the conflict revision list
     */
    ConflictException(@Nonnull String message,
                      @Nonnull Set<Revision> conflictRevisions) {
        super(checkNotNull(message));
        this.conflictRevisions = checkNotNull(conflictRevisions);
    }

    /**
     * @param message the exception / conflict message.
     */
    ConflictException(@Nonnull String message) {
        super(checkNotNull(message));
        this.conflictRevisions = Collections.emptySet();
    }

    /**
     * Convert this exception into a {@link CommitFailedException}. This
     * exception will be set as the cause of the returned exception.
     *
     * @return a {@link CommitFailedException}.
     */
    CommitFailedException asCommitFailedException() {
        if (!conflictRevisions.isEmpty()) {
            return new FailedWithConflictException(conflictRevisions, getMessage(), this);
        } else {
            return new CommitFailedException(MERGE, 1,
                    "Failed to merge changes to the underlying store", this);
        }
    }

    /**
     * List of conflict revisions.
     * 
     * @return a list of conflict revisions (may be empty)
     */
    @Nonnull
    Iterable<Revision> getConflictRevisions() {
        return conflictRevisions;
    }
}