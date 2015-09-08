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
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.CommitFailedException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.CommitFailedException.MERGE;

/**
 * A document store exception with an optional conflict revision. The
 * DocumentNodeStore implementation will throw this exception when a commit
 * or merge fails with a conflict.
 */
class ConflictException extends DocumentStoreException {

    private static final long serialVersionUID = 1418838561903727231L;

    /**
     * Optional conflict revision.
     */
    private final Revision conflictRevision;

    /**
     * @param message the exception / conflict message.
     * @param conflictRevision the conflict revision or {@code null} if unknown.
     */
    ConflictException(@Nonnull String message,
                      @Nullable Revision conflictRevision) {
        super(checkNotNull(message));
        this.conflictRevision = conflictRevision;
    }

    /**
     * Convert this exception into a {@link CommitFailedException}. This
     * exception will be set as the cause of the returned exception.
     *
     * @return a {@link CommitFailedException}.
     */
    CommitFailedException asCommitFailedException() {
        if (conflictRevision != null) {
            return new FailedWithConflictException(conflictRevision, getMessage(), this);
        } else {
            return new CommitFailedException(MERGE, 1,
                    "Failed to merge changes to the underlying store", this);
        }
    }
}
