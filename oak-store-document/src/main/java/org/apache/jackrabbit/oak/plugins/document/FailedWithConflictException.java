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

import java.util.Set;

/**
 * A {@link CommitFailedException} with a conflict revision.
 */
class FailedWithConflictException extends CommitFailedException {

    private static final long serialVersionUID = 2716279884065949789L;

    private final Set<Revision> conflictRevisions;

    FailedWithConflictException(@Nonnull Set<Revision> conflictRevisions,
                                @Nonnull String message,
                                @Nonnull Throwable cause) {
        super(OAK, MERGE, 4, checkNotNull(message), checkNotNull(cause));
        this.conflictRevisions = checkNotNull(conflictRevisions);
    }

    /**
     * @return the revision of another commit which caused a conflict.
     */
    @Nonnull
    Set<Revision> getConflictRevisions() {
        return conflictRevisions;
    }
}