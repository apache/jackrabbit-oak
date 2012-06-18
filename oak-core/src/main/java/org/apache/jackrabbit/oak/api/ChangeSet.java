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
package org.apache.jackrabbit.oak.api;

import javax.annotation.Nonnull;


/**
 * A {@code ChangeSet} instance describes a set of changes which was applied atomically
 * to a (sub-)tree.
 */
public interface ChangeSet {

    /**
     * Timestamp for when the changes occurred.
     * @return  time stamp in milliseconds
     */
    long getTimeStamp();

    /**
     * Commit message from the underlying Microkernel.
     *
     * TODO: implementation detail: we might need to encode JCR user data and session
     * IDs (for no-local support) here but we should expose them separately.
     * @return
     */
    @Nonnull
    String getCommitMessage();

    /**
     * The {@link Tree} how it was before the changes.
     * @return a read only tree.
     */
    @Nonnull
    Tree getTreeBeforeChange();

    /**
     * The {@link Tree} how it was after the changes.
     * @return a read only tree.
     */
    @Nonnull
    Tree getTreeAfterChange();
}
