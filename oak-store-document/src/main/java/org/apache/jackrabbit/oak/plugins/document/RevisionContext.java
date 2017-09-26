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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.stats.Clock;

/**
 * Provides revision related context.
 */
public interface RevisionContext {

    /**
     * @return the branches of the local DocumentMK instance, which are not yet
     *         merged.
     */
    UnmergedBranches getBranches();

    /**
     * @return the pending modifications.
     */
    UnsavedModifications getPendingModifications();

    /**
     * @return the cluster id of the local DocumentMK instance.
     */
    int getClusterId();

    /**
     * @return the current head revision.
     */
    @Nonnull
    RevisionVector getHeadRevision();

    /**
     * @return a new revision for the local document node store instance.
     */
    @Nonnull
    Revision newRevision();

    /**
     * @return the clock in use when a new revision is created.
     */
    @Nonnull
    Clock getClock();

    /**
     * Retrieves the commit value for a given change. This method returns the
     * following types of commit values:
     * <ul>
     *     <li>"c" : the change revision is committed as is.</li>
     *     <li>"c-rX-Y-Z" : the change revision is a branch commit merged in
     *          revision "rX-Y-Z".</li>
     *     <li>"brX-Y-Z" : the change revision is a branch commit done at
     *          "rX-Y-Z" but not yet merged.</li>
     *     <li>{@code null} : the change revision does not have an entry on
     *          the commit root document and is not committed.</li>
     * </ul>
     *
     * @param changeRevision the revision a change was made.
     * @param doc the document where the change was made.
     * @return the commit value or {@code null} if the change does not
     *          have a commit value (yet).
     */
    @CheckForNull
    String getCommitValue(@Nonnull Revision changeRevision,
                          @Nonnull NodeDocument doc);
}
