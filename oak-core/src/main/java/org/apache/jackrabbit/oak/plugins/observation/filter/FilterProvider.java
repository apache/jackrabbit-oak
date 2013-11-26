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

package org.apache.jackrabbit.oak.plugins.observation.filter;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventGenerator.Filter;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;

/**
 * Instance of this class provide a {@link Filter} for observation
 * events and a filter for commits.
 */
public interface FilterProvider {

    /**
     * Filter whole commits. Only commits for which this method returns
     * {@code true} will be further processed to create individual events.
     *
     * @param sessionId  id of the filtering (this) session
     * @param info       commit info of the commit or {@code null} if not available
     * @return           {@code true} if observation events should be created from this
     *                   commit, {@code false} otherwise.
     *
     * @see org.apache.jackrabbit.oak.spi.commit.Observer
     */
    boolean includeCommit(@Nonnull String sessionId, @CheckForNull CommitInfo info);

    /**
     * TODO document
     * @param beforeTree
     * @param afterTree
     * @param treePermission
     * @return
     */
    @Nonnull
    Filter getFilter(@Nonnull ImmutableTree beforeTree, @Nonnull ImmutableTree afterTree,
            @Nonnull TreePermission treePermission);

    /**
     * TODO document
     * @return
     */
    @Nonnull
    String getPath();
}
