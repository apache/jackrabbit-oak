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

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Instance of this class provide a {@link EventFilter} for observation
 * events and a filter for commits.
 * <p>
 * In order to support OAK-4908 a FilterProvider
 * extends ChangeSetFilter
 */
public interface FilterProvider extends ChangeSetFilter {

    /**
     * Filter whole commits. Only commits for which this method returns
     * {@code true} will be further processed to create individual events.
     *
     * @param sessionId id of the filtering (this) session
     * @param info      commit info of the commit or {@code null} if not available
     * @return {@code true} if observation events should be created from this
     * commit, {@code false} otherwise.
     * @see org.apache.jackrabbit.oak.spi.commit.Observer
     */
    boolean includeCommit(@Nonnull String sessionId, @CheckForNull CommitInfo info);

    /**
     * Factory method for creating a {@code Filter} for the passed before and after
     * states.
     *
     * @param before before state
     * @param after  after state
     * @return new {@code Filter} instance
     */
    @Nonnull
    EventFilter getFilter(@Nonnull NodeState before, @Nonnull NodeState after);


    /**
     * A set of paths whose subtrees include all events of this filter.
     * @return  list of paths
     * @see org.apache.jackrabbit.oak.plugins.observation.filter.FilterBuilder#addSubTree(String)
     */
    @Nonnull
    Iterable<String> getSubTrees();

    FilterConfigMBean getConfigMBean();

    /**
     * Allows providers to supply an optional EventAggregator that
     * is used to adjust (aggregate) the event identifier before event
     * creation (ie after event filtering).
     */
    @CheckForNull
    EventAggregator getEventAggregator();
}
