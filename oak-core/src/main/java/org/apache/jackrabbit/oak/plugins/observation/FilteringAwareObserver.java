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
package org.apache.jackrabbit.oak.plugins.observation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A FilteringAwareObserver is the stateless-variant of
 * an Observer which gets an explicit before as well as the
 * after NodeState.
 * <p>
 * It is used by the FilteringObserver (or more precisely
 * by the FilteringDispatcher) to support skipping (ie filtering)
 * of content changes.
 */
public interface FilteringAwareObserver {

    /**
     * Equivalent to the state-full contentChanged() method of the Observer
     * with one important difference being that this variation explicitly
     * passes the before NodeState (thus the observer must in this case
     * not remember the previous state)
     * @param before the before NodeState
     * @param after the after NodeState
     * @param info the associated CommitInfo
     */
    void contentChanged(@Nonnull NodeState before, @Nonnull NodeState after, @Nonnull CommitInfo info);
    
}
