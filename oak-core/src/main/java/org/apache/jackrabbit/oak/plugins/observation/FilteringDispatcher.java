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

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Part of the FilteringObserver: the FilteringDispatcher is used
 * to implement the skipping (filtering) of content changes
 * which the FilteringDispatcher flags as NOOP_CHANGE.
 * When the FilteringDispatcher notices a NOOP_CHANGE it does
 * not forward the change but only updates the before NodeState.
 */
public class FilteringDispatcher implements Observer {
    
    private final FilteringAwareObserver observer;

    private NodeState before;

    public FilteringDispatcher(FilteringAwareObserver observer) {
        this.observer = checkNotNull(observer);
    }

    @Override
    public void contentChanged(@Nonnull NodeState root,
                               @Nonnull CommitInfo info) {
        if (before != null) { 
            // avoid null being passed as before to observer
            // before == null happens only at startup
            if (info != FilteringObserver.NOOP_CHANGE) {
                observer.contentChanged(before, root, info);
            }
        }
        before = root;
    }
}