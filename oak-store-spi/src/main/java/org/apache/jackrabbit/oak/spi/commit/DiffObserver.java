/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.commit;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * Abstract base class for observers that use a content diff to determine
 * what changed between two consecutive observed states of the repository.
 * Subclasses just need to provide the diff handler by implementing the
 * {@link #getRootDiff(NodeState, NodeState, CommitInfo)} method.
 */
public abstract class DiffObserver implements Observer {

    private NodeState before = null;

    /**
     * Returns the diff handler to be used for the given content change.
     *
     * @param before state of the repository before this changes
     * @param after state of the repository after this changes
     * @param info local commit information, or {@code null} if not available
     * @return diff handler for this change
     */
    protected abstract NodeStateDiff getRootDiff(
            @Nonnull NodeState before, @Nonnull NodeState after,
            @Nonnull CommitInfo info);

    //----------------------------------------------------------< Observer >--

    @Override
    public final synchronized void contentChanged(
            @Nonnull NodeState root, @Nonnull CommitInfo info) {
        checkNotNull(root);
        if (before != null) {
            NodeStateDiff diff = getRootDiff(before, root, info);
            root.compareAgainstBaseState(before, diff);
        }
        before = root;
    }

}
