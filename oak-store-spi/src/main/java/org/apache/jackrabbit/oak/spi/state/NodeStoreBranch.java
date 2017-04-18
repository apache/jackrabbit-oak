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
package org.apache.jackrabbit.oak.spi.state;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;

/**
 * An instance of this class represents a private branch of the tree in a
 * {@link NodeStore} to which transient changes can be applied and later merged
 * back or discarded.
 */
public interface NodeStoreBranch {

    /**
     * Returns the base state of this branch.
     * The base state is the state of the tree as it was at the time
     * this branch was created.
     *
     * @return root node state
     */
    @Nonnull
    NodeState getBase();

    /**
     * Returns the head state of this branch.
     * The head state is the state resulting from the
     * base state by applying all subsequent modifications to this branch
     * by {@link #setRoot(NodeState)}.
     *
     * @return root node state
     * @throws IllegalStateException if the branch is already merged
     */
    @Nonnull
    NodeState getHead();

    /**
     * Updates the state of the content tree of this private branch.
     *
     * @param newRoot new root node state
     * @throws IllegalStateException if the branch is already merged
     */
    void setRoot(NodeState newRoot);

    /**
     * Merges the changes in this branch to the main content tree.
     * Merging is done by rebasing the changes in this branch on top of
     * the current head revision followed by a fast forward merge.
     *
     * @param hook the commit hook to apply while merging changes
     * @param info commit info associated with this merge operation
     * @return the node state resulting from the merge.
     * @throws CommitFailedException if the merge failed
     * @throws IllegalStateException if the branch is already merged
     */
    @Nonnull
    NodeState merge(@Nonnull CommitHook hook, @Nonnull CommitInfo info)
            throws CommitFailedException;

    /**
     * Rebase the changes from this branch on top of the current
     * root.
     */
    void rebase();

}

