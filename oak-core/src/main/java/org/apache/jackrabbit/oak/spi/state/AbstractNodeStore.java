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


import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.PostCommitHook;

/**
 * Abstract base class for {@link NodeStore} implementations.
 */
public abstract class AbstractNodeStore implements NodeStore {

    /**
     * Replaces the base state of the given builder and throws away all
     * changes in it. The effect of this method is equivalent to replacing
     * the builder (and the connected subtree) with a new builder returned
     * by {@code state.builder()}.
     *
     * @param state new base state
     * @throws IllegalArgumentException if the builder is not acquired
     *                                  from a root state of this store
     */
    protected abstract void reset(NodeBuilder builder, NodeState state);

    /**
     * This default implementation is equal to first rebasing the builder
     * and then applying it to a new branch and immediately merging it back.
     * <p>
     * <em>Note:</em> it is the caller's responsibility to ensure atomicity.
     *
     * @param builder  the builder whose changes to apply
     * @param commitHook the commit hook to apply while merging changes
     * @param committed  the pos commit hook
     * @return the node state resulting from the merge.
     * @throws CommitFailedException
     * @throws IllegalArgumentException if the builder is not acquired
     *                                  from a root state of this store
     */
    @Override
    public NodeState merge(@Nonnull NodeBuilder builder, @Nonnull CommitHook commitHook,
            PostCommitHook committed) throws CommitFailedException {
        checkNotNull(commitHook);
        rebase(checkNotNull(builder));
        NodeStoreBranch branch = branch();
        branch.setRoot(builder.getNodeState());
        NodeState merged = branch.merge(commitHook, committed);
        reset(builder, merged);
        return merged;
    }

    /**
     * This default implementation is equal to applying the differences between
     * the builders base state and its head state to a fresh builder on the
     * stores root state using {@link ConflictAnnotatingRebaseDiff} for resolving
     * conflicts.
     * @param builder  the builder to rebase
     * @return the node state resulting from the rebase.
     * @throws IllegalArgumentException if the builder is not acquired
     *                                  from a root state of this store
     */
    @Override
    public NodeState rebase(@Nonnull NodeBuilder builder) {
        NodeState head = checkNotNull(builder).getNodeState();
        NodeState base = builder.getBaseState();
        NodeState newBase = getRoot();
        if (base != newBase) {
            reset(builder, newBase);
            head.compareAgainstBaseState(
                    base, new ConflictAnnotatingRebaseDiff(builder));
            head = builder.getNodeState();
        }
        return head;
    }

    /**
     * This default implementation is equal resetting the builder to the root of
     * the store and returning the resulting node state from the builder.
     * @param builder the builder to reset
     * @return the node state resulting from the reset.
     * @throws IllegalArgumentException if the builder is not acquired
     *                                  from a root state of this store
     */
    @Override
    public NodeState reset(@Nonnull NodeBuilder builder) {
        NodeState head = getRoot();
        reset(builder, head);
        return head;
    }

//------------------------------------------------------------< Object >--

    /**
     * Returns a string representation the head state of this node store.
     */
    public String toString() {
        return getRoot().toString();
    }

}
