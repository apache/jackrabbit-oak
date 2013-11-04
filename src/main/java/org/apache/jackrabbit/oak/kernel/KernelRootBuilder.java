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
package org.apache.jackrabbit.oak.kernel;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

/**
 * This implementation tracks the number of pending changes and purges them to
 * a private branch of the underlying store if a certain threshold is met.
 */
class KernelRootBuilder extends MemoryNodeBuilder implements FastCopyMove {

    /**
     * Number of content updates that need to happen before the updates
     * are automatically purged to the private branch.
     */
    private static final int UPDATE_LIMIT = Integer.getInteger("update.limit", 1000);

    /**
     * The underlying store
     */
    private final KernelNodeStore store;

    /**
     * The base state of this builder, possibly non-existent if this builder
     * represents a new node that didn't yet exist in the base content tree.
     * This differs from the base state of super since the latter one reflects
     * the base created by the last purge.
     */
    @Nonnull
    private NodeState base;

    /**
     * Private branch used to hold pending changes exceeding {@link #UPDATE_LIMIT}
     */
    private NodeStoreBranch branch;

    /**
     * Number of updated not yet persisted to the private {@link #branch}
     */
    private int updates = 0;

    KernelRootBuilder(KernelNodeState base, KernelNodeStore store) {
        super(checkNotNull(base));
        this.base = base;
        this.store = store;
        this.branch = store.createBranch(base);
    }

    //--------------------------------------------------< MemoryNodeBuilder >---


    @Override @Nonnull
    public NodeState getBaseState() {
        return base;
    }

    @Override
    public void reset(@Nonnull NodeState newBase) {
        base = checkNotNull(newBase);
        super.reset(newBase);
    }

    @Override
    protected MemoryNodeBuilder createChildBuilder(String name) {
        return new KernelNodeBuilder(this, name, this);
    }

    @Override
    protected void updated() {
        if (updates++ > UPDATE_LIMIT) {
            purge();
        }
    }

    @Override
    public boolean moveFrom(KernelNodeBuilder source, String newName) {
        String sourcePath = source.getPath();
        return move(sourcePath, '/' + newName);
    }

    @Override
    public boolean copyFrom(KernelNodeBuilder source, String newName) {
        String sourcePath = source.getPath();
        return copy(sourcePath, '/' + newName);
    }

    //------------------------------------------------------------< internal >---

    /**
     * Rebase this builder on top of the head of the underlying store
     */
    NodeState rebase() {
        purge();
        branch.rebase();
        NodeState head = branch.getHead();
        reset(head);
        return head;
    }

    /**
     * Reset this builder by creating a new branch and setting the head
     * state of that branch as the new base state of this builder.
     */
    NodeState reset() {
        branch = store.createBranch(store.getRoot());
        NodeState head = branch.getHead();
        reset(head);
        return head;
    }

    /**
     * Merge all changes tracked in this builder into the underlying store.
     */
    NodeState merge(CommitHook hook, CommitInfo info) throws CommitFailedException {
        purge();
        branch.merge(hook, info);
        return reset();
    }

    /**
     * Applied all pending changes to the underlying branch and then
     * move the node as a separate operation on the underlying store.
     * This allows stores to optimise move operations instead of
     * seeing them as an added node followed by a deleted node.
     */
    boolean move(String source, String target) {
        purge();
        boolean success = branch.move(source, target);
        super.reset(branch.getHead());
        return success;
    }

    /**
     * Applied all pending changes to the underlying branch and then
     * copy the node as a separate operation on the underlying store.
     * This allows stores to optimise copy operations instead of
     * seeing them as an added node.
     */
    boolean copy(String source, String target) {
        purge();
        boolean success = branch.copy(source, target);
        super.reset(branch.getHead());
        return success;
    }

    private void purge() {
        branch.setRoot(getNodeState());
        super.reset(branch.getHead());
        updates = 0;
    }
}
