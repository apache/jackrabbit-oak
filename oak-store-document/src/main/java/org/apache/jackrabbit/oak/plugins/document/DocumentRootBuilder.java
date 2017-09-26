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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation tracks the number of pending changes and purges them to
 * a private branch of the underlying store if a certain threshold is met.
 */
class DocumentRootBuilder extends AbstractDocumentNodeBuilder {

    private static final Logger log = LoggerFactory.getLogger(DocumentRootBuilder.class);

    /**
     * The underlying store
     */
    protected final DocumentNodeStore store;

    /**
     * The base state of this builder, possibly non-existent if this builder
     * represents a new node that didn't yet exist in the base content tree.
     * This differs from the base state of super since the latter one reflects
     * the base created by the last purge.
     */
    @Nonnull
    private NodeState base;

    /**
     * Private branch used to hold pending changes exceeding {@link #updateLimit}
     */
    private DocumentNodeStoreBranch branch;

    /**
     * Number of content updates that need to happen before the updates
     * are automatically purged to the private branch.
     */
    private final int updateLimit;

    /**
     * Number of updated not yet persisted to the private {@link #branch}
     */
    private int updates;

    DocumentRootBuilder(@Nonnull DocumentNodeState base,
                        @Nonnull DocumentNodeStore store) {
        super(checkNotNull(base));
        this.store = checkNotNull(store);
        this.base = base;
        this.branch = store.createBranch(base);
        this.updateLimit = store.getUpdateLimit();
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
    protected DocumentNodeBuilder createChildBuilder(String name) {
        return new DocumentNodeBuilder(this, name, this);
    }

    @Override
    protected void updated() {
        if (++updates > updateLimit) {
            purge();
        }
    }

    @Nonnull
    @Override
    public NodeState getNodeState() {
        if (DocumentNodeStoreBranch.getCurrentBranch() != null) {
            purge();
            return branch.getHead();
        } else {
            return super.getNodeState();
        }
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return store.createBlob(stream);
    }

    //------------------------------------------------------------< internal >---

    /**
     * Rebase this builder on top of the head of the underlying store
     */
    NodeState rebase() {
        NodeState head = super.getNodeState();
        NodeState inMemBase = super.getBaseState();

        // Rebase branch
        branch.rebase();

        // Rebase in memory changes on top of the head of the rebased branch
        super.reset(branch.getHead());
        updates = 0;
        head.compareAgainstBaseState(inMemBase, new ConflictAnnotatingRebaseDiff(this));

        // Set new base and return rebased head
        base = branch.getBase();
        return super.getNodeState();
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
        boolean success = false;
        try {
            branch.merge(hook, info);
            success = true;
        } finally {
            if (!success) {
                // need to adjust base and head of this builder
                // in case branch.merge() did a rebase and then
                // a commit hook failed the merge
                super.reset(branch.getHead());
                this.base = branch.getBase();
            }
        }
        return reset();
    }

    void purge() {
        branch.setRoot(super.getNodeState());
        super.reset(branch.getHead());
        updates = 0;
    }

}
