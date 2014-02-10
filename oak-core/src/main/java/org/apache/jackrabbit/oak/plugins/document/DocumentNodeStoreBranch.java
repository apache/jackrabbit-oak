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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStoreBranch;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkState;

/**
 * Implementation of a DocumentMK based node store branch.
 */
public class DocumentNodeStoreBranch
        extends AbstractNodeStoreBranch<DocumentNodeStore, DocumentNodeState> {

    public DocumentNodeStoreBranch(DocumentNodeStore store,
                                   DocumentNodeState base) {
        super(store, new ChangeDispatcher(store.getRoot()), base);
    }

    @Override
    protected DocumentNodeState getRoot() {
        return store.getRoot();
    }

    @Override
    protected DocumentNodeState createBranch(DocumentNodeState state) {
        return store.getRoot(state.getRevision().asBranchRevision()).setBranch();
    }

    @Override
    protected DocumentNodeState rebase(DocumentNodeState branchHead,
                                    DocumentNodeState base) {
        return store.getRoot(store.rebase(branchHead.getRevision(), base.getRevision())).setBranch();
    }

    @Override
    protected DocumentNodeState merge(DocumentNodeState branchHead, CommitInfo info)
            throws CommitFailedException {
        return store.getRoot(store.merge(branchHead.getRevision(), info));
    }

    @Nonnull
    @Override
    protected DocumentNodeState reset(@Nonnull DocumentNodeState branchHead,
                                   @Nonnull DocumentNodeState ancestor) {
        return store.getRoot(store.reset(branchHead.getRevision(),
                ancestor.getRevision())).setBranch();
    }

    @Override
    protected DocumentNodeState persist(final NodeState toPersist,
                                     final DocumentNodeState base,
                                     final CommitInfo info) {
        DocumentNodeState state = persist(new Changes() {
            @Override
            public void with(Commit c) {
                toPersist.compareAgainstBaseState(base,
                        new CommitDiff(c, store.getBlobSerializer()));
            }
        }, base, info);
        if (base.isBranch()) {
            state.setBranch();
        }
        return state;
    }

    @Override
    protected DocumentNodeState copy(final String source,
                                  final String target,
                                  DocumentNodeState base) {
        final Node src = store.getNode(source, base.getRevision());
        checkState(src != null, "Source node %s@%s does not exist",
                source, base.getRevision());
        return persist(new Changes() {
            @Override
            public void with(Commit c) {
                store.copyNode(src, target, c);
            }
        }, base, null);
    }

    @Override
    protected DocumentNodeState move(final String source,
                                  final String target,
                                  DocumentNodeState base) {
        final Node src = store.getNode(source, base.getRevision());
        checkState(src != null, "Source node %s@%s does not exist",
                source, base.getRevision());
        return persist(new Changes() {
            @Override
            public void with(Commit c) {
                store.moveNode(src, target, c);
            }
        }, base, null);
    }

    //------------------------------< internal >--------------------------------

    /**
     * Persist some changes on top of the given base state.
     *
     * @param op the changes to persist.
     * @param base the base state.
     * @param info the commit info.
     * @return the result state.
     */
    private DocumentNodeState persist(Changes op,
                                      DocumentNodeState base,
                                      CommitInfo info) {
        boolean success = false;
        Commit c = store.newCommit(base.getRevision());
        Revision rev;
        try {
            op.with(c);
            if (c.isEmpty()) {
                // no changes to persist. return base state and let
                // finally clause cancel the commit
                return base;
            }
            rev = store.apply(c);
            success = true;
        } finally {
            if (success) {
                store.done(c, base.getRevision().isBranch(), info);
            } else {
                store.canceled(c);
            }
        }
        return store.getRoot(rev);
    }

    private interface Changes {

        void with(Commit c);
    }
}
