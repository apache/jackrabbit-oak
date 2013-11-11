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
package org.apache.jackrabbit.oak.plugins.mongomk;

import java.io.IOException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.kernel.BlobSerializer;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStoreBranch;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Implementation of a MongoMK based node store branch.
 */
public class MongoNodeStoreBranch
        extends AbstractNodeStoreBranch<MongoNodeStore, MongoNodeState> {

    /**
     * TODO: what is a reasonable value?
     * TODO: fall back to pessimistic approach? how does this work in a cluster?
     */
    private static final int MERGE_RETRIES = 10;

    private final BlobSerializer blobs = new BlobSerializer() {
        @Override
        public String serialize(Blob blob) {
            if (blob instanceof MongoBlob) {
                return blob.toString();
            }
            String id;
            try {
                id = store.createBlob(blob.getNewStream()).toString();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return id;
        }
    };

    public MongoNodeStoreBranch(MongoNodeStore store,
                                MongoNodeState base) {
        super(store, new ChangeDispatcher(store.getRoot()), base);
    }

    @Override
    protected MongoNodeState getRoot() {
        return store.getRoot();
    }

    @Override
    protected MongoNodeState createBranch(MongoNodeState state) {
        return store.getRoot(state.getRevision().asBranchRevision()).setBranch();
    }

    @Override
    protected MongoNodeState rebase(MongoNodeState branchHead,
                                    MongoNodeState base) {
        return store.getRoot(store.rebase(branchHead.getRevision(), base.getRevision())).setBranch();
    }

    @Override
    protected MongoNodeState merge(MongoNodeState branchHead, CommitInfo info) {
        return store.getRoot(store.merge(branchHead.getRevision(), info));
    }

    @Override
    protected MongoNodeState persist(final NodeState toPersist,
                                     final MongoNodeState base,
                                     final CommitInfo info) {
        MongoNodeState state = persist(new Changes() {
            @Override
            public void with(Commit c) {
                toPersist.compareAgainstBaseState(base, new CommitDiff(c, blobs));
            }
        }, base, info);
        if (base.isBranch()) {
            state.setBranch();
        }
        return state;
    }

    @Override
    protected MongoNodeState copy(final String source,
                                  final String target,
                                  MongoNodeState base) {
        return persist(new Changes() {
            @Override
            public void with(Commit c) {
                store.copyNode(source, target, c);
            }
        }, base, null);
    }

    @Override
    protected MongoNodeState move(final String source,
                                  final String target,
                                  MongoNodeState base) {
        return persist(new Changes() {
            @Override
            public void with(Commit c) {
                store.moveNode(source, target, c);
            }
        }, base, null);
    }

    //--------------------< AbstractNodeStoreBranch >---------------------------

    @Nonnull
    @Override
    public NodeState merge(@Nonnull CommitHook hook, @Nullable CommitInfo info)
            throws CommitFailedException {
        MicroKernelException ex = null;
        for (int i = 0; i < MERGE_RETRIES; i++) {
            try {
                return super.merge(hook, info);
            } catch (MicroKernelException e) {
                ex = e;
            }
        }
        throw new CommitFailedException(
                "Kernel", 1,
                "Failed to merge changes to the underlying store", ex);
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
    private MongoNodeState persist(Changes op,
                                   MongoNodeState base,
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
