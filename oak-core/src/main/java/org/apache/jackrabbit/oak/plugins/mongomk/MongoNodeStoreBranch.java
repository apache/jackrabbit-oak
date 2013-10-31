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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.kernel.BlobSerializer;
import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStoreBranch;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Implementation of a MongoMK based node store branch.
 */
public class MongoNodeStoreBranch
        extends AbstractNodeStoreBranch<MongoNodeStore, MongoNodeState> {

    private final BlobSerializer blobs = new BlobSerializer() {
        @Override
        public String serialize(Blob blob) {
            // TODO: implement correct blob handling
            return super.serialize(blob);
        }
    };

    public MongoNodeStoreBranch(MongoNodeStore store,
                                MongoNodeState base) {
        super(store, new ChangeDispatcher(store), base);
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
    protected MongoNodeState merge(MongoNodeState branchHead) {
        return store.getRoot(store.merge(branchHead.getRevision()));
    }

    @Override
    protected MongoNodeState persist(final NodeState toPersist,
                                     final MongoNodeState base) {
        MongoNodeState state = persist(new Changes() {
            @Override
            public void with(Commit c) {
                toPersist.compareAgainstBaseState(base, new CommitDiff(c, blobs));
            }
        }, base);
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
        }, base);
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
        }, base);
    }

    //------------------------------< internal >--------------------------------

    /**
     * Persist some changes on top of the given base state.
     *
     * @param op the changes to persist.
     * @param base the base state.
     * @return the result state.
     */
    private MongoNodeState persist(Changes op, MongoNodeState base) {
        boolean success = false;
        Commit c = store.newCommit(base.getRevision());
        Revision rev;
        try {
            op.with(c);
            rev = store.apply(c);
            success = true;
        } finally {
            if (success) {
                store.done(c, base.getRevision().isBranch());
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
