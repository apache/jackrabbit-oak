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

import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStoreBranch;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Implementation of a MongoMK based node store branch.
 */
public class MongoNodeStoreBranch
        extends AbstractNodeStoreBranch<MongoNodeStore, MongoNodeState> {

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
        return store.getRoot(state.getRevision().asBranchRevision());
    }

    @Override
    protected MongoNodeState rebase(MongoNodeState branchHead,
                                    MongoNodeState base) {
        return store.getRoot(store.rebase(branchHead.getRevision(), base.getRevision()));
    }

    @Override
    protected MongoNodeState merge(MongoNodeState branchHead) {
        return store.getRoot(store.merge(branchHead.getRevision()));
    }

    @Override
    protected MongoNodeState persist(NodeState toPersist, MongoNodeState base) {
        // TODO
        return null;
    }

    @Override
    protected MongoNodeState copy(String source,
                                  String target,
                                  MongoNodeState base) {
        boolean success = false;
        Commit c = store.newCommit(base.getRevision());
        Revision rev;
        try {
            store.copyNode(source, target, c);
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

    @Override
    protected MongoNodeState move(String source,
                                  String target,
                                  MongoNodeState base) {
        boolean success = false;
        Commit c = store.newCommit(base.getRevision());
        Revision rev;
        try {
            store.moveNode(source, target, c);
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
}
