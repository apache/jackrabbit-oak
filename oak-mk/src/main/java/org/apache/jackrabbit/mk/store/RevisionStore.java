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
package org.apache.jackrabbit.mk.store;

import org.apache.jackrabbit.mk.model.ChildNodeEntries;
import org.apache.jackrabbit.mk.model.ChildNodeEntriesMap;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.MutableCommit;
import org.apache.jackrabbit.mk.model.MutableNode;

/**
 * Write operations.
 */
public interface RevisionStore extends RevisionProvider {

    /**
     * Token that must be created first before invoking any put operation.
     */
    public abstract class PutToken {
        
        /* Prevent other implementations. */
        PutToken() {}
    }
    
    /**
     * Create a put token.
     * 
     * @return put token
     */
    PutToken createPutToken();
    
    Id /*id*/ putNode(PutToken token, MutableNode node) throws Exception;
    Id /*id*/ putCNEMap(PutToken token, ChildNodeEntries map) throws Exception;
    
    /**
     * Lock the head. Must be called prior to putting a new head commit.
     * 
     * @see #putHeadCommit(PutToken, MutableCommit, Id)
     * @see #unlockHead()
     */
    void lockHead();
    
    /**
     * Put a new head commit. Must be called while holding a lock on the head.
     * 
     * @param token
     *            put token
     * @param commit
     *            commit
     * @param branchRootId
     *            former branch root id, if this is a merge; otherwise
     *            {@code null}
     * @return branchRevId
     *            current branch head, i.e. last commit on this branch, 
     *            if this is a merge; otherwise {@code null}
     * @return head commit id
     * @throws Exception
     *             if an error occurs
     * @see #lockHead()
     */
    Id /*id*/ putHeadCommit(PutToken token, MutableCommit commit, Id branchRootId, Id branchRevId) throws Exception;
    
    /**
     * Unlock the head.
     *
     * @see #lockHead()
     */
    void unlockHead();

    /**
     * Store a new commit.
     * <p/>
     * Unlike {@code putHeadCommit(MutableCommit)}, this method
     * does not affect the current head commit and therefore doesn't
     * require a lock on the head.
     *
     * @param token put token
     * @param commit commit
     * @return new commit id
     * @throws Exception if an error occurs
     */
    Id /*id*/ putCommit(PutToken token, MutableCommit commit) throws Exception;
}
