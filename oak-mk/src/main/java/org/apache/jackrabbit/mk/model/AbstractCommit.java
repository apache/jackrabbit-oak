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
package org.apache.jackrabbit.mk.model;

import org.apache.jackrabbit.mk.store.Binding;
import org.apache.jackrabbit.mk.store.CacheObject;

/**
 *
 */
public abstract class AbstractCommit implements Commit, CacheObject {

    // id of root node associated with this commit
    protected Id rootNodeId;

    // commit timestamp
    protected long commitTS;

    // commit message
    protected String msg;

    // changes
    protected String changes;

    // id of parent commit
    protected Id parentId;

    // id of branch root commit
    protected Id branchRootId;

    protected AbstractCommit() {
    }

    protected AbstractCommit(Commit other) {
        this.parentId = other.getParentId();
        this.rootNodeId = other.getRootNodeId();
        this.msg = other.getMsg();
        this.changes = other.getChanges();
        this.commitTS = other.getCommitTS();
        this.branchRootId = other.getBranchRootId();
    }

    public Id getParentId() {
        return parentId;
    }

    public Id getRootNodeId() {
        return rootNodeId;
    }

    public long getCommitTS() {
        return commitTS;
    }

    public String getMsg() {
        return msg;
    }

    public String getChanges() {
        return changes;
    }

    public Id getBranchRootId() {
        return branchRootId;
    }

    public void serialize(Binding binding) throws Exception {
        binding.write("rootNodeId", rootNodeId.getBytes());
        binding.write("commitTS", commitTS);
        binding.write("msg", msg == null ? "" : msg);
        binding.write("changes", changes == null ? "" : changes);
        binding.write("parentId", parentId == null ? "" : parentId.toString());
        binding.write("branchRootId", branchRootId == null ? "" : branchRootId.toString());
    }

    //-----------------------------------------------------< Object overrides >

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("rootNodeId: '").append(rootNodeId.toString()).append("', ");
        sb.append("commitTS: ").append(commitTS).append(", ");
        sb.append("msg: '").append(msg == null ? "" : msg).append("', ");
        sb.append("changes: '").append(changes == null ? "" : changes).append("', ");
        sb.append("parentId: '").append(parentId == null ? "" : parentId.toString()).append("', ");
        sb.append("branchRootId: '").append(branchRootId == null ? "" : branchRootId.toString()).append("'");
        return sb.toString();
    }
    
    @Override
    public int getMemory() {
        int memory = 100;
        if (msg != null) {
            memory += 2 * msg.length();
        }
        if (changes != null) {
            memory += 2 * changes.length();
        }
        return memory;
    }
    
}
