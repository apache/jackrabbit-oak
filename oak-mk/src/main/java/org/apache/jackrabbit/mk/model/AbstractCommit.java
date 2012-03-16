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

/**
 *
 */
public abstract class AbstractCommit implements Commit {

    // id of root node associated with this commit
    protected Id rootNodeId;

    // commit timestamp
    protected long commitTS;

    // commit message
    protected String msg;

    // id of parent commit
    protected Id parentId;

    protected AbstractCommit() {
    }

    protected AbstractCommit(Commit other) {
        this.parentId = other.getParentId();
        this.rootNodeId = other.getRootNodeId();
        this.msg = other.getMsg();
        this.commitTS = other.getCommitTS();
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

    public void serialize(Binding binding) throws Exception {
        binding.write("rootNodeId", rootNodeId.getBytes());
        binding.write("commitTS", commitTS);
        binding.write("msg", msg == null ? "" : msg);
        binding.write("parentId", parentId == null ? "" : parentId.toString());
    }
}
