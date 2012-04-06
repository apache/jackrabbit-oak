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

import org.apache.jackrabbit.mk.model.ChildNodeEntriesMap;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.NodeState;
import org.apache.jackrabbit.mk.model.NodeStore;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.model.StoredNode;

/**
 * Read operations.
 */
public interface RevisionProvider extends NodeStore {

    /**
     * Adapts the given {@link StoredNode} to a corresponding
     * {@link NodeState} instance.
     *
     * @param node stored node instance
     * @return node state adapter
     */
    NodeState getNodeState(StoredNode node);

    /**
     * Adapts the given {@link NodeState} to the corresponding identifier.
     *
     * @param node node state
     * @return node identifier
     */
    Id getId(NodeState node);

    StoredNode getNode(Id id) throws NotFoundException, Exception;
    StoredCommit getCommit(Id id) throws NotFoundException, Exception;
    ChildNodeEntriesMap getCNEMap(Id id) throws NotFoundException, Exception;
    StoredNode getRootNode(Id commitId) throws NotFoundException, Exception;
    StoredCommit getHeadCommit() throws Exception;
    Id getHeadCommitId() throws Exception;
}
