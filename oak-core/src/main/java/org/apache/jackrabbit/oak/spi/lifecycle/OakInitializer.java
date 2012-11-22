/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.spi.lifecycle;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexHookManager;
import org.apache.jackrabbit.oak.plugins.index.IndexHookProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

public class OakInitializer {

    public static void initialize(NodeStore store,
            RepositoryInitializer initializer, IndexHookProvider indexHook) {

        // TODO refactor initializer to be able to first #branch, then
        // #initialize, next #index and finally #merge
        // This means that the RepositoryInitializer should receive a
        // NodeStoreBranch as a param

        initializer.initialize(store);

        NodeStoreBranch branch = store.branch();
        NodeBuilder root = branch.getRoot().builder();
        try {
            branch.setRoot(IndexHookManager.of(indexHook).processCommit(
                    MemoryNodeState.EMPTY_NODE, root.getNodeState()));
            branch.merge();
        } catch (CommitFailedException e) {
            throw new RuntimeException(e);
        }
    }

}
