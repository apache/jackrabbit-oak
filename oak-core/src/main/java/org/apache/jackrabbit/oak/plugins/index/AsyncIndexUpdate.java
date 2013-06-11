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
package org.apache.jackrabbit.oak.plugins.index;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncIndexUpdate implements Runnable {

    private static final Logger log = LoggerFactory
            .getLogger(AsyncIndexUpdate.class);

    private final String name;

    private final NodeStore store;

    private final CommitHook hook;
    
    private NodeState current = EmptyNodeState.EMPTY_NODE;

    public AsyncIndexUpdate(
            @Nonnull String name,
            @Nonnull NodeStore store,
            @Nonnull IndexEditorProvider provider) {
        this.name = checkNotNull(name);
        this.store = checkNotNull(store);
        this.hook = new EditorHook(
                new IndexUpdateProvider(checkNotNull(provider), name));
    }

    @Override
    public void run() {
        log.debug("Running background index task {}", name);
        NodeStoreBranch branch = store.branch();
        NodeState after = branch.getHead();
        try {
            NodeState processed = hook.processCommit(current, after);
            branch.setRoot(processed);
            branch.merge(EmptyHook.INSTANCE);
            current = after;
        } catch (CommitFailedException e) {
            log.warn("Background index update " + name + " failed", e);
        }
    }

}
