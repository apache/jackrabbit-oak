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
package org.apache.jackrabbit.oak;

import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;

public final class OakInitializer {

    public static final String SESSION_ID = "OakInitializer";

    private OakInitializer() {
    }

    public static void initialize(@NotNull NodeStore store,
                                  @NotNull RepositoryInitializer initializer,
                                  @NotNull CommitHook hook) {
        try {
            NodeBuilder builder = store.getRoot().builder();
            initializer.initialize(builder);
            store.merge(builder, hook, createCommitInfo());
        } catch (CommitFailedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void initialize(@NotNull Iterable<WorkspaceInitializer> initializer,
                                  @NotNull NodeStore store,
                                  @NotNull String workspaceName,
                                  @NotNull CommitHook hook) {
        NodeBuilder builder = store.getRoot().builder();
        for (WorkspaceInitializer wspInit : initializer) {
            wspInit.initialize(builder, workspaceName);
        }
        try {
            store.merge(builder, hook, createCommitInfo());
        } catch (CommitFailedException e) {
            throw new RuntimeException(e);
        }
    }

    private static CommitInfo createCommitInfo(){
        Map<String, Object> infoMap = Map.of(CommitContext.NAME, new SimpleCommitContext());
        return new CommitInfo(SESSION_ID, null, infoMap);
    }
}
