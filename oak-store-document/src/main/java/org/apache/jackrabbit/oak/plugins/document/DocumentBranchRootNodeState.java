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
package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The root node state of a persisted branch.
 */
class DocumentBranchRootNodeState extends DocumentNodeState {

    private final DocumentNodeStore store;
    private final DocumentNodeStoreBranch branch;

    DocumentBranchRootNodeState(@NotNull DocumentNodeStore store,
                                @NotNull DocumentNodeStoreBranch branch,
                                @NotNull Path path,
                                @NotNull RevisionVector rootRevision,
                                @Nullable RevisionVector lastRevision,
                                @NotNull BundlingContext bundlingContext,
                                int memory) {
        super(store, path, lastRevision, rootRevision, false, bundlingContext, memory);
        this.store = store;
        this.branch = checkNotNull(branch);
    }

    @NotNull
    @Override
    public NodeBuilder builder() {
        return new DocumentRootBuilder(store.getRoot(getRootRevision()), store, branch);
    }
}
