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

package org.apache.jackrabbit.oak.plugins.index.importer;


import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.ResetCommitAttributeHook;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singletonList;

final class NodeStoreUtils {

    static void mergeWithConcurrentCheck(NodeStore nodeStore, NodeBuilder builder) throws CommitFailedException {
        CompositeHook hooks = new CompositeHook(
                ResetCommitAttributeHook.INSTANCE,
                new ConflictHook(new AnnotatingConflictHandler()),
                new EditorHook(CompositeEditorProvider.compose(singletonList(new ConflictValidatorProvider())))
        );
        nodeStore.merge(builder, hooks, createCommitInfo());
    }

    static void mergeWithConcurrentCheck(NodeStore nodeStore, NodeBuilder builder,
                                         IndexEditorProvider indexEditorProvider) throws CommitFailedException {
        CompositeHook hooks = new CompositeHook(
                ResetCommitAttributeHook.INSTANCE,
                new EditorHook(new IndexUpdateProvider(indexEditorProvider, null, true)),
                new ConflictHook(new AnnotatingConflictHandler()),
                new EditorHook(CompositeEditorProvider.compose(singletonList(new ConflictValidatorProvider())))
        );
        nodeStore.merge(builder, hooks, createCommitInfo());
    }

    static NodeBuilder childBuilder(NodeBuilder nb, String path) {
        for (String name : PathUtils.elements(checkNotNull(path))) {
            nb = nb.child(name);
        }
        return nb;
    }

    private static CommitInfo createCommitInfo() {
        Map<String, Object> info = ImmutableMap.<String, Object>of(CommitContext.NAME, new SimpleCommitContext());
        return new CommitInfo(CommitInfo.OAK_UNKNOWN, CommitInfo.OAK_UNKNOWN, info);
    }
}
