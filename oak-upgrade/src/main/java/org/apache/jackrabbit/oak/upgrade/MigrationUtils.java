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
package org.apache.jackrabbit.oak.upgrade;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.commons.time.Stopwatch;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.migration.FilteringNodeState;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.ProgressNotificationEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;

final class MigrationUtils {

    static final Set<String> DEFAULT_INCLUDE_PATHS = FilteringNodeState.ALL;
    static final Set<String> DEFAULT_EXCLUDE_PATHS = FilteringNodeState.NONE;
    static final Set<String> DEFAULT_MERGE_PATHS = FilteringNodeState.NONE;

    private MigrationUtils() {
    }

    static Set<String> calculateEffectiveIncludePaths(Set<String> includePaths, NodeState sourceRoot) {
        if (!includePaths.contains("/")) {
            return Collections.unmodifiableSet(SetUtils.toLinkedSet(includePaths));
        }

        // include child nodes from source individually to avoid deleting other initialized content
        final Set<String> includes = new HashSet<>();
        for (String childNodeName : sourceRoot.getChildNodeNames()) {
            includes.add("/" + childNodeName);
        }
        return includes;
    }

    static EditorProvider createTypeEditorProvider(Logger logger) {
        return new EditorProvider() {
            @Override
            public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder, CommitInfo info)
                    throws CommitFailedException {
                Editor rootEditor = new TypeEditorProvider(false)
                        .getRootEditor(before, after, builder, info);
                return ProgressNotificationEditor.wrap(rootEditor, logger, "Checking node types:");
            }

            @Override
            public String toString() {
                return "TypeEditorProvider";
            }
        };
    }

    static EditorProvider createIndexEditorProvider(Logger logger) {
        final ProgressTicker ticker = new AsciiArtTicker();
        return new EditorProvider() {
            @Override
            public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder, CommitInfo info) {
                IndexEditorProvider editorProviders = new CompositeIndexEditorProvider(
                        new ReferenceEditorProvider(),
                        new PropertyIndexEditorProvider());

                return new IndexUpdate(editorProviders, null, after, builder, new IndexUpdateCallback() {
                    long t0;

                    @Override
                    public void indexUpdate() {
                        long t = System.currentTimeMillis();
                        if (t - t0 > 2000) {
                            logger.info("Updating indexes  {}", ticker.tick());
                            t0 = t;
                        }
                    }
                });
            }

            @Override
            public String toString() {
                return "IndexEditorProvider";
            }
        };
    }

    static CommitHook loggingCompositeHook(List<CommitHook> hooks, Logger logger) {
        return (before, after, info) -> {
            NodeState newState = after;
            Stopwatch watch = Stopwatch.createStarted();
            for (CommitHook hook : hooks) {
                logger.info("Processing commit via {}", hook);
                newState = hook.processCommit(before, newState, info);
                logger.info("Commit hook {} processed commit in {}", hook, watch);
                watch.reset().start();
            }
            return newState;
        };
    }
}
