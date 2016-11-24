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

import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A change dispatcher that pre-fetches visible external changes in a background
 * task.
 */
class PrefetchDispatcher extends ChangeDispatcher {

    private final Executor executor;
    private NodeState root;

    public PrefetchDispatcher(@Nonnull NodeState root,
                              @Nonnull Executor executor) {
        super(root);
        this.root = root;
        this.executor = checkNotNull(executor);
    }

    @Override
    public synchronized void contentChanged(@Nonnull NodeState root,
                                            @Nonnull CommitInfo info) {
        if (root instanceof DocumentNodeState) {
            final DocumentNodeState state = (DocumentNodeState) root;
            if (state.isFromExternalChange()) {
                executor.execute(new Runnable() {
                    private final NodeState before = PrefetchDispatcher.this.root;
                    @Override
                    public void run() {
                        EditorDiff.process(
                                new VisibleEditor(TraversingEditor.INSTANCE),
                                before, state);
                    }
                });
            }
        }
        super.contentChanged(root, info);
        this.root = root;
    }

    private static final class TraversingEditor extends DefaultEditor {

        static final Editor INSTANCE = new TraversingEditor();

        @Override
        public Editor childNodeAdded(String name, NodeState after)
                throws CommitFailedException {
            return this;
        }

        @Override
        public Editor childNodeChanged(String name,
                                       NodeState before,
                                       NodeState after)
                throws CommitFailedException {
            return this;
        }

        @Override
        public Editor childNodeDeleted(String name, NodeState before)
                throws CommitFailedException {
            return this;
        }
    }
}
