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

package org.apache.jackrabbit.oak.plugins.index.progress;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Editor to track traversal and notify the callback for each node traversed.
 * The editor also ensures that path is lazily constructed
 */
class ProgressTrackingEditor implements Editor, NodeTraversalCallback.PathSource {
    private final Editor editor;
    private final NodeTraversalCallback traversalCallback;
    private final ProgressTrackingEditor parent;
    private final String name;

    public ProgressTrackingEditor(Editor editor, String name, NodeTraversalCallback traversalCallback) {
        this.editor = editor;
        this.name = name;
        this.traversalCallback = traversalCallback;
        this.parent = null;
    }

    private ProgressTrackingEditor(Editor editor, String name, NodeTraversalCallback callback,
                                   ProgressTrackingEditor parent) {
        this.editor = editor;
        this.name = name;
        this.traversalCallback = callback;
        this.parent = parent;
    }

    @CheckForNull
    public static Editor wrap(@CheckForNull Editor editor, NodeTraversalCallback onProgress) {
        if (editor != null && !(editor instanceof ProgressTrackingEditor)) {
            return new ProgressTrackingEditor(editor, "/", onProgress);
        }
        return editor;
    }

    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        traversalCallback.traversedNode(this);
        editor.enter(before, after);
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        editor.leave(before, after);
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        editor.propertyAdded(after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        editor.propertyChanged(before, after);
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        editor.propertyDeleted(before);
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        return createChildEditor(editor.childNodeAdded(name, after), name);
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        return createChildEditor(editor.childNodeChanged(name, before, after), name);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        return createChildEditor(editor.childNodeDeleted(name, before), name);
    }

    public String getPath() {
        if (parent == null) {
            return PathUtils.ROOT_PATH;
        } else {
            StringBuilder sb = new StringBuilder(128);
            buildPath(sb);
            return sb.toString();
        }
    }

    private void buildPath(@Nonnull StringBuilder sb) {
        if (parent != null) {
            parent.buildPath(sb);
            sb.append('/').append(name);
        }
    }

    private ProgressTrackingEditor createChildEditor(Editor editor, String name) {
        if (editor == null) {
            return null;
        } else {
            return new ProgressTrackingEditor(editor, name, traversalCallback, this);
        }
    }
}
