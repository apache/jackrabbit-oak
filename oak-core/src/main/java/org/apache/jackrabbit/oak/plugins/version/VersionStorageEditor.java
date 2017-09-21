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
package org.apache.jackrabbit.oak.plugins.version;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONLABELS;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PathUtils.getDepth;
import static org.apache.jackrabbit.oak.commons.PathUtils.relativize;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.VERSION_NODE_TYPE_NAMES;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.VERSION_STORE_NT_NAMES;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.VERSION_STORE_PATH;

/**
 * Validates changes on the version store.
 */
class VersionStorageEditor extends DefaultEditor {

    private static final int VERSION_HISTORY_DEPTH = 6;

    private final NodeBuilder versionStorageNode;
    private final NodeBuilder workspaceRoot;
    private final NodeBuilder builder;
    private final String path;
    private ReadWriteVersionManager vMgr;

    VersionStorageEditor(@Nonnull NodeBuilder versionStorageNode,
                         @Nonnull NodeBuilder workspaceRoot) {
        this(versionStorageNode, workspaceRoot, versionStorageNode,
                VERSION_STORE_PATH);
    }

    private VersionStorageEditor(@Nonnull NodeBuilder versionStorageNode,
                                 @Nonnull NodeBuilder workspaceRoot,
                                 @Nonnull NodeBuilder builder,
                                 @Nonnull String path) {
        this.versionStorageNode = checkNotNull(versionStorageNode);
        this.workspaceRoot = checkNotNull(workspaceRoot);
        this.builder = checkNotNull(builder);
        this.path = checkNotNull(path);
    }

    @Override
    public Editor childNodeChanged(String name,
                                   NodeState before,
                                   NodeState after)
            throws CommitFailedException {
        int d = getDepth(path);
        String p = concat(path, name);
        if (d == VERSION_HISTORY_DEPTH
                && name.equals(JCR_VERSIONLABELS)) {
            return new VersionLabelsEditor(p, getVersionManager());
        }
        if (d < VERSION_HISTORY_DEPTH && !isVersionStorageNode(after)) {
            return null;
        }
        return new VersionStorageEditor(versionStorageNode, workspaceRoot,
                builder.child(name), p);
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        int d = getDepth(path);
        // allow child nodes under version storage node, unless an attempt
        // is made to create rep:versionStorage nodes manually.
        if (d == getDepth(VERSION_STORE_PATH) &&
                !isVersionStorageNode(after)) {
            return null;
        }
        return throwProtected(name);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        int d = getDepth(path);
        if (d == VERSION_HISTORY_DEPTH) {
            // restore version on builder
            builder.setChildNode(name, before);
            String relPath = relativize(VERSION_STORE_PATH, concat(path, name));
            // let version manager remove it properly
            getVersionManager().removeVersion(relPath);
            return null;
        } else if (isVersionStorageNode(before) || d > VERSION_HISTORY_DEPTH) {
            throwProtected(name);
        }
        return null;
    }

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        if (getDepth(path) < VERSION_HISTORY_DEPTH) {
            return;
        }
        throwProtected(after.getName());
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        if (getDepth(path) < VERSION_HISTORY_DEPTH) {
            return;
        }
        throwProtected(before.getName());
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        if (getDepth(path) < VERSION_HISTORY_DEPTH) {
            return;
        }
        throwProtected(before.getName());
    }

    //-------------------------< internal >-------------------------------------

    private static boolean isVersionStorageNode(NodeState state) {
        String ntName = state.getName(JCR_PRIMARYTYPE);
        return VERSION_STORE_NT_NAMES.contains(ntName)
                || VERSION_NODE_TYPE_NAMES.contains(ntName);
    }

    private ReadWriteVersionManager getVersionManager() {
        if (vMgr == null) {
            vMgr = new ReadWriteVersionManager(versionStorageNode, workspaceRoot);
        }
        return vMgr;
    }

    private Editor throwProtected(String name) throws CommitFailedException {
        return Utils.throwProtected(concat(path, name));
    }
}
