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
package org.apache.jackrabbit.oak.plugins.version;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONSTORAGE;

/**
 * This editor removes empty version histories for nodes that are no longer versionable
 * (eg. because were deleted). The class won't remove histories for nodes which
 * UUIDs are present in the {@link OrphanedVersionCleaner#existingVersionables}.
 * The set should be used to skip processing moved/renamed nodes and it can be
 * filled by the related {@link VersionableCollector} editor.
 */
class OrphanedVersionCleaner extends DefaultEditor {

    private final ReadWriteVersionManager vMgr;

    private final Set<String> existingVersionables;

    OrphanedVersionCleaner(ReadWriteVersionManager vMgr, Set<String> existingVersionables) {
        this.vMgr = vMgr;
        this.existingVersionables = existingVersionables;
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        if (vMgr.isVersionable(before) && !vMgr.isVersionable(after)) {
            String versionableUuid = Utils.uuidFromNode(before);
            if (!existingVersionables.contains(versionableUuid)) {
                vMgr.removeEmptyHistory(before);
            }
        }
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        return this;
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        return this;
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        return this;
    }

    static class Provider implements EditorProvider {

        private final Set<String> existingVersionables;

        Provider(Set<String> existingVersionables) {
            this.existingVersionables = existingVersionables;
        }

        @Override
        public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder, CommitInfo info) throws CommitFailedException {
            if (!builder.hasChildNode(JCR_SYSTEM)) {
                return null;
            }
            NodeBuilder system = builder.child(JCR_SYSTEM);
            if (!system.hasChildNode(JCR_VERSIONSTORAGE)) {
                return null;
            }
            NodeBuilder versionStorage = system.child(JCR_VERSIONSTORAGE);
            ReadWriteVersionManager vMgr = new ReadWriteVersionManager(versionStorage, builder);
            return new OrphanedVersionCleaner(vMgr, existingVersionables);
        }

    }
}
