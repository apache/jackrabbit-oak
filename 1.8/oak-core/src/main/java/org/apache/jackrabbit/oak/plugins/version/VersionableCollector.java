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

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONSTORAGE;

/**
 * This editor puts UUIDs of all processed versionables into
 * {@link VersionableCollector#existingVersionables} set. The main purpose
 * is to handle moved/renamed versionable nodes in the {@link OrphanedVersionCleaner}.
 */
class VersionableCollector extends DefaultEditor {

    private final ReadWriteVersionManager vMgr;

    private final Set<String> existingVersionables;

    VersionableCollector(ReadWriteVersionManager vMgr, Set<String> existingVersionables) {
        this.vMgr = vMgr;
        this.existingVersionables = existingVersionables;
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        if (vMgr.isVersionable(after)) {
            existingVersionables.add(Utils.uuidFromNode(after));
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
            return new VersionableCollector(vMgr, existingVersionables);
        }

    }
}
