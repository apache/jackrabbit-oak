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
package org.apache.jackrabbit.oak.plugins.version;

import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Iterator;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ReadOnlyRoot;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.util.TODO;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_BASEVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_CREATED;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENMIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENNODE;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENPRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENUUID;
import static org.apache.jackrabbit.JcrConstants.JCR_ISCHECKEDOUT;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PREDECESSORS;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_ROOTVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_SUCCESSORS;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONABLEUUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONHISTORY;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONLABELS;
import static org.apache.jackrabbit.JcrConstants.NT_FROZENNODE;
import static org.apache.jackrabbit.JcrConstants.NT_VERSION;
import static org.apache.jackrabbit.JcrConstants.NT_VERSIONHISTORY;
import static org.apache.jackrabbit.JcrConstants.NT_VERSIONLABELS;
import static org.apache.jackrabbit.oak.version.VersionConstants.REP_VERSIONSTORAGE;

/**
 * TODO document
 */
class ReadWriteVersionManager extends ReadOnlyVersionManager {

    private final NodeBuilder versionStorageNode;
    private final NodeBuilder workspaceRoot;

    ReadWriteVersionManager(NodeBuilder versionStorageNode,
                            NodeBuilder workspaceRoot) {
        this.versionStorageNode = checkNotNull(versionStorageNode);
        this.workspaceRoot = checkNotNull(workspaceRoot);
    }

    @Nonnull
    @Override
    protected Tree getVersionStorageTree() {
        return new ReadOnlyTree(versionStorageNode.getNodeState());
    }

    @Nonnull
    @Override
    protected Root getWorkspaceRoot() {
        return new ReadOnlyRoot(workspaceRoot.getNodeState());
    }

    @Nonnull
    @Override
    protected ReadOnlyNodeTypeManager getNodeTypeManager() {
        return ReadOnlyNodeTypeManager.getInstance(
                getWorkspaceRoot(), NamePathMapper.DEFAULT);
    }

    /**
     * Gets or creates the version history for the given
     * <code>versionable</code> node.
     *
     * @param versionable the versionable node.
     * @return the version history node.
     * @throws IllegalArgumentException if the given node does not have a
     *                                  <code>jcr:uuid</code> property.
     */
    @Nonnull
    NodeBuilder getOrCreateVersionHistory(@Nonnull NodeBuilder versionable) {
        checkNotNull(versionable);
        PropertyState p = versionable.getProperty(JCR_UUID);
        if (p == null) {
            throw new IllegalArgumentException("Not referenceable");
        }
        String vUUID = p.getValue(Type.STRING);
        String relPath = getVersionHistoryPath(vUUID);
        NodeBuilder node = versionStorageNode;
        for (Iterator<String> it = PathUtils.elements(relPath).iterator(); it.hasNext(); ) {
            String name = it.next();
            node = node.child(name);
            if (node.getProperty(JCR_PRIMARYTYPE) == null) {
                String nt;
                if (it.hasNext()) {
                    nt = REP_VERSIONSTORAGE;
                } else {
                    // last path element denotes nt:versionHistory node
                    nt = NT_VERSIONHISTORY;
                }
                node.setProperty(JCR_PRIMARYTYPE, nt, Type.NAME);
            }
        }
        // use jcr:versionLabels node to detect if we need to initialize the
        // version history
        if (!node.hasChildNode(JCR_VERSIONLABELS)) {
            // jcr:versionableUuuid property
            node.setProperty(JCR_VERSIONABLEUUID, vUUID, Type.STRING);
            node.setProperty(JCR_UUID,
                    IdentifierManager.generateUUID(), Type.STRING);

            // jcr:versionLabels child node
            NodeBuilder vLabels = node.child(JCR_VERSIONLABELS);
            vLabels.setProperty(JCR_PRIMARYTYPE, NT_VERSIONLABELS, Type.NAME);

            // jcr:rootVersion child node
            NodeBuilder rootVersion = node.child(JCR_ROOTVERSION);
            rootVersion.setProperty(JCR_UUID,
                    IdentifierManager.generateUUID(), Type.STRING);
            rootVersion.setProperty(JCR_PRIMARYTYPE, NT_VERSION, Type.NAME);
            String now = Conversions.convert(GregorianCalendar.getInstance()).toDate();
            rootVersion.setProperty(JCR_CREATED, now, Type.DATE);
            rootVersion.setProperty(JCR_PREDECESSORS,
                    Collections.<String>emptyList(), Type.REFERENCES);
            rootVersion.setProperty(JCR_SUCCESSORS,
                    Collections.<String>emptyList(), Type.REFERENCES);

            // jcr:frozenNode of jcr:rootVersion
            NodeBuilder frozenNode = rootVersion.child(JCR_FROZENNODE);
            frozenNode.setProperty(JCR_UUID,
                    IdentifierManager.generateUUID(), Type.STRING);
            frozenNode.setProperty(JCR_PRIMARYTYPE, NT_FROZENNODE, Type.NAME);
            Iterable<String> mixinTypes;
            if (versionable.getProperty(JCR_MIXINTYPES) != null) {
                mixinTypes = versionable.getProperty(JCR_MIXINTYPES).getValue(Type.NAMES);
            } else {
                mixinTypes = Collections.emptyList();
            }
            frozenNode.setProperty(JCR_FROZENMIXINTYPES, mixinTypes, Type.NAMES);
            frozenNode.setProperty(JCR_FROZENPRIMARYTYPE,
                    versionable.getProperty(JCR_PRIMARYTYPE).getValue(Type.NAME),
                    Type.NAME);
            frozenNode.setProperty(JCR_FROZENUUID, vUUID, Type.STRING);

            // set jcr:isCheckedOut, jcr:versionHistory, jcr:baseVersion and
            // jcr:predecessors on versionable node
            versionable.setProperty(JCR_ISCHECKEDOUT, Boolean.TRUE, Type.BOOLEAN);
            versionable.setProperty(JCR_VERSIONHISTORY,
                    node.getProperty(JCR_UUID).getValue(Type.STRING), Type.REFERENCE);
            String rootVersionUUID = rootVersion.getProperty(
                    JCR_UUID).getValue(Type.STRING);
            versionable.setProperty(JCR_BASEVERSION, rootVersionUUID, Type.REFERENCE);
            versionable.setProperty(JCR_PREDECESSORS,
                    Collections.singletonList(rootVersionUUID), Type.REFERENCES);
        }
        return node;
    }

    public void checkout(NodeBuilder versionable) {
        TODO.unimplemented();
    }

    public void checkin(NodeBuilder versionable) {
        TODO.unimplemented();
    }

    public void restore(NodeBuilder versionable) {
        TODO.unimplemented();
    }

    // TODO: more methods that modify versions
}
