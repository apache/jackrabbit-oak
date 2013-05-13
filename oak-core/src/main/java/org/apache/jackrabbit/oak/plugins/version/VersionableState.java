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

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.version.OnParentVersionAction;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENMIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENNODE;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENPRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENUUID;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONHISTORY;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.JcrConstants.NT_FROZENNODE;
import static org.apache.jackrabbit.oak.plugins.version.Utils.uuidFromNode;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.IdentifierManager;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * <code>VersionableState</code> provides methods to create a versionable state
 * for a version based on a versionable node.
 */
class VersionableState {

    private static final String JCR_CHILDVERSIONHISTORY = "jcr:childVersionHistory";

    private final NodeBuilder frozenNode;
    private final NodeBuilder versionable;
    private final ReadOnlyNodeTypeManager ntMgr;

    private VersionableState(@Nonnull NodeBuilder version,
                             @Nonnull NodeBuilder versionable,
                             @Nonnull ReadOnlyNodeTypeManager ntMgr) {
        this.frozenNode = checkNotNull(version).child(JCR_FROZENNODE);
        this.versionable = checkNotNull(versionable);
        this.ntMgr = checkNotNull(ntMgr);
        // initialize jcr:frozenNode
        frozenNode.setProperty(JCR_UUID, IdentifierManager.generateUUID(), Type.STRING);
        frozenNode.setProperty(JCR_PRIMARYTYPE, NT_FROZENNODE, Type.NAME);
        Iterable<String> mixinTypes;
        if (versionable.hasProperty(JCR_MIXINTYPES)) {
            mixinTypes = versionable.getNames(JCR_MIXINTYPES);
        } else {
            mixinTypes = Collections.emptyList();
        }
        frozenNode.setProperty(JCR_FROZENMIXINTYPES, mixinTypes, Type.NAMES);
        frozenNode.setProperty(JCR_FROZENPRIMARYTYPE,
                versionable.getName(JCR_PRIMARYTYPE), Type.NAME);
        frozenNode.setProperty(JCR_FROZENUUID, uuidFromNode(versionable), Type.STRING);
    }

    /**
     * Creates a frozen node under the version and initializes it with the basic
     * frozen properties (jcr:frozenPrimaryType, jcr:frozenMixinTypes and
     * jcr:frozenUuid) from the given versionable node.
     *
     * @param version the parent node of the frozen node.
     * @param versionable the versionable node.
     * @param ntMgr the node type manager.
     * @return a versionable state
     */
    @Nonnull
    static VersionableState fromVersion(@Nonnull NodeBuilder version,
                                        @Nonnull NodeBuilder versionable,
                                        @Nonnull ReadOnlyNodeTypeManager ntMgr) {
        return new VersionableState(version, versionable, ntMgr);
    }

    @Nonnull
    NodeBuilder getFrozenNode() {
        return frozenNode;
    }

    /**
     * Creates the versionable state under the version.
     *
     * @return the frozen node.
     * @throws CommitFailedException if the operation fails. E.g. because the
     *              versionable node has a property with OPV ABORT.
     */
    NodeBuilder create() throws CommitFailedException {
        try {
            createState(versionable, frozenNode);
            return frozenNode;
        } catch (RepositoryException e) {
            throw new CommitFailedException(CommitFailedException.VERSION, 0,
                    "Unexpected RepositoryException", e);
        }
    }

    private void createState(NodeBuilder src,
                             NodeBuilder dest)
            throws CommitFailedException, RepositoryException {
        copyProperties(src, dest, false, true);

        // add the frozen children and histories
        for (String name : src.getChildNodeNames()) {
            NodeBuilder child = src.getChildNode(name);
            int opv = getOPV(src, child);

            if (opv == OnParentVersionAction.ABORT) {
                throw new CommitFailedException(CommitFailedException.VERSION, 1,
                        "Checkin aborted due to OPV abort in " + name);
            }
            if (opv == OnParentVersionAction.VERSION) {
                if (ntMgr.isNodeType(new ReadOnlyTree(child.getNodeState()), MIX_VERSIONABLE)) {
                    // create frozen versionable child
                    versionedChild(child, dest.child(name));
                } else {
                    // else copy
                    copy(child, dest.child(name));
                }
            } else if (opv == OnParentVersionAction.COPY) {
                copy(child, dest.child(name));
            }
        }
    }

    private void versionedChild(NodeBuilder src, NodeBuilder dest) {
        String ref = src.getProperty(JCR_VERSIONHISTORY).getValue(Type.REFERENCE);
        dest.setProperty(JCR_CHILDVERSIONHISTORY, ref, Type.REFERENCE);
    }

    private void copy(NodeBuilder src,
                      NodeBuilder dest)
            throws RepositoryException, CommitFailedException {
        copyProperties(src, dest, true, false);
        for (String name : src.getChildNodeNames()) {
            NodeBuilder child = src.getChildNode(name);
            copy(child, dest.child(name));
        }
    }

    private void copyProperties(NodeBuilder src,
                                NodeBuilder dest,
                                boolean forceCopy,
                                boolean ignoreTypeAndUUID)
            throws RepositoryException, CommitFailedException {
        // add the properties
        for (PropertyState prop : src.getProperties()) {
            int opv;
            if (forceCopy) {
                opv = OnParentVersionAction.COPY;
            } else {
                opv = getOPV(src, prop);
            }

            String propName = prop.getName();
            if (opv == OnParentVersionAction.ABORT) {
                throw new CommitFailedException(CommitFailedException.VERSION, 1,
                        "Checkin aborted due to OPV abort in " + propName);
            }
            if (ignoreTypeAndUUID
                    && (propName.equals(JCR_PRIMARYTYPE)
                        || propName.equals(JCR_MIXINTYPES)
                        || propName.equals(JCR_UUID))) {
                continue;
            }
            if (opv == OnParentVersionAction.VERSION
                    || opv == OnParentVersionAction.COPY) {
                dest.setProperty(prop);
            }
        }
    }

    private int getOPV(NodeBuilder parent, NodeBuilder child)
            throws RepositoryException {
        return ntMgr.getDefinition(new ReadOnlyTree(parent.getNodeState()),
                new ReadOnlyTree(child.getNodeState())).getOnParentVersion();
    }

    private int getOPV(NodeBuilder node, PropertyState property)
            throws RepositoryException {
        return ntMgr.getDefinition(new ReadOnlyTree(node.getNodeState()),
                property, false).getOnParentVersion();
    }
}
