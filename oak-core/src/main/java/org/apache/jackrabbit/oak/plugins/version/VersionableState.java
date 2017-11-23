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

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.jcr.version.OnParentVersionAction.ABORT;
import static javax.jcr.version.OnParentVersionAction.COMPUTE;
import static javax.jcr.version.OnParentVersionAction.COPY;
import static javax.jcr.version.OnParentVersionAction.IGNORE;
import static javax.jcr.version.OnParentVersionAction.INITIALIZE;
import static javax.jcr.version.OnParentVersionAction.VERSION;
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
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONHISTORY;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.JcrConstants.NT_FROZENNODE;
import static org.apache.jackrabbit.JcrConstants.NT_VERSIONEDCHILD;
import static org.apache.jackrabbit.oak.plugins.version.Utils.primaryTypeOf;
import static org.apache.jackrabbit.oak.plugins.version.Utils.uuidFromNode;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.version.OnParentVersionAction;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code VersionableState} provides methods to create a versionable state
 * for a version based on a versionable node.
 * <p>
 * The restore implementation of this class does not handle the removeExisting
 * flag. It is expected that this is handled on a higher level. If this is not
 * done the uniqueness constraint on the jcr:uuid will kick in and fail the
 * commit.
 * </p>
 */
class VersionableState {

    private static final Logger log = LoggerFactory.getLogger(VersionableState.class);

    private static final String JCR_CHILDVERSIONHISTORY = "jcr:childVersionHistory";
    private static final Set<String> BASIC_PROPERTIES = new HashSet<String>();
    private static final Set<String> BASIC_FROZEN_PROPERTIES = new HashSet<String>();

    static {
        BASIC_PROPERTIES.add(JCR_PRIMARYTYPE);
        BASIC_PROPERTIES.add(JCR_UUID);
        BASIC_PROPERTIES.add(JCR_MIXINTYPES);
        BASIC_FROZEN_PROPERTIES.addAll(BASIC_PROPERTIES);
        BASIC_FROZEN_PROPERTIES.add(JCR_FROZENPRIMARYTYPE);
        BASIC_FROZEN_PROPERTIES.add(JCR_FROZENUUID);
        BASIC_FROZEN_PROPERTIES.add(JCR_FROZENMIXINTYPES);
    }

    private final NodeBuilder version;
    private final NodeBuilder history;
    private final NodeBuilder frozenNode;
    private final NodeBuilder versionable;
    private final ReadWriteVersionManager vMgr;
    private final ReadOnlyNodeTypeManager ntMgr;

    private VersionableState(@Nonnull NodeBuilder version,
                             @Nonnull NodeBuilder history,
                             @Nonnull NodeBuilder versionable,
                             @Nonnull ReadWriteVersionManager vMgr,
                             @Nonnull ReadOnlyNodeTypeManager ntMgr) {
        this.version = checkNotNull(version);
        this.history = checkNotNull(history);
        this.frozenNode = version.child(JCR_FROZENNODE);
        this.versionable = checkNotNull(versionable);
        this.vMgr = checkNotNull(vMgr);
        this.ntMgr = checkNotNull(ntMgr);
    }

    /**
     * Creates a frozen node under the version and initializes it with the basic
     * frozen properties (jcr:frozenPrimaryType, jcr:frozenMixinTypes and
     * jcr:frozenUuid) from the given versionable node.
     *
     * @param version the parent node of the frozen node.
     * @param history the history node of the version.
     * @param versionable the versionable node.
     * @param vMgr the version manager.
     * @param ntMgr the node type manager.
     * @return a versionable state
     */
    @Nonnull
    static VersionableState fromVersion(@Nonnull NodeBuilder version,
                                        @Nonnull NodeBuilder history,
                                        @Nonnull NodeBuilder versionable,
                                        @Nonnull ReadWriteVersionManager vMgr,
                                        @Nonnull ReadOnlyNodeTypeManager ntMgr) {
        VersionableState state = new VersionableState(
                version, history, versionable, vMgr, ntMgr);
        return state.initFrozen(version.child(JCR_FROZENNODE),
                versionable, uuidFromNode(versionable));
    }

    /**
     * Creates a versionable state for a restore.
     *
     * @param version the version to restore.
     * @param history the history node of the version.
     * @param versionable the versionable node.
     * @param vMgr the version manager.
     * @param ntMgr the node type manager.
     * @return a versionable state.
     */
    static VersionableState forRestore(@Nonnull NodeBuilder version,
                                       @Nonnull NodeBuilder history,
                                       @Nonnull NodeBuilder versionable,
                                       @Nonnull ReadWriteVersionManager vMgr,
                                       @Nonnull ReadOnlyNodeTypeManager ntMgr) {
        return new VersionableState(version, history, versionable, vMgr, ntMgr);
    }

    /**
     * Creates a frozen node and initializes it with the basic
     * frozen properties (jcr:frozenPrimaryType, jcr:frozenMixinTypes and
     * jcr:frozenUuid) from the given node.
     *
     * @return this versionable state.
     */
    private VersionableState initFrozen(NodeBuilder frozen,
                                        NodeBuilder node,
                                        String nodeId) {
        // initialize jcr:frozenNode
        frozen.setProperty(JCR_UUID, UUIDUtils.generateUUID(), Type.STRING);
        frozen.setProperty(JCR_PRIMARYTYPE, NT_FROZENNODE, Type.NAME);
        List<String> mixinTypes;
        if (node.hasProperty(JCR_MIXINTYPES)) {
            mixinTypes = Lists.newArrayList(node.getNames(JCR_MIXINTYPES));
        } else {
            mixinTypes = Collections.emptyList();
        }
        frozen.setProperty(JCR_FROZENUUID, nodeId, Type.STRING);
        frozen.setProperty(JCR_FROZENPRIMARYTYPE, primaryTypeOf(node), Type.NAME);
        if (mixinTypes.isEmpty()) {
            frozen.removeProperty(JCR_FROZENMIXINTYPES);
        } else {
            frozen.setProperty(JCR_FROZENMIXINTYPES, mixinTypes, Type.NAMES);
        }
        return this;
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
            createFrozen(versionable, uuidFromNode(versionable), frozenNode, VERSION);
            return frozenNode;
        } catch (RepositoryException e) {
            throw new CommitFailedException(CommitFailedException.VERSION,
                    VersionExceptionCode.UNEXPECTED_REPOSITORY_EXCEPTION.ordinal(),
                    "Unexpected RepositoryException", e);
        }
    }

    /**
     * Restore the versionable node to the given version.
     *
     * @param selector an optional version selector. If none is passed, this
     *                 method will use a date based version selector.
     * @return the versionable node.
     * @throws CommitFailedException if the operation fails.
     */
    public NodeBuilder restore(@Nullable VersionSelector selector)
            throws CommitFailedException {
        try {
            if (selector == null) {
                String created = version.getProperty(JCR_CREATED).getValue(Type.DATE);
                selector = new DateVersionSelector(created);
            }
            restoreFrozen(frozenNode, versionable, selector);
            restoreVersionable(versionable, version);
            return versionable;
        } catch (RepositoryException e) {
            throw new CommitFailedException(CommitFailedException.VERSION,
                    VersionExceptionCode.UNEXPECTED_REPOSITORY_EXCEPTION.ordinal(),
                    "Unexpected RepositoryException", e);
        }
    }

    //--------------------------< internal >------------------------------------

    /**
     * Restores the state from {@code src} to a child node of
     * {@code destParent} with the same name as {@code src}.
     *
     * @param src the source node.
     * @param destParent the parent of the destination node.
     * @param name the name of the source node.
     * @param selector the version selector.
     */
    private void restoreState(@Nonnull NodeBuilder src,
                              @Nonnull NodeBuilder destParent,
                              @Nonnull String name,
                              @Nonnull VersionSelector selector)
            throws RepositoryException, CommitFailedException {
        checkNotNull(name);
        checkNotNull(destParent);
        String primaryType = primaryTypeOf(src);
        if (primaryType.equals(NT_FROZENNODE)) {
            // replace with frozen state
            destParent.getChildNode(name).remove();
            restoreFrozen(src, destParent.child(name), selector);
        } else if (primaryType.equals(NT_VERSIONEDCHILD)) {
            // only perform chained restore if the node didn't exist
            // before. see 15.7.5 and RestoreTest#testRestoreName
            if (!destParent.hasChildNode(name)) {
                restoreVersionedChild(src, destParent.child(name), selector);
            }
        } else {
            // replace
            destParent.getChildNode(name).remove();
            restoreCopiedNode(src, destParent.child(name), selector);
        }
    }

    /**
     * Restore a nt:frozenNode.
     */
    private void restoreFrozen(@Nonnull NodeBuilder frozen,
                               @Nonnull NodeBuilder dest,
                               @Nonnull VersionSelector selector)
            throws RepositoryException, CommitFailedException {
        // 15.7.2 Restoring Type and Identifier
        restoreFrozenTypeAndUUID(frozen, dest);
        // 15.7.3 Restoring Properties
        for (PropertyState p : frozen.getProperties()) {
            if (BASIC_FROZEN_PROPERTIES.contains(p.getName())) {
                // ignore basic frozen properties we restored earlier
                continue;
            }
            int action = getOPV(dest, p);
            if (action == COPY || action == VERSION) {
                dest.setProperty(p);
            }
        }
        for (PropertyState p : dest.getProperties()) {
            String propName = p.getName();
            if (BASIC_PROPERTIES.contains(propName)) {
                continue;
            }
            if (frozen.hasProperty(propName)) {
                continue;
            }
            int action = getOPV(dest, p);
            if (action == COPY || action == VERSION || action == ABORT) {
                dest.removeProperty(propName);
            } else if (action == IGNORE) {
                // no action
            } else if (action == INITIALIZE) {
                resetToDefaultValue(dest, p);
            } else if (action == COMPUTE) {
                // only COMPUTE property definitions currently are
                // jcr:primaryType and jcr:mixinTypes
                // do nothing for now
                if (!(JCR_PRIMARYTYPE.equals(propName) || JCR_MIXINTYPES.equals(propName))) {
                    log.warn("OPV.COMPUTE not implemented for restoring property: " + propName);
                }
            }
        }
        restoreChildren(frozen, dest, selector);
    }

    /**
     * Restores the basic frozen properties (jcr:primaryType, jcr:mixinTypes
     * and jcr:uuid).
     */
    private void restoreFrozenTypeAndUUID(@Nonnull NodeBuilder frozen,
                                          @Nonnull NodeBuilder dest) {
        dest.setProperty(JCR_PRIMARYTYPE,
                frozen.getName(JCR_FROZENPRIMARYTYPE), Type.NAME);
        String id = frozen.getProperty(JCR_FROZENUUID).getValue(Type.STRING);
        if (id.indexOf('/') == -1) {
            // only restore jcr:uuid if id is in fact a uuid
            dest.setProperty(JCR_UUID, id, Type.STRING);
        }
        if (frozen.hasProperty(JCR_FROZENMIXINTYPES)) {
            dest.setProperty(JCR_MIXINTYPES,
                    frozen.getNames(JCR_FROZENMIXINTYPES), Type.NAMES);
        }
    }

    /**
     * Restore a copied node.
     */
    private void restoreCopiedNode(NodeBuilder src,
                                   NodeBuilder dest,
                                   VersionSelector selector)
            throws RepositoryException, CommitFailedException {
        if (primaryTypeOf(src).equals(NT_FROZENNODE)) {
            restoreFrozenTypeAndUUID(src, dest);
            copyProperties(src, dest, new OPVProvider() {
                @Override
                public int getAction(NodeBuilder src,
                                     NodeBuilder dest,
                                     PropertyState prop)
                        throws RepositoryException {
                    // copy back, except for basic frozen props
                    if (BASIC_FROZEN_PROPERTIES.contains(prop.getName())) {
                        return IGNORE;
                    } else {
                        return COPY;
                    }
                }
            }, true);
        } else {
            copyProperties(src, dest, OPVForceCopy.INSTANCE, false);
        }
        restoreChildren(src, dest, selector);
    }

    /**
     * Restore an nt:versionedChild node.
     */
    private void restoreVersionedChild(NodeBuilder versionedChild,
                                       NodeBuilder dest,
                                       VersionSelector selector)
            throws RepositoryException, CommitFailedException {
        // 15.7.5 Chained Versions on Restore
        PropertyState id = versionedChild.getProperty(JCR_CHILDVERSIONHISTORY);
        if (id == null) {
            throw new RepositoryException("Mandatory property " +
                    JCR_CHILDVERSIONHISTORY + " is missing.");
        }
        vMgr.restore(id.getValue(Type.REFERENCE), selector, dest);
    }

    /**
     * Restores children of a {@code src}.
     */
    private void restoreChildren(NodeBuilder src,
                                 NodeBuilder dest,
                                 VersionSelector selector)
            throws RepositoryException, CommitFailedException {
        // 15.7.6 Restoring Child Nodes
        for (String name : src.getChildNodeNames()) {
            NodeBuilder srcChild = src.getChildNode(name);
            int action = getOPV(dest, srcChild, name);
            if (action == COPY) {
                // replace on destination
                dest.getChildNode(name).remove();
                restoreCopiedNode(srcChild, dest.child(name), selector);
            } else if (action == VERSION) {
                restoreState(srcChild, dest, name, selector);
            }
        }
        for (String name : dest.getChildNodeNames()) {
            if (src.hasChildNode(name)) {
                continue;
            }
            NodeBuilder destChild = dest.getChildNode(name);
            int action = getOPV(dest, destChild, name);
            if (action == COPY || action == VERSION || action == ABORT) {
                dest.getChildNode(name).remove();
            } else if (action == IGNORE) {
                // no action
            } else if (action == INITIALIZE) {
                log.warn("OPV.INITIALIZE not implemented for restoring child nodes.");
            } else if (action == COMPUTE) {
                // there are currently no child node definitions
                // with OPV compute
                log.warn("OPV.COMPUTE not implemented for restoring child nodes: ");
            }
        }
    }

    /**
     * 15.7.7 Simple vs. Full Versioning after Restore
     */
    private void restoreVersionable(@Nonnull NodeBuilder versionable,
                                    @Nonnull NodeBuilder version) {
        checkNotNull(versionable).setProperty(JCR_ISCHECKEDOUT,
                false, Type.BOOLEAN);
        versionable.setProperty(JCR_VERSIONHISTORY,
                uuidFromNode(history), Type.REFERENCE);
        versionable.setProperty(JCR_BASEVERSION,
                uuidFromNode(version), Type.REFERENCE);
        versionable.setProperty(JCR_PREDECESSORS,
                Collections.<String>emptyList(), Type.REFERENCES);
    }

    private void resetToDefaultValue(NodeBuilder dest, PropertyState p)
            throws RepositoryException {
        Tree tree = TreeFactory.createReadOnlyTree(dest.getNodeState());
        PropertyDefinition def = ntMgr.getDefinition(tree, p, true);
        Value[] values = def.getDefaultValues();
        if (values != null) {
            if (p.isArray()) {
                p = PropertyStates.createProperty(p.getName(), values);
                dest.setProperty(p);
            } else if (values.length > 0) {
                p = PropertyStates.createProperty(p.getName(), values[0]);
                dest.setProperty(p);
            }
        }
    }

    private void createFrozen(NodeBuilder src, String srcId, NodeBuilder dest, int opva)
            throws CommitFailedException, RepositoryException {
        initFrozen(dest, src, srcId);
        OPVProvider opvProvider;
        if (opva == OnParentVersionAction.COPY) {
            opvProvider = OPVForceCopy.INSTANCE;
        } else {
            opvProvider = new OPVVersion();
        }
        copyProperties(src, dest, opvProvider, true);

        // add the frozen children and histories
        for (String name : src.getChildNodeNames()) {
            if (NodeStateUtils.isHidden(name)) {
                continue;
            }
            NodeBuilder child = src.getChildNode(name);
            String childId = getChildId(srcId, child, name);
            int opv = getOPV(src, child, name);

            if (opv == OnParentVersionAction.ABORT) {
                throw new CommitFailedException(CommitFailedException.VERSION,
                        VersionExceptionCode.OPV_ABORT_ITEM_PRESENT.ordinal(),
                        "Checkin aborted due to OPV abort in " + name);
            }
            if (opv == OnParentVersionAction.VERSION) {
                if (ntMgr.isNodeType(TreeFactory.createReadOnlyTree(child.getNodeState()), MIX_VERSIONABLE)) {
                    // create frozen versionable child
                    versionedChild(child, dest.child(name));
                } else {
                    // else copy
                    createFrozen(child, childId, dest.child(name), COPY);
                }
            } else if (opv == COPY) {
                createFrozen(child, childId, dest.child(name), COPY);
            }
        }
    }

    private void versionedChild(NodeBuilder src, NodeBuilder dest) {
        String ref = src.getProperty(JCR_VERSIONHISTORY).getValue(Type.REFERENCE);
        dest.setProperty(JCR_PRIMARYTYPE, NT_VERSIONEDCHILD, Type.NAME);
        dest.setProperty(JCR_CHILDVERSIONHISTORY, ref, Type.REFERENCE);
    }

    /**
     * Returns the id of the {@code child} node. The id is the value of the
     * jcr:uuid property of the child node if present, or the concatenation of
     * the {@code parentId} and the {@code name} of the child node.
     *
     * @param parentId the parentId.
     * @param child the child node.
     * @param name the name of the child node.
     * @return the identifier of the child node.
     */
    private String getChildId(String parentId, NodeBuilder child, String name) {
        if (child.hasProperty(JCR_UUID)) {
            return uuidFromNode(child);
        } else {
            return parentId + "/" + name;
        }
    }

    private void copyProperties(NodeBuilder src,
                                NodeBuilder dest,
                                OPVProvider opvProvider,
                                boolean ignoreTypeAndUUID)
            throws RepositoryException, CommitFailedException {
        // add the properties
        for (PropertyState prop : src.getProperties()) {
            int opv = opvProvider.getAction(src, dest, prop);

            String propName = prop.getName();
            if (opv == OnParentVersionAction.ABORT) {
                throw new CommitFailedException(CommitFailedException.VERSION,
                        VersionExceptionCode.OPV_ABORT_ITEM_PRESENT.ordinal(),
                        "Checkin aborted due to OPV abort in " + propName);
            }
            if (ignoreTypeAndUUID && BASIC_PROPERTIES.contains(propName)) {
                continue;
            }
            if (isHiddenProperty(propName)) {
                continue;
            }
            if (opv == OnParentVersionAction.VERSION
                    || opv == COPY) {
                dest.setProperty(prop);
            }
        }
    }

    private static boolean isHiddenProperty(@Nonnull String propName) {
        return NodeStateUtils.isHidden(propName) && !TreeConstants.OAK_CHILD_ORDER.equals(propName);
    }

    private int getOPV(NodeBuilder parent, NodeBuilder child, String childName)
            throws RepositoryException {
        // ignore hidden tree
        if (childName.startsWith(":")) {
            return IGNORE;
        }
        ImmutableTree parentTree = new ImmutableTree(parent.getNodeState());
        NodeState childState;
        if (NT_FROZENNODE.equals(child.getName(JCR_PRIMARYTYPE))) {
            // need to translate into a regular node to get proper OPV value
            NodeBuilder builder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
            builder.setProperty(JCR_PRIMARYTYPE, child.getName(JCR_FROZENPRIMARYTYPE), Type.NAME);
            builder.setProperty(JCR_MIXINTYPES, child.getNames(JCR_MIXINTYPES), Type.NAMES);
            childState = builder.getNodeState();
        } else {
            childState = child.getNodeState();
        }
        ImmutableTree childTree = new ImmutableTree(parentTree, childName, childState);
        return ntMgr.getDefinition(parentTree, childTree).getOnParentVersion();
    }

    private int getOPV(NodeBuilder node, PropertyState property)
            throws RepositoryException {
        if (property.getName().charAt(0) == ':') {
            // FIXME: handle child order properly
            return OnParentVersionAction.COPY;
        } else {
            return ntMgr.getDefinition(TreeFactory.createReadOnlyTree(node.getNodeState()),
                    property, false).getOnParentVersion();
        }
    }

    private interface OPVProvider {

        int getAction(NodeBuilder src,
                NodeBuilder dest,
                PropertyState prop)
                throws RepositoryException;
    }

    private static final class OPVForceCopy implements OPVProvider {

        private static final OPVProvider INSTANCE = new OPVForceCopy();

        @Override
        public int getAction(NodeBuilder src,
                             NodeBuilder dest,
                             PropertyState prop) {
            return COPY;
        }
    }

    private final class OPVVersion implements OPVProvider {

        @Override
        public int getAction(NodeBuilder src,
                             NodeBuilder dest,
                             PropertyState prop)
                throws RepositoryException {
            String propName = prop.getName();
            if (BASIC_FROZEN_PROPERTIES.contains(propName)) {
                // OAK-940: do not overwrite basic frozen properties
                return IGNORE;
            } else if (isHiddenProperty(propName)) {
                // don't copy hidden properties except for :childOrder
                return IGNORE;
            }
            return getOPV(src, prop);
        }
    }
}
