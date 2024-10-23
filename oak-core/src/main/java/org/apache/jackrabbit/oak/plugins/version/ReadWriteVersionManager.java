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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.plugins.tree.factories.RootFactory;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.JcrConstants.JCR_BASEVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_CREATED;
import static org.apache.jackrabbit.JcrConstants.JCR_ISCHECKEDOUT;
import static org.apache.jackrabbit.JcrConstants.JCR_PREDECESSORS;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_ROOTVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_SUCCESSORS;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONABLEUUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONHISTORY;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONLABELS;
import static org.apache.jackrabbit.JcrConstants.NT_VERSION;
import static org.apache.jackrabbit.JcrConstants.NT_VERSIONHISTORY;
import static org.apache.jackrabbit.JcrConstants.NT_VERSIONLABELS;
import static org.apache.jackrabbit.oak.plugins.version.Utils.uuidFromNode;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.JCR_COPIED_FROM;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.REP_VERSIONSTORAGE;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.VERSION_STORE_PATH;

/**
 * Extends the {@link ReadOnlyVersionManager} with methods to modify the
 * version store.
 */
public class ReadWriteVersionManager extends ReadOnlyVersionManager {

    private static final Logger LOG = LoggerFactory.getLogger(ReadWriteVersionManager.class);

    private final NodeBuilder versionStorageNode;
    private final NodeBuilder workspaceRoot;
    private final TypePredicate isVersion;
    private ReadOnlyNodeTypeManager ntMgr;

    public ReadWriteVersionManager(NodeBuilder versionStorageNode,
                            NodeBuilder workspaceRoot) {
        this.versionStorageNode = requireNonNull(versionStorageNode);
        this.workspaceRoot = requireNonNull(workspaceRoot);
        this.isVersion = new TypePredicate(workspaceRoot.getNodeState(), NT_VERSION);
    }

    @NotNull
    @Override
    protected Tree getVersionStorage() {
        return TreeFactory.createReadOnlyTree(versionStorageNode.getNodeState());
    }

    @NotNull
    @Override
    protected Root getWorkspaceRoot() {
        return RootFactory.createReadOnlyRoot(workspaceRoot.getNodeState());
    }

    @NotNull
    @Override
    protected ReadOnlyNodeTypeManager getNodeTypeManager() {
        if (ntMgr == null) {
            ntMgr = ReadOnlyNodeTypeManager.getInstance(
                    getWorkspaceRoot(), NamePathMapper.DEFAULT);
        }
        return ntMgr;
    }

    /**
     * Gets or creates the version history for the given
     * {@code versionable} node.
     *
     * @param versionable the versionable node.
     * @param infoMap The additional information as provided by {@link org.apache.jackrabbit.oak.spi.commit.CommitInfo#getInfo()}
     * @return the version history node.
     * @throws IllegalArgumentException if the given node does not have a
     *                                  {@code jcr:uuid} property.
     */
    @NotNull
    public NodeBuilder getOrCreateVersionHistory(@NotNull NodeBuilder versionable, @NotNull Map<String, Object> infoMap) {
        requireNonNull(versionable);
        String vUUID = uuidFromNode(versionable);
        String relPath = getVersionHistoryPath(vUUID);
        NodeBuilder node = versionStorageNode;
        for (Iterator<String> it = PathUtils.elements(relPath).iterator(); it.hasNext(); ) {
            String name = it.next();
            node = node.child(name);
            if (!node.hasProperty(JCR_PRIMARYTYPE)) {
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
        Object copiedFrom = infoMap.get(JCR_COPIED_FROM);
        if (copiedFrom != null) {
            node.setProperty(JCR_COPIED_FROM, copiedFrom.toString(), Type.WEAKREFERENCE);
        }

        // use jcr:rootVersion node to detect if we need to initialize the
        // version history
        if (!node.hasChildNode(JCR_ROOTVERSION)) {
            // jcr:versionableUuid property
            node.setProperty(JCR_VERSIONABLEUUID, vUUID, Type.STRING);
            node.setProperty(JCR_UUID,
                    UUIDUtils.generateUUID(), Type.STRING);

            // jcr:versionLabels child node
            NodeBuilder vLabels = node.child(JCR_VERSIONLABELS);
            vLabels.setProperty(JCR_PRIMARYTYPE, NT_VERSIONLABELS, Type.NAME);

            // jcr:rootVersion child node
            createRootVersion(node, versionable);
        } else if (!versionable.hasProperty(JCR_VERSIONHISTORY)) {
            // connect versionable node with existing history
            connectHistory(node.getChildNode(JCR_ROOTVERSION),
                    uuidFromNode(node), versionable);
        }
        return node;
    }

    void removeVersion(String versionRelPath) throws CommitFailedException {
        String historyRelPath = PathUtils.getAncestorPath(versionRelPath, 1);
        String versionName = Text.getName(versionRelPath);

        NodeBuilder vh = resolve(versionStorageNode, historyRelPath);

        if (JCR_ROOTVERSION.equals(versionName)) {
            String msg = "Removal of root version not allowed.";
            throw new CommitFailedException(CommitFailedException.VERSION, VersionExceptionCode.ROOT_VERSION_REMOVAL.ordinal(), msg);
        }

        NodeBuilder versionNode = vh.getChildNode(versionName);
        String versionId = versionNode.getProperty(JCR_UUID).getValue(Type.STRING);
        // unregister from labels
        for (String label : getVersionLabels(historyRelPath, versionId)) {
            removeVersionLabel(historyRelPath, label);
        }
        // reconnected predecessors and successors of the version being removed
        PropertyState successorIds = versionNode.getProperty(JCR_SUCCESSORS);
        PropertyState predecessorIds = versionNode.getProperty(JCR_PREDECESSORS);

        for (String succId :  successorIds.getValue(Type.REFERENCES)) {
            NodeBuilder successor = getVersionById(vh, succId);
            if (successor == null) {
                LOG.info("removeVersion : successor not found with uuid: {}, historyRelPath: {}, versionNode: {}, versionHistory: {}",
                        succId, historyRelPath, asLoggableString(versionNode), asLoggableString(vh));
                continue;
            }
            PropertyBuilder<String> pb = PropertyBuilder.array(Type.REFERENCE);
            pb.setName(JCR_PREDECESSORS);
            PropertyState successorsPredecessors = successor.getProperty(JCR_PREDECESSORS);
            if (successorsPredecessors == null) {
                LOG.info("removeVersion : successor has no jcr:predecessors property, uuid: {}, historyRelPath: {}, versionNode: {}, successor: {}, versionHistory: {}",
                        succId, historyRelPath, asLoggableString(versionNode), asLoggableString(successor), asLoggableString(vh));
            } else {
                pb.setValues(successorsPredecessors.getValue(Type.REFERENCES));
            }

            pb.removeValue(versionId);
            pb.addValues(predecessorIds.getValue(Type.REFERENCES));

            successor.setProperty(pb.getPropertyState());
        }

        for (String predId :  predecessorIds.getValue(Type.REFERENCES)) {
            NodeBuilder predecessor = getVersionById(vh, predId);
            if (predecessor == null) {
                LOG.info("removeVersion : predecessor not found with uuid: {}, historyRelPath: {}, versionNode: {}, versionHistory: {}",
                        predId, historyRelPath, asLoggableString(versionNode), asLoggableString(vh));
                continue;
            }
            PropertyBuilder<String> pb = PropertyBuilder.array(Type.REFERENCE);
            pb.setName(JCR_SUCCESSORS);
            PropertyState predecessorsSuccessors = predecessor.getProperty(JCR_SUCCESSORS);
            if (predecessorsSuccessors == null) {
                LOG.info("removeVersion : predecessor has no jcr:successors property, uuid: {}, historyRelPath: {}, versionNode: {}, predecessor: {}, versionHistory: {}",
                        predId, historyRelPath, asLoggableString(versionNode), asLoggableString(predecessor), asLoggableString(vh));
            } else {
                pb.setValues(predecessorsSuccessors.getValue(Type.REFERENCES));
            }

            pb.removeValue(versionId);
            pb.addValues(successorIds.getValue(Type.REFERENCES));

            predecessor.setProperty(pb.getPropertyState());
        }
        versionNode.remove();
    }

    /** small helper to log a node - useful for later debugging **/
    private static String asLoggableString(NodeBuilder nb) {
        try {
            final StringBuilder sb = new StringBuilder();
            boolean empty = true;
            for (PropertyState p : nb.getProperties()) {
                if (empty) {
                    empty = false;
                } else {
                    sb.append(", ");
                }
                sb.append(p);
            }
            return "{ " + sb + " }";
        } catch (Exception e) {
            return "{ exception: " + e + ", message: " + e.getMessage() + " }";
        }
    }

    public void checkout(NodeBuilder versionable) {
        versionable.setProperty(JCR_ISCHECKEDOUT, true, Type.BOOLEAN);
        PropertyState baseVersion = versionable.getProperty(JCR_BASEVERSION);
        List<String> predecessors = Collections.singletonList(baseVersion.getValue(Type.REFERENCE));
        versionable.setProperty(JCR_PREDECESSORS, predecessors, Type.REFERENCES);
    }

    public void checkin(@NotNull NodeBuilder versionable)
            throws CommitFailedException {
        NodeBuilder history = getOrCreateVersionHistory(versionable,
                Collections.<String, Object>emptyMap());
        createVersion(history, versionable);
    }

    public void restore(@NotNull NodeBuilder versionable,
                        @NotNull String versionUUID,
                        @Nullable VersionSelector selector)
            throws CommitFailedException {
        String versionPath = getIdentifierManager().getPath(versionUUID);
        NodeBuilder history = getOrCreateVersionHistory(versionable,
                Collections.<String, Object>emptyMap());
        NodeBuilder version = null;
        if (versionPath != null) {
            String versionName = PathUtils.getName(versionPath);
            if (history.hasChildNode(versionName)) {
                version = history.getChildNode(versionName);
            }
        }
        if (version == null) {
            throw new CommitFailedException(CommitFailedException.VERSION,
                    VersionExceptionCode.NO_SUCH_VERSION.ordinal(),
                    "The VersionHistory with UUID: " + uuidFromNode(versionable) +
                            " does not have a Version with UUID: " + versionUUID);
        }
        VersionableState versionableState = VersionableState.forRestore(
                version, history, versionable, this, ntMgr);
        versionableState.restore(selector);
    }

    /**
     * Restores a version from the history identified by {@code historyIdentifier}
     * using the given version {@code selector}.
     *
     * @param historyIdentifier identifier of the version history node.
     * @param selector the version selector.
     * @param versionable the versionable node where the version is restored to.
     * @throws CommitFailedException if an error occurs while restoring.
     */
    void restore(@NotNull String historyIdentifier,
                 @NotNull VersionSelector selector,
                 @NotNull NodeBuilder versionable)
            throws CommitFailedException, RepositoryException {
        String historyPath = getIdentifierManager().getPath(historyIdentifier);
        String historyRelPath = PathUtils.relativize(VERSION_STORE_PATH, historyPath);
        NodeBuilder history = resolve(versionStorageNode, historyRelPath);
        Validate.checkState(history.exists(), "Version history does not exist: " + historyPath);
        NodeBuilder version = selector.select(history);
        if (version == null) {
            throw new CommitFailedException(CommitFailedException.VERSION,
                    VersionExceptionCode.NO_VERSION_TO_RESTORE.ordinal(),
                    "VersionSelector did not select any version from " +
                            "history: " + historyPath);
        }
        // make sure versionable nodes has a jcr:uuid
        // (required to identify its version history)
        String versionableUUUID = history.getProperty(
                JCR_VERSIONABLEUUID).getValue(Type.STRING);
        versionable.setProperty(JCR_UUID, versionableUUUID, Type.STRING);
        restore(versionable, uuidFromNode(version), selector);
    }

    /**
     * Removes a version label from the jcr:versionLabels node of the referenced
     * version history.
     *
     * @param historyRelPath relative path from the jcr:versionStorage node to
     *                       the version history node.
     * @param label          the version label.
     * @throws CommitFailedException if there is no such version history or if
     * there is no label with the given name.
     */
    public void removeVersionLabel(@NotNull String historyRelPath,
                                   @NotNull String label)
            throws CommitFailedException {
        NodeBuilder labels = getVersionLabelsFor(requireNonNull(historyRelPath));
        if (!labels.hasProperty(requireNonNull(label))) {
            throw new CommitFailedException(CommitFailedException.VERSION,
                    VersionExceptionCode.NO_SUCH_VERSION_LABEL.ordinal(),
                    "Version label " + label + " does not exist on this version history");
        }
        labels.removeProperty(label);
    }

    /**
     * Removes the version history if it's empty.
     *
     * @param versionable the versionable node.
     */
    void removeEmptyHistory(@NotNull NodeState versionable) {
        NodeBuilder history = getVersionHistory(versionable);
        if (isEmptyHistory(history.getNodeState())) {
            history.remove();
        }
    }

    // TODO: more methods that modify versions

    //------------------------------< internal >--------------------------------

    /**
     * Resolves the {@code relPath} based on the given {@code node}
     * and returns the resulting node, possibly non-existing.
     *
     * @param node the resolved node.
     * @param relPath a relative path.
     * @return the resolved node.
     */
    @NotNull
    private NodeBuilder resolve(NodeBuilder node, String relPath) {
        checkArgument(!PathUtils.isAbsolute(relPath), "Not a relative path");
        for (String name : PathUtils.elements(relPath)) {
            node = node.getChildNode(name);
        }
        return node;
    }

    /**
     * Creates the root version in the given version history.
     *
     * @param vHistory the version history node.
     * @param versionable the versionable node.
     */
    private void createRootVersion(@NotNull NodeBuilder vHistory,
                                   @NotNull NodeBuilder versionable) {
        String versionUUID = UUIDUtils.generateUUID();
        NodeBuilder version = vHistory.child(JCR_ROOTVERSION);
        version.setProperty(JCR_UUID, versionUUID, Type.STRING);
        version.setProperty(JCR_PRIMARYTYPE, NT_VERSION, Type.NAME);
        version.setProperty(JCR_CREATED, ISO8601.format(Calendar.getInstance()), Type.DATE);
        version.setProperty(JCR_PREDECESSORS, Collections.emptyList(), Type.REFERENCES);
        version.setProperty(JCR_SUCCESSORS, Collections.emptyList(), Type.REFERENCES);

        // incomplete frozen node on root-version (don't call create on versionable-state)
        VersionableState.fromVersion(version, vHistory, versionable, this, getNodeTypeManager());

        // set jcr:isCheckedOut, jcr:versionHistory, jcr:baseVersion and
        // jcr:predecessors on versionable node
        versionable.setProperty(JCR_ISCHECKEDOUT, true, Type.BOOLEAN);
        versionable.setProperty(JCR_VERSIONHISTORY, uuidFromNode(vHistory), Type.REFERENCE);
        versionable.setProperty(JCR_BASEVERSION, versionUUID, Type.REFERENCE);
        // set predecessors to base version for the root version
        versionable.setProperty(JCR_PREDECESSORS, Collections.singletonList(versionUUID), Type.REFERENCES);
    }

    /**
     * Creates a version in the given version history. If the given version
     * history does not yet have a version, then a root version is created and
     * the versionable node is in a checked out state. Otherwise a version is
     * created and the versionable node is set to checked in.
     *
     * @param vHistory the version history node.
     * @param versionable the versionable node.
     * @throws CommitFailedException if creating the version fails. E.g. because
     * the versionable node contains a OPV item with ABORT.
     */
    private void createVersion(@NotNull NodeBuilder vHistory,
                                      @NotNull NodeBuilder versionable)
            throws IllegalArgumentException, CommitFailedException {
        if (!vHistory.hasChildNode(JCR_ROOTVERSION)) {
            createRootVersion(vHistory, versionable);
            return;
        }

        Validate.checkState(versionable.hasProperty(JCR_PREDECESSORS));
        PropertyState state = versionable.getProperty(JCR_PREDECESSORS);
        List<String> predecessors = ImmutableList.copyOf(state.getValue(Type.REFERENCES));
        NodeBuilder version = vHistory.child(calculateVersion(vHistory, versionable));

        String versionUUID = UUIDUtils.generateUUID();
        version.setProperty(JCR_UUID, versionUUID, Type.STRING);
        version.setProperty(JCR_PRIMARYTYPE, NT_VERSION, Type.NAME);
        version.setProperty(JCR_CREATED, ISO8601.format(Calendar.getInstance()), Type.DATE);
        version.setProperty(JCR_PREDECESSORS, predecessors, Type.REFERENCES);
        version.setProperty(JCR_SUCCESSORS, Collections.<String>emptyList(), Type.REFERENCES);

        // update successors of versions identified by predecessors
        for (String id : predecessors) {
            String name = PathUtils.getName(getIdentifierManager().getPath(id));
            NodeBuilder predecessor = vHistory.getChildNode(name);
            state = predecessor.getProperty(JCR_SUCCESSORS);
            if (state == null) {
                throw new IllegalStateException("Missing " + JCR_SUCCESSORS +
                        " property on " + predecessor);
            }
            Set<String> refs = CollectionUtils.toSet(state.getValue(Type.REFERENCES));
            refs.add(versionUUID);
            predecessor.setProperty(JCR_SUCCESSORS, refs, Type.REFERENCES);
        }

        // jcr:frozenNode of created version
        VersionableState versionableState = VersionableState.fromVersion(version, vHistory, versionable, this, getNodeTypeManager());
        versionableState.create();

        // set jcr:isCheckedOut, jcr:versionHistory, jcr:baseVersion and
        // jcr:predecessors on versionable node
        versionable.setProperty(JCR_ISCHECKEDOUT, false, Type.BOOLEAN);
        versionable.setProperty(JCR_VERSIONHISTORY, uuidFromNode(vHistory), Type.REFERENCE);
        versionable.setProperty(JCR_BASEVERSION, versionUUID, Type.REFERENCE);
        // clear predecessors for check-in
        versionable.setProperty(JCR_PREDECESSORS, Collections.emptyList(), Type.REFERENCES);
    }

    /**
     * Connects a versionable node with the root version of an existing version
     * history.
     *
     * @param rootVersion the root version of a version history.
     * @param vHistoryUUID the uuid of the version history node.
     * @param versionable the versionable node.
     */
    private void connectHistory(@NotNull NodeBuilder rootVersion,
                                @NotNull String vHistoryUUID,
                                @NotNull NodeBuilder versionable) {
        String rootVersionUUID = uuidFromNode(rootVersion);
        versionable.setProperty(JCR_ISCHECKEDOUT, true, Type.BOOLEAN);
        versionable.setProperty(JCR_VERSIONHISTORY, vHistoryUUID, Type.REFERENCE);
        versionable.setProperty(JCR_BASEVERSION, rootVersionUUID, Type.REFERENCE);
        versionable.setProperty(JCR_PREDECESSORS,
                Collections.singleton(rootVersionUUID), Type.REFERENCES);
    }

    /**
     * <i>Copied from Apache Jackrabbit Core</i>
     * <p>
     * Calculates the name of the new version that will be created by a
     * checkin call. The name is determined as follows:
     * <ul>
     * <li> first the predecessor version with the shortest name is searched.
     * <li> if that predecessor version is the root version, the new version gets
     *      the name "{number of successors}+1" + ".0"
     * <li> if that predecessor version has no successor, the last digit of it's
     *      version number is incremented.
     * <li> if that predecessor version has successors but the incremented name
     *      does not exist, that name is used.
     * <li> otherwise a ".0" is added to the name until a non conflicting name
     *      is found.
     * </ul>
     * <p>
     * Example Graph:
     * <pre>
     * jcr:rootVersion
     *  |     |
     * 1.0   2.0
     *  |
     * 1.1
     *  |
     * 1.2 ---\  ------\
     *  |      \        \
     * 1.3   1.2.0   1.2.0.0
     *  |      |
     * 1.4   1.2.1 ----\
     *  |      |        \
     * 1.5   1.2.2   1.2.1.0
     *  |      |        |
     * 1.6     |     1.2.1.1
     *  |-----/
     * 1.7
     * </pre>
     *
     * @param history the version history
     * @param versionable the node to checkin
     * @return the new version name
     * @throws IllegalStateException if mandatory version properties are missing.
     */
    protected String calculateVersion(@NotNull NodeBuilder history,
                                      @NotNull NodeBuilder versionable)
            throws IllegalStateException {

        // 1. search a predecessor, suitable for generating the new name
        PropertyState predecessors = versionable.getProperty(JCR_PREDECESSORS);

        if (predecessors == null || predecessors.count() == 0) {
            String message;
            if (predecessors == null) {
                message = "Mandatory jcr:predecessors property missing on node " + uuidFromNode(versionable);
            } else {
                message = "Mandatory jcr:predecessors property is empty on node " + uuidFromNode(versionable);
            }
            throw new IllegalStateException(message);
        }

        String best = null;
        for (String id : predecessors.getValue(Type.REFERENCES)) {
            String name = PathUtils.getName(getIdentifierManager().getPath(id));
            if (best == null || name.length() < best.length()) {
                best = name;
            }
        }

        if (best == null) {
            String message = "Could not find 'best' predecessor node for " +
                    uuidFromNode(versionable);
            throw new IllegalStateException(message);
        }

        // 2. generate version name (assume no namespaces in version names)
        String versionName = best;
        int pos = versionName.lastIndexOf('.');
        if (pos > 0) {
            String newVersionName = versionName.substring(0, pos + 1)
                    + (Integer.parseInt(versionName.substring(pos + 1)) + 1);
            while (history.hasChildNode(newVersionName)) {
                versionName += ".0";
                newVersionName = versionName;
            }
            return newVersionName;
        } else {
            // best is root version
            Validate.checkState(history.hasChildNode(JCR_ROOTVERSION));
            NodeBuilder v = history.getChildNode(JCR_ROOTVERSION);
            return String.valueOf(v.getProperty(JCR_SUCCESSORS).count() + 1) + ".0";
        }
    }

    /**
     * Returns the jcr:versionLabels node of the version history referenced
     * by the given path.
     *
     * @param historyRelPath relative path from the jcr:versionStorage node
     *                       to the history node.
     * @return the jcr:versionLabels node.
     * @throws CommitFailedException if there is no version history at the
     * given path.
     */
    private NodeBuilder getVersionLabelsFor(String historyRelPath)
            throws CommitFailedException {
        NodeBuilder history = resolve(versionStorageNode, historyRelPath);
        if (!history.exists()) {
            throw new CommitFailedException(CommitFailedException.VERSION,
                    VersionExceptionCode.UNEXPECTED_REPOSITORY_EXCEPTION.ordinal(),
                    "Version history does not exist: " + PathUtils.concat(
                            VERSION_STORE_PATH, historyRelPath));
        }
        return history.child(JCR_VERSIONLABELS);
    }

    @NotNull
    private Iterable<String> getVersionLabels(@NotNull String historyRelPath, @NotNull String versionId) throws CommitFailedException {
        List<String> labels = new ArrayList<String>();
        NodeBuilder labelNode = getVersionLabelsFor(historyRelPath);
        for (PropertyState ps : labelNode.getProperties()) {
            if (Type.REFERENCE == ps.getType()) {
                if (versionId.equals(ps.getValue(Type.REFERENCE))) {
                    labels.add(ps.getName());
                }
            }
        }
        return labels;
    }

    @Nullable
    private NodeBuilder getVersionById(@NotNull NodeBuilder vhBuilder, @NotNull String versionId) {
        for (String childName : vhBuilder.getChildNodeNames()) {
            NodeBuilder nb = vhBuilder.getChildNode(childName);
            PropertyState uuid = nb.getProperty(JCR_UUID);
            if (uuid != null && versionId.equals(uuid.getValue(Type.STRING))) {
                return nb;
            }
        }
        return null;
    }

    /**
     * Gets the version history for the given
     * {@code versionable} node.
     *
     * @param versionable the versionable node.
     * @return the version history node.
     * @throws IllegalArgumentException if the given node does not have a
     *                                  {@code jcr:uuid} property.
     */
    @NotNull
    private NodeBuilder getVersionHistory(@NotNull NodeState versionable) {
        requireNonNull(versionable);
        String vUUID = uuidFromNode(versionable);
        String relPath = getVersionHistoryPath(vUUID);
        NodeBuilder node = versionStorageNode;
        for (Iterator<String> it = PathUtils.elements(relPath).iterator(); it.hasNext(); ) {
            String name = it.next();
            node = node.getChildNode(name);
            if (!node.exists()) {
                throw new IllegalArgumentException("No version history for this node");
            }
        }
        return node;
    }

    /**
     * Checks whether the passed node history hasn't been modified since its
     * creation. It means that: (1) there's just one version, called jcr:rootVersion
     * and (2) there are no custom labels.
     *
     * @param versionHistory to test
     * @return {@code true} if the version history hasn't been changed yet
     */
    private boolean isEmptyHistory(NodeState versionHistory) {
        for (ChildNodeEntry entry : versionHistory.getChildNodeEntries()) {
            String name = entry.getName();
            NodeState node = entry.getNodeState();
            if (!JCR_ROOTVERSION.equals(name) && isVersion.test(node)) {
                return false; // a checked-in version
            }
        }
        NodeState labels = versionHistory.getChildNode(JCR_VERSIONLABELS);
        for (PropertyState prop : labels.getProperties()) {
            if (prop.getType() == Type.REFERENCE) {
                return false; // custom label
            }
        }
        return true;
    }

}
