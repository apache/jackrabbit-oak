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

import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.version.OnParentVersionAction;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.plugins.tree.impl.TreeProviderService;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.lock.LockConstants;
import org.apache.jackrabbit.oak.spi.state.MoveDetector;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.JcrConstants.JCR_BASEVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_ISCHECKEDOUT;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.RESTORE_PREFIX;

/**
 * TODO document
 */
class VersionEditor implements Editor {

    private final VersionEditor parent;
    private final ReadWriteVersionManager vMgr;
    private final NodeBuilder node;
    private final String name;
    private Boolean isVersionable = null;
    private NodeState before;
    private NodeState after;
    private boolean isReadOnly;
    private CommitInfo commitInfo;

    public VersionEditor(@NotNull NodeBuilder versionStore,
                         @NotNull NodeBuilder workspaceRoot,
                         @NotNull CommitInfo commitInfo) {
        this(null, new ReadWriteVersionManager(requireNonNull(versionStore),
                requireNonNull(workspaceRoot)), workspaceRoot, "", commitInfo);
    }

    VersionEditor(@Nullable VersionEditor parent,
                  @NotNull ReadWriteVersionManager vMgr,
                  @NotNull NodeBuilder node,
                  @NotNull String name,
                  @NotNull CommitInfo commitInfo) {
        this.parent = parent;
        this.vMgr = requireNonNull(vMgr);
        this.node = requireNonNull(node);
        this.name = requireNonNull(name);
        this.commitInfo = commitInfo;
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        this.before = before;
        this.after = after;
        if (isVersionable()) {
            vMgr.getOrCreateVersionHistory(node, commitInfo.getInfo());
        }
        // calculate isReadOnly state
        if (after.exists() || isVersionable()) {
            // deleted or versionable -> check if it was checked in
            // a node cannot be modified if it was checked in
            // unless it has a new identifier
            isReadOnly = wasCheckedIn() && !hasNewIdentifier() && !isIgnoreOnOPV();
        } else {
            // otherwise inherit from parent
            isReadOnly = parent != null && parent.isReadOnly && !isIgnoreOnOPV();
        }
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
    }

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        if (after.getName().equals(JCR_BASEVERSION)
                && this.after.hasProperty(JcrConstants.JCR_VERSIONHISTORY)
                && !this.after.hasProperty(JCR_ISCHECKEDOUT)
                && !this.before.exists()) {
            Tree tree = new TreeProviderService().createReadOnlyTree(this.node.getNodeState());
            if (vMgr.getNodeTypeManager().isNodeType(
                    TreeUtil.getPrimaryTypeName(tree), () -> TreeUtil.getMixinTypeNames(tree), MIX_VERSIONABLE)) {
                // OAK-10462: the node has mix:versionable, but not the mandatory property jcr:isCheckedOut,
                // so it has to be sentinel node for a restore operation.
                // Unfortunately, there is no API available to detect that.
                vMgr.restore(node, after.getValue(Type.REFERENCE), null);
                return;
            }
        }
        if (!isReadOnly || getOPV(after) == OnParentVersionAction.IGNORE) {
            return;
        }
        // JCR allows to put a lock on a checked in node.
        if (after.getName().equals(JcrConstants.JCR_LOCKOWNER)
                || after.getName().equals(JcrConstants.JCR_LOCKISDEEP)) {
            return;
        }
        throwCheckedIn("Cannot add property " + after.getName()
                + " on checked in node");
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        if (!isVersionable()) {
            if (!isVersionProperty(after) && isReadOnly && getOPV(after) != OnParentVersionAction.IGNORE) {
                throwCheckedIn("Cannot change property " + after.getName()
                        + " on checked in node");
            }
            return;
        }
        String propName = after.getName();

        // Updates the checked-out / checked-in state of the currently processed node when
        // the JCR_ISCHECKEDOUT property change is processed.
        if (propName.equals(JCR_ISCHECKEDOUT)) {
            if (wasCheckedIn()) {
                vMgr.checkout(node);
            } else {
                vMgr.checkin(node);
            }
        } else if (propName.equals(JCR_BASEVERSION)) {

            // Completes the restore of a version from version history.
            //
            // When the JCR_BASEVERSION property is processed, a check is made for the current
            // base version property.
            // If a restore is currently in progress for the current base version (the check for
            // this is that the current base version name has the format "restore-[UUID of the
            // version to restore to]"), then the restore is completed for the current node
            // to the version specified by the UUID.
            //
            // If a node that was moved or copied to the location of a deleted node is currently
            // being processed (see OAK-8848 for context), the restore operation must NOT be
            // performed when the JCR_BASEVERSION property change is processed for the node.
            if (!nodeWasMoved()) {

                String baseVersion = after.getValue(Type.REFERENCE);
                if (baseVersion.startsWith(RESTORE_PREFIX)) {
                    baseVersion = baseVersion.substring(RESTORE_PREFIX.length());
                    node.setProperty(JCR_BASEVERSION, baseVersion, Type.REFERENCE);
                }

                vMgr.restore(node, baseVersion, null);
            }
        } else if (isVersionProperty(after)) {
            // Checks if a version property is being changed and throws a CommitFailedException
            // with the message "Constraint Violation Exception" if this is not allowed.
            // JCR_ISCHECKEDOUT and JCR_BASEVERSION properties should be ignored, since changes
            // to them are allowed for specific use cases (for example, completing the check-in
            // / check-out for a node or completing a node restore).
            //
            // The only situation when the update of a version property is allowed is when this
            // occurs as a result of the current node being moved over a previously deleted node
            // - see OAK-8848 for context.
            //
            // OAK-8848: moving a versionable node in the same location as a node deleted in the
            // same session should be allowed.
            // This check works because the only way that moving a node in a location is allowed
            // is if there is no existing (undeleted) node in that location.
            // Property comparison should not fail for two jcr:versionHistory properties in this case.
            if (!nodeWasMoved()) {
                throwProtected(after.getName());
            }
        } else if (isReadOnly && getOPV(after) != OnParentVersionAction.IGNORE) {
            throwCheckedIn("Cannot change property " + after.getName()
                    + " on checked in node");
        }
    }

    /**
     * Returns true if and only if the given node was moved or copied from another location.
     */
    private boolean nodeWasMoved() {
        return !this.before.hasProperty(MoveDetector.SOURCE_PATH) && this.after.hasProperty(MoveDetector.SOURCE_PATH);
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        if (isReadOnly) {
            if (!isVersionProperty(before) && !isLockProperty(before) && getOPV(before) != OnParentVersionAction.IGNORE) {
                throwCheckedIn("Cannot delete property on checked in node");
            }
        }
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) {
        return childNodeChanged(name, MISSING_NODE, after);
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before,
            NodeState after) {
        return new VersionEditor(this, vMgr, node.child(name), name, commitInfo);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) {
        return new VersionEditor(this, vMgr, MISSING_NODE.builder(), name, commitInfo);
    }

    /**
     * Returns {@code true} if the node of this VersionDiff is versionable;
     * {@code false} otherwise.
     *
     * @return whether the node is versionable.
     */
    private boolean isVersionable() {
        if (isVersionable == null) {
            isVersionable = vMgr.isVersionable(after);
        }
        return isVersionable;
    }

    private boolean isVersionProperty(PropertyState state) {
        return VersionConstants.VERSION_PROPERTY_NAMES
                .contains(state.getName());
    }

    private boolean isLockProperty(PropertyState state) {
        return Objects.nonNull(state) && LockConstants.LOCK_PROPERTY_NAMES.contains(state.getName());
    }

    /**
     * @return {@code true} if this node <b>was</b> checked in. That is,
     *         this method checks the before state for the jcr:isCheckedOut
     *         property.
     */
    private boolean wasCheckedIn() {
        PropertyState prop = before.getProperty(JCR_ISCHECKEDOUT);
        if (prop != null) {
            return !prop.getValue(Type.BOOLEAN) && isVersionable();
        }
        // new node or not versionable, check parent
        return parent != null && parent.wasCheckedIn();
    }

    private boolean hasNewIdentifier() {
        String beforeId = buildBeforeIdentifier(new StringBuilder()).toString();
        String afterId = buildAfterIdentifier(new StringBuilder()).toString();
        return !beforeId.equals(afterId);
    }

    private StringBuilder buildBeforeIdentifier(StringBuilder identifier) {
        String uuid = before.getString(JCR_UUID);
        if (uuid != null) {
            identifier.append(uuid);
        } else if (parent != null) {
            parent.buildBeforeIdentifier(identifier);
            identifier.append("/").append(name);
        }
        return identifier;
    }

    private StringBuilder buildAfterIdentifier(StringBuilder identifier) {
        String uuid = after.getString(JCR_UUID);
        if (uuid != null) {
            identifier.append(uuid);
        } else if (parent != null) {
            parent.buildAfterIdentifier(identifier);
            identifier.append("/").append(name);
        }
        return identifier;
    }

    private static void throwCheckedIn(String msg)
            throws CommitFailedException {
        throw new CommitFailedException(CommitFailedException.VERSION,
                VersionExceptionCode.NODE_CHECKED_IN.ordinal(), msg);
    }

    private static void throwProtected(String name)
            throws CommitFailedException {
        throw new CommitFailedException(CommitFailedException.CONSTRAINT, 100,
                "Property is protected: " + name);
    }

    private boolean isIgnoreOnOPV() throws CommitFailedException {
        if (this.parent != null) {
            try {
                NodeDefinition definition = this.vMgr.getNodeTypeManager().getDefinition(TreeFactory.createTree(parent.node), this.name);
                return definition.getOnParentVersion() == OnParentVersionAction.IGNORE;
            } catch (Exception e) {
                throw new CommitFailedException(CommitFailedException.VERSION,
                        VersionExceptionCode.UNEXPECTED_REPOSITORY_EXCEPTION.ordinal(), e.getMessage());
            }
        }
        return false;
    }

    private int getOPV(PropertyState property) throws CommitFailedException {
        try {
            return this.vMgr.getNodeTypeManager().getDefinition(TreeFactory.createReadOnlyTree(this.node.getNodeState()),
                    property, false).getOnParentVersion();
        } catch (Exception e) {
            throw new CommitFailedException(CommitFailedException.VERSION,
                    VersionExceptionCode.UNEXPECTED_REPOSITORY_EXCEPTION.ordinal(), e.getMessage());
        }
    }
}
