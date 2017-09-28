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
import static org.apache.jackrabbit.JcrConstants.JCR_BASEVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_ISCHECKEDOUT;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.RESTORE_PREFIX;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.version.OnParentVersionAction;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.lock.LockConstants;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;

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

    public VersionEditor(@Nonnull NodeBuilder versionStore,
                         @Nonnull NodeBuilder workspaceRoot,
                         @Nonnull CommitInfo commitInfo) {
        this(null, new ReadWriteVersionManager(checkNotNull(versionStore),
                checkNotNull(workspaceRoot)), workspaceRoot, "", commitInfo);
    }

    VersionEditor(@Nullable VersionEditor parent,
                  @Nonnull ReadWriteVersionManager vMgr,
                  @Nonnull NodeBuilder node,
                  @Nonnull String name,
                  @Nonnull CommitInfo commitInfo) {
        this.parent = parent;
        this.vMgr = checkNotNull(vMgr);
        this.node = checkNotNull(node);
        this.name = checkNotNull(name);
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
            // sentinel node for restore
            vMgr.restore(node, after.getValue(Type.REFERENCE), null);
            return;
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
        if (propName.equals(JCR_ISCHECKEDOUT)) {
            if (wasCheckedIn()) {
                vMgr.checkout(node);
            } else {
                vMgr.checkin(node);
            }
        } else if (propName.equals(JCR_BASEVERSION)) {
            String baseVersion = after.getValue(Type.REFERENCE);
            if (baseVersion.startsWith(RESTORE_PREFIX)) {
                baseVersion = baseVersion.substring(RESTORE_PREFIX.length());
                node.setProperty(JCR_BASEVERSION, baseVersion, Type.REFERENCE);
            }
            vMgr.restore(node, baseVersion, null);
        } else if (isVersionProperty(after)) {
            throwProtected(after.getName());
        } else if (isReadOnly && getOPV(after) != OnParentVersionAction.IGNORE) {
            throwCheckedIn("Cannot change property " + after.getName()
                    + " on checked in node");
        }
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
        return LockConstants.LOCK_PROPERTY_NAMES.contains(state.getName());
    }

    /**
     * @return {@code true} if this node <b>was</b> checked in. That is,
     *         this method checks the before state for the jcr:isCheckedOut
     *         property.
     */
    private boolean wasCheckedIn() {
        PropertyState prop = before.getProperty(JCR_ISCHECKEDOUT);
        if (prop != null) {
            return !prop.getValue(Type.BOOLEAN);
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
