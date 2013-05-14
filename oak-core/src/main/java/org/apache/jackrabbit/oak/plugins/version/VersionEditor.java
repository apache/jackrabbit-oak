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
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * TODO document
 */
class VersionEditor implements Editor {

    private final VersionEditor parent;
    private final ReadWriteVersionManager vMgr;
    private final NodeBuilder node;
    private Boolean isVersionable = null;
    private NodeState before;
    private NodeState after;
    private boolean wasReadOnly;

    public VersionEditor(@Nonnull NodeBuilder versionStore,
            @Nonnull NodeBuilder workspaceRoot) {
        this(null, new ReadWriteVersionManager(checkNotNull(versionStore),
                checkNotNull(workspaceRoot)), workspaceRoot);
    }

    VersionEditor(@Nullable VersionEditor parent,
            @Nonnull ReadWriteVersionManager vMgr, @Nonnull NodeBuilder node) {
        this.parent = parent;
        this.vMgr = checkNotNull(vMgr);
        this.node = checkNotNull(node);
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        this.before = before;
        this.after = after;
        if (isVersionable()) {
            vMgr.getOrCreateVersionHistory(node);
        }
        // calculate wasReadOnly state
        if (after.exists() || isVersionable()) {
            // deleted or versionable -> check if it was checked in
            wasReadOnly = wasCheckedIn();
        } else {
            // otherwise inherit from parent
            wasReadOnly = parent != null && parent.wasReadOnly;
        }
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
    }

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        if (!wasReadOnly) {
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
            if (!isVersionProperty(after) && wasCheckedIn()) {
                throwCheckedIn("Cannot change property " + after.getName()
                        + " on checked in node");
            }
            return;
        }
        String propName = after.getName();
        if (propName.equals(VersionConstants.JCR_ISCHECKEDOUT)) {
            if (wasCheckedIn()) {
                vMgr.checkout(node);
            } else {
                vMgr.checkin(node);
            }
        } else if (propName.equals(VersionConstants.JCR_BASEVERSION)) {
            vMgr.restore(node);
        } else if (isVersionProperty(after)) {
            throwProtected(after.getName());
        } else if (wasReadOnly) {
            throwCheckedIn("Cannot change property " + after.getName()
                    + " on checked in node");
        }
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        if (wasReadOnly) {
            if (!isVersionProperty(before)) {
                throwProtected("Cannot delete property on checked in node");
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
        return new VersionEditor(this, vMgr, node.child(name));
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) {
        return new VersionEditor(this, vMgr, MISSING_NODE.builder());
    }

    /**
     * Returns <code>true</code> if the node of this VersionDiff is versionable;
     * <code>false</code> otherwise.
     * 
     * @return whether the node is versionable.
     */
    private boolean isVersionable() {
        if (isVersionable == null) {
            // this is not 100% correct, because t.getPath() will
            // not return the correct path for node after, but is
            // sufficient to check if it is versionable
            Tree t = new ReadOnlyTree(after);
            isVersionable = vMgr.isVersionable(t);
        }
        return isVersionable;
    }

    private boolean isVersionProperty(PropertyState state) {
        return VersionConstants.VERSION_PROPERTY_NAMES
                .contains(state.getName());
    }

    /**
     * @return <code>true</code> if this node <b>was</b> checked in. That is,
     *         this method checks the base state for the jcr:isCheckedOut
     *         property.
     */
    private boolean wasCheckedIn() {
        PropertyState prop = before
                .getProperty(VersionConstants.JCR_ISCHECKEDOUT);
        if (prop != null) {
            return !prop.getValue(Type.BOOLEAN);
        }
        // new node or not versionable, check parent
        return parent != null && parent.wasCheckedIn();
    }

    private static void throwCheckedIn(String msg)
            throws CommitFailedException {
        throw new CommitFailedException(CommitFailedException.VERSION,
                VersionExceptionType.NODE_CHECKED_IN.ordinal(), msg);
    }

    private static void throwProtected(String name)
            throws CommitFailedException {
        throw new CommitFailedException(CommitFailedException.CONSTRAINT, 100,
                "Property is protected: " + name);
    }
}
