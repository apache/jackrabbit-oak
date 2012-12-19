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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.version.VersionException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.version.VersionConstants;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * TODO document
 */
public class VersionHook implements CommitHook {

    @Nonnull
    @Override
    public NodeState processCommit(final NodeState before, NodeState after)
            throws CommitFailedException {
        NodeBuilder builder = after.builder();
        // FIXME? depends on InitialContent creating the nodes and setting
        // the jcr:primaryType accordingly
        NodeBuilder vsRoot = builder.child(NodeTypeConstants.JCR_SYSTEM)
                .child(NodeTypeConstants.JCR_VERSIONSTORAGE);
        ReadWriteVersionManager vMgr = new ReadWriteVersionManager(vsRoot, builder);
        try {
            after.compareAgainstBaseState(before,
                    new VersionDiff(null, vMgr, builder));
        } catch (UncheckedRepositoryException e) {
            throw new CommitFailedException(e.getCause());
        }
        return builder.getNodeState();
    }

    private static class VersionDiff implements NodeStateDiff {

        private final VersionDiff parent;
        private final ReadWriteVersionManager vMgr;
        private final NodeBuilder nodeAfter;
        private Boolean isVersionable = null;

        VersionDiff(@Nullable VersionDiff parent,
                    @Nonnull ReadWriteVersionManager vMgr,
                    @Nonnull NodeBuilder node) {
            this.parent = parent;
            this.vMgr = checkNotNull(vMgr);
            this.nodeAfter = checkNotNull(node);
            if (isVersionable()) {
                vMgr.getOrCreateVersionHistory(node);
            }
        }

        @Override
        public void propertyAdded(PropertyState after) {
            if (!isVersionable()) {
                return;
            }
            // JCR allows to put a lock on a checked in node.
            if (after.getName().equals(JcrConstants.JCR_LOCKOWNER)
                    || after.getName().equals(JcrConstants.JCR_LOCKISDEEP)) {
                return;
            }
            if (wasCheckedIn()) {
                throwCheckedIn("Cannot add property " + after.getName() +
                        " on checked in node");
            }
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            if (!isVersionable()) {
                if (!isVersionProperty(after) && wasCheckedIn()) {
                    throwCheckedIn("Cannot change property " + after.getName() +
                            " on checked in node");
                }
                return;
            }
            String propName = after.getName();
            if (propName.equals(VersionConstants.JCR_ISCHECKEDOUT)) {
                if (wasCheckedIn()) {
                    vMgr.checkout(nodeAfter);
                } else {
                    vMgr.checkin(nodeAfter);
                }
            } else if (propName.equals(VersionConstants.JCR_BASEVERSION)) {
                vMgr.restore(nodeAfter);
            } else if (isVersionProperty(after)) {
                throwProtected(after.getName());
            } else if (wasCheckedIn()) {
                throwCheckedIn("Cannot change property " + after.getName() +
                        " on checked in node");
            }
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            if (!isVersionable()) {
                if (!isVersionProperty(before) && wasCheckedIn()) {
                    throwProtected("Cannot delete property on checked in node");
                }
            }
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            childNodeChanged(name, MemoryNodeState.EMPTY_NODE, after);
        }

        @Override
        public void childNodeChanged(String name,
                                     NodeState before,
                                     NodeState after) {
            after.compareAgainstBaseState(before,
                    new VersionDiff(this, vMgr, nodeAfter.child(name)));
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            NodeState after = MemoryNodeState.EMPTY_NODE;
            after.compareAgainstBaseState(before,
                    new VersionDiff(this, vMgr, after.builder()));
        }

        /**
         * Returns <code>true</code> if the node of this VersionDiff
         * is versionable; <code>false</code> otherwise.
         *
         * @return whether the node is versionable.
         */
        private boolean isVersionable() {
            try {
                if (isVersionable == null) {
                    // this is not 100% correct, because t.getPath() will
                    // not return the correct path for nodeAfter, but is
                    // sufficient to check if it is versionable
                    Tree t = new ReadOnlyTree(nodeAfter.getNodeState());
                    isVersionable = vMgr.isVersionable(t);
                }
                return isVersionable;
            } catch (RepositoryException e) {
                throw new UncheckedRepositoryException(e);
            }
        }

        private boolean isVersionProperty(PropertyState state) {
            return VersionConstants.VERSION_PROPERTY_NAMES.contains(state.getName());
        }

        /**
         * @return <code>true</code> if this node <b>was</b> checked in. That
         *         is, this method checks the base state for the
         *         jcr:isCheckedOut property.
         */
        private boolean wasCheckedIn() {
            NodeState state = nodeAfter.getBaseState();
            if (state != null) {
                PropertyState prop = state.getProperty(VersionConstants.JCR_ISCHECKEDOUT);
                if (prop != null) {
                    return !prop.getValue(Type.BOOLEAN);
                }
            }
            // new node or not versionable, check parent
            return parent != null && parent.wasCheckedIn();
        }

        private static void throwCheckedIn(String msg)
                throws UncheckedRepositoryException {
            throwUnchecked(new VersionException(msg));
        }

        private static void throwProtected(String name)
                throws UncheckedRepositoryException {
            throwUnchecked(new ConstraintViolationException(
                    "Property is protected: " + name));
        }
    }

    private static void throwUnchecked(RepositoryException e)
            throws UncheckedRepositoryException {
        throw new UncheckedRepositoryException(e);
    }

    private static class UncheckedRepositoryException extends RuntimeException {

        private static final long serialVersionUID = 5220620245610340169L;

        public UncheckedRepositoryException(RepositoryException cause) {
            super(cause);
        }

        public RepositoryException getCause() {
            return (RepositoryException) super.getCause();
        }
    }
}
