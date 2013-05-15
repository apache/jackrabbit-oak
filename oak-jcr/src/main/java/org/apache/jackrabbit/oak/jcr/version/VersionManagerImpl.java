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
package org.apache.jackrabbit.oak.jcr.version;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.ItemExistsException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PathNotFoundException;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.lock.LockException;
import javax.jcr.lock.LockManager;
import javax.jcr.nodetype.NodeType;
import javax.jcr.util.TraversingItemVisitor;
import javax.jcr.version.Version;
import javax.jcr.version.VersionException;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.commons.iterator.NodeIteratorAdapter;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.SessionContext;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionOperation;
import org.apache.jackrabbit.oak.jcr.delegate.VersionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.VersionHistoryDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.VersionManagerDelegate;
import org.apache.jackrabbit.oak.util.TODO;

import static com.google.common.base.Preconditions.checkNotNull;

public class VersionManagerImpl implements VersionManager {

    private final SessionContext sessionContext;
    private final VersionManagerDelegate versionManagerDelegate;

    public VersionManagerImpl(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        this.versionManagerDelegate = VersionManagerDelegate.create(sessionContext.getSessionDelegate());
    }

    @Override
    public Node setActivity(Node activity) throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public void restoreByLabel(
            String absPath, String versionLabel, boolean removeExisting)
            throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public void restore(final String absPath,
                        final Version version,
                        final boolean removeExisting)
            throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(absPath);
                NodeDelegate nodeDelegate = sessionDelegate.getNode(oakPath);
                if (nodeDelegate != null) {
                    throw new VersionException(
                            "VersionManager.restore(String, Version, boolean)"
                                    + " not allowed on existing nodes; use"
                                    + " VersionManager.restore(Version, boolean) instead: "
                                    + absPath);
                }
                // check if parent exists
                NodeDelegate parent = ensureParentExists(sessionDelegate, absPath);
                // check for pending changes
                checkPendingChangesForRestore(sessionDelegate);
                // check lock status
                checkNotLocked(parent.getPath());
                // check for existing nodes
                List<NodeDelegate> existing = getExisting(version,
                        Collections.<String>emptySet());
                boolean success = false;
                try {
                    if (!existing.isEmpty()) {
                        if (removeExisting) {
                            removeExistingNodes(existing);
                        } else {
                            List<String> paths = new ArrayList<String>();
                            for (NodeDelegate nd : existing) {
                                paths.add(nd.getPath());
                            }
                            throw new ItemExistsException("Unable to restore with " +
                                    "removeExisting=false. Existing nodes in " +
                                    "workspace: " + paths);
                        }
                    }
                    // ready for restore
                    VersionDelegate vd = versionManagerDelegate.getVersionByIdentifier(
                            version.getIdentifier());
                    versionManagerDelegate.restore(
                            parent, PathUtils.getName(oakPath), vd);
                    sessionDelegate.getRoot().commit();
                    success = true;
                } catch (CommitFailedException e) {
                    throw new RepositoryException(e);
                } finally {
                    if (!success) {
                        // refresh if one of the modifying operations fail
                        sessionDelegate.refresh(false);
                    }
                }
                return null;
            }
        });
    }

    @Override
    public void restore(final String absPath,
                        final String versionName,
                        final boolean removeExisting)
            throws RepositoryException {
        VersionHistory history = getVersionHistory(absPath);
        restore(new Version[]{history.getVersion(versionName)}, removeExisting);
    }

    @Override
    public void restore(Version version, boolean removeExisting)
            throws RepositoryException {
        restore(new Version[]{version}, removeExisting);
    }

    @Override
    public void restore(final Version[] versions,
                        final boolean removeExisting)
            throws ItemExistsException,
            UnsupportedRepositoryOperationException, VersionException,
            LockException, InvalidItemStateException, RepositoryException {
        if (versions.length > 1) {
            // TODO: implement restore of multiple versions
            TODO.unimplemented().doNothing(); // TODO: RETURN
        }
        final Version version = versions[0];
        VersionHistory history = (VersionHistory) version.getParent();
        final String versionableId = history.getVersionableIdentifier();
        if (history.getRootVersion().isSame(version)) {
            throw new VersionException("Restore of root version not possible");
        }
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                // check for pending changes
                checkPendingChangesForRestore(sessionDelegate);
                NodeDelegate n = sessionDelegate.getNodeByIdentifier(versionableId);
                if (n == null) {
                    throw new VersionException("Unable to restore version. " +
                            "No versionable node with identifier: " + versionableId);
                }
                // check lock status
                checkNotLocked(n.getPath());
                // check for existing nodes
                List<NodeDelegate> existing = getExisting(version,
                        Collections.singleton(n.getPath()));
                boolean success = false;
                try {
                    if (!existing.isEmpty()) {
                        if (removeExisting) {
                            removeExistingNodes(existing);
                        } else {
                            List<String> paths = new ArrayList<String>();
                            for (NodeDelegate nd : existing) {
                                paths.add(nd.getPath());
                            }
                            throw new ItemExistsException("Unable to restore with " +
                                    "removeExisting=false. Existing nodes in " +
                                    "workspace: " + paths);
                        }
                    }
                    // ready for restore
                    VersionDelegate vd = versionManagerDelegate.getVersionByIdentifier(
                            version.getIdentifier());
                    versionManagerDelegate.restore(
                            n.getParent(), n.getName(), vd);
                    sessionDelegate.getRoot().commit();
                    success = true;
                } catch (CommitFailedException e) {
                    throw new RepositoryException(e);
                } finally {
                    if (!success) {
                        // refresh if one of the modifying operations fail
                        sessionDelegate.refresh(false);
                    }
                }
                return null;
            }
        });
    }

    @Override
    public void removeActivity(Node activityNode)
            throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public NodeIterator merge(
            String absPath, String srcWorkspace,
            boolean bestEffort, boolean isShallow)
            throws RepositoryException {
        return TODO.unimplemented().returnValue(NodeIteratorAdapter.EMPTY);
    }

    @Override
    public NodeIterator merge(
            String absPath, String srcWorkspace, boolean bestEffort)
            throws RepositoryException {
        return TODO.unimplemented().returnValue(NodeIteratorAdapter.EMPTY);
    }

    @Override
    public NodeIterator merge(Node activityNode) throws RepositoryException {
        return TODO.unimplemented().returnValue(NodeIteratorAdapter.EMPTY);
    }

    private String getOakPathOrThrowNotFound(String absPath) throws PathNotFoundException {
        return sessionContext.getOakPathOrThrowNotFound(absPath);
    }

    @Override
    public boolean isCheckedOut(final String absPath) throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(absPath);
                NodeDelegate nodeDelegate = sessionDelegate.getNode(oakPath);
                if (nodeDelegate == null) {
                    throw new PathNotFoundException(absPath);
                }
                return versionManagerDelegate.isCheckedOut(nodeDelegate);
            }
        });
    }

    @Override
    public VersionHistory getVersionHistory(final String absPath)
            throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        return sessionDelegate.perform(new SessionOperation<VersionHistory>() {
            @Override
            public VersionHistory perform() throws RepositoryException {
                return new VersionHistoryImpl(
                        internalGetVersionHistory(absPath), sessionContext);
            }
        });
    }

    @Override
    public Version getBaseVersion(final String absPath) throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        return sessionDelegate.perform(new SessionOperation<Version>() {
            @Override
            public Version perform() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(absPath);
                NodeDelegate nodeDelegate = sessionDelegate.getNode(oakPath);
                if (nodeDelegate == null) {
                    throw new PathNotFoundException(absPath);
                }
                return new VersionImpl(
                        versionManagerDelegate.getBaseVersion(nodeDelegate), sessionContext);
            }
        });
    }

    @Override
    public Node getActivity() throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public void doneMerge(String absPath, Version version)
            throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public Node createConfiguration(String absPath) throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public Node createActivity(String title) throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public Version checkpoint(String absPath) throws RepositoryException {
        // FIXME: atomic?
        Version v = checkin(absPath);
        checkout(absPath);
        return v;
    }

    @Nonnull
    private LockManager getLockManager() {
        return sessionContext.getLockManager();
    }

    @Override
    public void checkout(final String absPath) throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(absPath);
                NodeDelegate nodeDelegate = sessionDelegate.getNode(oakPath);
                if (nodeDelegate == null) {
                    throw new PathNotFoundException(absPath);
                }
                checkNotLocked(absPath);
                versionManagerDelegate.checkout(nodeDelegate);
                return null;
            }
        });
    }

    @Override
    public Version checkin(final String absPath) throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        return sessionDelegate.perform(new SessionOperation<Version>() {
            @Override
            public Version perform() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(absPath);
                NodeDelegate nodeDelegate = sessionDelegate.getNode(oakPath);
                if (nodeDelegate == null) {
                    throw new PathNotFoundException(absPath);
                }
                checkNotLocked(absPath);
                return new VersionImpl(versionManagerDelegate.checkin(nodeDelegate), sessionContext);
            }
        });
    }

    @Override
    public void cancelMerge(String absPath, Version version)
            throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

    //----------------------------< internal >----------------------------------

    private void checkPendingChangesForRestore(SessionDelegate sessionDelegate)
            throws InvalidItemStateException {
        if (sessionDelegate.hasPendingChanges()) {
            throw new InvalidItemStateException(
                    "Unable to restore. Session has pending changes.");
        }
    }

    private void checkNotLocked(String absPath) throws RepositoryException {
        if (getLockManager().isLocked(absPath)) {
            throw new LockException("Node at " + absPath + " is locked");
        }
    }

    /**
     * Returns the parent for the given <code>absPath</code> or throws a
     * {@link PathNotFoundException} if it doesn't exist.
     *
     * @param sessionDelegate session delegate.
     * @param absPath an absolute path
     * @return the parent for the given <code>absPath</code>.
     * @throws PathNotFoundException if the node does not exist.
     */
    @Nonnull
    private NodeDelegate ensureParentExists(@Nonnull SessionDelegate sessionDelegate,
                                            @Nonnull String absPath)
            throws PathNotFoundException {
        String oakParentPath = getOakPathOrThrowNotFound(
                PathUtils.getParentPath(checkNotNull(absPath)));
        NodeDelegate parent = checkNotNull(sessionDelegate).getNode(oakParentPath);
        if (parent == null) {
            throw new PathNotFoundException(PathUtils.getParentPath(absPath));
        }
        return parent;
    }

    /**
     * Returns referenceable nodes outside of the versionable sub-graphs
     * identified by <code>versionablePaths</code>, which are also present
     * in the versionable state captured by <code>version</code>.
     *
     * @param version the version.
     * @param versionablePaths identifies the starting points of the versionable
     *                         sub-graphs.
     * @return existing nodes in this workspace.
     */
    private List<NodeDelegate> getExisting(@Nonnull Version version,
                                           @Nonnull Set<String> versionablePaths)
            throws RepositoryException {
        // collect uuids
        final List<String> uuids = new ArrayList<String>();
        version.getFrozenNode().accept(new TraversingItemVisitor.Default() {
            @Override
            protected void entering(Node node, int level)
                    throws RepositoryException {
                if (node.isNodeType(NodeType.NT_FROZEN_NODE)) {
                    uuids.add(node.getProperty(Property.JCR_FROZEN_UUID).getString());
                } else if (node.isNodeType(NodeType.NT_VERSIONED_CHILD)) {
                    // TODO: handle?
                }
            }
        });
        SessionDelegate delegate = sessionContext.getSessionDelegate();
        if (uuids.isEmpty()) {
            return Collections.emptyList();
        }
        List<NodeDelegate> existing = new ArrayList<NodeDelegate>();
        for (String uuid : uuids) {
            NodeDelegate node = delegate.getNodeByIdentifier(uuid);
            if (node != null) {
                boolean inSubGraph = false;
                for (String versionablePath : versionablePaths) {
                    if (node.getPath().startsWith(versionablePath)) {
                        inSubGraph = true;
                        break;
                    }
                }
                if (!inSubGraph) {
                    existing.add(node);
                }
            }
        }
        return existing;
    }

    /**
     * Removes existing nodes and throws a {@link RepositoryException} if
     * removing one of them fails.
     *
     * @param existing nodes to remove.
     * @throws RepositoryException if the operation fails.
     */
    private void removeExistingNodes(List<NodeDelegate> existing)
            throws RepositoryException {
        for (NodeDelegate nd : existing) {
            if (!nd.remove()) {
                throw new RepositoryException(
                        "Unable to remove existing node: " + nd.getPath());
            }
        }
    }

    /**
     * Returns the version history for the versionable node at the given path.
     *
     * @param absPathVersionable path to a versionable node.
     * @return the version history.
     * @throws PathNotFoundException if the given path does not reference an
     *                               existing node.
     * @throws UnsupportedRepositoryOperationException
     *                               if the node at the given path is not
     *                               mix:versionable.
     * @throws RepositoryException if some other error occurs.
     */
    @Nonnull
    private VersionHistoryDelegate internalGetVersionHistory(
            @Nonnull String absPathVersionable)
            throws RepositoryException, UnsupportedRepositoryOperationException {
        SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        String oakPath = getOakPathOrThrowNotFound(checkNotNull(absPathVersionable));
        NodeDelegate nodeDelegate = sessionDelegate.getNode(oakPath);
        if (nodeDelegate == null) {
            throw new PathNotFoundException(absPathVersionable);
        }
        return versionManagerDelegate.getVersionHistory(nodeDelegate);

    }
}
