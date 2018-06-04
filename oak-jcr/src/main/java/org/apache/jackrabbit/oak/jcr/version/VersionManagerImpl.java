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

import static com.google.common.base.Preconditions.checkNotNull;

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
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.util.TraversingItemVisitor;
import javax.jcr.version.OnParentVersionAction;
import javax.jcr.version.Version;
import javax.jcr.version.VersionException;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.VersionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.VersionHistoryDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.VersionManagerDelegate;
import org.apache.jackrabbit.oak.jcr.lock.LockManagerImpl;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.apache.jackrabbit.oak.plugins.nodetype.write.ReadWriteNodeTypeManager;

public class VersionManagerImpl implements VersionManager {

    private final SessionContext sessionContext;
    private final VersionManagerDelegate versionManagerDelegate;

    public VersionManagerImpl(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        this.versionManagerDelegate = VersionManagerDelegate.create(sessionContext.getSessionDelegate());
    }

    @Override
    public Node setActivity(Node activity) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("OAK-827: Activities not implemented.");
    }

    @Override
    public void restoreByLabel(
            String absPath, String versionLabel, boolean removeExisting)
            throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("OAK-168: Restore of by label not implemented.");
    }

    @Override
    public void restore(final String absPath,
                        final Version version,
                        final boolean removeExisting)
            throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        sessionDelegate.performVoid(new SessionOperation<Void>("restore", true) {
            @Override
            public void performVoid() throws RepositoryException {
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
                    sessionDelegate.commit();
                    success = true;
                } catch (CommitFailedException e) {
                    throw e.asRepositoryException();
                } finally {
                    if (!success) {
                        // refresh if one of the modifying operations fail
                        sessionDelegate.refresh(false);
                    }
                }
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
            throw new UnsupportedRepositoryOperationException("OAK-168: Restore of multiple versions not implemented.");
        }
        final Version version = versions[0];
        VersionHistory history = (VersionHistory) version.getParent();
        final String versionableId = history.getVersionableIdentifier();
        if (history.getRootVersion().isSame(version)) {
            throw new VersionException("Restore of root version not possible");
        }
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        sessionDelegate.performVoid(new SessionOperation<Void>("restore", true) {
            @Override
            public void performVoid() throws RepositoryException {
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
                    VersionDelegate vd = versionManagerDelegate.getVersionByIdentifier(version.getIdentifier());
                    versionManagerDelegate.restore(n.getParent(), n.getName(), vd);
                    sessionDelegate.commit();
                    success = true;
                } catch (CommitFailedException e) {
                    throw new RepositoryException(e);
                } finally {
                    if (!success) {
                        // refresh if one of the modifying operations fail
                        sessionDelegate.refresh(false);
                    }
                }
            }
        });
    }

    @Override
    public void removeActivity(Node activityNode)
            throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("OAK-827: Activities not implemented.");
    }

    @Override
    public NodeIterator merge(
            String absPath, String srcWorkspace,
            boolean bestEffort, boolean isShallow)
            throws RepositoryException {
        // TODO mind OAK-1370 when implementing this
        throw new UnsupportedRepositoryOperationException("OAK-1402: Merge not implemented.");
    }

    @Override
    public NodeIterator merge(
            String absPath, String srcWorkspace, boolean bestEffort)
            throws RepositoryException {
        // TODO mind OAK-1370 when implementing this
        throw new UnsupportedRepositoryOperationException("OAK-1402: Merge not implemented.");
    }

    @Override
    public NodeIterator merge(Node activityNode) throws RepositoryException {
        // TODO mind OAK-1370 when implementing this
        throw new UnsupportedRepositoryOperationException("OAK-1402: Merge not implemented.");
    }

    private String getOakPathOrThrowNotFound(String absPath) throws PathNotFoundException {
        return sessionContext.getOakPathOrThrowNotFound(absPath);
    }

    @Override
    public boolean isCheckedOut(final String absPath) throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        return sessionDelegate.perform(new SessionOperation<Boolean>("isCheckoutOut") {
            @Nonnull
            @Override
            public Boolean perform() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(absPath);
                NodeDelegate nodeDelegate = sessionDelegate.getNode(oakPath);
                if (nodeDelegate == null) {
                    throw new PathNotFoundException(absPath);
                }
                boolean isCheckedOut = versionManagerDelegate.isCheckedOut(nodeDelegate);
                if (!isCheckedOut) {
                    // check OPV
                    ReadWriteNodeTypeManager ntMgr = sessionContext.getWorkspace().getNodeTypeManager();
                    NodeDelegate parent = nodeDelegate.getParent();
                    NodeDefinition definition;
                    if (parent == null) {
                        definition = ntMgr.getRootDefinition();
                    } else {
                        definition = ntMgr.getDefinition(parent.getTree(), nodeDelegate.getTree());
                    }
                    isCheckedOut = definition.getOnParentVersion() == OnParentVersionAction.IGNORE;
                }
                return isCheckedOut;
            }
        });
    }

    @Override
    public VersionHistory getVersionHistory(final String absPath)
            throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        return sessionDelegate.perform(new SessionOperation<VersionHistory>("getVersionHistory") {
            @Nonnull
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
        return sessionDelegate.perform(new SessionOperation<Version>("getBaseVersion") {
            @Nonnull
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
        throw new UnsupportedRepositoryOperationException("OAK-827: Activities not implemented.");
    }

    @Override
    public void doneMerge(String absPath, Version version)
            throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("OAK-1402: Merge not implemented.");
    }

    @Override
    public Node createConfiguration(String absPath) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("OAK-1403: Configurations not implemented.");
    }

    @Override
    public Node createActivity(String title) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("OAK-827: Activities not implemented.");
    }

    @Override
    public Version checkpoint(String absPath) throws RepositoryException {
        // FIXME: atomic?
        Version v = checkin(absPath);
        checkout(absPath);
        return v;
    }

    @Override
    public void checkout(final String absPath) throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        sessionDelegate.performVoid(new SessionOperation<Void>("checkout", true) {
            @Override
            public void performVoid() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(absPath);
                NodeDelegate nodeDelegate = sessionDelegate.getNode(oakPath);
                if (nodeDelegate == null) {
                    throw new PathNotFoundException(absPath);
                }
                checkNotLocked(absPath);
                versionManagerDelegate.checkout(nodeDelegate);
            }
        });
    }

    @Override
    public Version checkin(final String absPath) throws RepositoryException {
        final SessionDelegate sessionDelegate = sessionContext.getSessionDelegate();
        return sessionDelegate.perform(new SessionOperation<Version>("checkin", true) {
            @Nonnull
            @Override
            public Version perform() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(absPath);
                NodeDelegate nodeDelegate = sessionDelegate.getNode(oakPath);
                if (nodeDelegate == null) {
                    throw new PathNotFoundException(absPath);
                }
                return new VersionImpl(versionManagerDelegate.checkin(nodeDelegate), sessionContext);
            }
        });
    }

    @Override
    public void cancelMerge(String absPath, Version version) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("OAK-1402: Merge not implemented.");
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
        // TODO: avoid nested calls
        LockManagerImpl lockManager = sessionContext.getWorkspace().getLockManager();
        if (lockManager.isLocked(absPath)) {
            NodeDelegate node = sessionContext.getSessionDelegate().getNode(absPath);
            if (!lockManager.canUnlock(node)) {
                throw new LockException("Node at " + absPath + " is locked");    
            }
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
                    String id = node.getProperty(Property.JCR_FROZEN_UUID).getString();
                    if (id.length() > 0) {
                        uuids.add(id);
                    }
                } else if (node.isNodeType(NodeType.NT_VERSIONED_CHILD)) {
                    Node history = node.getProperty(
                            Property.JCR_CHILD_VERSION_HISTORY).getNode();
                    uuids.add(history.getProperty(Property.JCR_VERSIONABLE_UUID).getString());
                    // TODO: further traverse versioned children with some selector (date?)
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
