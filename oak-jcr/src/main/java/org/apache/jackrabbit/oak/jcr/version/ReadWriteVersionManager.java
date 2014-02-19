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

import java.util.Collections;

import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.version.LabelExistsVersionException;
import javax.jcr.version.VersionException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.version.ReadOnlyVersionManager;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.test.ISO8601;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.JcrConstants.JCR_CREATED;
import static org.apache.jackrabbit.JcrConstants.JCR_ISCHECKEDOUT;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONLABELS;
import static org.apache.jackrabbit.oak.plugins.version.VersionConstants.REP_ADD_VERSION_LABELS;
import static org.apache.jackrabbit.oak.plugins.version.VersionConstants.REP_REMOVE_VERSION_LABELS;

/**
 * {@code ReadWriteVersionManager}...
 */
public class ReadWriteVersionManager extends ReadOnlyVersionManager {

    private final SessionDelegate sessionDelegate;

    private final VersionStorage versionStorage;

    public ReadWriteVersionManager(@Nonnull SessionDelegate sessionDelegate) {
        this.sessionDelegate = sessionDelegate;
        this.versionStorage = new VersionStorage(sessionDelegate.getRoot());
    }

    /**
     * Called by the write methods to refresh the state of the possible
     * session associated with this instance. The default implementation
     * of this method does nothing, but a subclass can use this callback
     * to keep a session in sync with the persisted version changes.
     *
     * @throws RepositoryException if the session could not be refreshed
     */
    protected void refresh() throws RepositoryException {
        sessionDelegate.refresh(true);
    }

    @Override
    @Nonnull
    protected Tree getVersionStorage() {
        return versionStorage.getTree();
    }

    @Override
    @Nonnull
    protected Root getWorkspaceRoot() {
        return sessionDelegate.getRoot();
    }

    @Override
    @Nonnull
    protected ReadOnlyNodeTypeManager getNodeTypeManager() {
        return ReadOnlyNodeTypeManager.getInstance(
                sessionDelegate.getRoot(), NamePathMapper.DEFAULT);
    }

    /**
     * Performs a checkin on a versionable tree and returns the tree that
     * represents the created version.
     *
     * @param versionable the versionable node to check in.
     * @return the created version.
     * @throws InvalidItemStateException if the current root has pending
     *                                   changes.
     * @throws UnsupportedRepositoryOperationException
     *                                   if the versionable tree isn't actually
     *                                   versionable.
     * @throws RepositoryException       if an error occurs while checking the
     *                                   node type of the tree.
     */
    @Nonnull
    public Tree checkin(@Nonnull Tree versionable)
            throws RepositoryException, InvalidItemStateException,
            UnsupportedRepositoryOperationException {
        if (sessionDelegate.hasPendingChanges()) {
            throw new InvalidItemStateException("Unable to perform checkin. " +
                    "Session has pending changes.");
        }
        if (!isVersionable(versionable)) {
            throw new UnsupportedRepositoryOperationException(
                    versionable.getPath() + " is not versionable");
        }
        if (isCheckedOut(versionable)) {
            versionable.setProperty(JCR_ISCHECKEDOUT,
                    Boolean.FALSE, Type.BOOLEAN);
            Tree baseVersion = getBaseVersion(versionable);
            PropertyState created = baseVersion.getProperty(JCR_CREATED);
            if (created != null) {
                long c = ISO8601.parse(created.getValue(Type.DATE)).getTimeInMillis();
                while (System.currentTimeMillis() == c) {
                    // busy-wait for System.currentTimeMillis to change
                    // so that the new version has a distinct timestamp
                }
            }
            try {
                sessionDelegate.commit();
                refresh();
            } catch (CommitFailedException e) {
                sessionDelegate.refresh(true);
                throw e.asRepositoryException();
            }
        }
        return getBaseVersion(getWorkspaceRoot().getTree(versionable.getPath()));
    }

    /**
     * Performs a checkout on a versionable tree.
     *
     * @param workspaceRoot a fresh workspace root without pending changes.
     * @param versionablePath the absolute path to the versionable node to check out.
     * @throws UnsupportedRepositoryOperationException
     *                             if the versionable tree isn't actually
     *                             versionable.
     * @throws RepositoryException if an error occurs while checking the
     *                             node type of the tree.
     * @throws IllegalStateException if the workspaceRoot has pending changes.
     * @throws IllegalArgumentException if the <code>versionablePath</code> is
     *                             not absolute.
     */
    public void checkout(@Nonnull Root workspaceRoot,
                         @Nonnull String versionablePath)
            throws UnsupportedRepositoryOperationException,
            InvalidItemStateException, RepositoryException {
        checkState(!workspaceRoot.hasPendingChanges());
        checkArgument(PathUtils.isAbsolute(versionablePath));
        Tree versionable = workspaceRoot.getTree(versionablePath);
        if (!isVersionable(versionable)) {
            throw new UnsupportedRepositoryOperationException(
                    versionable.getPath() + " is not versionable");
        }
        if (!isCheckedOut(versionable)) {
            versionable.setProperty(JCR_ISCHECKEDOUT,
                    Boolean.TRUE, Type.BOOLEAN);
            try {
                workspaceRoot.commit();
                refresh();
            } catch (CommitFailedException e) {
                workspaceRoot.refresh();
                throw e.asRepositoryException();
            }
        }
    }

    public void addVersionLabel(@Nonnull VersionStorage versionStorage,
                                @Nonnull String versionHistoryOakRelPath,
                                @Nonnull String versionOakName,
                                @Nonnull String oakVersionLabel,
                                boolean moveLabel) throws RepositoryException {
        Tree versionHistory = TreeUtil.getTree(checkNotNull(versionStorage.getTree()),
                checkNotNull(versionHistoryOakRelPath));
        Tree labels = checkNotNull(versionHistory).getChild(JCR_VERSIONLABELS);
        if (labels.hasProperty(checkNotNull(oakVersionLabel))) {
            if (moveLabel) {
                String labelPath = PathUtils.concat(versionHistoryOakRelPath,
                        JCR_VERSIONLABELS, oakVersionLabel, "dummy");
                versionStorage.getTree().setProperty(REP_REMOVE_VERSION_LABELS,
                        Collections.singleton(labelPath), Type.PATHS);
            } else {
                throw new LabelExistsVersionException("Version label '"
                        + oakVersionLabel + "' already exists on this version history");
            }
        }
        String labelPath = PathUtils.concat(versionHistoryOakRelPath,
                JCR_VERSIONLABELS, oakVersionLabel, checkNotNull(versionOakName));
        versionStorage.getTree().setProperty(REP_ADD_VERSION_LABELS,
                Collections.singleton(labelPath), Type.PATHS);
        try {
            sessionDelegate.commit(versionStorage.getRoot());
            refresh();
        } catch (CommitFailedException e) {
            versionStorage.refresh();
            throw e.asRepositoryException();
        }
    }

    public void removeVersionLabel(@Nonnull VersionStorage versionStorage,
                                   @Nonnull String versionHistoryOakRelPath,
                                   @Nonnull String oakVersionLabel)
            throws RepositoryException {
        Tree versionHistory = TreeUtil.getTree(checkNotNull(versionStorage.getTree()),
                checkNotNull(versionHistoryOakRelPath));
        Tree labels = checkNotNull(versionHistory).getChild(JCR_VERSIONLABELS);
        if (!labels.hasProperty(oakVersionLabel)) {
            throw new VersionException("Version label " + oakVersionLabel +
                    " does not exist on this version history");
        }
        String labelPath = PathUtils.concat(versionHistoryOakRelPath,
                JCR_VERSIONLABELS, oakVersionLabel, "dummy");
        versionStorage.getTree().setProperty(REP_REMOVE_VERSION_LABELS,
                Collections.singleton(labelPath), Type.PATHS);
        try {
            sessionDelegate.commit(versionStorage.getRoot());
            refresh();
        } catch (CommitFailedException e) {
            versionStorage.refresh();
            throw e.asRepositoryException();
        }
    }

    // TODO: more methods that modify versions
}
