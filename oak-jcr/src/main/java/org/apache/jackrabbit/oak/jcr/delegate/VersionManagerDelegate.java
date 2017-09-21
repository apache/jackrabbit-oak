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
package org.apache.jackrabbit.oak.jcr.delegate;

import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.version.LabelExistsVersionException;
import javax.jcr.version.VersionException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.version.ReadWriteVersionManager;
import org.apache.jackrabbit.oak.jcr.version.VersionStorage;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_BASEVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENMIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENPRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENUUID;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONHISTORY;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.RESTORE_PREFIX;

/**
 * {@code VersionManagerDelegate}...
 */
public class VersionManagerDelegate {

    private final SessionDelegate sessionDelegate;

    private final ReadWriteVersionManager versionManager;

    public static VersionManagerDelegate create(SessionDelegate sessionDelegate) {
        return new VersionManagerDelegate(sessionDelegate);
    }

    private VersionManagerDelegate(SessionDelegate sessionDelegate) {
        this.sessionDelegate = sessionDelegate;
        this.versionManager = new ReadWriteVersionManager(sessionDelegate);
    }

    @Nonnull
    public VersionDelegate checkin(@Nonnull NodeDelegate nodeDelegate)
            throws RepositoryException {
        return VersionDelegate.create(sessionDelegate,
                versionManager.checkin(getTree(nodeDelegate)));
    }

    public void checkout(@Nonnull NodeDelegate nodeDelegate)
            throws RepositoryException {
        // perform the operation on a fresh root because
        // it must not save pending changes in the workspace
        Root fresh = sessionDelegate.getContentSession().getLatestRoot();
        versionManager.checkout(fresh, nodeDelegate.getPath());
    }

    public boolean isCheckedOut(@Nonnull NodeDelegate nodeDelegate)
            throws RepositoryException {
        return versionManager.isCheckedOut(getTree(nodeDelegate));
    }

    @Nonnull
    public VersionHistoryDelegate createVersionHistory(@Nonnull NodeDelegate versionHistory)
            throws RepositoryException {
        return new VersionHistoryDelegate(sessionDelegate, getTree(versionHistory));
    }

    @Nonnull
    public VersionDelegate createVersion(@Nonnull NodeDelegate version)
            throws RepositoryException {
        return VersionDelegate.create(sessionDelegate, getTree(version));
    }

    @Nonnull
    public VersionHistoryDelegate getVersionHistory(@Nonnull NodeDelegate versionable)
            throws RepositoryException {
        Tree vh = versionManager.getVersionHistory(getTree(versionable));
        if (vh == null) {
            throw new UnsupportedRepositoryOperationException("Node does not" +
                    " have a version history: " + versionable.getPath());
        }
        return new VersionHistoryDelegate(sessionDelegate, vh);
    }

    @Nonnull
    public VersionDelegate getBaseVersion(@Nonnull NodeDelegate versionable)
            throws RepositoryException {
        Tree v = versionManager.getBaseVersion(getTree(versionable));
        if (v == null) {
            throw new UnsupportedRepositoryOperationException("Node does not" +
                    " have a base version: " + versionable.getPath());
        }
        return VersionDelegate.create(sessionDelegate, v);
    }

    @Nonnull
    public VersionDelegate getVersionByIdentifier(@Nonnull String identifier)
            throws RepositoryException {
        Tree t = sessionDelegate.getIdManager().getTree(identifier);
        if (t == null || !t.exists()) {
            throw new RepositoryException("No such Version with identifier: " +
                    identifier);
        }
        return VersionDelegate.create(sessionDelegate, t);
    }

    public void restore(@Nonnull NodeDelegate parent,
                        @Nonnull String oakName,
                        @Nonnull VersionDelegate vd)
            throws RepositoryException {
        NodeDelegate frozen = vd.getFrozenNode();
        PropertyState primaryType = frozen.getProperty(
                JCR_FROZENPRIMARYTYPE).getPropertyState();
        PropertyState uuid = frozen.getProperty(
                JCR_FROZENUUID).getPropertyState();
        PropertyDelegate mixinTypes = frozen.getPropertyOrNull(JCR_FROZENMIXINTYPES);
        if (parent.getChild(oakName) == null) {
            // create a sentinel node with a jcr:baseVersion pointing
            // to the version to restore
            Tree t = parent.getTree().addChild(oakName);
            t.setProperty(JCR_PRIMARYTYPE, primaryType.getValue(Type.NAME), Type.NAME);
            t.setProperty(JCR_UUID, uuid.getValue(Type.STRING), Type.STRING);
            if (mixinTypes != null && mixinTypes.getPropertyState().count() > 0) {
                t.setProperty(JCR_MIXINTYPES,
                        mixinTypes.getPropertyState().getValue(Type.NAMES),
                        Type.NAMES);
            }
            t.setProperty(JCR_BASEVERSION, vd.getIdentifier(), Type.REFERENCE);
            t.setProperty(JCR_VERSIONHISTORY, vd.getParent().getIdentifier(), Type.REFERENCE);
        } else {
            Tree t = parent.getChild(oakName).getTree();
            t.setProperty(JCR_BASEVERSION, RESTORE_PREFIX + vd.getIdentifier(), Type.REFERENCE);
        }
    }

    /**
     * Add a version label to the given version history.
     *
     * @param versionHistory the version history.
     * @param version the version.
     * @param oakVersionLabel the version label.
     * @param moveLabel whether to move the label if it already exists.
     * @throws InvalidItemStateException if any of the nodes is stale.
     * @throws LabelExistsVersionException if moveLabel is false, and an attempt
     * is made to add a label that already exists in this version history.
     * @throws VersionException if the specified version does not exist in this
     * version history or if the specified version is the root version (jcr:rootVersion).
     * @throws RepositoryException if another error occurs.
     */
    public void addVersionLabel(@Nonnull VersionHistoryDelegate versionHistory,
                                @Nonnull VersionDelegate version,
                                @Nonnull String oakVersionLabel,
                                boolean moveLabel)
            throws InvalidItemStateException, LabelExistsVersionException,
            VersionException, RepositoryException {
        // perform operation on fresh storage to not interfere
        // with pending changes in the workspace.
        Root fresh = sessionDelegate.getContentSession().getLatestRoot();
        VersionStorage storage = new VersionStorage(fresh);
        String vhRelPath = PathUtils.relativize(VersionStorage.VERSION_STORAGE_PATH,
                checkNotNull(versionHistory).getPath());
        versionManager.addVersionLabel(storage, vhRelPath,
                checkNotNull(version).getIdentifier(),
                checkNotNull(oakVersionLabel),
                moveLabel);
    }

    /**
     * Removes a version label from the given history.
     *
     * @param versionHistory the version history.
     * @param oakVersionLabel the version label.
     * @throws InvalidItemStateException if any of the nodes is stale.
     * @throws VersionException if the name label does not exist in this version history.
     * @throws RepositoryException if another error occurs.
     */
    public void removeVersionLabel(@Nonnull VersionHistoryDelegate versionHistory,
                                   @Nonnull String oakVersionLabel)
            throws InvalidItemStateException, VersionException, RepositoryException {
        // perform operation on fresh storage to not interfere
        // with pending changes in the workspace.
        Root fresh = sessionDelegate.getContentSession().getLatestRoot();
        VersionStorage storage = new VersionStorage(fresh);
        String vhRelPath = PathUtils.relativize(VersionStorage.VERSION_STORAGE_PATH,
                checkNotNull(versionHistory).getPath());
        versionManager.removeVersionLabel(storage, vhRelPath,
                checkNotNull(oakVersionLabel));
    }

    /**
     * Removes a version from the given history.
     *
     * @param versionHistory the version history delegate.
     * @param oakVersionName the version name
     * @throws RepositoryException if an error occurs.
     */
    public void removeVersion(@Nonnull VersionHistoryDelegate versionHistory,
                              @Nonnull String oakVersionName) throws RepositoryException {
        // perform operation on fresh storage to not interfere
        // with pending changes in the workspace.
        Root fresh = sessionDelegate.getContentSession().getLatestRoot();
        VersionStorage storage = new VersionStorage(fresh);
        String vhRelPath = PathUtils.relativize(VersionStorage.VERSION_STORAGE_PATH,
                checkNotNull(versionHistory).getPath());
        versionManager.removeVersion(storage, vhRelPath, oakVersionName);
    }

    //----------------------------< internal >----------------------------------

    /**
     * Returns the underlying tree.
     *
     * @param nodeDelegate the node delegate.
     * @return the underlying tree.
     * @throws InvalidItemStateException if the location points to a stale item.
     */
    @Nonnull
    private static Tree getTree(@Nonnull NodeDelegate nodeDelegate)
            throws InvalidItemStateException {
        return checkNotNull(nodeDelegate).getTree();
    }

}
