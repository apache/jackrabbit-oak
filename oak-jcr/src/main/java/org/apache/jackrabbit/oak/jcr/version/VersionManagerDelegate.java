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

import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.jcr.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.SessionDelegate;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <code>VersionManagerDelegate</code>...
 */
public class VersionManagerDelegate {

    /**
     * TODO: this assumes the version store is in the same workspace.
     */
    private static final String VERSION_STORAGE_PATH
            = "/" + JcrConstants.JCR_SYSTEM + "/" + JcrConstants.JCR_VERSIONSTORAGE;

    private final SessionDelegate sessionDelegate;

    private final ReadWriteVersionManager versionManager;

    static VersionManagerDelegate create(SessionDelegate sessionDelegate) {
        TreeLocation location = sessionDelegate.getRoot().getLocation(VERSION_STORAGE_PATH);
        return new VersionManagerDelegate(sessionDelegate, location);
    }

    private VersionManagerDelegate(SessionDelegate sessionDelegate,
                                   TreeLocation versionStorageLocation) {
        this.sessionDelegate = sessionDelegate;
        this.versionManager = new ReadWriteVersionManager(
                versionStorageLocation,
                sessionDelegate.getRoot()) {
            @Override
            protected void refresh() throws RepositoryException {
                VersionManagerDelegate.this.sessionDelegate.refresh(true);
            }
        };
    }

    SessionDelegate getSessionDelegate() {
        return sessionDelegate;
    }

    @Nonnull
    public VersionDelegate checkin(@Nonnull NodeDelegate nodeDelegate)
            throws RepositoryException {
        return VersionDelegate.create(sessionDelegate,
                versionManager.checkin(getTree(nodeDelegate)));
    }

    public void checkout(@Nonnull NodeDelegate nodeDelegate)
            throws RepositoryException {
        versionManager.checkout(getTree(nodeDelegate));
    }

    public boolean isCheckedOut(@Nonnull NodeDelegate nodeDelegate)
            throws RepositoryException {
        return versionManager.isCheckedOut(getTree(nodeDelegate));
    }

    @Nonnull
    public VersionHistoryDelegate getVersionHistory(@Nonnull NodeDelegate nodeDelegate)
            throws RepositoryException {
        Tree vh = versionManager.getVersionHistory(getTree(nodeDelegate));
        if (vh == null) {
            throw new UnsupportedRepositoryOperationException("Node does not" +
                    " have a version history: " + nodeDelegate.getPath());
        }
        return new VersionHistoryDelegate(sessionDelegate, vh);
    }

    @Nonnull
    public VersionDelegate getBaseVersion(@Nonnull NodeDelegate nodeDelegate)
            throws RepositoryException {
        Tree v = versionManager.getBaseVersion(getTree(nodeDelegate));
        if (v == null) {
            throw new UnsupportedRepositoryOperationException("Node does not" +
                    " have a base version: " + nodeDelegate.getPath());
        }
        return VersionDelegate.create(sessionDelegate, v);
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
        Tree t = checkNotNull(nodeDelegate).getLocation().getTree();
        if (t == null) {
            throw new InvalidItemStateException("Node does not exist: " +
                    nodeDelegate.getPath());
        }
        return t;
    }
}
