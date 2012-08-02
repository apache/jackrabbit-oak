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

import javax.jcr.InvalidItemStateException;
import javax.jcr.ItemExistsException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.lock.LockException;
import javax.jcr.version.Version;
import javax.jcr.version.VersionException;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.commons.iterator.NodeIteratorAdapter;
import org.apache.jackrabbit.oak.jcr.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.SessionDelegate;
import org.apache.jackrabbit.oak.util.TODO;

public class VersionManagerImpl implements VersionManager {

    private final SessionDelegate sessionDelegate;

    public VersionManagerImpl(SessionDelegate sessionDelegate) {
        this.sessionDelegate = sessionDelegate;
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
    public void restore(
            String absPath, Version version, boolean removeExisting)
            throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public void restore(
            String absPath, String versionName, boolean removeExisting)
            throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public void restore(Version version, boolean removeExisting)
            throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public void restore(Version[] versions, boolean removeExisting)
            throws ItemExistsException,
            UnsupportedRepositoryOperationException, VersionException,
            LockException, InvalidItemStateException, RepositoryException {
        TODO.unimplemented().doNothing();
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

    @Override
    public boolean isCheckedOut(String absPath) throws RepositoryException {
        return TODO.unimplemented().returnValue(true);
    }

    @Override
    public VersionHistory getVersionHistory(String absPath)
            throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public Version getBaseVersion(String absPath) throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
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
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public void checkout(String absPath) throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public Version checkin(String absPath) throws RepositoryException {
        String oakPath = sessionDelegate.getOakPathOrThrowNotFound(absPath);
        NodeDelegate nodeDelegate = sessionDelegate.getNode(oakPath);
        return TODO.dummyImplementation().returnValue(
                new VersionImpl(nodeDelegate));
    }

    @Override
    public void cancelMerge(String absPath, Version version)
            throws RepositoryException {
        TODO.unimplemented().doNothing();
    }

}
