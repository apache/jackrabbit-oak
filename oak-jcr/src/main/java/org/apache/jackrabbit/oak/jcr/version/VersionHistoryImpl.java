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

import javax.jcr.AccessDeniedException;
import javax.jcr.NodeIterator;
import javax.jcr.ReferentialIntegrityException;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.version.LabelExistsVersionException;
import javax.jcr.version.Version;
import javax.jcr.version.VersionException;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionIterator;

import org.apache.jackrabbit.commons.iterator.FrozenNodeIteratorAdapter;
import org.apache.jackrabbit.oak.jcr.NodeImpl;
import org.apache.jackrabbit.oak.util.TODO;

/**
 * <code>VersionHistoryImpl</code>...
 */
public class VersionHistoryImpl extends NodeImpl<VersionHistoryDelegate>
        implements VersionHistory {

    public VersionHistoryImpl(VersionHistoryDelegate dlg) {
        super(dlg);
    }

    @Override
    public String getVersionableUUID() throws RepositoryException {
        return getVersionableIdentifier();
    }

    @Override
    public String getVersionableIdentifier() throws RepositoryException {
        return dlg.getVersionableIdentifier();
    }

    @Override
    public Version getRootVersion() throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public VersionIterator getAllLinearVersions() throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public VersionIterator getAllVersions() throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public NodeIterator getAllLinearFrozenNodes() throws RepositoryException {
        return new FrozenNodeIteratorAdapter(getAllLinearVersions());
    }

    @Override
    public NodeIterator getAllFrozenNodes() throws RepositoryException {
        return new FrozenNodeIteratorAdapter(getAllVersions());
    }

    @Override
    public Version getVersion(String versionName)
            throws VersionException, RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public Version getVersionByLabel(String label)
            throws VersionException, RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public void addVersionLabel(String versionName,
                                String label,
                                boolean moveLabel)
            throws LabelExistsVersionException, VersionException,
            RepositoryException {
        TODO.unimplemented();
    }

    @Override
    public void removeVersionLabel(String label)
            throws VersionException, RepositoryException {
        TODO.unimplemented();
    }

    @Override
    public boolean hasVersionLabel(String label) throws RepositoryException {
        return TODO.unimplemented().returnValue(Boolean.FALSE);
    }

    @Override
    public boolean hasVersionLabel(Version version, String label)
            throws VersionException, RepositoryException {
        return TODO.unimplemented().returnValue(Boolean.FALSE);
    }

    @Override
    public String[] getVersionLabels() throws RepositoryException {
        return TODO.unimplemented().returnValue(new String[0]);
    }

    @Override
    public String[] getVersionLabels(Version version)
            throws VersionException, RepositoryException {
        return TODO.unimplemented().returnValue(new String[0]);
    }

    @Override
    public void removeVersion(String versionName)
            throws ReferentialIntegrityException, AccessDeniedException,
            UnsupportedRepositoryOperationException, VersionException,
            RepositoryException {
        TODO.unimplemented();
    }
}
