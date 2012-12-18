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
import java.util.Arrays;
import java.util.List;

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
import org.apache.jackrabbit.commons.iterator.VersionIteratorAdapter;
import org.apache.jackrabbit.oak.jcr.NodeImpl;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.util.TODO;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

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
        return new VersionImpl(dlg.getRootVersion());
    }

    @Override
    public VersionIterator getAllLinearVersions() throws RepositoryException {
        return TODO.unimplemented().returnValue(null);
    }

    @Override
    public VersionIterator getAllVersions() throws RepositoryException {
        return new VersionIteratorAdapter(Iterators.transform(
                dlg.getAllVersions(), new Function<VersionDelegate, Version>() {
            @Override
            public Version apply(VersionDelegate input) {
                return new VersionImpl(input);
            }
        }));
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
        return new VersionImpl(dlg.getVersion(versionName));
    }

    @Override
    public Version getVersionByLabel(String label)
            throws VersionException, RepositoryException {
        String oakLabel = sessionDelegate.getOakNameOrThrow(label);
        return new VersionImpl(dlg.getVersionByLabel(oakLabel));
    }

    @Override
    public void addVersionLabel(String versionName,
                                String label,
                                boolean moveLabel)
            throws LabelExistsVersionException, VersionException,
            RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public void removeVersionLabel(String label)
            throws VersionException, RepositoryException {
        TODO.unimplemented().doNothing();
    }

    @Override
    public boolean hasVersionLabel(String label) throws RepositoryException {
        return Arrays.asList(getVersionLabels()).contains(label);
    }

    @Override
    public boolean hasVersionLabel(Version version, String label)
            throws VersionException, RepositoryException {
        return Arrays.asList(getVersionLabels(version)).contains(label);
    }

    @Override
    public String[] getVersionLabels() throws RepositoryException {
        NamePathMapper mapper = sessionDelegate.getNamePathMapper();
        List<String> labels = new ArrayList<String>();
        for (String label : dlg.getVersionLabels()) {
            labels.add(mapper.getJcrName(label));
        }
        return labels.toArray(new String[labels.size()]);
    }

    @Override
    public String[] getVersionLabels(Version version)
            throws VersionException, RepositoryException {
        if (!version.getContainingHistory().getPath().equals(getPath())) {
            throw new VersionException("Version is not contained in this " +
                    "VersionHistory");
        }
        NamePathMapper mapper = sessionDelegate.getNamePathMapper();
        List<String> labels = new ArrayList<String>();
        for (String label : dlg.getVersionLabels(version.getIdentifier())) {
            labels.add(mapper.getJcrName(label));
        }
        return labels.toArray(new String[labels.size()]);
    }

    @Override
    public void removeVersion(String versionName)
            throws ReferentialIntegrityException, AccessDeniedException,
            UnsupportedRepositoryOperationException, VersionException,
            RepositoryException {
        TODO.unimplemented().doNothing();
    }
}
