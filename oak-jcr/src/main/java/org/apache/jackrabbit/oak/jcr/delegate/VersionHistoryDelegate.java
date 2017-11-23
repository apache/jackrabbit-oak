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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.RepositoryException;
import javax.jcr.version.LabelExistsVersionException;
import javax.jcr.version.VersionException;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_BASEVERSION;

/**
 * {@code VersionHistoryDelegate}...
 */
public class VersionHistoryDelegate extends NodeDelegate {

    VersionHistoryDelegate(@Nonnull SessionDelegate sessionDelegate,
                           @Nonnull Tree vhTree) {
        super(sessionDelegate, checkNotNull(vhTree));
    }

    public String getVersionableIdentifier() throws InvalidItemStateException {
        return getTree().getProperty(JcrConstants.JCR_VERSIONABLEUUID).getValue(Type.STRING);
    }

    @Nonnull
    public VersionDelegate getRootVersion() throws RepositoryException {
        Tree rootVersion = getTree().getChild(VersionConstants.JCR_ROOTVERSION);
        if (!rootVersion.exists()) {
            throw new RepositoryException("Inconsistent version storage. " +
                    "VersionHistory does not have a root version");
        }
        return VersionDelegate.create(sessionDelegate, rootVersion);
    }

    /**
     * Gets the version with the given name.
     *
     * @param versionName a version name.
     * @return the version delegate.
     * @throws VersionException if there is no version with the given name.
     * @throws RepositoryException if another error occurs.
     */
    @Nonnull
    public VersionDelegate getVersion(@Nonnull String versionName)
            throws VersionException, RepositoryException {
        checkNotNull(versionName);
        Tree version = getTree().getChild(versionName);
        if (!version.exists()) {
            throw new VersionException("No such Version: " + versionName);
        }
        return VersionDelegate.create(sessionDelegate, version);
    }

    @Nonnull
    public VersionDelegate getVersionByLabel(@Nonnull String label)
            throws VersionException, RepositoryException {
        checkNotNull(label);
        Tree versionLabels = getVersionLabelsTree();
        PropertyState p = versionLabels.getProperty(label);
        if (p == null) {
            throw new VersionException("Unknown label: " + label);
        }
        String id = p.getValue(Type.REFERENCE);
        Tree version = sessionDelegate.getIdManager().getTree(id);
        if (version == null || !version.exists()) {
            throw new VersionException("Invalid label: " + label + '(' + id + ')');
        }
        return VersionDelegate.create(sessionDelegate, version);
    }

    @Nonnull
    public Iterable<String> getVersionLabels() throws RepositoryException {
        Tree versionLabels = getVersionLabelsTree();
        List<String> labels = new ArrayList<String>();
        for (PropertyState p : versionLabels.getProperties()) {
            if (p.getType() == Type.REFERENCE) {
                labels.add(p.getName());
            }
        }
        return labels;
    }

    @Nonnull
    public Iterable<String> getVersionLabels(@Nonnull String identifier)
            throws RepositoryException {
        checkNotNull(identifier);
        Tree versionLabels = getVersionLabelsTree();
        List<String> labels = new ArrayList<String>();
        for (PropertyState p : versionLabels.getProperties()) {
            if (p.getType() == Type.REFERENCE
                    && identifier.equals(p.getValue(Type.REFERENCE))) {
                labels.add(p.getName());
            }
        }
        return labels;
    }

    @Nonnull
    public Iterator<VersionDelegate> getAllVersions() throws RepositoryException {
        List<NodeDelegate> versions = new ArrayList<NodeDelegate>();
        for (Iterator<NodeDelegate> it = getChildren(); it.hasNext();) {
            NodeDelegate n = it.next();
            String primaryType = n.getProperty(JcrConstants.JCR_PRIMARYTYPE).getString();
            if (primaryType.equals(VersionConstants.NT_VERSION)) {
                versions.add(n);
            }
        }
        // best-effort sort by created time stamp, see JCR 2.0, 15.1.1.2
        Collections.sort(versions, new Comparator<NodeDelegate>() {
            @Override
            public int compare(NodeDelegate n1, NodeDelegate n2) {
                try {
                    PropertyDelegate c1 = n1.getPropertyOrNull(JcrConstants.JCR_CREATED);
                    PropertyDelegate c2 = n2.getPropertyOrNull(JcrConstants.JCR_CREATED);
                    if (c1 != null && c2 != null) {
                        return c1.getDate().compareTo(c2.getDate());
                    } else if (c1 != null) {
                        return 1;
                    } else if (c2 != null) {
                        return -1;
                    } else {
                        return 0;
                    }
                } catch (RepositoryException ex) {
                    // best effort
                    return 0;
                }
            }
        });
        final Tree thisTree = getTree();
        return Iterators.transform(versions.iterator(), new Function<NodeDelegate, VersionDelegate>() {
            @Override
            public VersionDelegate apply(NodeDelegate nd) {
                return VersionDelegate.create(sessionDelegate, thisTree.getChild(nd.getName()));
            }
        });
    }

    @Nonnull
    public Iterator<VersionDelegate> getAllLinearVersions()
            throws RepositoryException {
        String id = getVersionableIdentifier();
        NodeDelegate versionable = sessionDelegate.getNodeByIdentifier(id);
        if (versionable == null
                || versionable.getPropertyOrNull(JCR_BASEVERSION) == null) {
            return Iterators.emptyIterator();
        }
        Deque<VersionDelegate> linearVersions = new ArrayDeque<VersionDelegate>();
        VersionManagerDelegate vMgr = VersionManagerDelegate.create(sessionDelegate);
        VersionDelegate version = vMgr.getVersionByIdentifier(
                versionable.getProperty(JCR_BASEVERSION).getString());
        while (version != null) {
            linearVersions.add(version);
            version = version.getLinearPredecessor();
        }
        return linearVersions.descendingIterator();
    }

    public void addVersionLabel(@Nonnull VersionDelegate version,
                                @Nonnull String oakVersionLabel,
                                boolean moveLabel)
            throws LabelExistsVersionException, VersionException, RepositoryException {
        VersionManagerDelegate vMgr = VersionManagerDelegate.create(sessionDelegate);
        vMgr.addVersionLabel(this, version, oakVersionLabel, moveLabel);
    }

    public void removeVersionLabel(@Nonnull String oakVersionLabel)
            throws VersionException, RepositoryException {
        VersionManagerDelegate vMgr = VersionManagerDelegate.create(sessionDelegate);
        vMgr.removeVersionLabel(this, oakVersionLabel);
    }

    public void removeVersion(@Nonnull String oakVersionName) throws RepositoryException {
        VersionManagerDelegate vMgr = VersionManagerDelegate.create(sessionDelegate);
        vMgr.removeVersion(this, oakVersionName);
    }

    //-----------------------------< internal >---------------------------------

    /**
     * @return the jcr:versionLabels tree or throws a {@code RepositoryException}
     *         if it doesn't exist.
     * @throws RepositoryException if the jcr:versionLabels child does not
     *                             exist.
     */
    @Nonnull
    private Tree getVersionLabelsTree() throws RepositoryException {
        Tree versionLabels = getTree().getChild(VersionConstants.JCR_VERSIONLABELS);
        if (!versionLabels.exists()) {
            throw new RepositoryException("Inconsistent version storage. " +
                    "VersionHistory does not have jcr:versionLabels child node");
        }
        return versionLabels;
    }
}
