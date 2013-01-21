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
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.RepositoryException;
import javax.jcr.version.VersionException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.jcr.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.PropertyDelegate;
import org.apache.jackrabbit.oak.jcr.SessionDelegate;
import org.apache.jackrabbit.oak.version.VersionConstants;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <code>VersionHistoryDelegate</code>...
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
        if (rootVersion == null) {
            throw new RepositoryException("Inconsistent version storage. " +
                    "VersionHistory does not have a root version");
        }
        return VersionDelegate.create(sessionDelegate, rootVersion);
    }

    @Nonnull
    public VersionDelegate getVersion(@Nonnull String versionName)
            throws VersionException, RepositoryException {
        checkNotNull(versionName);
        Tree version = getTree().getChild(versionName);
        if (version == null) {
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
        if (version == null) {
            throw new VersionException("Invalid label: " + label + "(" + id + ")");
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
    public Iterator<VersionDelegate> getAllVersions()
            throws RepositoryException {
        SortedMap<Calendar, String> versions = new TreeMap<Calendar, String>();
        for (Iterator<NodeDelegate> it = getChildren(); it.hasNext(); ) {
            NodeDelegate n = it.next();
            String primaryType = n.getProperty(JcrConstants.JCR_PRIMARYTYPE).getValue().getString();
            if (primaryType.equals(VersionConstants.NT_VERSION)) {
                PropertyDelegate created = n.getProperty(JcrConstants.JCR_CREATED);
                if (created != null) {
                    versions.put(created.getValue().getDate(), n.getName());
                }
            }
        }
        final Tree thisTree = getTree();
        return Iterators.transform(versions.values().iterator(), new Function<String, VersionDelegate>() {
            @Override
            public VersionDelegate apply(String name) {
                return VersionDelegate.create(sessionDelegate, thisTree.getChild(name));
            }
        });
    }

    //-----------------------------< internal >---------------------------------

    /**
     * @return the jcr:versionLabels tree or throws a <code>RepositoryException</code>
     *         if it doesn't exist.
     * @throws RepositoryException if the jcr:versionLabels child does not
     *                             exist.
     */
    @Nonnull
    private Tree getVersionLabelsTree() throws RepositoryException {
        Tree versionLabels = getTree().getChild(VersionConstants.JCR_VERSIONLABELS);
        if (versionLabels == null) {
            throw new RepositoryException("Inconsistent version storage. " +
                    "VersionHistory does not have jcr:versionLabels child node");
        }
        return versionLabels;
    }
}
