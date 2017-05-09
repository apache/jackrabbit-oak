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

import static com.google.common.collect.Iterators.transform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;
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

import com.google.common.base.Function;
import org.apache.jackrabbit.commons.iterator.FrozenNodeIteratorAdapter;
import org.apache.jackrabbit.commons.iterator.VersionIteratorAdapter;
import org.apache.jackrabbit.oak.jcr.delegate.VersionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.VersionHistoryDelegate;
import org.apache.jackrabbit.oak.jcr.session.NodeImpl;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;

/**
 * {@code VersionHistoryImpl}...
 */
public class VersionHistoryImpl extends NodeImpl<VersionHistoryDelegate>
        implements VersionHistory {

    public VersionHistoryImpl(VersionHistoryDelegate dlg, SessionContext sessionContext) {
        super(dlg, sessionContext);
    }

    @Override
    public String getVersionableUUID() throws RepositoryException {
        return getVersionableIdentifier();
    }

    @Override
    public String getVersionableIdentifier() throws RepositoryException {
        return perform(new SessionOperation<String>("getVersionableIdentifier") {
            @Nonnull
            @Override
            public String perform() throws RepositoryException {
                return dlg.getVersionableIdentifier();
            }
        });
    }

    @Override
    public Version getRootVersion() throws RepositoryException {
        return perform(new SessionOperation<Version>("getRootVersion") {
            @Nonnull
            @Override
            public Version perform() throws RepositoryException {
                return new VersionImpl(dlg.getRootVersion(), sessionContext);
            }
        });
    }

    @Override
    public VersionIterator getAllLinearVersions() throws RepositoryException {
        return perform(new SessionOperation<VersionIterator>("getAllLinearVersions") {
            @Nonnull
            @Override
            public VersionIterator perform() throws RepositoryException {
                Iterator<Version> versions = transform(dlg.getAllLinearVersions(),
                        new Function<VersionDelegate, Version>() {
                            @Override
                            public Version apply(VersionDelegate input) {
                                return new VersionImpl(input, sessionContext);
                            }
                        });
                return new VersionIteratorAdapter(sessionDelegate.sync(versions));
            }
        });
    }

    @Override
    public VersionIterator getAllVersions() throws RepositoryException {
        return perform(new SessionOperation<VersionIterator>("getAllVersions") {
            @Nonnull
            @Override
            public VersionIterator perform() throws RepositoryException {
                Iterator<Version> versions = transform(dlg.getAllVersions(),
                        new Function<VersionDelegate, Version>() {
                    @Override
                    public Version apply(VersionDelegate input) {
                        return new VersionImpl(input, sessionContext);
                    }
                });
                return new VersionIteratorAdapter(sessionDelegate.sync(versions));
            }
        });
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
    public Version getVersion(final String versionName)
            throws VersionException, RepositoryException {
        return perform(new SessionOperation<Version>("getVersion") {
            @Nonnull
            @Override
            public Version perform() throws RepositoryException {
                return new VersionImpl(dlg.getVersion(versionName), sessionContext);
            }
        });
    }

    @Override
    public Version getVersionByLabel(final String label)
            throws VersionException, RepositoryException {
        return perform(new SessionOperation<Version>("getVersionByLabel") {
            @Nonnull
            @Override
            public Version perform() throws RepositoryException {
                String oakLabel = sessionContext.getOakName(label);
                return new VersionImpl(dlg.getVersionByLabel(oakLabel), sessionContext);
            }
        });
    }

    @Override
    public void addVersionLabel(final String versionName,
                                final String label,
                                final boolean moveLabel)
            throws LabelExistsVersionException, VersionException,
            RepositoryException {
        sessionDelegate.performVoid(new SessionOperation<Void>("addVersionLabel", true) {
            @Override
            public void performVoid() throws RepositoryException {
                String oakLabel = sessionContext.getOakName(label);
                // will throw VersionException if version does not exist
                VersionDelegate version = dlg.getVersion(versionName);
                dlg.addVersionLabel(version, oakLabel, moveLabel);
            }
        });
    }

    @Override
    public void removeVersionLabel(final String label)
            throws VersionException, RepositoryException {
        sessionDelegate.performVoid(new SessionOperation<Void>("removeVersionLabel", true) {
            @Override
            public void performVoid() throws RepositoryException {
                String oakLabel = sessionContext.getOakName(label);
                dlg.removeVersionLabel(oakLabel);
            }
        });
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
        return perform(new SessionOperation<String[]>("getVersionLabels") {
            @Nonnull
            @Override
            public String[] perform() throws RepositoryException {
                List<String> labels = new ArrayList<String>();
                for (String label : dlg.getVersionLabels()) {
                    labels.add(sessionContext.getJcrName(label));
                }
                return labels.toArray(new String[labels.size()]);
            }
        });
    }

    @Override
    public String[] getVersionLabels(final Version version)
            throws VersionException, RepositoryException {
        if (!version.getContainingHistory().getPath().equals(getPath())) {
            throw new VersionException("Version is not contained in this " +
                    "VersionHistory");
        }
        return perform(new SessionOperation<String[]>("getVersionLabels") {
            @Nonnull
            @Override
            public String[] perform() throws RepositoryException {
                List<String> labels = new ArrayList<String>();
                for (String label : dlg.getVersionLabels(version.getIdentifier())) {
                    labels.add(sessionContext.getJcrName(label));
                }
                return labels.toArray(new String[labels.size()]);
            }
        });
    }

    @Override
    public void removeVersion(final String versionName)
            throws ReferentialIntegrityException, AccessDeniedException,
            UnsupportedRepositoryOperationException, VersionException,
            RepositoryException {

        sessionDelegate.performVoid(new SessionOperation<Void>("removeVersion", true) {
            @Override
            public void performVoid() throws RepositoryException {
                String oakName = sessionContext.getOakName(versionName);
                dlg.removeVersion(oakName);
            }
        });
    }
}
