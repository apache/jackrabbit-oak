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
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.jcr.session.NodeImpl;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.jcr.delegate.PropertyDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.VersionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.VersionHistoryDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.VersionManagerDelegate;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;

public class VersionImpl extends NodeImpl<VersionDelegate> implements Version {

    public VersionImpl(VersionDelegate dlg, SessionContext sessionContext) {
        super(dlg, sessionContext);
    }

    @Override
    public VersionHistory getContainingHistory() throws RepositoryException {
        return perform(new SessionOperation<VersionHistory>("getContainingHistory") {
            @Override
            public VersionHistory perform() throws RepositoryException {
                return new VersionHistoryImpl(
                        getVersionManagerDelegate().createVersionHistory(
                                dlg.getParent()), sessionContext);
            }
        });
    }

    @Override
    public Calendar getCreated() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Calendar>("getCreated") {
            @Override
            public Calendar perform() throws RepositoryException {
                PropertyDelegate dlg = getPropertyOrThrow(JcrConstants.JCR_CREATED);
                return ValueFactoryImpl.createValue(dlg.getSingleState(), sessionContext).getDate();
            }
        });
    }

    @Override
    public Version getLinearPredecessor() throws RepositoryException {
        return perform(new SessionOperation<Version>("getLinearPredecessor") {
            @Override
            public Version perform() throws RepositoryException {
                VersionDelegate predecessor = dlg.getLinearPredecessor();
                if (predecessor == null) {
                    return null;
                } else {
                    return new VersionImpl(predecessor, sessionContext);
                }
            }
        });
    }

    @Override
    public Version getLinearSuccessor() throws RepositoryException {
        return perform(new SessionOperation<Version>("getLinearSuccessor") {
            @Override
            public Version perform() throws RepositoryException {
                VersionHistoryDelegate vHistory = getVersionManagerDelegate()
                        .createVersionHistory(dlg.getParent());
                Iterator<VersionDelegate> it = vHistory.getAllLinearVersions();
                // search for this version ...
                while (it.hasNext()) {
                    VersionDelegate vDlg = it.next();
                    if (vDlg.getIdentifier().equals(dlg.getIdentifier())
                            && it.hasNext()) {
                        // ... and return next
                        return new VersionImpl(it.next(), sessionContext);
                    }
                }
                // none found
                return null;
            }
        });
    }

    private List<Value> getValues(PropertyDelegate p) throws InvalidItemStateException, ValueFormatException {
        return ValueFactoryImpl.createValues(p.getMultiState(), sessionContext);
    }

    @Override
    public Version[] getPredecessors() throws RepositoryException {
        return perform(new SessionOperation<Version[]>("getPredecessors") {
            @Override
            public Version[] perform() throws RepositoryException {
                List<Version> predecessors = new ArrayList<Version>();
                for (VersionDelegate vDelegate : dlg.getPredecessors()) {
                    predecessors.add(new VersionImpl(vDelegate, sessionContext));
                }
                return predecessors.toArray(new Version[predecessors.size()]);
            }
        });
    }

    @Override
    public Version[] getSuccessors() throws RepositoryException {
        return perform(new SessionOperation<Version[]>("getSuccessors") {
            @Override
            public Version[] perform() throws RepositoryException {
                PropertyDelegate p = getPropertyOrThrow(VersionConstants.JCR_SUCCESSORS);
                List<Version> successors = new ArrayList<Version>();
                VersionManagerDelegate vMgr = getVersionManagerDelegate();
                for (Value v : getValues(p)) {
                    String id = v.getString();
                    successors.add(new VersionImpl(vMgr.getVersionByIdentifier(id), sessionContext));
                }
                return successors.toArray(new Version[successors.size()]);
            }
        });
    }

    @Override
    public Node getFrozenNode() throws RepositoryException {
        return perform(new SessionOperation<Node>("getFrozenNode") {
            @Override
            public Node perform() throws RepositoryException {
                return NodeImpl.createNodeOrNull(
                        dlg.getChild(VersionConstants.JCR_FROZENNODE),
                        sessionContext);
            }
        });
    }

    //------------------------------< internal >--------------------------------

    @Nonnull
    private VersionManagerDelegate getVersionManagerDelegate() {
        return VersionManagerDelegate.create(sessionContext.getSessionDelegate());
    }

    @Nonnull
    private PropertyDelegate getPropertyOrThrow(@Nonnull String name)
            throws RepositoryException {
        PropertyDelegate p = dlg.getPropertyOrNull(checkNotNull(name));
        if (p == null) {
            throw new RepositoryException("Inconsistent version storage. " +
                    "Version does not have a " + name + " property.");
        }
        return p;
    }
}
