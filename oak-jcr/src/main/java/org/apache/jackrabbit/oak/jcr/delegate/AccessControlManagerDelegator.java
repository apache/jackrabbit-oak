/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.jcr.delegate;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;

/**
 * This implementation of {@code AccessControlManager} delegates back to a
 * delegatee wrapping each call into a {@link SessionOperation} closure.
 * 
 * @see SessionDelegate#perform(SessionOperation)
 */
public class AccessControlManagerDelegator implements AccessControlManager {
    private final SessionDelegate delegate;
    private final AccessControlManager acManager;

    public AccessControlManagerDelegator(SessionDelegate delegate, AccessControlManager acManager) {
        this.acManager = acManager;
        this.delegate = delegate;
    }

    @Override
    public Privilege[] getSupportedPrivileges(final String absPath) throws RepositoryException {
        return delegate.perform(new SessionOperation<Privilege[]>("getSupportedPrivileges") {
            @Nonnull
            @Override
            public Privilege[] perform() throws RepositoryException {
                return acManager.getSupportedPrivileges(absPath);
            }
        });
    }

    @Override
    public Privilege privilegeFromName(final String privilegeName) throws RepositoryException {
        return delegate.perform(new SessionOperation<Privilege>("privilegeFromName") {
            @Nonnull
            @Override
            public Privilege perform() throws RepositoryException {
                return acManager.privilegeFromName(privilegeName);
            }
        });
    }

    @Override
    public boolean hasPrivileges(final String absPath, final Privilege[] privileges)
            throws RepositoryException {
        return delegate.perform(new SessionOperation<Boolean>("hasPrivileges") {
            @Nonnull
            @Override
            public Boolean perform() throws RepositoryException {
                return acManager.hasPrivileges(absPath, privileges);
            }
        });
    }

    @Override
    public Privilege[] getPrivileges(final String absPath) throws RepositoryException {
        return delegate.perform(new SessionOperation<Privilege[]>("getPrivileges") {
            @Nonnull
            @Override
            public Privilege[] perform() throws RepositoryException {
                return acManager.getPrivileges(absPath);
            }
        });
    }

    @Override
    public AccessControlPolicy[] getPolicies(final String absPath) throws RepositoryException {
        return delegate.perform(new SessionOperation<AccessControlPolicy[]>("getPolicies") {
            @Nonnull
            @Override
            public AccessControlPolicy[] perform() throws RepositoryException {
                return acManager.getPolicies(absPath);
            }
        });
    }

    @Override
    public AccessControlPolicy[] getEffectivePolicies(final String absPath)
            throws RepositoryException {
        return delegate.perform(new SessionOperation<AccessControlPolicy[]>("getEffectivePolicies") {
            @Nonnull
            @Override
            public AccessControlPolicy[] perform() throws RepositoryException {
                return acManager.getEffectivePolicies(absPath);
            }
        });
    }

    @Override
    public AccessControlPolicyIterator getApplicablePolicies(final String absPath)
            throws RepositoryException {
        return delegate.perform(new SessionOperation<AccessControlPolicyIterator>("getApplicablePolicies") {
            @Nonnull
            @Override
            public AccessControlPolicyIterator perform() throws RepositoryException {
                return acManager.getApplicablePolicies(absPath);
            }
        });
    }

    @Override
    public void setPolicy(final String absPath, final AccessControlPolicy policy)
            throws RepositoryException {
        delegate.performVoid(new SessionOperation<Void>("setPolicy", true) {
            @Override
            public void performVoid() throws RepositoryException {
                acManager.setPolicy(absPath, policy);
            }
        });
    }

    @Override
    public void removePolicy(final String absPath, final AccessControlPolicy policy)
            throws RepositoryException {
        delegate.performVoid(new SessionOperation<Void>("removePolicy", true) {
            @Override
            public void performVoid() throws RepositoryException {
                acManager.removePolicy(absPath, policy);
            }
        });
    }
}
