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

import java.security.Principal;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;

/**
 * This implementation of {@code JackrabbitAccessControlManager} delegates back to a
 * delegatee wrapping each call into a {@link SessionOperation} closure.
 *
 * @see SessionDelegate#perform(SessionOperation)
 */
public class JackrabbitAccessControlManagerDelegator implements JackrabbitAccessControlManager {
    private final JackrabbitAccessControlManager jackrabbitACManager;
    private final SessionDelegate delegate;
    private final AccessControlManagerDelegator jcrACManager;

    public JackrabbitAccessControlManagerDelegator(SessionDelegate delegate,
            JackrabbitAccessControlManager acManager) {
        this.jackrabbitACManager = acManager;
        this.delegate = delegate;
        this.jcrACManager = new AccessControlManagerDelegator(delegate, acManager);
    }

    @Override
    public JackrabbitAccessControlPolicy[] getApplicablePolicies(final Principal principal)
            throws RepositoryException {
        return delegate.perform(new SessionOperation<JackrabbitAccessControlPolicy[]>("getApplicablePolicies") {
            @Nonnull
            @Override
            public JackrabbitAccessControlPolicy[] perform() throws RepositoryException {
                return jackrabbitACManager.getApplicablePolicies(principal);
            }
        });
    }

    @Override
    public JackrabbitAccessControlPolicy[] getPolicies(final Principal principal)
            throws RepositoryException {
        return delegate.perform(new SessionOperation<JackrabbitAccessControlPolicy[]>("getPolicies") {
            @Nonnull
            @Override
            public JackrabbitAccessControlPolicy[] perform() throws RepositoryException {
                return jackrabbitACManager.getPolicies(principal);
            }
        });
    }

    @Override
    public AccessControlPolicy[] getEffectivePolicies(final Set<Principal> principals)
            throws RepositoryException {
        return delegate.perform(new SessionOperation<AccessControlPolicy[]>("getEffectivePolicies") {
            @Nonnull
            @Override
            public AccessControlPolicy[] perform() throws RepositoryException {
                return jackrabbitACManager.getEffectivePolicies(principals);
            }
        });
    }

    @Override
    public boolean hasPrivileges(final String absPath, final Set<Principal> principals,
            final Privilege[] privileges) throws RepositoryException {
        return delegate.perform(new SessionOperation<Boolean>("hasPrivileges") {
            @Nonnull
            @Override
            public Boolean perform() throws RepositoryException {
                return jackrabbitACManager.hasPrivileges(absPath, principals, privileges);
            }
        });
    }

    @Override
    public Privilege[] getPrivileges(final String absPath, final Set<Principal> principals)
            throws RepositoryException {
        return delegate.perform(new SessionOperation<Privilege[]>("getPrivileges") {
            @Nonnull
            @Override
            public Privilege[] perform() throws RepositoryException {
                return jackrabbitACManager.getPrivileges(absPath, principals);
            }
        });
    }

    @Override
    public Privilege[] getSupportedPrivileges(String absPath) throws RepositoryException {
        return jcrACManager.getSupportedPrivileges(absPath);
    }

    @Override
    public Privilege privilegeFromName(String privilegeName) throws RepositoryException {
        return jcrACManager.privilegeFromName(privilegeName);
    }

    @Override
    public boolean hasPrivileges(String absPath, Privilege[] privileges)
            throws RepositoryException {
        return jcrACManager.hasPrivileges(absPath, privileges);
    }

    @Override
    public Privilege[] getPrivileges(String absPath) throws RepositoryException {
        return jcrACManager.getPrivileges(absPath);
    }

    @Override
    public AccessControlPolicy[] getPolicies(String absPath) throws RepositoryException {
        return jcrACManager.getPolicies(absPath);
    }

    @Override
    public AccessControlPolicy[] getEffectivePolicies(String absPath) throws RepositoryException {
        return jcrACManager.getEffectivePolicies(absPath);
    }

    @Override
    public AccessControlPolicyIterator getApplicablePolicies(String absPath)
            throws RepositoryException {
        return jcrACManager.getApplicablePolicies(absPath);
    }

    @Override
    public void setPolicy(String absPath, AccessControlPolicy policy) throws RepositoryException {
        jcrACManager.setPolicy(absPath, policy);
    }

    @Override
    public void removePolicy(String absPath, AccessControlPolicy policy)
            throws RepositoryException {
        jcrACManager.removePolicy(absPath, policy);
    }
}
