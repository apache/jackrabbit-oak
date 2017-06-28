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
import javax.jcr.AccessDeniedException;
import javax.jcr.NamespaceException;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;

/**
 * This implementation of {@code PrivilegeManager} delegates back to a
 * delegatee wrapping each call into a {@link SessionOperation} closure.
 *
 * @see SessionDelegate#perform(SessionOperation)
 */
public class PrivilegeManagerDelegator implements PrivilegeManager {
    private final PrivilegeManager pm;
    private final SessionDelegate delegate;

    public PrivilegeManagerDelegator(SessionDelegate delegate, PrivilegeManager pm) {
        this.pm = pm;
        this.delegate = delegate;
    }

    @Override
    public Privilege[] getRegisteredPrivileges() throws RepositoryException {
        return delegate.perform(new SessionOperation<Privilege[]>("getRegisteredPrivileges") {
            @Nonnull
            @Override
            public Privilege[] perform() throws RepositoryException {
                return pm.getRegisteredPrivileges();
            }
        });
    }

    @Override
    public Privilege getPrivilege(final String privilegeName) throws AccessControlException, RepositoryException {
        return delegate.perform(new SessionOperation<Privilege>("getPrivilege") {
            @Nonnull
            @Override
            public Privilege perform() throws RepositoryException {
                return pm.getPrivilege(privilegeName);
            }
        });
    }

    @Override
    public Privilege registerPrivilege(final String privilegeName, final boolean isAbstract, final String[] declaredAggregateNames) throws AccessDeniedException, NamespaceException, RepositoryException {
        return delegate.perform(new SessionOperation<Privilege>("registerPrivilege", true) {
            @Nonnull
            @Override
            public Privilege perform() throws RepositoryException {
                return pm.registerPrivilege(privilegeName, isAbstract, declaredAggregateNames);
            }
        });
    }
}
