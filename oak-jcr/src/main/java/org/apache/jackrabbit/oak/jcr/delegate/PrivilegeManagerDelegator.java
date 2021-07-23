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

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.RepositoryException;
import javax.jcr.security.Privilege;

/**
 * This implementation of {@code PrivilegeManager} delegates back to a
 * delegatee wrapping each call into a {@link SessionOperation} closure.
 *
 * @see SessionDelegate#perform(SessionOperation)
 */
public class PrivilegeManagerDelegator implements PrivilegeManager {
    
    private final PrivilegeManager pm;
    private final SessionDelegate delegate;

    public PrivilegeManagerDelegator(@NotNull SessionDelegate delegate, @NotNull PrivilegeManager pm) {
        this.pm = pm;
        this.delegate = delegate;
    }

    @NotNull
    @Override
    public Privilege[] getRegisteredPrivileges() throws RepositoryException {
        return delegate.perform(new SessionOperation<Privilege[]>("getRegisteredPrivileges") {
            @Override
            public Privilege @NotNull [] perform() throws RepositoryException {
                return pm.getRegisteredPrivileges();
            }
        });
    }

    @NotNull
    @Override
    public Privilege getPrivilege(@NotNull final String privilegeName) throws RepositoryException {
        return delegate.perform(new SessionOperation<Privilege>("getPrivilege") {
            @NotNull
            @Override
            public Privilege perform() throws RepositoryException {
                return pm.getPrivilege(privilegeName);
            }
        });
    }

    @NotNull
    @Override
    public Privilege registerPrivilege(@NotNull final String privilegeName, final boolean isAbstract, @Nullable final String[] declaredAggregateNames) throws RepositoryException {
        return delegate.perform(new SessionOperation<Privilege>("registerPrivilege", true) {
            @NotNull
            @Override
            public Privilege perform() throws RepositoryException {
                return pm.registerPrivilege(privilegeName, isAbstract, declaredAggregateNames);
            }
        });
    }
}
