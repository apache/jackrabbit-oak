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

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;

import java.security.Principal;

import javax.jcr.RepositoryException;
import javax.security.auth.Subject;

import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.jetbrains.annotations.NotNull;

/**
 * This implementation of {@code Impersonation} delegates back to a
 * delegatee wrapping each call into a {@link SessionOperation} closure.
 *
 * @see SessionDelegate#perform(SessionOperation)
 */
final class ImpersonationDelegator implements Impersonation {
    
    private final SessionDelegate sessionDelegate;
    private final Impersonation impersonationDelegate;

    private ImpersonationDelegator(@NotNull SessionDelegate sessionDelegate, @NotNull Impersonation impersonationDelegate) {
        checkArgument(!(impersonationDelegate instanceof ImpersonationDelegator));
        this.sessionDelegate = sessionDelegate;
        this.impersonationDelegate = impersonationDelegate;
    }

    @NotNull
    static Impersonation wrap(@NotNull SessionDelegate sessionDelegate, @NotNull Impersonation impersonation) {
        return new ImpersonationDelegator(sessionDelegate, impersonation);
    }

    @NotNull
    @Override
    public PrincipalIterator getImpersonators() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<PrincipalIterator>("getImpersonators") {
            @NotNull
            @Override
            public PrincipalIterator perform() throws RepositoryException {
                return impersonationDelegate.getImpersonators();
            }
        });
    }

    @Override
    public boolean grantImpersonation(@NotNull final Principal principal) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("grantImpersonation", true) {
            @NotNull
            @Override
            public Boolean perform() throws RepositoryException {
                return impersonationDelegate.grantImpersonation(principal);
            }
        });
    }

    @Override
    public boolean revokeImpersonation(@NotNull final Principal principal) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("revokeImpersonation", true) {
            @NotNull
            @Override
            public Boolean perform() throws RepositoryException {
                return impersonationDelegate.revokeImpersonation(principal);
            }
        });
    }

    @Override
    public boolean allows(@NotNull final Subject subject) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("allows") {
            @NotNull
            @Override
            public Boolean perform() throws RepositoryException {
                return impersonationDelegate.allows(subject);
            }
        });
    }
}
