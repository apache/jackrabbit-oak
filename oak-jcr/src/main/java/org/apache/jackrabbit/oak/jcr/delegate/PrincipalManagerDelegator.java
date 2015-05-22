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

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;

/**
 * This implementation of {@code PrincipalManager} delegates back to a
 * delegatee wrapping each call into a {@link SessionOperation} closure.
 *
 * @see SessionDelegate#perform(SessionOperation)
 */
public class PrincipalManagerDelegator implements PrincipalManager {
    private final SessionDelegate delegate;
    private final PrincipalManager principalManager;

    public PrincipalManagerDelegator(SessionDelegate delegate,
            PrincipalManager principalManager) {
        this.principalManager = principalManager;
        this.delegate = delegate;
    }

    @Override
    public boolean hasPrincipal(final String principalName) {
        return delegate.safePerform(new SessionOperation<Boolean>("hasPrincipal") {
            @Nonnull
            @Override
            public Boolean perform() {
                return principalManager.hasPrincipal(principalName);
            }
        });
    }

    @Override
    public Principal getPrincipal(final String principalName) {
        try {
            return delegate.performNullable(new SessionOperation<Principal>("getPrincipal") {
                @Override
                public Principal performNullable() {
                    return principalManager.getPrincipal(principalName);
                }
            });
        } catch (RepositoryException e) {
            throw new RuntimeException("Unexpected exception thrown by operation 'getPrincipal'", e);
        }
    }

    @Override
    public PrincipalIterator findPrincipals(final String simpleFilter) {
        return delegate.safePerform(new SessionOperation<PrincipalIterator>("findPrincipals") {
            @Nonnull
            @Override
            public PrincipalIterator perform() {
                return principalManager.findPrincipals(simpleFilter);
            }
        });
    }

    @Override
    public PrincipalIterator findPrincipals(final String simpleFilter, final int searchType) {
        return delegate.safePerform(new SessionOperation<PrincipalIterator>("findPrincipals") {
            @Nonnull
            @Override
            public PrincipalIterator perform() {
                return principalManager.findPrincipals(simpleFilter, searchType);
            }
        });
    }

    @Override
    public PrincipalIterator getPrincipals(final int searchType) {
        return delegate.safePerform(new SessionOperation<PrincipalIterator>("getPrincipals") {
            @Nonnull
            @Override
            public PrincipalIterator perform() {
                return principalManager.getPrincipals(searchType);
            }
        });
    }

    @Override
    public PrincipalIterator getGroupMembership(final Principal principal) {
        return delegate.safePerform(new SessionOperation<PrincipalIterator>("getGroupMembership") {
            @Nonnull
            @Override
            public PrincipalIterator perform() {
                return principalManager.getGroupMembership(principal);
            }
        });
    }

    @Override
    public Principal getEveryone() {
        return delegate.safePerform(new SessionOperation<Principal>("getEveryone") {
            @Nonnull
            @Override
            public Principal perform() {
                return principalManager.getEveryone();
            }
        });
    }
}
