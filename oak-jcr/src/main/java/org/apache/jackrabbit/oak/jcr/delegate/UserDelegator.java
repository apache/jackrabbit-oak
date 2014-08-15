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
import javax.jcr.Credentials;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;

/**
 * This implementation of {@code User} delegates back to a
 * delegatee wrapping each call into a {@link SessionOperation} closure.
 *
 * @see SessionDelegate#perform(SessionOperation)
 */
final class UserDelegator extends AuthorizableDelegator implements User {

    private UserDelegator(SessionDelegate sessionDelegate, User userDelegate) {
        super(sessionDelegate, userDelegate);
    }

    static User wrap(SessionDelegate sessionDelegate, User user) {
        if (user == null) {
            return null;
        } else {
            return new UserDelegator(sessionDelegate, user);
        }
    }

    @Nonnull
    static User unwrap(@Nonnull User user) {
        if (user instanceof UserDelegator) {
            return ((UserDelegator) user).getDelegate();
        } else {
            return user;
        }
    }

    private User getDelegate() {
        return (User) delegate;
    }

    //---------------------------------------------------------------< User >---
    @Override
    public boolean isAdmin() {
        return sessionDelegate.safePerform(new SessionOperation<Boolean>("isAdmin") {
            @Override
            public Boolean perform() {
                return getDelegate().isAdmin();
            }
        });
    }

    @Override
    public boolean isSystemUser() {
        return sessionDelegate.safePerform(new SessionOperation<Boolean>("isSystemUser") {
            @Override
            public Boolean perform() {
                return getDelegate().isSystemUser();
            }
        });
    }

    @Override
    public Credentials getCredentials() {
        return sessionDelegate.safePerform(new SessionOperation<Credentials>("getCredentials") {
            @Override
            public Credentials perform() throws RepositoryException {
                return getDelegate().getCredentials();
            }
        });
    }

    @Override
    public Impersonation getImpersonation() {
        return sessionDelegate.safePerform(new SessionOperation<Impersonation>("getImpersonation") {
            @Override
            public Impersonation perform() throws RepositoryException {
                Impersonation impersonation = getDelegate().getImpersonation();
                return ImpersonationDelegator.wrap(sessionDelegate, impersonation);
            }
        });
    }

    @Override
    public void changePassword(final String password) throws RepositoryException {
        sessionDelegate.perform(new SessionOperation<Void>("changePassword") {
            @Override
            public Void perform() throws RepositoryException {
                getDelegate().changePassword(password);
                return null;
            }
        });
    }

    @Override
    public void changePassword(final String password, final String oldPassword) throws RepositoryException {
        sessionDelegate.perform(new SessionOperation<Void>("changePassword") {
            @Override
            public Void perform() throws RepositoryException {
                getDelegate().changePassword(password, oldPassword);
                return null;
            }
        });
    }

    @Override
    public void disable(final String reason) throws RepositoryException {
        sessionDelegate.perform(new SessionOperation<Void>("disable") {
            @Override
            public Void perform() throws RepositoryException {
                getDelegate().disable(reason);
                return null;
            }
        });
    }

    @Override
    public boolean isDisabled() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("isDisabled") {
            @Override
            public Boolean perform() throws RepositoryException {
                return getDelegate().isDisabled();
            }
        });
    }

    @Override
    public String getDisabledReason() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<String>("getDisabledReason") {
            @Override
            public String perform() throws RepositoryException {
                return getDelegate().getDisabledReason();
            }
        });
    }
}
