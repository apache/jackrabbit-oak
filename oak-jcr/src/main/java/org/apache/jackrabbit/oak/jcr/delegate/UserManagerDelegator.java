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

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.jcr.session.operation.UserManagerOperation;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.Iterator;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;

/**
 * This implementation of {@code UserManager} delegates back to a
 * delegatee wrapping each call into a {@link UserManager} closure.
 *
 * @see SessionDelegate#perform(org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation)
 */
public class UserManagerDelegator implements UserManager {

    private final SessionDelegate sessionDelegate;
    private final UserManager userManagerDelegate;

    public UserManagerDelegator(final SessionDelegate sessionDelegate, UserManager userManagerDelegate) {
        checkArgument(!(userManagerDelegate instanceof UserManagerDelegator));
        this.sessionDelegate = sessionDelegate;
        this.userManagerDelegate = userManagerDelegate;
    }

    @Nullable
    @Override
    public Authorizable getAuthorizable(@NotNull final String id) throws RepositoryException {
        return sessionDelegate.performNullable(new UserManagerOperation<Authorizable>(sessionDelegate, "getAuthorizable") {
            @Override
            public Authorizable performNullable() throws RepositoryException {
                Authorizable authorizable = userManagerDelegate.getAuthorizable(id);
                return AuthorizableDelegator.wrap(sessionDelegate, authorizable);
            }
        });
    }

    @Nullable
    @Override
    public <T extends Authorizable> T getAuthorizable(@NotNull final String id, @NotNull final Class<T> authorizableClass) throws RepositoryException {
        return sessionDelegate.performNullable(new UserManagerOperation<T>(sessionDelegate, "getAuthorizable") {
            @Override
            public T performNullable() throws RepositoryException {
                Authorizable authorizable = userManagerDelegate.getAuthorizable(id);
                return UserUtil.castAuthorizable(AuthorizableDelegator.wrap(sessionDelegate, authorizable), authorizableClass);

            }
        }
        );
    }

    @Nullable
    @Override
    public Authorizable getAuthorizable(@NotNull final Principal principal) throws RepositoryException {
        return sessionDelegate.performNullable(new UserManagerOperation<Authorizable>(sessionDelegate, "getAuthorizable") {
            @Override
            public Authorizable performNullable() throws RepositoryException {
                Authorizable authorizable = userManagerDelegate.getAuthorizable(principal);
                return AuthorizableDelegator.wrap(sessionDelegate, authorizable);
            }
        });
    }

    @Nullable
    @Override
    public Authorizable getAuthorizableByPath(@NotNull final String path) throws RepositoryException {
        return sessionDelegate.performNullable(new UserManagerOperation<Authorizable>(sessionDelegate, "getAuthorizableByPath") {
            @Override
            public Authorizable performNullable() throws RepositoryException {
                Authorizable authorizable = userManagerDelegate.getAuthorizableByPath(path);
                return AuthorizableDelegator.wrap(sessionDelegate, authorizable);
            }
        });
    }

    @NotNull
    @Override
    public Iterator<Authorizable> findAuthorizables(@NotNull final String relPath, @Nullable final String value) throws RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Iterator<Authorizable>>(sessionDelegate, "findAuthorizables") {
            @NotNull
            @Override
            public Iterator<Authorizable> perform() throws RepositoryException {
                Iterator<Authorizable> authorizables = userManagerDelegate.findAuthorizables(relPath, value);
                return Iterators.transform(authorizables, authorizable -> AuthorizableDelegator.wrap(sessionDelegate, authorizable));
            }
        });
    }

    @NotNull
    @Override
    public Iterator<Authorizable> findAuthorizables(@NotNull final String relPath, @Nullable final String value, final int searchType) throws RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Iterator<Authorizable>>(sessionDelegate, "findAuthorizables") {
            @NotNull
            @Override
            public Iterator<Authorizable> perform() throws RepositoryException {
                Iterator<Authorizable> authorizables = userManagerDelegate.findAuthorizables(relPath, value, searchType);
                return Iterators.transform(authorizables, authorizable -> AuthorizableDelegator.wrap(sessionDelegate, authorizable));
            }
        });
    }

    @NotNull
    @Override
    public Iterator<Authorizable> findAuthorizables(@NotNull final Query query) throws RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Iterator<Authorizable>>(sessionDelegate, "findAuthorizables") {
            @NotNull
            @Override
            public Iterator<Authorizable> perform() throws RepositoryException {
                Iterator<Authorizable> authorizables = userManagerDelegate.findAuthorizables(query);
                return Iterators.transform(authorizables, authorizable -> AuthorizableDelegator.wrap(sessionDelegate, authorizable));
            }
        });
    }

    @NotNull
    @Override
    public User createUser(@NotNull final String userID, @Nullable final String password) throws RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<User>(sessionDelegate, "createUser", true) {
            @NotNull
            @Override
            public User perform() throws RepositoryException {
                User user = userManagerDelegate.createUser(userID, password);
                return UserDelegator.wrap(sessionDelegate, user);
            }
        });
    }

    @NotNull
    @Override
    public User createUser(@NotNull final String userID, @Nullable final String password, @NotNull final Principal principal, @Nullable final String intermediatePath) throws RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<User>(sessionDelegate, "createUser", true) {
            @NotNull
            @Override
            public User perform() throws RepositoryException {
                User user = userManagerDelegate.createUser(userID, password, principal, intermediatePath);
                return UserDelegator.wrap(sessionDelegate, user);
            }
        });
    }

    @NotNull
    @Override
    public User createSystemUser(@NotNull final String userID, @Nullable final String intermediatePath) throws RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<User>(sessionDelegate, "createUser", true) {
            @NotNull
            @Override
            public User perform() throws RepositoryException {
                User user = userManagerDelegate.createSystemUser(userID, intermediatePath);
                return UserDelegator.wrap(sessionDelegate, user);
            }
        });
    }

    @NotNull
    @Override
    public Group createGroup(@NotNull final String groupID) throws RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Group>(sessionDelegate, "createGroup", true) {
            @NotNull
            @Override
            public Group perform() throws RepositoryException {
                Group group = userManagerDelegate.createGroup(groupID);
                return GroupDelegator.wrap(sessionDelegate, group);
            }
        });
    }

    @NotNull
    @Override
    public Group createGroup(@NotNull final Principal principal) throws RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Group>(sessionDelegate, "createGroup", true) {
            @NotNull
            @Override
            public Group perform() throws RepositoryException {
                Group group = userManagerDelegate.createGroup(principal);
                return GroupDelegator.wrap(sessionDelegate, group);
            }
        });
    }

    @NotNull
    @Override
    public Group createGroup(@NotNull final Principal principal, @Nullable final String intermediatePath) throws RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Group>(sessionDelegate, "createGroup", true) {
            @NotNull
            @Override
            public Group perform() throws RepositoryException {
                Group group = userManagerDelegate.createGroup(principal, intermediatePath);
                return GroupDelegator.wrap(sessionDelegate, group);
            }
        });
    }

    @NotNull
    @Override
    public Group createGroup(@NotNull final String groupID, @NotNull final Principal principal, @Nullable final String intermediatePath) throws RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Group>(sessionDelegate, "createGroup", true) {
            @NotNull
            @Override
            public Group perform() throws RepositoryException {
                Group group = userManagerDelegate.createGroup(groupID, principal, intermediatePath);
                return GroupDelegator.wrap(sessionDelegate, group);
            }
        });
    }

    @Override
    public boolean isAutoSave() {
        return sessionDelegate.safePerform(new UserManagerOperation<Boolean>(sessionDelegate, "isAutoSave") {
            @NotNull
            @Override
            public Boolean perform() {
                return userManagerDelegate.isAutoSave();
            }
        });
    }

    @Override
    public void autoSave(final boolean enable) throws RepositoryException {
        sessionDelegate.performVoid(new UserManagerOperation<Void>(sessionDelegate, "autoSave") {
            @Override
            public void performVoid() throws RepositoryException {
                userManagerDelegate.autoSave(enable);
            }
        });
    }
}
