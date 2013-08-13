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
import java.util.Iterator;

import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.jcr.operation.UserManagerOperation;

/**
 * This implementation of {@code UserManager} delegates back to a
 * delegatee wrapping each call into a {@link UserManager} closure.
 *
 * @see SessionDelegate#perform(org.apache.jackrabbit.oak.jcr.operation.SessionOperation)
 */
public class UserManagerDelegator implements UserManager {
    private final UserManager userManagerDelegate;
    private final SessionDelegate sessionDelegate;

    public UserManagerDelegator(final SessionDelegate sessionDelegate, UserManager userManagerDelegate) {
        this.userManagerDelegate = userManagerDelegate;
        this.sessionDelegate = sessionDelegate;
    }

    @Override
    public Authorizable getAuthorizable(final String id) throws RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Authorizable>(sessionDelegate) {
            @Override
            public Authorizable perform() throws RepositoryException {
                return userManagerDelegate.getAuthorizable(id);
            }
        });
    }

    @Override
    public Authorizable getAuthorizable(final Principal principal) throws RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Authorizable>(sessionDelegate) {
            @Override
            public Authorizable perform() throws RepositoryException {
                return userManagerDelegate.getAuthorizable(principal);
            }
        });
    }

    @Override
    public Authorizable getAuthorizableByPath(final String path) throws UnsupportedRepositoryOperationException, RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Authorizable>(sessionDelegate) {
            @Override
            public Authorizable perform() throws RepositoryException {
                return userManagerDelegate.getAuthorizableByPath(path);
            }
        });
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(final String relPath, final String value) throws RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Iterator<Authorizable>>(sessionDelegate) {
            @Override
            public Iterator<Authorizable> perform() throws RepositoryException {
                return userManagerDelegate.findAuthorizables(relPath, value);
            }
        });
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(final String relPath, final String value, final int searchType) throws RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Iterator<Authorizable>>(sessionDelegate) {
            @Override
            public Iterator<Authorizable> perform() throws RepositoryException {
                return userManagerDelegate.findAuthorizables(relPath, value, searchType);
            }
        });
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(final Query query) throws RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Iterator<Authorizable>>(sessionDelegate) {
            @Override
            public Iterator<Authorizable> perform() throws RepositoryException {
                return userManagerDelegate.findAuthorizables(query);
            }
        });
    }

    @Override
    public User createUser(final String userID, final String password) throws AuthorizableExistsException, RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<User>(sessionDelegate) {
            @Override
            public User perform() throws RepositoryException {
                return userManagerDelegate.createUser(userID, password);
            }
        });
    }

    @Override
    public User createUser(final String userID, final String password, final Principal principal, final String intermediatePath) throws AuthorizableExistsException, RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<User>(sessionDelegate) {
            @Override
            public User perform() throws RepositoryException {
                return userManagerDelegate.createUser(userID, password, principal, intermediatePath);
            }
        });
    }

    @Override
    public Group createGroup(final String groupID) throws AuthorizableExistsException, RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Group>(sessionDelegate) {
            @Override
            public Group perform() throws RepositoryException {
                return userManagerDelegate.createGroup(groupID);
            }
        });
    }

    @Override
    public Group createGroup(final Principal principal) throws AuthorizableExistsException, RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Group>(sessionDelegate) {
            @Override
            public Group perform() throws RepositoryException {
                return userManagerDelegate.createGroup(principal);
            }
        });
    }

    @Override
    public Group createGroup(final Principal principal, final String intermediatePath) throws AuthorizableExistsException, RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Group>(sessionDelegate) {
            @Override
            public Group perform() throws RepositoryException {
                return userManagerDelegate.createGroup(principal, intermediatePath);
            }
        });
    }

    @Override
    public Group createGroup(final String groupID, final Principal principal, final String intermediatePath) throws AuthorizableExistsException, RepositoryException {
        return sessionDelegate.perform(new UserManagerOperation<Group>(sessionDelegate) {
            @Override
            public Group perform() throws RepositoryException {
                return userManagerDelegate.createGroup(groupID, principal, intermediatePath);
            }
        });
    }

    @Override
    public boolean isAutoSave() {
        return sessionDelegate.safePerform(new UserManagerOperation<Boolean>(sessionDelegate) {
            @Override
            public Boolean perform() {
                return userManagerDelegate.isAutoSave();
            }
        });
    }

    @Override
    public void autoSave(final boolean enable) throws UnsupportedRepositoryOperationException, RepositoryException {
        sessionDelegate.perform(new UserManagerOperation<Void>(sessionDelegate) {
            @Override
            public Void perform() throws RepositoryException {
                userManagerDelegate.autoSave(enable);
                return null;
            }
        });
    }
}
