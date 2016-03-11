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
package org.apache.jackrabbit.oak.security.user.autosave;

import java.security.Principal;
import java.util.Iterator;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;

/**
 * Implementation of the user management that allows to set the autosave flag.
 * Since OAK does no longer support the auto-save flag out of the box and this
 * part of the user management is targeted for deprecation, this {@code UserManager}
 * implementation should only be used for those cases where strict backwards
 * compatibility is really required.
 *
 * <p>In general any consumer of the Jackrabbit user management API should stick
 * to the API contract and verify that the autosave flag is enabled before
 * relying on the implementation to have it turned on:</p>
 *
 * <pre>
 *     JackrabbitSession session = ...;
 *     UserManager userManager = session.getUserManager();
 *
 *     // modify some user related content
 *
 *     if (!userManager#isAutosave()) {
 *         session.save();
 *     }
 * </pre>
 */
public class AutoSaveEnabledManager implements UserManager {

    private final UserManager dlg;
    private final Root root;
    private boolean autosave = true;

    public AutoSaveEnabledManager(UserManager dlg, Root root) {
        this.dlg = dlg;
        this.root = root;
    }

    @Override
    public Authorizable getAuthorizable(String id) throws RepositoryException {
        return wrap(dlg.getAuthorizable(id));
    }

    @Override
    public <T extends Authorizable> T getAuthorizable(String id, Class<T> authorizableClass) throws RepositoryException {
        return UserUtil.castAuthorizable(wrap(dlg.getAuthorizable(id)), authorizableClass);
    }

    @Override
    public Authorizable getAuthorizable(Principal principal) throws RepositoryException {
        return wrap(dlg.getAuthorizable(principal));
    }

    @Override
    public Authorizable getAuthorizableByPath(String path) throws RepositoryException {
        return wrap(dlg.getAuthorizableByPath(path));
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(String relPath, String value) throws RepositoryException {
        return AuthorizableWrapper.createIterator(dlg.findAuthorizables(relPath, value), this);
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(String relPath, String value, int searchType) throws RepositoryException {
        return AuthorizableWrapper.createIterator(dlg.findAuthorizables(relPath, value, searchType), this);
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(Query query) throws RepositoryException {
        return AuthorizableWrapper.createIterator(dlg.findAuthorizables(query), this);
    }

    @Override
    public User createUser(String userID, String password) throws RepositoryException {
        try {
            return wrap(dlg.createUser(userID, password));
        } finally {
            autosave();
        }
    }

    @Override
    public User createUser(String userID, String password, Principal principal, @Nullable String intermediatePath) throws RepositoryException {
        try {
            return wrap(dlg.createUser(userID, password, principal, intermediatePath));
        } finally {
            autosave();
        }
    }

    @Override
    public User createSystemUser(String userID, String intermediatePath) throws RepositoryException {
        try {
            return wrap(dlg.createUser(userID, intermediatePath));
        } finally {
            autosave();
        }
    }

    @Override
    public Group createGroup(String groupId) throws RepositoryException {
        try {
            return wrap(dlg.createGroup(groupId));
        } finally {
            autosave();
        }
    }

    @Override
    public Group createGroup(Principal principal) throws RepositoryException {
        try {
            return wrap(dlg.createGroup(principal));
        } finally {
            autosave();
        }
    }

    @Override
    public Group createGroup(Principal principal, String groupId) throws RepositoryException {
        try {
            return wrap(dlg.createGroup(principal, groupId));
        } finally {
            autosave();
        }
    }

    @Override
    public Group createGroup(String groupID, Principal principal, @Nullable String intermediatePath) throws RepositoryException {
        try {
            return wrap(dlg.createGroup(groupID, principal, intermediatePath));
        } finally {
            autosave();
        }
    }

    @Override
    public boolean isAutoSave() {
        return autosave;
    }

    @Override
    public void autoSave(boolean enable) throws RepositoryException {
        autosave = enable;
    }

    //--------------------------------------------------------------------------

    void autosave() throws RepositoryException {
        if (autosave) {
            try {
                root.commit();
            } catch (CommitFailedException e) {
                throw e.asRepositoryException();
            } finally {
                root.refresh();
            }
        }
    }

    @CheckForNull
    Authorizable wrap(@Nullable Authorizable authorizable) {
        if (authorizable == null) {
            return null;
        }
        if (authorizable.isGroup()) {
            return wrap((Group) authorizable);
        } else {
            return wrap((User) authorizable);
        }
    }

    @Nonnull
    User wrap(@Nonnull User user) {
        return new UserImpl(user, this);
    }

    @Nonnull
    Group wrap(@Nonnull Group group) {
        return new GroupImpl(group, this);
    }
}