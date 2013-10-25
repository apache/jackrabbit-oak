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
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.user.UserManagerImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;

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
public class AutoSaveEnabledManager extends UserManagerImpl {

    private final Root root;
    private boolean autosave = true;

    public AutoSaveEnabledManager(Root root, NamePathMapper namePathMapper, SecurityProvider securityProvider) {
        super(root, namePathMapper, securityProvider);
        this.root = root;
    }

    @Override
    public User createUser(String userID, String password, Principal principal, @Nullable String intermediatePath) throws RepositoryException {
        try {
            return new UserImpl(super.createUser(userID, password, principal, intermediatePath), this);
        } finally {
            autosave();
        }
    }

    @Override
    public Group createGroup(String groupID, Principal principal, @Nullable String intermediatePath) throws RepositoryException {
        try {
            return new GroupImpl(super.createGroup(groupID, principal, intermediatePath), this);
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
}