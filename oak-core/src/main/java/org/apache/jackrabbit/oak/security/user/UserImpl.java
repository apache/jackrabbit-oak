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
package org.apache.jackrabbit.oak.security.user;

import java.security.Principal;
import javax.jcr.Credentials;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserIdCredentials;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.jackrabbit.oak.api.Type.STRING;

/**
 * UserImpl...
 */
class UserImpl extends AuthorizableImpl implements User {

    private final boolean isAdmin;
    private final PasswordHistory pwHistory;

    UserImpl(String id, Tree tree, UserManagerImpl userManager) throws RepositoryException {
        super(id, tree, userManager);

        ConfigurationParameters configurationParameters = userManager.getConfig();
        isAdmin = UserUtil.isAdmin(configurationParameters, id);
        pwHistory = new PasswordHistory(configurationParameters);
    }

    //---------------------------------------------------< AuthorizableImpl >---
    @Override
    void checkValidTree(@NotNull Tree tree) throws RepositoryException {
        if (!UserUtil.isType(tree, AuthorizableType.USER)) {
            throw new IllegalArgumentException("Invalid user node: node type rep:User expected.");
        }
    }

    //-------------------------------------------------------< Authorizable >---
    @Override
    public boolean isGroup() {
        return false;
    }

    @NotNull
    @Override
    public Principal getPrincipal() throws RepositoryException {
        Tree userTree = getTree();
        String principalName = getPrincipalName();
        NamePathMapper npMapper = getUserManager().getNamePathMapper();
        if (isAdmin()) {
            return new AdminPrincipalImpl(principalName, userTree, npMapper);
        } else {
            return new TreeBasedPrincipal(principalName, userTree, npMapper);
        }
    }

    //---------------------------------------------------------------< User >---
    @Override
    public boolean isAdmin() {
        return isAdmin;
    }

    @Override
    public boolean isSystemUser() {
        return false;
    }

    @NotNull
    @Override
    public Credentials getCredentials() {
        String pwHash = getPasswordHash();
        if (pwHash == null) {
            return new UserIdCredentials(getID());
        } else {
            return new CredentialsImpl(getID(), pwHash);
        }
    }

    @NotNull
    @Override
    public Impersonation getImpersonation() {
        return new ImpersonationImpl(this);
    }

    @Override
    public void changePassword(@Nullable String password) throws RepositoryException {
        if (password == null) {
            throw new RepositoryException("Attempt to set 'null' password for user " + getID());
        }
        UserManagerImpl userManager = getUserManager();
        userManager.onPasswordChange(this, password);

        pwHistory.updatePasswordHistory(getTree(), password);

        userManager.setPassword(getTree(), getID(),  password, false);
    }

    @Override
    public void changePassword(@Nullable String password, @NotNull String oldPassword) throws RepositoryException {
        // make sure the old password matches.
        String pwHash = getPasswordHash();
        if (!PasswordUtil.isSame(pwHash, oldPassword)) {
            throw new RepositoryException("Failed to change password: Old password does not match.");
        }
        changePassword(password);
    }

    @Override
    public void disable(@Nullable String reason) throws RepositoryException {
        validateDisableUser();

        getUserManager().onDisable(this, reason);

        Tree tree = getTree();
        if (reason == null) {
            if (tree.hasProperty(REP_DISABLED)) {
                // enable the user again.
                tree.removeProperty(REP_DISABLED);
            } // else: not disabled -> nothing to
        } else {
            tree.setProperty(REP_DISABLED, reason);
        }
    }

    private void validateDisableUser() throws RepositoryException {
        if (isAdmin) {
            throw new RepositoryException("The administrator user cannot be disabled.");
        }

        boolean isAnonymous = UserUtil.getAnonymousId(getUserManager().getConfig()).equals(getID());
        boolean allowDisableAnonymous = getUserManager().getConfig().getConfigValue(UserConstants.PARAM_ALLOW_DISABLE_ANONYMOUS, true);
        if (isAnonymous && !allowDisableAnonymous) {
            throw new RepositoryException("The anonymous user cannot be disabled.");
        }
    }

    @Override
    public boolean isDisabled() {
        return getTree().hasProperty(REP_DISABLED);
    }

    @Nullable
    @Override
    public String getDisabledReason() {
        PropertyState disabled = getTree().getProperty(REP_DISABLED);
        if (disabled != null) {
            return disabled.getValue(STRING);
        } else {
            return null;
        }
    }

    //------------------------------------------------------------< private >---
    @Nullable
    private String getPasswordHash() {
        return TreeUtil.getString(getTree(), UserConstants.REP_PASSWORD);
    }
}
