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
import javax.annotation.CheckForNull;
import javax.jcr.Credentials;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.security.principal.AdminPrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.TreeBasedPrincipal;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtility;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtility;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.api.Type.STRING;

/**
 * UserImpl...
 */
class UserImpl extends AuthorizableImpl implements User {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(UserImpl.class);

    private final boolean isAdmin;

    UserImpl(String id, Tree tree, UserManagerImpl userManager) throws RepositoryException {
        super(id, tree, userManager);

        isAdmin = UserUtility.getAdminId(userManager.getConfig()).equals(id);
    }

    void checkValidTree(Tree tree) throws RepositoryException {
        if (tree == null || !UserUtility.isType(tree, AuthorizableType.USER)) {
            throw new IllegalArgumentException("Invalid user node: node type rep:User expected.");
        }
    }

    //-------------------------------------------------------< Authorizable >---
    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#isGroup()
     */
    @Override
    public boolean isGroup() {
        return false;
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#getPrincipal()
     */
    @Override
    public Principal getPrincipal() throws RepositoryException {
        Tree userTree = getTree();
        String principalName = getPrincipalName(userTree);
        if (isAdmin()) {
            return new AdminPrincipalImpl(principalName, userTree, getUserManager().getNamePathMapper());
        } else {
            return new TreeBasedPrincipal(principalName, userTree, getUserManager().getNamePathMapper());
        }
    }

    //---------------------------------------------------------------< User >---
    /**
     * @see org.apache.jackrabbit.api.security.user.User#isAdmin()
     */
    @Override
    public boolean isAdmin() {
        return isAdmin;
    }

    /**
     * Always throws {@code UnsupportedRepositoryOperationException}
     *
     * @see org.apache.jackrabbit.api.security.user.User#getCredentials()
     */
    @Override
    public Credentials getCredentials() {
        return new CredentialsImpl(getID(), getPasswordHash());
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.User#getImpersonation()
     */
    @Override
    public Impersonation getImpersonation() throws RepositoryException {
        return new ImpersonationImpl(this);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.User#changePassword(String)
     */
    @Override
    public void changePassword(String password) throws RepositoryException {
        if (password == null) {
            throw new RepositoryException("Attempt to set 'null' password for user " + getID());
        }
        UserManagerImpl userManager = getUserManager();
        userManager.onPasswordChange(this, password);
        userManager.setPassword(getTree(), password, true);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.User#changePassword(String, String)
     */
    @Override
    public void changePassword(String password, String oldPassword) throws RepositoryException {
        // make sure the old password matches.
        String pwHash = getPasswordHash();
        if (!PasswordUtility.isSame(pwHash, oldPassword)) {
            throw new RepositoryException("Failed to change password: Old password does not match.");
        }
        changePassword(password);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.User#disable(String)
     */
    @Override
    public void disable(String reason) throws RepositoryException {
        if (isAdmin) {
            throw new RepositoryException("The administrator user cannot be disabled.");
        }
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

    /**
     * @see org.apache.jackrabbit.api.security.user.User#isDisabled()
     */
    @Override
    public boolean isDisabled() throws RepositoryException {
        return getTree().hasProperty(REP_DISABLED);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.User#getDisabledReason()
     */
    @Override
    public String getDisabledReason() throws RepositoryException {
        PropertyState disabled = getTree().getProperty(REP_DISABLED);
        if (disabled != null) {
            return disabled.getValue(STRING);
        } else {
            return null;
        }
    }

    //--------------------------------------------------------------------------
    @CheckForNull
    private String getPasswordHash() {
        NodeUtil n = new NodeUtil(getTree());
        return n.getString(UserConstants.REP_PASSWORD, null);
    }
}