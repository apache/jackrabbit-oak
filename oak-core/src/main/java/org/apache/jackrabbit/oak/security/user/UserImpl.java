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
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.principal.TreeBasedPrincipal;
import org.apache.jackrabbit.oak.spi.security.user.PasswordUtility;
import org.apache.jackrabbit.oak.spi.security.user.Type;
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

    UserImpl(String id, Tree tree, UserManagerImpl userManager) throws RepositoryException {
        super(id, tree, userManager);
    }

    void checkValidTree(Tree tree) throws RepositoryException {
        if (tree == null || !getUserProvider().isAuthorizableType(tree, Type.USER)) {
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
        String principalName = getPrincipalName();
        return new TreeBasedPrincipal(principalName, getTree(), getUserManager().getNamePathMapper());

    }

    //---------------------------------------------------------------< User >---
    /**
     * @see org.apache.jackrabbit.api.security.user.User#isAdmin()
     */
    @Override
    public boolean isAdmin() {
        return getUserProvider().isAdminUser(getTree());
    }

    /**
     * Always throws {@code UnsupportedRepositoryOperationException}
     *
     * @see org.apache.jackrabbit.api.security.user.User#getCredentials()
     */
    @Override
    public Credentials getCredentials() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("Not implemented.");
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
        UserManagerImpl userManager = getUserManager();
        userManager.onPasswordChange(this, password);
        getUserProvider().setPassword(getTree(), password, true);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.User#changePassword(String, String)
     */
    @Override
    public void changePassword(String password, String oldPassword) throws RepositoryException {
        // make sure the old password matches.
        String pwHash = null;
        PropertyState pwProp = getTree().getProperty(REP_PASSWORD);
        if (pwProp != null) {
            pwHash = pwProp.getValue(STRING);
        }
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
        if (isAdmin()) {
            throw new RepositoryException("The administrator user cannot be disabled.");
        }
        Tree userTree = getTree();
        if (reason == null) {
            if (isDisabled()) {
                // enable the user again.
                setProtectedProperty(REP_DISABLED, (String) null);
            }
        } else {
            setProtectedProperty(REP_DISABLED, reason);
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
        } else
            return null;
    }

    //--------------------------------------------------------------------------

    void setProtectedProperty(String oakName, String value) throws RepositoryException {
        getUserProvider().setProtectedProperty(getTree(), oakName, value, PropertyType.STRING);
    }

    void setProtectedProperty(String oakName, String[] values) throws RepositoryException {
        getUserProvider().setProtectedProperty(getTree(), oakName, values, PropertyType.STRING);
    }
}